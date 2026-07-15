package org.flowable.eventregistry.spring.nats.escalation;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.threeai.nats.core.headers.DlqHeaders;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.impl.NatsJetStreamMetaData;
import org.flowable.common.engine.api.FlowableException;
import org.flowable.eventregistry.api.EventRegistry;
import org.flowable.eventregistry.impl.EventRegistryEngineConfiguration;
import org.flowable.eventregistry.model.InboundChannelModel;
import org.flowable.eventregistry.spring.nats.NatsChannelDefinitionProcessor;
import org.flowable.eventregistry.spring.nats.NatsInboundEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes the shared DLQ (everything except the {@code dlq.jobs.>} slice A2's incident-bridges
 * own) and re-delivers the original payload/headers as a Flowable failure-event, so a
 * non-interrupting/interrupting BPMN escalation can react to it (HLD §2.6, BR-FLW-003/005,
 * FR-B3/B5, US-B3/B5, ADR-0004).
 *
 * <p><b>CODER-NOTE (cross-reference gap):</b> the LLD text points to "08_config.md §3" for the
 * consumer-side routing rule that keeps {@code dlq.jobs.>} out of this bridge — that section
 * does not exist in the approved LLD (08_config.md only has §1/§2/§4). This class implements the
 * routing itself: it subscribes to the whole {@code dlq.>} wildcard and skips (acks without
 * processing) any message whose {@code X-Cadenzaflow-Dlq-Original-Subject} starts with
 * {@code jobs.} — those belong to {@code A2IncidentBridge} in the Camunda/CadenzaFlow modules.
 *
 * <p><b>Sentinel Phase 5.5 QA fix (HIGH, 2026-07-15):</b> {@code
 * EventReceivedNoMatchBehaviorTest} proved {@code eventRegistry.eventReceived(...)} does NOT
 * throw {@link FlowableException} on "no waiting subscription" — it returns silently. The real
 * trigger for {@code RES_FAILURE_EVENT_CORRELATION_MISS} is now {@link
 * FailureEventCorrelationMissConsumer}, registered onto the host's {@link
 * EventRegistryEngineConfiguration} in {@link #subscribe()} via the {@code
 * EventRegistryNonMatchingEventConsumer} SPI. The {@code catch (FlowableException noMatch)}
 * branch below is kept purely as defensive fallback (e.g. a future Flowable version, or a
 * pipeline component that legitimately throws it) — it is not expected to fire in practice.
 */
public class FailureEventBridge {

    private static final Logger log = LoggerFactory.getLogger(FailureEventBridge.class);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);
    private static final String A2_RESERVED_PREFIX = "jobs.";

    private final Connection connection;
    private final JetStream jetStream;
    private final String dlqWildcardSubject;
    private final EventRegistry eventRegistry;
    private final NatsChannelDefinitionProcessor channelModelLookup;
    private final CircuitBreaker circuitBreaker;
    private final NatsChannelMetrics metrics;
    private final EventRegistryEngineConfiguration eventRegistryEngineConfiguration;
    private final FailureEventCorrelationMissConsumer correlationMissConsumer;

    private Dispatcher dispatcher;
    private ExecutorService executor;

    public FailureEventBridge(Connection connection, JetStream jetStream, String dlqWildcardSubject,
            EventRegistry eventRegistry, NatsChannelDefinitionProcessor channelModelLookup,
            CircuitBreaker circuitBreaker, NatsChannelMetrics metrics,
            EventRegistryEngineConfiguration eventRegistryEngineConfiguration) {
        this.connection = connection;
        this.jetStream = jetStream;
        this.dlqWildcardSubject = dlqWildcardSubject;
        this.eventRegistry = eventRegistry;
        this.channelModelLookup = channelModelLookup;
        this.circuitBreaker = circuitBreaker;
        this.metrics = metrics;
        this.eventRegistryEngineConfiguration = eventRegistryEngineConfiguration;
        this.correlationMissConsumer = new FailureEventCorrelationMissConsumer(metrics);
    }

    public void subscribe() {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.dispatcher = connection.createDispatcher();
        registerCorrelationMissConsumer();
        try {
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .ackWait(Duration.ofSeconds(30))
                    .build();
            PushSubscribeOptions opts = PushSubscribeOptions.builder().configuration(cc).build();
            JetStreamSubscription sub = jetStream.subscribe(dlqWildcardSubject, dispatcher,
                    msg -> executor.submit(() -> handleDlqMessage(msg)), false, opts);
            log.info("Subscribed to FailureEventBridge DLQ subject", kv("subject", dlqWildcardSubject));
        } catch (Exception e) {
            log.error("Failed to subscribe to FailureEventBridge DLQ subject", kv("subject", dlqWildcardSubject), e);
            throw new FlowableException("Failed to subscribe to JetStream subject '" + dlqWildcardSubject + "'", e);
        }
    }

    /**
     * Registers {@link #correlationMissConsumer} as the engine-wide {@code
     * EventRegistryNonMatchingEventConsumer} — this IS the RES_FAILURE_EVENT_CORRELATION_MISS
     * trigger point (see class Javadoc). {@code eventRegistryEngineConfiguration} may be absent
     * if the host application does not expose it as a Spring bean; degrade with a startup WARN
     * rather than failing bootstrap (the bridge still functions, it simply cannot observe misses).
     */
    private void registerCorrelationMissConsumer() {
        if (eventRegistryEngineConfiguration == null) {
            log.warn("EventRegistryEngineConfiguration bean not available — "
                    + "RES_FAILURE_EVENT_CORRELATION_MISS will not be triggered by real no-match "
                    + "events (EventRegistryNonMatchingEventConsumer SPI not registered)");
            return;
        }
        eventRegistryEngineConfiguration.setNonMatchingEventConsumer(correlationMissConsumer);
    }

    public void unsubscribe() {
        if (dispatcher != null) {
            try {
                dispatcher.drain(Duration.ofSeconds(10));
            } catch (Exception e) {
                log.warn("Error draining FailureEventBridge dispatcher", e);
            }
        }
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
            }
        }
    }

    void handleDlqMessage(Message dlqMsg) {
        String originalSubject = originalSubjectOf(dlqMsg);
        if (originalSubject != null && originalSubject.startsWith(A2_RESERVED_PREFIX)) {
            dlqMsg.ack(); // A2's own DLQ slice — not this bridge's concern, custody already transferred
            return;
        }

        Optional<InboundChannelModel> model = channelModelLookup.findBySubject(originalSubject);
        if (model.isEmpty()) {
            log.error("FailureEventBridge: no inbound channel model registered for original subject — "
                            + "cannot reconstruct failure-event, routing back to DLQ is not possible (no dlq-of-dlq)",
                    kv("original_subject", originalSubject)); // SYS_DLQ_BRIDGE_PROCESSING_FAILED
            dlqMsg.nakWithDelay(calculateBackoff(deliveryCountOf(dlqMsg)));
            return;
        }

        try {
            circuitBreaker.executeCallable(() -> {
                // BR-FLW-003: same correlation keys (BpmHeaders already verbatim on the DLQ copy, Fix#1).
                NatsInboundEvent failureEvent = new NatsInboundEvent(dlqMsg);
                // Thread-hand-off for FailureEventCorrelationMissConsumer (see class Javadoc) —
                // eventReceived(...) dispatches to the SPI synchronously on this same thread.
                correlationMissConsumer.bindChannelKeyForCurrentThread(model.get().getKey());
                try {
                    eventRegistry.eventReceived(model.get(), failureEvent);
                } finally {
                    correlationMissConsumer.clearChannelKeyForCurrentThread();
                }
                return null;
            });
            if (metrics != null) {
                metrics.ackCount(dlqMsg.getSubject(), model.get().getKey()).increment();
            }
            dlqMsg.ack(); // ack-after-correlate

        } catch (CallNotPermittedException cbOpen) {
            dlqMsg.nakWithDelay(calculateBackoff(deliveryCountOf(dlqMsg))); // CB OPEN — fail-fast

        } catch (FlowableException noMatch) {
            // RESOLVED (TEST_SPECIFICATIONS.md (d) / EventReceivedNoMatchBehaviorTest, Sentinel
            // Phase 5.5): eventReceived(...) does NOT throw FlowableException on no-match — it
            // returns silently. This branch is DEFENSIVE ONLY (kept for a hypothetical future
            // Flowable version, or a pipeline component that legitimately throws it); the real
            // no-match trigger is FailureEventCorrelationMissConsumer (registered in subscribe()).
            // BAQ-8: if this branch ever does fire, treat it the same way — benign single-event
            // race, WARN + metric, not ERROR; ack (the message itself was handled).
            log.warn("FailureEventBridge: no waiting subscription for failure-event — "
                            + "instance likely already resolved via another path",
                    kv("original_subject", originalSubject));
            if (metrics != null) {
                metrics.failureEventCorrelationMissCount(model.get().getKey()).increment(); // RES_FAILURE_EVENT_CORRELATION_MISS
            }
            dlqMsg.ack();

        } catch (Exception downstreamFailure) {
            log.error("FailureEventBridge processing failed",
                    kv("original_subject", originalSubject), downstreamFailure); // SYS_DLQ_BRIDGE_PROCESSING_FAILED
            dlqMsg.nakWithDelay(calculateBackoff(deliveryCountOf(dlqMsg)));
        }
    }

    private String originalSubjectOf(Message msg) {
        if (msg.getHeaders() == null) {
            return null;
        }
        return msg.getHeaders().getLast(DlqHeaders.ORIGINAL_SUBJECT);
    }

    private long deliveryCountOf(Message msg) {
        try {
            NatsJetStreamMetaData metaData = msg.metaData();
            return metaData.deliveredCount();
        } catch (Exception e) {
            return 1;
        }
    }

    private Duration calculateBackoff(long deliveryCount) {
        long seconds = (long) Math.pow(2, deliveryCount - 1);
        Duration backoff = Duration.ofSeconds(seconds);
        return backoff.compareTo(MAX_BACKOFF) > 0 ? MAX_BACKOFF : backoff;
    }
}
