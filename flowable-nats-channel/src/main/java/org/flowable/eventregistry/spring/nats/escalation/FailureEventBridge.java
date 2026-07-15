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

    private Dispatcher dispatcher;
    private ExecutorService executor;

    public FailureEventBridge(Connection connection, JetStream jetStream, String dlqWildcardSubject,
            EventRegistry eventRegistry, NatsChannelDefinitionProcessor channelModelLookup,
            CircuitBreaker circuitBreaker, NatsChannelMetrics metrics) {
        this.connection = connection;
        this.jetStream = jetStream;
        this.dlqWildcardSubject = dlqWildcardSubject;
        this.eventRegistry = eventRegistry;
        this.channelModelLookup = channelModelLookup;
        this.circuitBreaker = circuitBreaker;
        this.metrics = metrics;
    }

    public void subscribe() {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.dispatcher = connection.createDispatcher();
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
                eventRegistry.eventReceived(model.get(), failureEvent);
                return null;
            });
            if (metrics != null) {
                metrics.ackCount(dlqMsg.getSubject(), model.get().getKey()).increment();
            }
            dlqMsg.ack(); // ack-after-correlate

        } catch (CallNotPermittedException cbOpen) {
            dlqMsg.nakWithDelay(calculateBackoff(deliveryCountOf(dlqMsg))); // CB OPEN — fail-fast

        } catch (FlowableException noMatch) {
            // CODER-QUESTION: whether eventReceived actually throws FlowableException (or a
            // subtype) on no-match, or instead returns silently, is unresolved pending
            // TEST_SPECIFICATIONS.md (d) — see class Javadoc. BAQ-8: treat as benign single-event
            // race — WARN + metric, not ERROR; ack (the message itself was handled).
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
