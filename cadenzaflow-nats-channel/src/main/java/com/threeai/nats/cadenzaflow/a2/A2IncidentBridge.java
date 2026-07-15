package com.threeai.nats.cadenzaflow.a2;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.time.Duration;
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
import io.nats.client.support.NatsJetStreamConstants;
import org.cadenzaflow.bpm.engine.ExternalTaskService;
import org.cadenzaflow.bpm.engine.exception.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes {@code dlq.jobs.<topic>} (delivery-budget-exceeded replies) and converts them into a
 * Cockpit incident with residual-lock-free retries (HLD §2.5, BR-A2-009/010, FR-A10/A11, US-A6,
 * ADR-0004).
 */
public class A2IncidentBridge {

    private static final Logger log = LoggerFactory.getLogger(A2IncidentBridge.class);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    private final Connection connection;
    private final JetStream jetStream;
    private final ExternalTaskService externalTaskService;
    private final String sentinelWorkerId;
    private final A2ConsumerConfig config;
    private final CircuitBreaker circuitBreaker;
    private final NatsChannelMetrics metrics;

    private Dispatcher dispatcher;
    private ExecutorService executor;

    public A2IncidentBridge(Connection connection, JetStream jetStream, ExternalTaskService externalTaskService,
            String sentinelWorkerId, A2ConsumerConfig config, CircuitBreaker circuitBreaker, NatsChannelMetrics metrics) {
        this.connection = connection;
        this.jetStream = jetStream;
        this.externalTaskService = externalTaskService;
        this.sentinelWorkerId = sentinelWorkerId;
        this.config = config;
        this.circuitBreaker = circuitBreaker;
        this.metrics = metrics;
    }

    public void subscribe() {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.dispatcher = connection.createDispatcher();
        try {
            ConsumerConfiguration.Builder ccBuilder = ConsumerConfiguration.builder()
                    .ackWait(Duration.ofSeconds(config.getAckWaitSeconds()))
                    .maxDeliver(config.getMaxDeliver() + 1);
            if (config.getDurableName() != null) {
                ccBuilder.durable(config.getDurableName());
            }
            PushSubscribeOptions opts = PushSubscribeOptions.builder().configuration(ccBuilder.build()).build();
            JetStreamSubscription sub = jetStream.subscribe(config.getSubject(), dispatcher,
                    msg -> executor.submit(() -> handleDlqMessage(msg)), false, opts);
            log.info("Subscribed to A2 incident-bridge DLQ subject", kv("subject", config.getSubject()));
        } catch (Exception e) {
            log.error("Failed to subscribe to A2 incident-bridge DLQ subject", kv("subject", config.getSubject()), e);
            throw new IllegalStateException(
                    "Failed to subscribe to JetStream subject '" + config.getSubject() + "'", e);
        }
    }

    public void unsubscribe() {
        if (dispatcher != null) {
            try {
                dispatcher.drain(Duration.ofSeconds(10));
            } catch (Exception e) {
                log.warn("Error draining A2 incident-bridge dispatcher", kv("subject", config.getSubject()), e);
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

    void handleDlqMessage(Message msg) {
        String externalTaskId = extractExternalTaskId(msg); // present because contract-fix #1 preserves original headers
        try {
            circuitBreaker.executeCallable(() -> {
                // BAQ-2: retryDuration fixed at 0 — no residual-lock delay on Cockpit retry.
                // NOTE (review MAJOR-1a): if handleFailure throws NotFoundException here, the
                // circuit breaker's ignoreExceptions list keeps it OUT of the CB's success/failure
                // accounting entirely (nats-core §4.2).
                externalTaskService.handleFailure(externalTaskId, sentinelWorkerId,
                        "Delivery budget exhausted (deliveryCount > M)", dlqReasonOf(msg),
                        /* retries */ 0, /* retryDuration */ 0L);
                return null;
            });
            msg.ack();
        } catch (NotFoundException alreadyResolved) {
            // Reaches here WITHOUT affecting the CB's counters (ignoreExceptions) — same
            // behavior as before the review fix: idempotent swallow + ack.
            log.warn("Task already resolved via another path — idempotent ack",
                    kv("external_task_id", externalTaskId), alreadyResolved); // RES_EXTERNAL_TASK_NOT_FOUND
            msg.ack();
        } catch (CallNotPermittedException cbOpen) {
            msg.nakWithDelay(calculateBackoff(deliveryCountOf(msg))); // CB OPEN — fail-fast, message waits in the stream
        } catch (Exception downstreamFailure) {
            log.error("Incident-bridge processing failed",
                    kv("external_task_id", externalTaskId), downstreamFailure); // SYS_DLQ_BRIDGE_PROCESSING_FAILED
            msg.nakWithDelay(calculateBackoff(deliveryCountOf(msg)));
        }
    }

    private static final String DLQ_MSG_ID_SUFFIX = ".dlq";

    /**
     * CODER-NOTE: on {@code dlq.jobs.<topic>}, {@code Nats-Msg-Id} carries
     * {@code <externalTaskId>.dlq} (contract-fix #3, BR-SUB-003 — {@code DlqPublisher} overwrites
     * the header to guarantee dedup-safe DLQ copies). The original externalTaskId is recovered
     * by stripping that fixed suffix — the asyncapi {@code ReplyHeaders} schema carries no other
     * field the worker is required to echo back with the plain task id.
     */
    private String extractExternalTaskId(Message msg) {
        if (msg.getHeaders() == null) {
            return null;
        }
        String msgId = msg.getHeaders().getLast(NatsJetStreamConstants.MSG_ID_HDR);
        if (msgId != null && msgId.endsWith(DLQ_MSG_ID_SUFFIX)) {
            return msgId.substring(0, msgId.length() - DLQ_MSG_ID_SUFFIX.length());
        }
        return msgId;
    }

    private String dlqReasonOf(Message msg) {
        if (msg.getHeaders() == null) {
            return "UNKNOWN";
        }
        String reason = msg.getHeaders().getLast(DlqHeaders.REASON);
        return reason != null ? reason : "UNKNOWN";
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
