package com.threeai.nats.flowable.externalworker;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.threeai.nats.core.dlq.DlqPublishOutcome;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.dlq.DlqReason;
import com.threeai.nats.core.headers.BpmHeaders;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import org.flowable.common.engine.api.FlowableIllegalArgumentException;
import org.flowable.common.engine.api.FlowableObjectNotFoundException;
import org.flowable.engine.ManagementService;
import org.flowable.job.api.ExternalWorkerJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Consumes {@code ewjobs.<topic>.reply} and applies worker results to the engine
 * (docs/11 §2 md. 2-4).
 *
 * <p><b>Trust boundary (decision N-red-1):</b> the wire supplies ONLY the envelope {@code jobId}
 * and the {@code X-Cadenzaflow-Lock-Nonce} header. The lock token is constructed HERE from the
 * locally configured sentinel id + the echoed nonce; a reply without a valid nonce is
 * dead-lettered ({@code VAL_MISSING_LOCK_NONCE}), never silently acked — acking it would leave
 * the job cycling through reset+sweep forever with zero errors.
 *
 * <p><b>Late-reply tree (exception-type-rooted):</b> {@link FlowableObjectNotFoundException} =
 * job gone → idempotent ack. {@link FlowableIllegalArgumentException} = lock-ownership mismatch
 * → query the job and classify by owner: {@code null} → engine reset (ack + counter);
 * sentinel-grammar with a DIFFERENT generation → superseded by a sweep re-dispatch (ack +
 * counter, benign race); sentinel-grammar with the SAME generation → impossible (the complete
 * should have succeeded) → PAGE; any other owner → foreign worker / config divergence →
 * {@code SYS_SENTINEL_WORKER_CONFLICT} PAGE (neither ack nor nak — deliberate human
 * intervention, A2 parity). Routine lock expiry never pages.
 */
public class EwCompletionBridge {

    private static final Logger log = LoggerFactory.getLogger(EwCompletionBridge.class);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    private final Connection connection;
    private final JetStream jetStream;
    private final ManagementService managementService;
    private final EwConsumerConfig config;
    private final String sentinelWorkerId;
    private final DlqPublisher dlqPublisher;
    private final NatsChannelMetrics metrics;

    private Dispatcher dispatcher;
    private ExecutorService executor;

    public EwCompletionBridge(Connection connection, JetStream jetStream,
            ManagementService managementService, EwConsumerConfig config, String sentinelWorkerId,
            DlqPublisher dlqPublisher, NatsChannelMetrics metrics) {
        this.connection = connection;
        this.jetStream = jetStream;
        this.managementService = managementService;
        this.config = config;
        this.sentinelWorkerId = sentinelWorkerId;
        this.dlqPublisher = dlqPublisher;
        this.metrics = metrics;
    }

    public void subscribe() {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.dispatcher = connection.createDispatcher();
        try {
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .ackWait(Duration.ofSeconds(config.getAckWaitSeconds()))
                    // One over budget so THIS bridge sees the over-budget delivery and can DLQ it.
                    .maxDeliver(config.getMaxDeliver() + 1)
                    .durable(config.getDurableName())
                    .deliverGroup(config.getDeliverGroup())
                    .build();
            PushSubscribeOptions opts = PushSubscribeOptions.builder().configuration(cc).build();
            jetStream.subscribe(config.getSubject(), config.getDeliverGroup(), dispatcher,
                    msg -> executor.submit(() -> handleReply(msg)), false, opts);
            log.info("Subscribed to external-worker reply subject",
                    kv("subject", config.getSubject()), kv("durable", config.getDurableName()));
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to subscribe to JetStream subject '" + config.getSubject() + "'", e);
        }
    }

    public void unsubscribe() {
        if (dispatcher != null) {
            try {
                dispatcher.drain(Duration.ofSeconds(10));
            } catch (Exception e) {
                log.warn("Error draining external-worker completion dispatcher",
                        kv("subject", config.getSubject()), e);
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

    void handleReply(Message msg) {
        MDC.put("trace_id", BpmHeaders.extractTraceIdWithFallback(msg));
        try {
            byte[] data = msg.getData();
            if (data == null || data.length == 0) {
                routeToDlqAndDecide(msg, DlqReason.EMPTY_MESSAGE_BODY);
                return;
            }
            long deliveryCount = deliveryCountOf(msg);
            if (deliveryCount > config.getMaxDeliver()) {
                routeToDlqAndDecide(msg, DlqReason.DELIVERY_BUDGET_EXCEEDED);
                return;
            }

            EwReplyDecoder.DecodedReply reply;
            try {
                reply = EwReplyDecoder.decode(msg);
            } catch (InvalidEwReplyException invalid) {
                log.warn("Reply rejected by decoder — routing to DLQ",
                        kv("subject", msg.getSubject()), invalid);
                routeToDlqAndDecide(msg, invalid.reason());
                return;
            }

            // Trust boundary BEFORE any engine call (N-red-1): envelope jobId + echoed nonce.
            String jobId = reply.jobId();
            String nonce = msg.getHeaders() == null ? null : msg.getHeaders().getFirst(EwHeaders.LOCK_NONCE);
            if (jobId == null || jobId.isEmpty()
                    || nonce == null || !EwHeaders.NONCE_PATTERN.matcher(nonce).matches()) {
                log.warn("Reply without a valid jobId/lock-nonce — routing to DLQ, never acking",
                        kv("job_id", jobId), kv("subject", msg.getSubject()));
                routeToDlqAndDecide(msg, DlqReason.MISSING_LOCK_NONCE);
                return;
            }
            String token = sentinelWorkerId + "#" + nonce;

            if (reply.transientVariablesDropped()) {
                // D-C'v2: the failure builder has no variables parameter; dropped loudly, not silently.
                log.warn("TRANSIENT reply carried variables — dropped (no engine parameter)",
                        kv("job_id", jobId), kv("topic", config.getTopic()));
            }

            try {
                applyToEngine(reply, jobId, token);
            } catch (FlowableObjectNotFoundException gone) {
                // (i) Job already completed/cleaned — duplicate or late reply. Idempotent ack.
                log.warn("Reply for a job that no longer exists — acking (idempotent)",
                        kv("job_id", jobId), kv("topic", config.getTopic()));
                msg.ack();
                return;
            } catch (FlowableIllegalArgumentException lockMismatch) {
                classifyLockMismatch(msg, jobId, token);
                return;
            } catch (Exception transientEngineFailure) {
                log.warn("Engine call failed — NAK with backoff",
                        kv("job_id", jobId), transientEngineFailure);
                if (metrics != null) {
                    metrics.nakCount(config.getSubject(), config.getTopic()).increment();
                }
                msg.nakWithDelay(calculateBackoff(deliveryCount));
                return;
            }
            // Custody transfer: ack strictly AFTER the engine call succeeded.
            msg.ack();
            if (metrics != null) {
                metrics.ackCount(config.getSubject(), config.getTopic()).increment();
            }
        } finally {
            MDC.remove("trace_id");
        }
    }

    private void applyToEngine(EwReplyDecoder.DecodedReply reply, String jobId, String token) {
        switch (reply.type()) {
            case SUCCESS -> managementService.createExternalWorkerCompletionBuilder(jobId, token)
                    .variables(reply.variables())
                    .complete();
            case BPMN_ERROR -> managementService.createExternalWorkerCompletionBuilder(jobId, token)
                    .variables(reply.variables()) // includes the degraded natsErrorMessage
                    .bpmnError(reply.errorCode());
            case TRANSIENT -> managementService.createExternalWorkerJobFailureBuilder(jobId, token)
                    .errorMessage(reply.errorMessage())
                    .errorDetails(reply.errorDetails())
                    .retries(reply.retries())
                    .retryTimeout(Duration.ofMillis(config.getRetryTimeoutMillis()))
                    .fail();
        }
    }

    /**
     * (ii)/(iii) of the late-reply tree. Flowable squeezes "lock was reset" and "someone else
     * holds it" into the same exception type, so the owner is re-read here and classified.
     */
    private void classifyLockMismatch(Message msg, String jobId, String token) {
        ExternalWorkerJob job = managementService.createExternalWorkerJobQuery()
                .jobId(jobId).singleResult();
        if (job == null) {
            // Fourth branch: completed between the engine call and this query — idempotent ack.
            msg.ack();
            return;
        }
        String owner = job.getLockOwner();
        if (owner == null) {
            // (ii-a) Engine reset released the lock; the sweep will re-dispatch. Routine, no page.
            log.warn("Reply arrived after the engine reset released the lock — sweep will re-dispatch",
                    kv("job_id", jobId), kv("topic", config.getTopic()));
            if (metrics != null) {
                metrics.lockResetCount(config.getTopic()).increment();
            }
            msg.ack();
            return;
        }
        if (owner.startsWith(sentinelWorkerId + "#")) {
            if (owner.equals(token)) {
                // (iii-a) Same generation still holds the lock yet the complete failed — impossible
                // by construction. Genuine invariant violation.
                page(msg, jobId, owner, "same-generation lock present but completion rejected");
            } else {
                // (ii-b) A sweep re-acquire minted a newer generation; this reply is stale. Benign.
                log.warn("Stale-generation reply superseded by a sweep re-dispatch — acking",
                        kv("job_id", jobId), kv("topic", config.getTopic()));
                if (metrics != null) {
                    metrics.replySupersededCount(config.getTopic()).increment();
                }
                msg.ack();
            }
            return;
        }
        // (iii-b) Foreign owner on a topic reserved to this adapter: worker fleet misconfiguration
        // or a classic poller takeover. SYS_SENTINEL_WORKER_CONFLICT, A2 parity.
        page(msg, jobId, owner, "job locked by a non-sentinel owner");
    }

    private void page(Message msg, String jobId, String owner, String why) {
        // Neither ack nor nak: the message stays pending for deliberate human intervention (BAQ-7).
        log.error("PAGE [SYS_SENTINEL_WORKER_CONFLICT] {} — neither acking nor naking",
                why, kv("job_id", jobId), kv("lock_owner", owner), kv("topic", config.getTopic()));
        if (metrics != null) {
            metrics.sentinelWorkerConflictCount(config.getTopic()).increment();
        }
    }

    private void routeToDlqAndDecide(Message msg, DlqReason reason) {
        DlqPublishOutcome outcome = dlqPublisher.publish(msg, config.getDlqSubject(), reason,
                msg.getSubject(), config.getTopic());
        switch (outcome) {
            case PUBLISHED_JETSTREAM, PUBLISHED_CORE_FALLBACK -> msg.ack();
            case FAILED_NO_DLQ_SUBJECT, FAILED_BOTH_PUBLISH ->
                    msg.nakWithDelay(calculateBackoff(deliveryCountOf(msg)));
        }
        if (metrics != null) {
            metrics.dlqCount(config.getSubject(), config.getTopic()).increment();
        }
    }

    private long deliveryCountOf(Message msg) {
        try {
            return msg.metaData().deliveredCount();
        } catch (Exception e) {
            return 1;
        }
    }

    private Duration calculateBackoff(long deliveryCount) {
        long seconds = (long) Math.pow(2, Math.max(0, deliveryCount - 1));
        Duration backoff = Duration.ofSeconds(seconds);
        return backoff.compareTo(MAX_BACKOFF) > 0 ? MAX_BACKOFF : backoff;
    }
}
