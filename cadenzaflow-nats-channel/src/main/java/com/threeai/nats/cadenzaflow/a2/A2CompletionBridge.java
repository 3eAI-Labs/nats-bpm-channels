package com.threeai.nats.cadenzaflow.a2;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.time.Duration;
import java.util.Optional;
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
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.impl.NatsJetStreamMetaData;
import io.nats.client.support.NatsJetStreamConstants;
import org.cadenzaflow.bpm.engine.BadUserRequestException;
import org.cadenzaflow.bpm.engine.ExternalTaskService;
import org.cadenzaflow.bpm.engine.exception.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * Consumes {@code jobs.<topic>.reply} and completes/fails the matching external task (HLD §2.4,
 * BR-A2-008/011, FR-A7/A12, US-A4/A7, decision matrix 2). Evolved from
 * {@code JetStreamMessageCorrelationSubscriber}'s subscribe/backoff/DLQ skeleton (carried over
 * unchanged); the message handler calls {@code externalTaskService.complete(...)} instead of
 * {@code correlateWithResult()}.
 */
public class A2CompletionBridge {

    private static final Logger log = LoggerFactory.getLogger(A2CompletionBridge.class);
    private static final Marker PAGE_MARKER = MarkerFactory.getMarker("PAGE");
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    private final Connection connection;
    private final JetStream jetStream;
    private final ExternalTaskService externalTaskService;
    private final String sentinelWorkerId;
    private final A2ConsumerConfig config;
    private final DlqPublisher dlqPublisher;
    private final NatsChannelMetrics metrics;

    private Dispatcher dispatcher;
    private ExecutorService executor;

    public A2CompletionBridge(Connection connection, JetStream jetStream, ExternalTaskService externalTaskService,
            String sentinelWorkerId, A2ConsumerConfig config, DlqPublisher dlqPublisher, NatsChannelMetrics metrics) {
        this.connection = connection;
        this.jetStream = jetStream;
        this.externalTaskService = externalTaskService;
        this.sentinelWorkerId = sentinelWorkerId;
        this.config = config;
        this.dlqPublisher = dlqPublisher;
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
            // Queue group => the durable is shared by every engine node and each reply is handled
            // exactly once. Without it the durable is exclusive and only one node can ever bind.
            if (config.getDeliverGroup() != null) {
                ccBuilder.deliverGroup(config.getDeliverGroup());
            }
            PushSubscribeOptions opts = PushSubscribeOptions.builder().configuration(ccBuilder.build()).build();
            JetStreamSubscription sub = config.getDeliverGroup() != null
                    ? jetStream.subscribe(config.getSubject(), config.getDeliverGroup(), dispatcher,
                            msg -> executor.submit(() -> handleReply(msg)), false, opts)
                    : jetStream.subscribe(config.getSubject(), dispatcher,
                            msg -> executor.submit(() -> handleReply(msg)), false, opts);
            log.info("Subscribed to A2 completion reply subject",
                    kv("subject", config.getSubject()), kv("message_name", config.getMessageName()));
        } catch (Exception e) {
            log.error("Failed to subscribe to A2 completion reply subject",
                    kv("subject", config.getSubject()), e);
            throw new IllegalStateException(
                    "Failed to subscribe to JetStream subject '" + config.getSubject() + "'", e);
        }
    }

    public void unsubscribe() {
        if (dispatcher != null) {
            try {
                dispatcher.drain(Duration.ofSeconds(10));
            } catch (Exception e) {
                log.warn("Error draining A2 completion dispatcher", kv("subject", config.getSubject()), e);
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
                routeToDlqAndDecide(msg, DlqReason.EMPTY_MESSAGE_BODY); // BR-SUB-007
                return;
            }

            long deliveryCount = deliveryCountOf(msg);
            if (deliveryCount > config.getMaxDeliver()) {
                routeToDlqAndDecide(msg, DlqReason.DELIVERY_BUDGET_EXCEEDED); // -> A2IncidentBridge (dlq.jobs.<topic>)
                return;
            }

            String externalTaskId = extractExternalTaskId(msg);
            Optional<ReplyType> replyType = A2ReplyPayloadDecoder.classify(msg);
            if (replyType.isEmpty()) {
                // VAL_INVALID_REPLY_TYPE (QA review fix, reply discriminator) — the
                // mandatory `type` field is missing or not one of SUCCESS|BPMN_ERROR|TRANSIENT.
                routeToDlqAndDecide(msg, DlqReason.INVALID_REPLY_TYPE);
                return;
            }

            A2ReplyPayloadDecoder.ReplyVariables replyVariables;
            try {
                replyVariables = A2ReplyPayloadDecoder.replyVariablesOf(msg, config.getReplyPayloadVariable());
            } catch (InvalidReplyVariablesException invalidVariables) {
                // Completing WITHOUT the variables the worker asked for would let the process
                // continue on wrong data (a gateway reads a variable that was never written and
                // takes the default path, silently) — so the reply goes to the DLQ instead.
                log.warn("Reply variables cannot be converted — routing to DLQ",
                        kv("external_task_id", externalTaskId), invalidVariables); // VAL_INVALID_REPLY_VARIABLES
                routeToDlqAndDecide(msg, DlqReason.INVALID_REPLY_VARIABLES);
                return;
            }

            switch (replyType.get()) {
                // The 3-arg overload is kept for a reply without local variables so the legacy
                // opaque contract keeps executing the exact engine call it always has; the 4-arg
                // overload only enters the picture when a structured reply asks for local scope.
                case SUCCESS -> {
                    if (replyVariables.localVariables().isEmpty()) {
                        externalTaskService.complete(externalTaskId, sentinelWorkerId,
                                replyVariables.variables());
                    } else {
                        externalTaskService.complete(externalTaskId, sentinelWorkerId,
                                replyVariables.variables(), replyVariables.localVariables());
                    }
                }
                case BPMN_ERROR -> {
                    if (!replyVariables.localVariables().isEmpty()) {
                        // The engine's handleBpmnError API has no local-variables parameter
                        // (verified against ExternalTaskService — largest overload is
                        // (id, workerId, errorCode, errorMessage, variables)). Dropping the
                        // scope silently is worse than refusing the reply.
                        log.warn("BPMN_ERROR reply carries localVariables, which the engine API"
                                        + " cannot accept — routing to DLQ",
                                kv("external_task_id", externalTaskId)); // VAL_INVALID_REPLY_VARIABLES
                        routeToDlqAndDecide(msg, DlqReason.INVALID_REPLY_VARIABLES);
                        return;
                    }
                    externalTaskService.handleBpmnError(externalTaskId, sentinelWorkerId,
                            A2ReplyPayloadDecoder.errorCodeOf(msg), A2ReplyPayloadDecoder.errorMessageOf(msg),
                            replyVariables.variables());
                }
                case TRANSIENT -> {
                    // Gated on the CONTRACT, not on map emptiness: under the legacy contract the
                    // variables map may hold the natsPayload passthrough, which TRANSIENT has
                    // never written (handleFailure predates variablesOf here) and must not start
                    // writing now. Only a structured reply asks for variables on a failure.
                    if (!replyVariables.structured()) {
                        externalTaskService.handleFailure(externalTaskId, sentinelWorkerId,
                                A2ReplyPayloadDecoder.errorMessageOf(msg), A2ReplyPayloadDecoder.errorDetailsOf(msg),
                                A2ReplyPayloadDecoder.retriesOf(msg), config.getRetryTimeoutMillis());
                    } else {
                        externalTaskService.handleFailure(externalTaskId, sentinelWorkerId,
                                A2ReplyPayloadDecoder.errorMessageOf(msg), A2ReplyPayloadDecoder.errorDetailsOf(msg),
                                A2ReplyPayloadDecoder.retriesOf(msg), config.getRetryTimeoutMillis(),
                                replyVariables.variables(), replyVariables.localVariables());
                    }
                }
            }
            if (metrics != null) {
                metrics.ackCount(msg.getSubject(), config.getMessageName()).increment();
            }
            msg.ack(); // AFTER complete/handleX succeeds (custody-transfer, BR-A2-008)

        } catch (NotFoundException notFound) {
            // HandleExternalTaskCmd — task not found (late/duplicate reply). Idempotent swallow.
            log.warn("External task not found — late/duplicate reply, acking (idempotent)",
                    kv("external_task_id", extractExternalTaskId(msg)), notFound); // RES_EXTERNAL_TASK_NOT_FOUND
            msg.ack();

        } catch (BadUserRequestException workerConflict) {
            // HandleExternalTaskCmd.validateWorkerViolation — must never happen, invariant.
            log.error(PAGE_MARKER, "SENTINEL WORKER CONFLICT — invariant violated, paging on-call, NOT acking",
                    kv("external_task_id", extractExternalTaskId(msg)), workerConflict); // SYS_SENTINEL_WORKER_CONFLICT
            if (metrics != null) {
                metrics.sentinelWorkerConflictCount(config.getMessageName()).increment();
            }
            // NO ack, NO nak — human intervention expected (locked decision, BAQ-7).

        } catch (Exception transientDbFailure) {
            log.error("Transient failure during complete/handleFailure — nak, redelivery expected",
                    kv("external_task_id", extractExternalTaskId(msg)), transientDbFailure);
            msg.nakWithDelay(calculateBackoff(deliveryCountOf(msg)));
        } finally {
            MDC.remove("trace_id");
        }
    }

    private void routeToDlqAndDecide(Message msg, DlqReason reason) {
        DlqPublishOutcome outcome = dlqPublisher.publish(msg, config.getDlqSubject(), reason,
                msg.getSubject(), config.getMessageName());
        switch (outcome) {
            case PUBLISHED_JETSTREAM, PUBLISHED_CORE_FALLBACK -> msg.ack();
            case FAILED_NO_DLQ_SUBJECT, FAILED_BOTH_PUBLISH -> msg.nakWithDelay(calculateBackoff(deliveryCountOf(msg)));
        }
    }

    private String extractExternalTaskId(Message msg) {
        if (msg.getHeaders() == null) {
            return null;
        }
        return msg.getHeaders().getLast(NatsJetStreamConstants.MSG_ID_HDR);
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
