package com.threeai.nats.flowable.externalworker;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.threeai.nats.core.headers.DlqHeaders;
import com.threeai.nats.core.headers.BpmHeaders;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import org.flowable.common.engine.api.FlowableException;
import org.flowable.engine.ManagementService;
import org.flowable.job.api.ExternalWorkerJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes {@code dlq.ewjobs.>} and turns dead-lettered replies into the engine's operational
 * surface (docs/11 D-D'v2): {@code failureBuilder(jobId, <queried owner>).retries(0).fail()}
 * moves the job to Flowable's deadletter table — the Cockpit-incident analog.
 *
 * <p><b>Taxonomy:</b> the job is queried FIRST. Job gone → ack. Lock held by a sentinel-grammar
 * owner (the normal case: the ADR-0001 floor guarantees the dispatch generation still holds the
 * lock when the reply exhausts its delivery budget) → {@code fail()} with the QUERIED owner
 * string — the fail API needs the exact lock owner, and reading it locally keeps the wire
 * untrusted. Lock null or foreign → ack + {@code ew.incident.deferred}: the sweep will
 * re-dispatch, or the foreign owner completes it; a lone DLQ consumer cannot deadletter an
 * unlocked job through the public API. If the owner CHANGES between the query and the
 * {@code fail()} call, one retry re-runs the taxonomy on a fresh query (N-red-1 rule).
 *
 * <p>Deliberately no circuit breaker (A2's incident bridge carries one): the volume here is
 * bounded by the reply DLQ rate; nak-with-backoff covers transient engine outages.
 */
public class EwIncidentConsumer {

    private static final Logger log = LoggerFactory.getLogger(EwIncidentConsumer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String DURABLE = "flw-ew-incident";
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    private final Connection connection;
    private final JetStream jetStream;
    private final ManagementService managementService;
    private final String sentinelWorkerId;
    private final long ackWaitSeconds;
    private final NatsChannelMetrics metrics;

    private Dispatcher dispatcher;
    private ExecutorService executor;

    public EwIncidentConsumer(Connection connection, JetStream jetStream,
            ManagementService managementService, String sentinelWorkerId, long ackWaitSeconds,
            NatsChannelMetrics metrics) {
        this.connection = connection;
        this.jetStream = jetStream;
        this.managementService = managementService;
        this.sentinelWorkerId = sentinelWorkerId;
        this.ackWaitSeconds = ackWaitSeconds;
        this.metrics = metrics;
    }

    public void subscribe() {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.dispatcher = connection.createDispatcher();
        try {
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .ackWait(Duration.ofSeconds(ackWaitSeconds))
                    .durable(DURABLE)
                    .deliverGroup(DURABLE)
                    .build();
            PushSubscribeOptions opts = PushSubscribeOptions.builder().configuration(cc).build();
            jetStream.subscribe("dlq.ewjobs.>", DURABLE, dispatcher,
                    msg -> executor.submit(() -> handleDlqMessage(msg)), false, opts);
            log.info("Subscribed to external-worker DLQ", kv("subject", "dlq.ewjobs.>"));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to subscribe to 'dlq.ewjobs.>'", e);
        }
    }

    public void unsubscribe() {
        if (dispatcher != null) {
            try {
                dispatcher.drain(Duration.ofSeconds(10));
            } catch (Exception e) {
                log.warn("Error draining external-worker incident dispatcher", e);
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
        String topic = topicOf(msg.getSubject());
        String jobId = extractJobId(msg);
        if (jobId == null) {
            // Unidentifiable DLQ entry (e.g. an EMPTY_MESSAGE_BODY reply with no headers): the
            // job itself is untouched and will resurface via lock expiry + sweep.
            log.warn("DLQ message without a recoverable jobId — acking, sweep owns the job",
                    kv("subject", msg.getSubject()));
            deferred(msg, topic);
            return;
        }
        applyTaxonomy(msg, topic, jobId, true);
    }

    private void applyTaxonomy(Message msg, String topic, String jobId, boolean retryAllowed) {
        ExternalWorkerJob job = managementService.createExternalWorkerJobQuery().jobId(jobId).singleResult();
        if (job == null) {
            msg.ack();
            return;
        }
        String owner = job.getLockOwner();
        if (owner == null || !owner.startsWith(sentinelWorkerId + "#")) {
            log.warn("DLQ'd job not sentinel-locked — deferring to sweep/foreign owner",
                    kv("job_id", jobId), kv("lock_owner", owner));
            deferred(msg, topic);
            return;
        }
        try {
            managementService.createExternalWorkerJobFailureBuilder(jobId, owner)
                    .errorMessage("Reply delivery budget exhausted (deliveryCount > M)")
                    .errorDetails(dlqReasonOf(msg))
                    .retries(0)
                    // No residual lock delay: an operator retry from the ops surface should not wait
                    // out a stale lock (BAQ-2 parity).
                    .retryTimeout(Duration.ZERO)
                    .fail();
            msg.ack();
            log.warn("External-worker job moved to the engine deadletter table",
                    kv("job_id", jobId), kv("topic", topic), kv("dlq_reason", dlqReasonOf(msg)));
        } catch (FlowableException ownerChanged) {
            if (retryAllowed) {
                // Owner changed between query and fail(): one retry on a FRESH query (N-red-1 rule).
                applyTaxonomy(msg, topic, jobId, false);
            } else {
                log.warn("Owner still churning after retry — deferring to sweep",
                        kv("job_id", jobId), ownerChanged);
                deferred(msg, topic);
            }
        } catch (Exception transientFailure) {
            log.warn("Engine call failed while deadlettering — NAK with backoff",
                    kv("job_id", jobId), transientFailure);
            msg.nakWithDelay(backoff(msg));
        }
    }

    private void deferred(Message msg, String topic) {
        if (metrics != null) {
            metrics.incidentDeferredCount(topic).increment();
        }
        msg.ack();
    }

    /** Envelope jobId first (identity source); header correlation-id as fallback. */
    private String extractJobId(Message msg) {
        byte[] data = msg.getData();
        if (data != null && data.length > 0) {
            try {
                JsonNode root = MAPPER.readTree(new String(data, StandardCharsets.UTF_8));
                JsonNode id = root == null ? null : root.get("jobId");
                if (id != null && id.isTextual() && !id.asText().isEmpty()) {
                    return id.asText();
                }
            } catch (Exception ignored) {
                // fall through to header
            }
        }
        if (msg.getHeaders() != null) {
            String correlation = msg.getHeaders().getFirst(BpmHeaders.CORRELATION_ID);
            if (correlation != null && !correlation.isEmpty()) {
                return correlation;
            }
        }
        return null;
    }

    private String dlqReasonOf(Message msg) {
        if (msg.getHeaders() == null) {
            return "unknown";
        }
        String reason = msg.getHeaders().getFirst(DlqHeaders.REASON);
        return reason != null ? reason : "unknown";
    }

    private String topicOf(String dlqSubject) {
        return dlqSubject != null && dlqSubject.startsWith("dlq.ewjobs.")
                ? dlqSubject.substring("dlq.ewjobs.".length()) : "unknown";
    }

    private Duration backoff(Message msg) {
        long count;
        try {
            count = msg.metaData().deliveredCount();
        } catch (Exception e) {
            count = 1;
        }
        Duration d = Duration.ofSeconds((long) Math.pow(2, Math.max(0, count - 1)));
        return d.compareTo(MAX_BACKOFF) > 0 ? MAX_BACKOFF : d;
    }
}
