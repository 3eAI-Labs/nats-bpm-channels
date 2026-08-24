package com.threeai.nats.cadenzaflow.a2;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.util.Map;

import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.Timer;
import io.nats.client.JetStream;
import io.nats.client.impl.NatsMessage;
import org.cadenzaflow.bpm.engine.impl.persistence.entity.ExternalTaskEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes a freshly-locked external task to {@code jobs.<topic>} (HLD §2.2, BR-A2-001/004,
 * FR-A1/A4, US-A1/A3). Called ONLY from the COMMITTED transaction-listener registered by
 * {@link A2ExternalTaskBehavior#execute} — this runs after commit, outside the transaction, so
 * a slow/unavailable JetStream broker can never hold the engine DB transaction open.
 *
 * <p><b>DB-query-free guarantee:</b> takes the entity the creating node already holds in memory
 * — no {@code findExternalTaskById}/query call (BR-A2-004 condition 1). The optional {@code
 * capturedVariables} map (QA review fix, item 5) is likewise already-resolved — it
 * was captured IN-TX by {@link A2ExternalTaskBehavior#execute}, so accepting it here adds no DB
 * access of its own.
 */
public class A2PostCommitPublisher {

    /** docs/13 §2.6: non-null only in sharded mode — stamps the envelope Reply-Subject. */
    private com.threeai.nats.core.shard.ShardTopology shardTopology;

    public void setShardTopology(com.threeai.nats.core.shard.ShardTopology shardTopology) {
        this.shardTopology = shardTopology;
    }

    private String replySubjectFor(String topic) {
        return shardTopology == null ? null : com.threeai.nats.core.shard.ShardSubjects.scoped(
                shardTopology.getShardId(), "jobs." + topic + ".reply");
    }

    private static final Logger log = LoggerFactory.getLogger(A2PostCommitPublisher.class);

    private final JetStream jetStream;
    private final NatsChannelMetrics metrics;
    private final UmbrellaLockValidator lockValidator;
    /** Non-null = async mod (G4-P, default). Null = senkron kacis kapisi. */
    private final com.threeai.nats.core.jetstream.BoundedAsyncPublisher asyncPublisher;

    public A2PostCommitPublisher(JetStream jetStream, NatsChannelMetrics metrics, UmbrellaLockValidator lockValidator) {
        this(jetStream, metrics, lockValidator, null);
    }

    public A2PostCommitPublisher(JetStream jetStream, NatsChannelMetrics metrics,
            UmbrellaLockValidator lockValidator,
            com.threeai.nats.core.jetstream.BoundedAsyncPublisher asyncPublisher) {
        this.jetStream = jetStream;
        this.metrics = metrics;
        this.lockValidator = lockValidator;
        this.asyncPublisher = asyncPublisher;
    }

    /**
     * Convenience overload — no captured variables (identity-only envelope). {@link
     * A2OrphanSweep}'s cold re-publish path calls {@link A2JobMessageFactory} directly, not this
     * publisher, so it is unaffected either way (documented gap: sweep re-publish does not carry
     * captured variables — see {@link A2JobMessageFactory} class Javadoc).
     */
    public void publish(ExternalTaskEntity task) {
        publish(task, Map.of());
    }

    public void publish(ExternalTaskEntity task, Map<String, Object> capturedVariables) {
        if (lockValidator.isUnsafe(task.getTopicName())) {
            // BAQ-3 "warn every cycle, never silently once".
            log.warn("Topic running with unsafe umbrella-lock duration (L < floor) — "
                    + "allow-unsafe-lock-duration=true", kv("topic", task.getTopicName()));
        }

        String subject = "jobs." + task.getTopicName();
        String topic = task.getTopicName();
        String taskId = task.getId();
        Timer.Sample dispatchSample = metrics != null ? Timer.start() : null;
        NatsMessage msg;
        try {
            msg = A2JobMessageFactory.build(task, capturedVariables, replySubjectFor(topic));
        } catch (Exception e) {
            log.warn("Post-commit JetStream publish failed — orphan will be collected by sweep",
                    kv("external_task_id", taskId), kv("topic", topic), e);
            if (metrics != null) {
                metrics.jsPublishErrorCount(subject, topic).increment();
            }
            return;
        }
        if (asyncPublisher != null) {
            // G4-P (2026-08-25): ACK beklemesi motor thread'inden cikar; ayni WARN + ayni
            // sayac callback'te. Kayip publish'in kaderi degismez: sweep toplar (NFR-R3).
            // dispatchLatencyTimer artik ENQUEUE->ACK suresini async olcer (anlami degisti,
            // not its name — noted in the CHANGELOG).
            asyncPublisher.publish(msg, () -> {
                if (metrics != null) {
                    metrics.jsPublishCount(subject, topic).increment();
                    if (dispatchSample != null) {
                        dispatchSample.stop(metrics.dispatchLatencyTimer(topic));
                    }
                }
            }, error -> {
                log.warn("Post-commit JetStream publish failed (async) — orphan will be"
                        + " collected by sweep",
                        kv("external_task_id", taskId), kv("topic", topic), error);
                if (metrics != null) {
                    metrics.jsPublishErrorCount(subject, topic).increment();
                }
            });
            return;
        }
        try {
            jetStream.publish(msg); // Nats-Msg-Id dedup (BR-SUB-005)
            if (metrics != null) {
                metrics.jsPublishCount(subject, topic).increment();
                if (dispatchSample != null) {
                    dispatchSample.stop(metrics.dispatchLatencyTimer(topic));
                }
            }
        } catch (Exception e) {
            // EXT_JETSTREAM_PUBLISH_UNAVAILABLE — WARN only, no special action by design.
            // Orphan will be collected by the sweep within <= L+S (BR-A2-004 row 3, NFR-R3).
            log.warn("Post-commit JetStream publish failed — orphan will be collected by sweep",
                    kv("external_task_id", taskId), kv("topic", topic), e);
            if (metrics != null) {
                metrics.jsPublishErrorCount(subject, topic).increment();
            }
        }
    }
}
