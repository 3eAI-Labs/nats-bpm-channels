package com.threeai.nats.cadenzaflow.a2;

import static net.logstash.logback.argument.StructuredArguments.kv;

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
 * — no {@code findExternalTaskById}/query call (BR-A2-004 condition 1).
 */
public class A2PostCommitPublisher {

    private static final Logger log = LoggerFactory.getLogger(A2PostCommitPublisher.class);

    private final JetStream jetStream;
    private final NatsChannelMetrics metrics;
    private final UmbrellaLockValidator lockValidator;

    public A2PostCommitPublisher(JetStream jetStream, NatsChannelMetrics metrics, UmbrellaLockValidator lockValidator) {
        this.jetStream = jetStream;
        this.metrics = metrics;
        this.lockValidator = lockValidator;
    }

    public void publish(ExternalTaskEntity task) {
        if (lockValidator.isUnsafe(task.getTopicName())) {
            // BAQ-3 "warn every cycle, never silently once" (08_config.md §1.4.1).
            log.warn("Topic running with unsafe umbrella-lock duration (L < floor) — "
                    + "allow-unsafe-lock-duration=true", kv("topic", task.getTopicName()));
        }

        String subject = "jobs." + task.getTopicName();
        Timer.Sample dispatchSample = metrics != null ? Timer.start() : null;
        try {
            NatsMessage msg = A2JobMessageFactory.build(task);
            jetStream.publish(msg); // Nats-Msg-Id dedup (BR-SUB-005)
            if (metrics != null) {
                metrics.jsPublishCount(subject, task.getTopicName()).increment();
                if (dispatchSample != null) {
                    dispatchSample.stop(metrics.dispatchLatencyTimer(task.getTopicName()));
                }
            }
        } catch (Exception e) {
            // EXT_JETSTREAM_PUBLISH_UNAVAILABLE — WARN only, no special action by design.
            // Orphan will be collected by the sweep within <= L+S (BR-A2-004 row 3, NFR-R3).
            log.warn("Post-commit JetStream publish failed — orphan will be collected by sweep",
                    kv("external_task_id", task.getId()), kv("topic", task.getTopicName()), e);
            if (metrics != null) {
                metrics.jsPublishErrorCount(subject, task.getTopicName()).increment();
            }
        }
    }
}
