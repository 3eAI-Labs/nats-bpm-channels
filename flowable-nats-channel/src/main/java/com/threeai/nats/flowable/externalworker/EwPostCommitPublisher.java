package com.threeai.nats.flowable.externalworker;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.util.Map;

import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.JetStream;
import io.nats.client.impl.NatsMessage;
import org.flowable.job.api.ExternalWorkerJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes a born-locked external-worker job to {@code ewjobs.<topic>} (docs/11 §2 md. 1).
 * Called ONLY from the COMMITTED transaction listener registered by
 * {@link EwCreateJobInterceptor} — after commit, outside the transaction, so a slow broker can
 * never hold the engine transaction open. Publish failure is WARN-only by design: the orphan is
 * collected by {@link EwOrphanSweep} once the engine reset thread releases the expired lock
 * (recovery SLO: L + resetInterval + S).
 */
public class EwPostCommitPublisher {

    private static final Logger log = LoggerFactory.getLogger(EwPostCommitPublisher.class);

    private final JetStream jetStream;
    private final NatsChannelMetrics metrics;
    private final EwLockConfig lockConfig;

    /** Non-null = async mod (G4-P/3, default). Null = senkron kacis kapisi. */
    private final com.threeai.nats.core.jetstream.BoundedAsyncPublisher asyncPublisher;

    public EwPostCommitPublisher(JetStream jetStream, NatsChannelMetrics metrics, EwLockConfig lockConfig) {
        this(jetStream, metrics, lockConfig, null);
    }

    public EwPostCommitPublisher(JetStream jetStream, NatsChannelMetrics metrics, EwLockConfig lockConfig,
            com.threeai.nats.core.jetstream.BoundedAsyncPublisher asyncPublisher) {
        this.jetStream = jetStream;
        this.metrics = metrics;
        this.lockConfig = lockConfig;
        this.asyncPublisher = asyncPublisher;
    }

    public void publish(ExternalWorkerJob job, String topic, String nonce,
            String businessKey, Map<String, Object> variables) {
        if (lockConfig.isUnsafe()) {
            // BAQ-3: warn every cycle, never silently once.
            log.warn("External-worker dispatch running with unsafe umbrella-lock duration (L < floor)"
                    + " — allow-unsafe-lock-duration=true", kv("topic", topic));
        }
        String subject = "ewjobs." + topic;
        try {
            NatsMessage msg = EwJobMessageFactory.build(job, topic, nonce, businessKey, variables);
            if (asyncPublisher != null) {
                // G4-P/3: A2 ile ayni desen — kayip publish'i sweep toplar, yuzey degismez.
                String jobId = job.getId();
                asyncPublisher.publish(msg, () -> {
                    if (metrics != null) {
                        metrics.jsPublishCount(subject, topic).increment();
                    }
                }, error -> {
                    log.warn("Post-commit JetStream publish failed (async) — orphan will be"
                            + " collected by sweep after lock expiry + engine reset",
                            kv("job_id", jobId), kv("topic", topic), error);
                    if (metrics != null) {
                        metrics.jsPublishErrorCount(subject, topic).increment();
                    }
                });
                return;
            }
            jetStream.publish(msg);
            if (metrics != null) {
                metrics.jsPublishCount(subject, topic).increment();
            }
        } catch (Exception e) {
            log.warn("Post-commit JetStream publish failed — orphan will be collected by sweep"
                    + " after lock expiry + engine reset", kv("job_id", job.getId()), kv("topic", topic), e);
            if (metrics != null) {
                metrics.jsPublishErrorCount(subject, topic).increment();
            }
        }
    }
}
