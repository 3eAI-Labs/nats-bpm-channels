package com.threeai.nats.core.jetstream;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.io.IOException;
import java.time.Duration;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Idempotent {@code ensureStream}: creates the stream if missing (404), leaves it untouched if
 * it already exists.
 *
 * <p><b>{@code maxAge} default (Sentinel Phase 5.5 QA fix, item 6, Levent karari 2026-07-15):</b>
 * {@code dlq.}-prefixed subjects auto-create with a 14-day retention default
 * (DATA_CLASSIFICATION.md §5 Q3 karari — DLQ payload is RESTRICTED/PII, 14 gün the primary
 * governance control for its exposure window) unless the caller passes an explicit {@code
 * maxAge}. Non-DLQ subjects keep the pre-existing behavior (no age limit) unless the caller
 * opts in explicitly.
 */
public class JetStreamStreamManager {

    private static final Logger log = LoggerFactory.getLogger(JetStreamStreamManager.class);

    private static final String DLQ_SUBJECT_PREFIX = "dlq.";
    private static final Duration DLQ_DEFAULT_MAX_AGE = Duration.ofDays(14);

    /** Convenience overload — {@code maxAge} defaults per {@link #defaultMaxAgeFor(String)}. */
    public void ensureStream(String streamName, String subject, Connection connection) {
        ensureStream(streamName, subject, connection, defaultMaxAgeFor(subject));
    }

    /**
     * @param maxAge retention age limit for a newly-created stream ({@code null}/{@link
     *               Duration#ZERO} = no age limit, matching the JetStream default). Ignored if
     *               the stream already exists — existing streams are never reconfigured here.
     */
    public void ensureStream(String streamName, String subject, Connection connection, Duration maxAge) {
        try {
            JetStreamManagement jsm = connection.jetStreamManagement();
            try {
                jsm.getStreamInfo(streamName);
                log.debug("Stream exists", kv("stream", streamName));
            } catch (JetStreamApiException e) {
                if (e.getErrorCode() == 404) {
                    StreamConfiguration.Builder configBuilder = StreamConfiguration.builder()
                            .name(streamName)
                            .subjects(subject)
                            .retentionPolicy(RetentionPolicy.Limits)
                            .storageType(StorageType.File);
                    if (maxAge != null && !maxAge.isZero()) {
                        configBuilder.maxAge(maxAge);
                    }
                    jsm.addStream(configBuilder.build());
                    log.info("Stream created", kv("stream", streamName), kv("subject", subject),
                            kv("max_age_seconds", maxAge != null ? maxAge.toSeconds() : null));
                } else {
                    throw new IllegalStateException("Failed to check stream '" + streamName + "'", e);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("I/O error while managing stream '" + streamName + "'", e);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Unexpected error managing stream '" + streamName + "'", e);
        }
    }

    private static Duration defaultMaxAgeFor(String subject) {
        return subject != null && subject.startsWith(DLQ_SUBJECT_PREFIX) ? DLQ_DEFAULT_MAX_AGE : null;
    }
}
