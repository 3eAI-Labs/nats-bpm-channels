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
import io.nats.client.api.SubjectTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Idempotent {@code ensureStream}: creates the stream if missing (404), leaves it untouched if
 * it already exists.
 *
 * <p><b>{@code maxAge} default (QA review fix, item 6, decision 2026-07-15):</b>
 * {@code dlq.}-prefixed subjects auto-create with a 14-day retention default
 * (data-classification decision Q3 — DLQ payload is RESTRICTED/PII, and 14 days is the primary
 * governance control for its exposure window) unless the caller passes an explicit {@code
 * maxAge}. Non-DLQ subjects keep the pre-existing behavior (no age limit) unless the caller
 * opts in explicitly.
 *
 * <p><b>{@code retentionPolicy} default (post-review follow-up fix F-2, decision
 * 2026-07-15):</b> {@code asyncapi.yaml}'s {@code a2JobDispatch}/{@code a2JobReply} channels
 * declare {@code streamRetention: WorkQueue} (each message claimed by exactly one consumer, per
 * D-E), but this auto-create path previously hardcoded {@link RetentionPolicy#Limits}
 * everywhere, regardless of subject — a dev/test auto-create accidentally used in production
 * would silently diverge from the declared wire contract. {@code jobs.}-prefixed subjects
 * (the A2 job-dispatch/reply namespace, BR-SUB-004) now auto-create with {@link
 * RetentionPolicy#WorkQueue} by default; {@code dlq.}-prefixed and all other subjects keep the
 * pre-existing {@link RetentionPolicy#Limits} default. Production stream provisioning remains
 * an ops concern (PR-reviewed YAML) — this default only aligns the
 * repo's own dev/test/preflight auto-create path with the declared contract.
 */
public class JetStreamStreamManager {

    private static final Logger log = LoggerFactory.getLogger(JetStreamStreamManager.class);

    private static final String DLQ_SUBJECT_PREFIX = "dlq.";
    private static final String JOBS_SUBJECT_PREFIX = "jobs.";
    private static final String EWJOBS_SUBJECT_PREFIX = "ewjobs.";
    private static final Duration DLQ_DEFAULT_MAX_AGE = Duration.ofDays(14);

    private final int replicas;

    /** Single-replica streams — the historical behaviour, and what a single-node server allows. */
    public JetStreamStreamManager() {
        this(1);
    }

    /**
     * @param replicas replica count for streams this instance creates, from {@code
     *                 spring.nats.jetstream.stream-replicas}. Existing streams are never
     *                 reconfigured, so changing this does not touch what is already provisioned.
     */
    public JetStreamStreamManager(int replicas) {
        if (replicas < 1) {
            throw new IllegalArgumentException("stream replicas must be >= 1, got " + replicas);
        }
        this.replicas = replicas;
    }

    /**
     * A single-replica stream on a clustered server is a silent trap: it works perfectly until the
     * one node holding it is lost, at which point the stream stops serving entirely — no failover,
     * no degraded mode. Nothing here changes that (the replica count is the operator's call, and 1
     * is a legitimate choice for a dev stream on a shared cluster), but it is said out loud rather
     * than left to be discovered during the outage.
     *
     * <p>Deliberately keyed off {@code ServerInfo.getCluster()} — the server's own statement about
     * how it is running — rather than the stream's {@code ClusterInfo}, which describes the
     * stream's replicas and therefore looks identical for a single-replica stream whether or not a
     * cluster exists around it.
     */
    private void warnIfSingleReplicaOnCluster(String streamName, Connection connection) {
        if (replicas > 1) {
            return;
        }
        var serverInfo = connection.getServerInfo();
        String clusterName = serverInfo != null ? serverInfo.getCluster() : null;
        if (clusterName == null || clusterName.isBlank()) {
            return;
        }
        log.warn("Stream created with a single replica on a clustered server — it will stop "
                        + "serving if the node holding it is lost. Set "
                        + "spring.nats.jetstream.stream-replicas to change this.",
                kv("stream", streamName),
                kv("replicas", replicas),
                kv("cluster", clusterName));
    }

    /**
     * Convenience overload — {@code maxAge} defaults per {@link #defaultMaxAgeFor(String)},
     * {@code retentionPolicy} defaults per {@link #defaultRetentionPolicyFor(String)}.
     */
    public void ensureStream(String streamName, String subject, Connection connection) {
        ensureStream(streamName, subject, connection, defaultMaxAgeFor(subject), defaultRetentionPolicyFor(subject));
    }

    /**
     * Convenience overload — {@code retentionPolicy} defaults per {@link
     * #defaultRetentionPolicyFor(String)}.
     *
     * @param maxAge retention age limit for a newly-created stream ({@code null}/{@link
     *               Duration#ZERO} = no age limit, matching the JetStream default). Ignored if
     *               the stream already exists — existing streams are never reconfigured here.
     */
    public void ensureStream(String streamName, String subject, Connection connection, Duration maxAge) {
        ensureStream(streamName, subject, connection, maxAge, defaultRetentionPolicyFor(subject));
    }

    /**
     * @param maxAge          retention age limit for a newly-created stream ({@code null}/{@link
     *                        Duration#ZERO} = no age limit, matching the JetStream default).
     *                        Ignored if the stream already exists.
     * @param retentionPolicy retention policy for a newly-created stream ({@code null} = {@link
     *                        RetentionPolicy#Limits}, matching the JetStream default). Ignored
     *                        if the stream already exists — existing streams are never
     *                        reconfigured here.
     */
    public void ensureStream(String streamName, String subject, Connection connection, Duration maxAge,
            RetentionPolicy retentionPolicy) {
        ensureStream(streamName, subject, connection, maxAge, retentionPolicy, null);
    }

    /**
     * @param subjectTransform increment 2 subject-mapped partitioning (ARCH-Q3/LLD-Q2,
     *                         {@link com.threeai.nats.core.jetstream.JetStreamSubjectPartitioner}) —
     *                         {@code null} = no transform (increment 1 default, unchanged). Ignored
     *                         if the stream already exists — existing streams are never
     *                         reconfigured here.
     */
    public void ensureStream(String streamName, String subject, Connection connection, Duration maxAge,
            RetentionPolicy retentionPolicy, SubjectTransform subjectTransform) {
        try {
            JetStreamManagement jsm = connection.jetStreamManagement();
            try {
                jsm.getStreamInfo(streamName);
                log.debug("Stream exists", kv("stream", streamName));
            } catch (JetStreamApiException e) {
                if (e.getErrorCode() == 404) {
                    RetentionPolicy resolvedRetention = retentionPolicy != null ? retentionPolicy : RetentionPolicy.Limits;
                    StreamConfiguration.Builder configBuilder = StreamConfiguration.builder()
                            .name(streamName)
                            .subjects(subject)
                            .retentionPolicy(resolvedRetention)
                            .replicas(replicas)
                            .storageType(StorageType.File);
                    if (maxAge != null && !maxAge.isZero()) {
                        configBuilder.maxAge(maxAge);
                    }
                    if (subjectTransform != null) {
                        configBuilder.subjectTransform(subjectTransform);
                    }
                    jsm.addStream(configBuilder.build());
                    warnIfSingleReplicaOnCluster(streamName, connection);
                    log.info("Stream created", kv("stream", streamName), kv("subject", subject),
                            kv("retention_policy", resolvedRetention),
                            kv("max_age_seconds", maxAge != null ? maxAge.toSeconds() : null),
                            kv("subject_transform", subjectTransform != null));
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

    private static RetentionPolicy defaultRetentionPolicyFor(String subject) {
        if (subject == null) {
            return RetentionPolicy.Limits;
        }
        // docs/11 G4: ewjobs. carries external-worker job dispatch — same work-queue nature as jobs.
        return subject.startsWith(JOBS_SUBJECT_PREFIX) || subject.startsWith(EWJOBS_SUBJECT_PREFIX)
                ? RetentionPolicy.WorkQueue
                : RetentionPolicy.Limits;
    }
}
