package com.threeai.nats.bench.history;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.time.Duration;

import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import com.threeai.nats.core.jetstream.JetStreamSubjectPartitioner;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.SubjectTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends basamak-1 {@code BenchEnvironment.ensureStreams()} — provisions {@code HISTORY}
 * (Limits, 7-day retention, {@code SubjectTransform Partition(8,3)}, ARCH-Q3/LLD-Q2) and {@code
 * DLQ_HISTORY} (Limits, 14-day retention, separate stream CQ-6) for bench runs
 * (`03_classes/5_bench.md` §2). Production stream provisioning is the SAME ops/PR'lı-YAML
 * discipline as basamak-1 (`99_deployment.md` §5, `JetStreamStreamManager`'s own class-level
 * doc) — this class exists so bench/CI environments can self-provision the same shape.
 *
 * <p><b>CODER-NOTE (VAL_HISTORY_STREAM_PROVISIONING_MISSING, ERROR_REGISTRY.md §3.1 row 3):</b>
 * the registry names THIS class as the log source, severity ERROR in production / WARN in bench
 * — i.e. auto-creating a missing stream is normal/expected here (bench self-provisions), but the
 * SAME log line in a production deployment (where ops was supposed to have provisioned it ahead
 * of time via the PR'd-YAML runbook) is a signal something was skipped. This class always logs
 * at WARN (its own context IS bench) with a pre-existence check so callers embedding this same
 * provisioning logic in a stricter context can escalate the log level themselves.
 */
public class HistoryStreamProvisioner {

    private static final Logger log = LoggerFactory.getLogger(HistoryStreamProvisioner.class);

    static final String HISTORY_STREAM_NAME = "HISTORY";
    static final String HISTORY_BASE_SUBJECT = "history";
    static final String DLQ_HISTORY_STREAM_NAME = "DLQ_HISTORY";
    static final String DLQ_HISTORY_SUBJECT = "dlq.history.>";
    private static final int WILDCARD_COUNT = 3; // <engineId>.<class>.<processInstanceId>
    private static final int PARTITION_TOKEN_INDEX = 3; // processInstanceId, ARCH-Q3/D-E
    private static final int PARTITION_COUNT = 8; // LLD-Q2 default
    private static final Duration HISTORY_RETENTION_DAYS = Duration.ofDays(7);
    private static final Duration DLQ_HISTORY_RETENTION_DAYS = Duration.ofDays(14);

    public void ensureHistoryStreams(JetStreamStreamManager streamManager, Connection connection) {
        warnIfMissing(HISTORY_STREAM_NAME, connection);
        SubjectTransform partitionTransform = JetStreamSubjectPartitioner.buildPartitionTransform(
                HISTORY_BASE_SUBJECT, WILDCARD_COUNT, PARTITION_TOKEN_INDEX, PARTITION_COUNT);
        streamManager.ensureStream(HISTORY_STREAM_NAME, HISTORY_BASE_SUBJECT + ".>", connection,
                HISTORY_RETENTION_DAYS, RetentionPolicy.Limits, partitionTransform);

        warnIfMissing(DLQ_HISTORY_STREAM_NAME, connection);
        streamManager.ensureStream(DLQ_HISTORY_STREAM_NAME, DLQ_HISTORY_SUBJECT, connection,
                DLQ_HISTORY_RETENTION_DAYS, RetentionPolicy.Limits);
    }

    private void warnIfMissing(String streamName, Connection connection) {
        try {
            JetStreamManagement jsm = connection.jetStreamManagement();
            jsm.getStreamInfo(streamName);
        } catch (JetStreamApiException e) {
            if (e.getErrorCode() == 404) {
                log.warn("History stream provisioning missing — auto-provisioning now",
                        kv("stream", streamName)); // VAL_HISTORY_STREAM_PROVISIONING_MISSING
            }
        } catch (Exception e) {
            log.debug("Failed to pre-check stream existence — proceeding to ensureStream anyway",
                    kv("stream", streamName), e);
        }
    }
}
