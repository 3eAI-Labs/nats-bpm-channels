package com.threeai.nats.core.jetstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.threeai.nats.core.jetstream.JetStreamTopologyCheck.ConsumerBinding;
import com.threeai.nats.core.jetstream.JetStreamTopologyCheck.SubjectBinding;
import com.threeai.nats.core.jetstream.JetStreamTopologyCheck.Finding;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.ServerInfo;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class JetStreamTopologyCheckTest {

    private static final ConsumerBinding A2 = new ConsumerBinding("JOBS", "a2-completion-orders");

    private Connection connection;
    private JetStreamManagement jsm;

    @BeforeEach
    void setUp() throws IOException {
        connection = mock(Connection.class);
        jsm = mock(JetStreamManagement.class);
        when(connection.jetStreamManagement()).thenReturn(jsm);
        singleNodeServer();
    }

    @Test
    void nullJetStreamManagement_neverFailsAndStillEvaluatesTheKvCheck() throws Exception {
        Connection bare = mock(Connection.class); // jetStreamManagement() -> null (mock default)

        List<Finding> findings = JetStreamTopologyCheck.inspectAndWarnSubjects(bare,
                List.of(new SubjectBinding("jobs.orders.reply", "a2-completion-orders")), 3);

        assertThat(findings).singleElement()
                .satisfies(f -> assertThat(f.code())
                        .isEqualTo(JetStreamTopologyCheck.KV_REPLICAS_WITHOUT_CLUSTER));
    }

    // --- subject resolution -----------------------------------------------------------------

    @Test
    void subjectBinding_resolvesOwningStreamAndRunsTheSameChecks() throws Exception {
        when(jsm.getStreamNames("jobs.orders.reply")).thenReturn(List.of("JOBS"));
        consumerExists(A2, "_INBOX.deliver", null);

        List<Finding> findings = JetStreamTopologyCheck.inspectAndWarnSubjects(connection,
                List.of(new SubjectBinding("jobs.orders.reply", "a2-completion-orders")), 1);

        assertThat(findings).singleElement()
                .satisfies(f -> assertThat(f.code())
                        .isEqualTo(JetStreamTopologyCheck.DURABLE_WITHOUT_DELIVER_GROUP));
    }

    @Test
    void subjectWithNoStreamBehindItYet_isSkippedNotReported() throws Exception {
        when(jsm.getStreamNames("jobs.orders.reply")).thenReturn(List.of());

        assertThat(JetStreamTopologyCheck.inspectAndWarnSubjects(connection,
                List.of(new SubjectBinding("jobs.orders.reply", "a2-completion-orders")), 1)).isEmpty();
    }

    @Test
    void subjectResolutionFailure_doesNotSuppressTheKvFinding() throws Exception {
        when(jsm.getStreamNames("jobs.orders.reply")).thenThrow(new IOException("broker down"));

        List<Finding> findings = JetStreamTopologyCheck.inspectAndWarnSubjects(connection,
                List.of(new SubjectBinding("jobs.orders.reply", "a2-completion-orders")), 3);

        assertThat(findings).singleElement()
                .satisfies(f -> assertThat(f.code())
                        .isEqualTo(JetStreamTopologyCheck.KV_REPLICAS_WITHOUT_CLUSTER));
    }

    // --- durable consumers ------------------------------------------------------------------

    @Test
    void pushDurableWithoutDeliverGroup_isReported() throws Exception {
        consumerExists(A2, "_INBOX.deliver", null);

        List<Finding> findings = JetStreamTopologyCheck.inspect(connection, List.of(A2), 1);

        assertThat(findings).singleElement()
                .satisfies(f -> {
                    assertThat(f.code()).isEqualTo(JetStreamTopologyCheck.DURABLE_WITHOUT_DELIVER_GROUP);
                    assertThat(f.message())
                            .contains("a2-completion-orders")
                            .contains("[SUB-90012]")
                            .contains("nats consumer rm JOBS a2-completion-orders");
                });
    }

    @Test
    void pushDurableWithDeliverGroup_isClean() throws Exception {
        consumerExists(A2, "_INBOX.deliver", "a2-completion");

        assertThat(JetStreamTopologyCheck.inspect(connection, List.of(A2), 1)).isEmpty();
    }

    /**
     * A pull consumer has no deliver subject and is shared between clients by design — the
     * exclusivity rule this check exists for does not apply to it, and flagging one would train
     * operators to ignore the warning.
     */
    @Test
    void pullDurableWithoutDeliverGroup_isClean() throws Exception {
        consumerExists(A2, null, null);

        assertThat(JetStreamTopologyCheck.inspect(connection, List.of(A2), 1)).isEmpty();
    }

    @Test
    void consumerNotYetCreated_isClean() throws Exception {
        JetStreamApiException notFound = apiError(404);
        when(jsm.getConsumerInfo(A2.stream(), A2.durableName())).thenThrow(notFound);

        assertThat(JetStreamTopologyCheck.inspect(connection, List.of(A2), 1)).isEmpty();
    }

    // --- KV replicas ------------------------------------------------------------------------

    @Test
    void kvReplicasAboveOneOnSingleNode_isReported() {
        List<Finding> findings = JetStreamTopologyCheck.inspect(connection, List.of(), 3);

        assertThat(findings).singleElement()
                .satisfies(f -> {
                    assertThat(f.code()).isEqualTo(JetStreamTopologyCheck.KV_REPLICAS_WITHOUT_CLUSTER);
                    assertThat(f.message()).contains("[10074]");
                });
    }

    @Test
    void kvReplicasAboveOneOnCluster_isClean() {
        clusteredServer();

        assertThat(JetStreamTopologyCheck.inspect(connection, List.of(), 3)).isEmpty();
    }

    // --- stream replicas --------------------------------------------------------------------

    @Test
    void singleReplicaStreamOnCluster_isReported() throws Exception {
        clusteredServer();
        consumerExists(A2, "_INBOX.deliver", "a2-completion");
        streamExists("JOBS", 1);

        List<Finding> findings = JetStreamTopologyCheck.inspect(connection, List.of(A2), 3);

        assertThat(findings).singleElement()
                .satisfies(f -> {
                    assertThat(f.code()).isEqualTo(JetStreamTopologyCheck.SINGLE_REPLICA_STREAM_ON_CLUSTER);
                    assertThat(f.message()).contains("nats stream edit JOBS --replicas 3");
                });
    }

    @Test
    void replicatedStreamOnCluster_isClean() throws Exception {
        clusteredServer();
        consumerExists(A2, "_INBOX.deliver", "a2-completion");
        streamExists("JOBS", 3);

        assertThat(JetStreamTopologyCheck.inspect(connection, List.of(A2), 3)).isEmpty();
    }

    /**
     * On a single-node server a one-replica stream is the only thing that can exist, so the
     * REPLICA warning would be one nobody can act on — it stays suppressed. The stream itself is
     * still fetched, because the retention check below applies everywhere.
     */
    @Test
    void singleReplicaStreamWithoutCluster_replicaFindingIsSuppressed() throws Exception {
        consumerExists(A2, "_INBOX.deliver", "a2-completion");
        streamExists("JOBS", 1);

        assertThat(JetStreamTopologyCheck.inspect(connection, List.of(A2), 1)).isEmpty();
    }

    // --- stream retention -------------------------------------------------------------------

    @Test
    void uncappedLimitsStream_isReportedEvenWithoutCluster() throws Exception {
        consumerExists(A2, "_INBOX.deliver", "a2-completion");
        uncappedLimitsStreamExists("JOBS", 1);

        List<Finding> findings = JetStreamTopologyCheck.inspect(connection, List.of(A2), 1);

        assertThat(findings).singleElement()
                .satisfies(f -> {
                    assertThat(f.code()).isEqualTo(JetStreamTopologyCheck.LIMITS_STREAM_WITHOUT_SIZE_CAP);
                    assertThat(f.message()).contains("nats stream edit JOBS --max-bytes");
                });
    }

    @Test
    void byteCappedLimitsStream_isClean() throws Exception {
        consumerExists(A2, "_INBOX.deliver", "a2-completion");
        streamExists("JOBS", 1, RetentionPolicy.Limits, 268_435_456L, -1L, -1L);

        assertThat(JetStreamTopologyCheck.inspect(connection, List.of(A2), 1)).isEmpty();
    }

    @Test
    void uncappedInterestRetentionStream_isClean() throws Exception {
        consumerExists(A2, "_INBOX.deliver", "a2-completion");
        streamExists("JOBS", 1, RetentionPolicy.Interest, -1L, -1L, -1L);

        assertThat(JetStreamTopologyCheck.inspect(connection, List.of(A2), 1)).isEmpty();
    }

    @Test
    void singleReplicaAndUncappedOnCluster_reportsBothFindings() throws Exception {
        clusteredServer();
        consumerExists(A2, "_INBOX.deliver", "a2-completion");
        uncappedLimitsStreamExists("JOBS", 1);

        List<Finding> findings = JetStreamTopologyCheck.inspect(connection, List.of(A2), 3);

        assertThat(findings).extracting(Finding::code).containsExactlyInAnyOrder(
                JetStreamTopologyCheck.SINGLE_REPLICA_STREAM_ON_CLUSTER,
                JetStreamTopologyCheck.LIMITS_STREAM_WITHOUT_SIZE_CAP);
    }

    // --- the check must never be the thing that breaks a boot -------------------------------

    @Test
    void jetStreamUnavailable_yieldsNothingAndDoesNotThrow() throws Exception {
        when(connection.jetStreamManagement()).thenThrow(new IOException("no JetStream"));

        assertThatCode(() -> assertThat(JetStreamTopologyCheck.inspect(connection, List.of(A2), 1)).isEmpty())
                .doesNotThrowAnyException();
    }

    @Test
    void brokerErrorOnConsumerLookup_yieldsNothingAndDoesNotThrow() throws Exception {
        JetStreamApiException brokerError = apiError(500);
        when(jsm.getConsumerInfo(A2.stream(), A2.durableName())).thenThrow(brokerError);

        assertThatCode(() -> assertThat(JetStreamTopologyCheck.inspect(connection, List.of(A2), 1)).isEmpty())
                .doesNotThrowAnyException();
    }

    @Test
    void nullConnection_yieldsNothing() {
        assertThat(JetStreamTopologyCheck.inspect(null, List.of(A2), 3)).isEmpty();
    }

    // --- logging ----------------------------------------------------------------------------

    @Test
    void inspectAndWarn_logsOneWarningPerFinding() throws Exception {
        consumerExists(A2, "_INBOX.deliver", null);
        Logger logger = (Logger) LoggerFactory.getLogger(JetStreamTopologyCheck.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            JetStreamTopologyCheck.inspectAndWarn(connection, List.of(A2), 3);
        } finally {
            logger.detachAppender(appender);
        }

        assertThat(appender.list)
                .filteredOn(event -> event.getLevel() == Level.WARN)
                .hasSize(2)
                .allSatisfy(event -> assertThat(event.getFormattedMessage()).startsWith("NATS topology check:"));
    }

    // --- fixtures ---------------------------------------------------------------------------

    private void singleNodeServer() {
        ServerInfo info = mock(ServerInfo.class);
        when(info.getCluster()).thenReturn(null);
        when(connection.getServerInfo()).thenReturn(info);
    }

    private void clusteredServer() {
        ServerInfo info = mock(ServerInfo.class);
        when(info.getCluster()).thenReturn("perf");
        when(connection.getServerInfo()).thenReturn(info);
    }

    private void consumerExists(ConsumerBinding binding, String deliverSubject, String deliverGroup)
            throws Exception {
        ConsumerConfiguration config = mock(ConsumerConfiguration.class);
        when(config.getDeliverSubject()).thenReturn(deliverSubject);
        when(config.getDeliverGroup()).thenReturn(deliverGroup);
        ConsumerInfo info = mock(ConsumerInfo.class);
        when(info.getConsumerConfiguration()).thenReturn(config);
        when(jsm.getConsumerInfo(binding.stream(), binding.durableName())).thenReturn(info);
    }

    private void streamExists(String stream, int replicas) throws Exception {
        StreamConfiguration config = mock(StreamConfiguration.class);
        when(config.getReplicas()).thenReturn(replicas);
        StreamInfo info = mock(StreamInfo.class);
        when(info.getConfiguration()).thenReturn(config);
        when(jsm.getStreamInfo(stream)).thenReturn(info);
    }

    private void streamExists(String stream, int replicas, RetentionPolicy retention,
            long maxBytes, long maxMsgs, long maxMsgsPerSubject) throws Exception {
        StreamConfiguration config = mock(StreamConfiguration.class);
        when(config.getReplicas()).thenReturn(replicas);
        when(config.getRetentionPolicy()).thenReturn(retention);
        when(config.getMaxBytes()).thenReturn(maxBytes);
        when(config.getMaxMsgs()).thenReturn(maxMsgs);
        when(config.getMaxMsgsPerSubject()).thenReturn(maxMsgsPerSubject);
        StreamInfo info = mock(StreamInfo.class);
        when(info.getConfiguration()).thenReturn(config);
        when(jsm.getStreamInfo(stream)).thenReturn(info);
    }

    private void uncappedLimitsStreamExists(String stream, int replicas) throws Exception {
        streamExists(stream, replicas, RetentionPolicy.Limits, -1L, -1L, -1L);
    }

    private JetStreamApiException apiError(int code) {
        JetStreamApiException e = mock(JetStreamApiException.class);
        when(e.getErrorCode()).thenReturn(code);
        return e;
    }
}
