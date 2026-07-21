package com.threeai.nats.bench.history;

import static org.assertj.core.api.Assertions.assertThat;

import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.StreamInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Real-NATS regression test for the subject-transform bug this class's own bench slice found
 *  (see {@code JetStreamSubjectPartitioner}'s CODER-NOTE) — a mocked JetStream would never have
 *  caught {@code invalid subject (10049)}. */
@Testcontainers
class HistoryStreamProvisionerTest {

    private static GenericContainer<?> natsContainer;
    private static Connection connection;

    @BeforeAll
    static void startContainer() throws Exception {
        natsContainer = new GenericContainer<>("nats:2.10-alpine").withCommand("--jetstream").withExposedPorts(4222);
        natsContainer.start();
        connection = Nats.connect("nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222));
    }

    @AfterAll
    static void stopContainer() throws Exception {
        connection.close();
        natsContainer.stop();
    }

    @Test
    void ensureHistoryStreams_createsHistoryAndDlqHistoryStreams() throws Exception {
        new HistoryStreamProvisioner().ensureHistoryStreams(new JetStreamStreamManager(), connection);

        JetStreamManagement jsm = connection.jetStreamManagement();
        StreamInfo historyInfo = jsm.getStreamInfo(HistoryStreamProvisioner.HISTORY_STREAM_NAME);
        StreamInfo dlqInfo = jsm.getStreamInfo(HistoryStreamProvisioner.DLQ_HISTORY_STREAM_NAME);

        assertThat(historyInfo.getConfiguration().getSubjects()).contains(HistoryStreamProvisioner.HISTORY_BASE_SUBJECT + ".>");
        assertThat(historyInfo.getConfiguration().getSubjectTransform()).isNotNull();
        assertThat(dlqInfo.getConfiguration().getSubjects()).contains(HistoryStreamProvisioner.DLQ_HISTORY_SUBJECT);
    }

    @Test
    void ensureHistoryStreams_idempotent_secondCallDoesNotThrow() throws Exception {
        HistoryStreamProvisioner provisioner = new HistoryStreamProvisioner();
        JetStreamStreamManager streamManager = new JetStreamStreamManager();

        provisioner.ensureHistoryStreams(streamManager, connection);
        provisioner.ensureHistoryStreams(streamManager, connection); // must not throw "stream already exists"
    }
}
