package com.threeai.nats.camunda.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.UUID;

import com.threeai.nats.core.db.SqlMigrationRunner;
import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.core.history.PseudonymTokenGenerator;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.jetstream.SweepLeaderLease;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import org.camunda.bpm.engine.impl.history.event.UserOperationLogEntryEventEntity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * BR-REL-001, `TEST_SPECIFICATIONS.md` (b) — Phase 5.5 (QA): the three custody-transfer
 * crash-points the audit-critical outbox/relay design must survive without loss (NFR-R1). Real
 * Postgres + real NATS JetStream (Testcontainers) throughout; {@code HistoryOutboxRelayTest}
 * already covers the leader/non-leader/dedup DB-read paths this class does not re-test.
 */
@Testcontainers
class HistoryOutboxCustodyTransferTest {

    private static PostgreSQLContainer<?> postgres;
    private static GenericContainer<?> natsContainer;
    private static PGSimpleDataSource dataSource;
    private static io.nats.client.Connection natsConnection;
    private static JetStream jetStream;

    @BeforeAll
    static void startContainers() throws Exception {
        postgres = new PostgreSQLContainer<>("postgres:16");
        postgres.start();
        dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgres.getJdbcUrl());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/history/V1__compact_history_outbox.sql");

        natsContainer = new GenericContainer<>("nats:2.10-alpine").withCommand("--jetstream").withExposedPorts(4222);
        natsContainer.start();
        natsConnection = Nats.connect("nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222));
        jetStream = natsConnection.jetStream();

        JetStreamManagement jsm = natsConnection.jetStreamManagement();
        jsm.addStream(StreamConfiguration.builder()
                .name("HISTORY_CUSTODY_TEST")
                .subjects("history.>")
                .retentionPolicy(RetentionPolicy.Limits)
                .storageType(StorageType.File)
                .build());
    }

    @AfterAll
    static void stopContainers() throws Exception {
        natsConnection.close();
        natsContainer.stop();
        postgres.stop();
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement("DELETE FROM compact_history_outbox")) {
            stmt.executeUpdate();
        }
    }

    private long countOutboxRows() throws Exception {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT count(*) FROM compact_history_outbox");
             ResultSet rs = stmt.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private HistoryOutboxRelay newRelayAsLeader() throws Exception {
        String bucket = "history-relay-leader-custody-" + UUID.randomUUID();
        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket(bucket, Duration.ofSeconds(60), 1, natsConnection);
        SweepLeaderLease leaderLease = new SweepLeaderLease(jetStream, kvManager, natsConnection,
                bucket, "relay-leader.", "camunda", "test-node", Duration.ofSeconds(60));
        assertThat(leaderLease.tryAcquireOrRenew()).isTrue();
        return new HistoryOutboxRelay(dataSource, jetStream, leaderLease, new HistoryOutboxProperties(), null, "camunda");
    }

    /**
     * Crash-point 1 (commit-öncesi): a tx-injected failure AFTER {@code
     * CompactHistoryOutboxWriter.write(...)} but BEFORE the engine's own {@code commit()} must
     * leave NO trace in {@code compact_history_outbox} — the outbox row shares the engine's
     * runtime transaction (BR-HDL-003), it cannot survive independently of it.
     */
    @Test
    void commitBeforeCrash_txRolledBack_outboxRowNeverPersists() throws Exception {
        UserOperationLogEntryEventEntity event = new UserOperationLogEntryEventEntity();
        event.setId(UUID.randomUUID().toString());
        event.setEventType("UserOperationLog");
        event.setProcessInstanceId("proc-commit-crash");
        event.setUserId("user-1");

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            new CompactHistoryOutboxWriter(dataSource, new PseudonymTokenGenerator(),
                    new HistoryClassificationProperties(), null)
                    .write(event, HistoryClassNames.OP_LOG, "camunda", connection);
            // Simulated crash BEFORE the engine's own runtime write commits (e.g. ACT_HI_* insert
            // in the SAME tx fails right after this call) -- the caller never reaches commit().
            connection.rollback();
        }

        assertThat(countOutboxRows()).isZero();
    }

    /**
     * Crash-point 2 (publish-öncesi / relay node restart): the outbox row is durable and
     * untouched for as long as no relay instance is running against it; once a relay DOES start
     * (post-"restart"), custody-transfer completes normally (publish + PubAck + delete).
     */
    @Test
    void publishBeforeCrash_relayNotYetStarted_rowSurvivesThenRelayCompletesOnRestart() throws Exception {
        UserOperationLogEntryEventEntity event = new UserOperationLogEntryEventEntity();
        event.setId(UUID.randomUUID().toString());
        event.setEventType("UserOperationLog");
        event.setProcessInstanceId("proc-publish-crash");
        event.setUserId("user-1");
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            new CompactHistoryOutboxWriter(dataSource, new PseudonymTokenGenerator(),
                    new HistoryClassificationProperties(), null)
                    .write(event, HistoryClassNames.OP_LOG, "camunda", connection);
            connection.commit();
        }

        // No relay has run yet (simulated relay-node-down window) -- row must still be there.
        assertThat(countOutboxRows()).isEqualTo(1);

        // Relay "restarts" (a fresh HistoryOutboxRelay instance, as would happen on node restart)
        // and completes custody-transfer.
        HistoryOutboxRelay relay = newRelayAsLeader();
        relay.relayCycle();

        assertThat(countOutboxRows()).isZero();
    }

    /**
     * Crash-point 3 (PubAck-öncesi broker-kesintisi): the real NATS container is Docker-paused
     * WHILE the relay attempts to publish -- the row must NOT be deleted while the broker is
     * unreachable (retry/backoff, {@code SYS_OUTBOX_RELAY_PUBLISH_FAILED}). Once the broker
     * resumes, the NEXT relay cycle completes custody-transfer normally.
     */
    @Test
    void pubAckBeforeCrash_brokerPausedDuringPublish_rowRetainedThenDeletedAfterBrokerResumes() throws Exception {
        UserOperationLogEntryEventEntity event = new UserOperationLogEntryEventEntity();
        event.setId(UUID.randomUUID().toString());
        event.setEventType("UserOperationLog");
        event.setProcessInstanceId("proc-broker-outage");
        event.setUserId("user-1");
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            new CompactHistoryOutboxWriter(dataSource, new PseudonymTokenGenerator(),
                    new HistoryClassificationProperties(), null)
                    .write(event, HistoryClassNames.OP_LOG, "camunda", connection);
            connection.commit();
        }

        HistoryOutboxRelay relay = newRelayAsLeader();

        natsContainer.getDockerClient().pauseContainerCmd(natsContainer.getContainerId()).exec();
        try {
            // publish() uses PublishOptions.DEFAULT_TIMEOUT (2s) -- bounded wait, no hang.
            relay.relayCycle();
        } finally {
            natsContainer.getDockerClient().unpauseContainerCmd(natsContainer.getContainerId()).exec();
        }

        // Broker was unreachable -- PubAck was never received, row MUST NOT have been deleted.
        assertThat(countOutboxRows()).isEqualTo(1);

        // Broker is back -- the next relay cycle (retry) completes custody-transfer.
        relay.relayCycle();

        assertThat(countOutboxRows()).isZero();
    }
}
