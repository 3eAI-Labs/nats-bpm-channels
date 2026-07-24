package com.threeai.nats.core.outbound;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.UUID;

import com.threeai.nats.core.db.SqlMigrationRunner;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.jetstream.SweepLeaderLease;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Phase 5.5 (QA) reliability suite -- BR-REL-001, `TEST_SPECIFICATIONS.md` (b) applied to
 * basamak-4's {@code OutboundMessageRelay} (D-F' custody-transfer): the SAME three crash-points
 * {@code HistoryOutboxCustodyTransferTest} proves for the history outbox/relay pair, mirrored here
 * for {@code outbound_message_outbox}/{@code OutboundMessageRelay} -- engine-neutral, so this
 * single class covers both camunda and cadenzaflow (neither fork type appears anywhere in this
 * path; {@link OutboundMessageRelayTest} already covers the same class's leader/dedup/metrics unit
 * behavior, this class adds the real Docker-pause broker-outage crash point it does not).
 *
 * <p>Heavy (real Docker/NATS/PG, Docker pause/unpause) -- {@code @Tag("reliability")}, excluded
 * from the default {@code mvn test} run. Run explicitly via
 * {@code mvn test -Dgroups=reliability -Dreliability.excludedGroups=}.
 */
@Tag("reliability")
@Testcontainers
class OutboundMessageRelayCustodyTransferTest {

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
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/outbound/V1__outbound_message_outbox.sql");

        natsContainer = new GenericContainer<>("nats:2.10-alpine").withCommand("--jetstream").withExposedPorts(4222);
        natsContainer.start();
        natsConnection = Nats.connect("nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222));
        jetStream = natsConnection.jetStream();

        JetStreamManagement jsm = natsConnection.jetStreamManagement();
        jsm.addStream(StreamConfiguration.builder()
                .name("EVENTS_CUSTODY_TEST")
                .subjects("events.>")
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
             PreparedStatement stmt = c.prepareStatement("DELETE FROM outbound_message_outbox")) {
            stmt.executeUpdate();
        }
    }

    private long countOutboxRows() throws Exception {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT count(*) FROM outbound_message_outbox");
             ResultSet rs = stmt.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private UUID insertOutboxRow(String processInstanceId) throws Exception {
        OutboundMessageDraft draft = new OutboundMessageDraft("camunda", "order.created", processInstanceId,
                "biz-1", "trace-1", "events.camunda.order.created." + processInstanceId,
                "{\"foo\":1}".getBytes(StandardCharsets.UTF_8));
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            UUID id = new OutboundMessageOutboxWriter(null).write(connection, draft);
            connection.commit();
            return id;
        }
    }

    private OutboundMessageRelay newRelayAsLeader() throws Exception {
        String bucket = "outbound-relay-leader-custody-" + UUID.randomUUID();
        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket(bucket, Duration.ofSeconds(60), 1, natsConnection);
        SweepLeaderLease leaderLease = new SweepLeaderLease(jetStream, kvManager, natsConnection,
                bucket, "relay-leader.", "camunda", "test-node", Duration.ofSeconds(60));
        assertThat(leaderLease.tryAcquireOrRenew()).isTrue();
        return new OutboundMessageRelay(dataSource, jetStream, leaderLease, new OutboundMessageOutboxProperties(), null, "camunda");
    }

    /**
     * Crash-point 1 (commit-öncesi): a tx-injected failure AFTER {@code
     * OutboundMessageOutboxWriter.write(...)} but BEFORE the engine's own {@code commit()} must
     * leave NO trace in {@code outbound_message_outbox} -- the outbox row shares the engine's
     * runtime transaction (D-A'), it cannot survive independently of it.
     */
    @Test
    void commitBeforeCrash_txRolledBack_outboxRowNeverPersists() throws Exception {
        OutboundMessageDraft draft = new OutboundMessageDraft("camunda", "order.created", "proc-commit-crash",
                "biz-1", "trace-1", "events.camunda.order.created.proc-commit-crash",
                "{\"foo\":1}".getBytes(StandardCharsets.UTF_8));

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            new OutboundMessageOutboxWriter(null).write(connection, draft);
            // Simulated crash BEFORE the engine's own runtime write commits -- the caller never
            // reaches commit().
            connection.rollback();
        }

        assertThat(countOutboxRows()).isZero();
    }

    /**
     * Crash-point 2 (publish-öncesi / relay node restart): the outbox row is durable and untouched
     * for as long as no relay instance is running against it; once a relay DOES start (post-
     * "restart"), custody-transfer completes normally (publish + PubAck + delete).
     */
    @Test
    void publishBeforeCrash_relayNotYetStarted_rowSurvivesThenRelayCompletesOnRestart() throws Exception {
        insertOutboxRow("proc-publish-crash");

        // No relay has run yet (simulated relay-node-down window) -- row must still be there.
        assertThat(countOutboxRows()).isEqualTo(1);

        // Relay "restarts" (a fresh OutboundMessageRelay instance, as would happen on node restart)
        // and completes custody-transfer.
        OutboundMessageRelay relay = newRelayAsLeader();
        relay.relayCycle();

        assertThat(countOutboxRows()).isZero();
    }

    /**
     * Crash-point 3 (PubAck-öncesi broker-kesintisi): the real NATS container is Docker-paused
     * WHILE the relay attempts to publish -- the row must NOT be deleted while the broker is
     * unreachable (retry/backoff, at-least-once). Once the broker resumes, the NEXT relay cycle
     * completes custody-transfer normally.
     */
    @Test
    void pubAckBeforeCrash_brokerPausedDuringPublish_rowRetainedThenDeletedAfterBrokerResumes() throws Exception {
        insertOutboxRow("proc-broker-outage");

        OutboundMessageRelay relay = newRelayAsLeader();

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
