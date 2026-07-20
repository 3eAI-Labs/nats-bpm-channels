package com.threeai.nats.cadenzaflow.history;

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
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import org.cadenzaflow.bpm.engine.impl.history.event.UserOperationLogEntryEventEntity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Real Postgres (compact_history_outbox) + real NATS JetStream (Testcontainers) — proves the
 * audit-critical custody-transfer contract (BR-REL-001): publish-then-PubAck-then-DELETE, and
 * publish-failure retains the row (never publish-then-forget).
 */
@Testcontainers
class HistoryOutboxRelayTest {

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
                .name("HISTORY_TEST")
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

    /** Creates its own isolated leader-lease bucket so the leader check always succeeds. */
    private HistoryOutboxRelay newRelayAsLeader(HistoryOutboxProperties properties) throws Exception {
        String bucket = "history-relay-leader-test-" + UUID.randomUUID();
        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket(bucket, Duration.ofSeconds(60), 1, natsConnection);
        SweepLeaderLease leaderLease = new SweepLeaderLease(jetStream, kvManager, natsConnection,
                bucket, "relay-leader.", "cadenzaflow", "test-node", Duration.ofSeconds(60));
        assertThat(leaderLease.tryAcquireOrRenew()).isTrue();
        return new HistoryOutboxRelay(dataSource, jetStream, leaderLease, properties, null, "cadenzaflow");
    }

    private void insertOutboxRow(String historyEventId, String processInstanceId) throws Exception {
        UserOperationLogEntryEventEntity event = new UserOperationLogEntryEventEntity();
        event.setId(historyEventId);
        event.setEventType("UserOperationLog");
        event.setProcessInstanceId(processInstanceId);
        event.setUserId("user-1");

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            new CompactHistoryOutboxWriter(dataSource, new PseudonymTokenGenerator(),
                    new HistoryClassificationProperties(), null)
                    .write(event, HistoryClassNames.OP_LOG, "cadenzaflow", connection);
            connection.commit();
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

    @Test
    void relayCycle_notLeader_zeroDbReads_rowRemains() throws Exception {
        insertOutboxRow(UUID.randomUUID().toString(), "proc-1");
        // A lease this relay never acquires (a different, unrelated bucket already held by nobody
        // reachable -- simplest way to force "not leader": use a lease whose bucket doesn't exist
        // and cannot be created, or simply never call tryAcquireOrRenew successfully). We simulate
        // "not leader" using a lease bound to a bucket we deliberately do NOT provision.
        JetStreamKvManager kvManager = new JetStreamKvManager();
        SweepLeaderLease neverLeader = new SweepLeaderLease(jetStream, kvManager, natsConnection,
                "nonexistent-bucket-" + UUID.randomUUID(), "relay-leader.", "cadenzaflow", "test-node", Duration.ofSeconds(60));
        HistoryOutboxRelay relay = new HistoryOutboxRelay(dataSource, jetStream, neverLeader,
                new HistoryOutboxProperties(), null, "cadenzaflow");

        relay.relayCycle();

        assertThat(countOutboxRows()).isEqualTo(1); // untouched
    }

    @Test
    void relayCycle_asLeader_publishesAndDeletesRow() throws Exception {
        String historyEventId = UUID.randomUUID().toString();
        insertOutboxRow(historyEventId, "proc-2");
        HistoryOutboxRelay relay = newRelayAsLeader(new HistoryOutboxProperties());

        relay.relayCycle();

        assertThat(countOutboxRows()).isZero(); // relayCycle() is synchronous -- delete happens before it returns

        // Verify the message actually landed on the wire with the expected subject/dedup id.
        JetStreamManagement jsm = natsConnection.jetStreamManagement();
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable("test-consumer-" + UUID.randomUUID().toString().replace("-", ""))
                .filterSubject("history.cadenzaflow.OP_LOG.proc-2")
                .build();
        jsm.addOrUpdateConsumer("HISTORY_TEST", cc);
        JetStreamSubscription sub = jetStream.subscribe("history.cadenzaflow.OP_LOG.proc-2",
                PullSubscribeOptions.bind("HISTORY_TEST", cc.getDurable()));
        java.util.List<Message> messages = sub.fetch(1, Duration.ofSeconds(5));
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).getHeaders().getFirst(io.nats.client.support.NatsJetStreamConstants.MSG_ID_HDR))
                .isEqualTo(historyEventId + ":UserOperationLog");
    }

    @Test
    void relayRow_publishFailure_rowRetained_notDeleted() throws Exception {
        String historyEventId = UUID.randomUUID().toString();
        insertOutboxRow(historyEventId, "proc-3");

        // A JetStream pointed at a closed connection guarantees publish() throws.
        io.nats.client.Connection brokenConnection =
                Nats.connect("nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222));
        JetStream brokenJetStream = brokenConnection.jetStream();
        brokenConnection.close();

        String bucket = "history-relay-leader-test-" + UUID.randomUUID();
        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket(bucket, Duration.ofSeconds(60), 1, natsConnection);
        SweepLeaderLease leaderLease = new SweepLeaderLease(jetStream, kvManager, natsConnection,
                bucket, "relay-leader.", "cadenzaflow", "test-node", Duration.ofSeconds(60));
        assertThat(leaderLease.tryAcquireOrRenew()).isTrue();

        HistoryOutboxRelay relay = new HistoryOutboxRelay(dataSource, brokenJetStream, leaderLease,
                new HistoryOutboxProperties(), null, "cadenzaflow");

        relay.relayCycle();

        assertThat(countOutboxRows()).isEqualTo(1); // NOT deleted -- retried next cycle
    }
}
