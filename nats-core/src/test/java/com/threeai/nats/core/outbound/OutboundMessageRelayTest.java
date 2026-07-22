package com.threeai.nats.core.outbound;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import com.threeai.nats.core.db.SqlMigrationRunner;
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
import io.nats.client.support.NatsJetStreamConstants;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Real Postgres ({@code outbound_message_outbox}) + real NATS JetStream (Testcontainers) — proves
 * the critical-path custody-transfer contract (D-F'): publish-then-PubAck-then-DELETE, and
 * publish-failure retains the row (never publish-then-forget). Direct mirror of basamak-2's
 * {@code HistoryOutboxRelayTest}.
 */
@Testcontainers
class OutboundMessageRelayTest {

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
                .name("EVENTS_TEST")
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

    private OutboundMessageRelay newRelayAsLeader(OutboundMessageOutboxProperties properties) throws Exception {
        String bucket = "outbound-relay-leader-test-" + UUID.randomUUID();
        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket(bucket, Duration.ofSeconds(60), 1, natsConnection);
        SweepLeaderLease leaderLease = new SweepLeaderLease(jetStream, kvManager, natsConnection,
                bucket, "relay-leader.", "camunda", "test-node", Duration.ofSeconds(60));
        assertThat(leaderLease.tryAcquireOrRenew()).isTrue();
        return new OutboundMessageRelay(dataSource, jetStream, leaderLease, properties, null, "camunda");
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

    private long countOutboxRows() throws Exception {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT count(*) FROM outbound_message_outbox");
             ResultSet rs = stmt.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    void relayCycle_notLeader_zeroDbReads_rowRemains() throws Exception {
        insertOutboxRow("proc-1");
        JetStreamKvManager kvManager = new JetStreamKvManager();
        SweepLeaderLease neverLeader = new SweepLeaderLease(jetStream, kvManager, natsConnection,
                "nonexistent-bucket-" + UUID.randomUUID(), "relay-leader.", "camunda", "test-node", Duration.ofSeconds(60));
        OutboundMessageRelay relay = new OutboundMessageRelay(dataSource, jetStream, neverLeader,
                new OutboundMessageOutboxProperties(), null, "camunda");

        relay.relayCycle();

        assertThat(countOutboxRows()).isEqualTo(1); // untouched
    }

    @Test
    void relayCycle_asLeader_publishesAndDeletesRow() throws Exception {
        UUID id = insertOutboxRow("proc-2");
        OutboundMessageRelay relay = newRelayAsLeader(new OutboundMessageOutboxProperties());

        relay.relayCycle();

        assertThat(countOutboxRows()).isZero();

        JetStreamManagement jsm = natsConnection.jetStreamManagement();
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable("test-consumer-" + UUID.randomUUID().toString().replace("-", ""))
                .filterSubject("events.camunda.order.created.proc-2")
                .build();
        jsm.addOrUpdateConsumer("EVENTS_TEST", cc);
        JetStreamSubscription sub = jetStream.subscribe("events.camunda.order.created.proc-2",
                PullSubscribeOptions.bind("EVENTS_TEST", cc.getDurable()));
        List<Message> messages = sub.fetch(1, Duration.ofSeconds(5));
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).getHeaders().getFirst(NatsJetStreamConstants.MSG_ID_HDR)).isEqualTo(id.toString());
        assertThat(new String(messages.get(0).getData(), StandardCharsets.UTF_8)).isEqualTo("{\"foo\":1}");
    }

    @Test
    void relayRow_publishFailure_rowRetained_notDeleted() throws Exception {
        insertOutboxRow("proc-3");

        io.nats.client.Connection brokenConnection =
                Nats.connect("nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222));
        JetStream brokenJetStream = brokenConnection.jetStream();
        brokenConnection.close();

        String bucket = "outbound-relay-leader-test-" + UUID.randomUUID();
        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket(bucket, Duration.ofSeconds(60), 1, natsConnection);
        SweepLeaderLease leaderLease = new SweepLeaderLease(jetStream, kvManager, natsConnection,
                bucket, "relay-leader.", "camunda", "test-node", Duration.ofSeconds(60));
        assertThat(leaderLease.tryAcquireOrRenew()).isTrue();

        OutboundMessageRelay relay = new OutboundMessageRelay(dataSource, brokenJetStream, leaderLease,
                new OutboundMessageOutboxProperties(), null, "camunda");

        relay.relayCycle();

        assertThat(countOutboxRows()).isEqualTo(1); // NOT deleted -- retried next cycle
    }

    @Test
    void relayCycle_multipleEngines_filtersByEngineId() throws Exception {
        insertOutboxRow("proc-multi-1");
        // Insert a row for a DIFFERENT engine id directly (writer always uses "camunda" via draft engineId field).
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            OutboundMessageDraft cadenzaflowDraft = new OutboundMessageDraft("cadenzaflow", "order.created",
                    "proc-multi-2", "biz-2", "trace-2", "events.cadenzaflow.order.created.proc-multi-2",
                    "{}".getBytes(StandardCharsets.UTF_8));
            new OutboundMessageOutboxWriter(null).write(connection, cadenzaflowDraft);
            connection.commit();
        }

        OutboundMessageRelay relay = newRelayAsLeader(new OutboundMessageOutboxProperties());
        relay.relayCycle();

        // Only the "camunda" row was relayed -- the "cadenzaflow" row is untouched by this relay instance.
        assertThat(countOutboxRows()).isEqualTo(1);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "SELECT engine_id FROM outbound_message_outbox")) {
            try (ResultSet rs = stmt.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("engine_id")).isEqualTo("cadenzaflow");
            }
        }
    }
}
