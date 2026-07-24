package com.threeai.nats.core.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

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
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.KeyValue;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
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
        return newRelayAsLeader(properties, (NatsChannelMetrics) null);
    }

    private OutboundMessageRelay newRelayAsLeader(OutboundMessageOutboxProperties properties, NatsChannelMetrics metrics)
            throws Exception {
        String bucket = "outbound-relay-leader-test-" + UUID.randomUUID();
        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket(bucket, Duration.ofSeconds(60), 1, natsConnection);
        SweepLeaderLease leaderLease = new SweepLeaderLease(jetStream, kvManager, natsConnection,
                bucket, "relay-leader.", "camunda", "test-node", Duration.ofSeconds(60));
        assertThat(leaderLease.tryAcquireOrRenew()).isTrue();
        return new OutboundMessageRelay(dataSource, jetStream, leaderLease, properties, metrics, "camunda");
    }

    /** Same as {@link #newRelayAsLeader(OutboundMessageOutboxProperties)} but against a caller-supplied
     *  (possibly broken) engine {@link javax.sql.DataSource} instead of the real Testcontainers pool. */
    private OutboundMessageRelay newRelayAsLeader(OutboundMessageOutboxProperties properties,
            javax.sql.DataSource engineDataSource) throws Exception {
        String bucket = "outbound-relay-leader-test-" + UUID.randomUUID();
        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket(bucket, Duration.ofSeconds(60), 1, natsConnection);
        SweepLeaderLease leaderLease = new SweepLeaderLease(jetStream, kvManager, natsConnection,
                bucket, "relay-leader.", "camunda", "test-node", Duration.ofSeconds(60));
        assertThat(leaderLease.tryAcquireOrRenew()).isTrue();
        return new OutboundMessageRelay(engineDataSource, jetStream, leaderLease, properties, null, "camunda");
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

    /** Inserts a row with a CALLER-CHOSEN id — needed to stage a specific {@code Nats-Msg-Id}
     *  collision for the JetStream-redelivery-dedup test. */
    private void insertOutboxRowWithId(UUID id, String processInstanceId, String subject) throws Exception {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "INSERT INTO outbound_message_outbox "
                   + "(id, engine_id, message_type, process_instance_id, business_key, trace_id, subject, payload) "
                   + "VALUES (?,?,?,?,?,?,?,?)")) {
            stmt.setObject(1, id);
            stmt.setString(2, "camunda");
            stmt.setString(3, "order.created");
            stmt.setString(4, processInstanceId);
            stmt.setString(5, "biz-1");
            stmt.setString(6, "trace-1");
            stmt.setString(7, subject);
            stmt.setBytes(8, "{\"foo\":1}".getBytes(StandardCharsets.UTF_8));
            stmt.executeUpdate();
        }
    }

    private PGSimpleDataSource unreachableDataSource() {
        PGSimpleDataSource broken = new PGSimpleDataSource();
        broken.setUrl("jdbc:postgresql://127.0.0.1:1/nonexistent");
        broken.setUser("nobody");
        broken.setPassword("nobody");
        return broken;
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
    void relayRow_publishFailure_withMetrics_incrementsFailedCounter() throws Exception {
        insertOutboxRow("proc-3-metrics");

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

        NatsChannelMetrics metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        OutboundMessageRelay relay = new OutboundMessageRelay(dataSource, brokenJetStream, leaderLease,
                new OutboundMessageOutboxProperties(), metrics, "camunda");

        relay.relayCycle();

        assertThat(countOutboxRows()).isEqualTo(1);
        assertThat(metrics.outboundOutboxRelayedCount("order.created", "failed").count()).isEqualTo(1.0);
    }

    /**
     * A relay crash AFTER a successful PubAck but BEFORE the row's DELETE (real basamak-4 failure
     * mode) leaves the row in the outbox; the NEXT relayCycle() re-publishes it with the SAME
     * dedup id (the row's own UUID, per {@code buildMessage}'s CODER-NOTE) — JetStream correctly
     * reports the redelivery as a duplicate. {@code relayRow} must still treat this as a SUCCESS
     * (delete the row, count it "published"), never retry it forever.
     */
    @Test
    void relayRow_redeliveredDuplicate_stillDeletesRow_countsAsPublished() throws Exception {
        UUID rowId = UUID.randomUUID();
        String subject = "events.camunda.order.created.proc-dup-1";
        insertOutboxRowWithId(rowId, "proc-dup-1", subject);

        // Simulate the earlier (crashed-before-delete) successful publish attempt using the SAME
        // Nats-Msg-Id the relay will derive from the row's own id.
        Headers headers = new Headers();
        headers.add(NatsJetStreamConstants.MSG_ID_HDR, rowId.toString());
        jetStream.publish(NatsMessage.builder().subject(subject).headers(headers)
                .data("{\"foo\":1}".getBytes(StandardCharsets.UTF_8)).build());

        NatsChannelMetrics metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        OutboundMessageRelay relay = newRelayAsLeader(new OutboundMessageOutboxProperties(), metrics);

        relay.relayCycle();

        assertThat(countOutboxRows()).isZero(); // treated as successfully relayed, not retried forever
        assertThat(metrics.outboundOutboxRelayedCount("order.created", "published").count()).isEqualTo(1.0);
    }

    @Test
    void relayCycle_leadershipStolenBetweenCycles_logsWarningAndSkipsWithoutTouchingDb() throws Exception {
        insertOutboxRow("proc-lease-steal");

        String bucket = "outbound-relay-leader-test-" + UUID.randomUUID();
        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket(bucket, Duration.ofSeconds(60), 1, natsConnection);
        SweepLeaderLease leaderLease = new SweepLeaderLease(jetStream, kvManager, natsConnection,
                bucket, "relay-leader.", "camunda", "test-node", Duration.ofSeconds(60));
        OutboundMessageRelay relay = new OutboundMessageRelay(dataSource, jetStream, leaderLease,
                new OutboundMessageOutboxProperties(), null, "camunda");

        relay.relayCycle(); // first cycle: acquires leadership, relays+deletes the row
        assertThat(leaderLease.isLeader()).isTrue();
        assertThat(countOutboxRows()).isZero();

        insertOutboxRow("proc-lease-steal-2");
        // Simulate another node stealing the lease key directly (bypassing the lease API) --
        // relayCycle()'s "wasLeader was true, tryAcquireOrRenew now fails" branch is otherwise
        // only reachable by waiting out a real TTL expiry.
        KeyValue kv = natsConnection.keyValue(bucket);
        kv.put(leaderLease.getKey(), "another-node".getBytes(StandardCharsets.UTF_8));

        relay.relayCycle();

        assertThat(leaderLease.isLeader()).isFalse();
        assertThat(countOutboxRows()).isEqualTo(1); // second row untouched -- zero DB reads once leadership is lost
    }

    @Test
    void relayCycle_engineDataSourceUnreachable_doesNotThrow_skipsCycle() throws Exception {
        OutboundMessageRelay relay = newRelayAsLeader(new OutboundMessageOutboxProperties(), unreachableDataSource());

        assertThatCode(relay::relayCycle).doesNotThrowAnyException();
    }

    /**
     * The oldest-row-age gauge supplier ({@code oldestRowAgeSecondsSafe}, registered against
     * {@code metrics} in the constructor) is invoked by Micrometer independently of {@code
     * relayCycle()} — a scrape can happen while the engine DB is unreachable. Its own SQLException
     * catch (distinct from {@code fetchOldestFirst}'s) must return a safe {@code 0.0}, not throw
     * and break the metrics scrape.
     */
    @Test
    void oldestRowAgeGauge_engineDataSourceUnreachable_returnsZero_doesNotThrow() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        NatsChannelMetrics metrics = new NatsChannelMetrics(registry);
        String bucket = "outbound-relay-leader-test-" + UUID.randomUUID();
        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket(bucket, Duration.ofSeconds(60), 1, natsConnection);
        SweepLeaderLease leaderLease = new SweepLeaderLease(jetStream, kvManager, natsConnection,
                bucket, "relay-leader.", "camunda", "test-node", Duration.ofSeconds(60));
        new OutboundMessageRelay(unreachableDataSource(), jetStream, leaderLease,
                new OutboundMessageOutboxProperties(), metrics, "camunda");

        io.micrometer.core.instrument.Gauge gauge = registry.find("nats.outbound.outbox.oldest_row_age_seconds")
                .tag("engine_id", "camunda").gauge();

        assertThat(gauge).isNotNull();
        assertThatCode(() -> assertThat(gauge.value()).isEqualTo(0.0)).doesNotThrowAnyException();
    }

    @Test
    void checkStuckRows_ageExceedsThreshold_doesNotThrow_rowStillPresentPastThreshold() throws Exception {
        insertOutboxRow("proc-stuck");

        io.nats.client.Connection brokenConnection =
                Nats.connect("nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222));
        JetStream brokenJetStream = brokenConnection.jetStream();
        brokenConnection.close(); // every publish on this JetStream handle fails -> row is retained

        OutboundMessageOutboxProperties properties = new OutboundMessageOutboxProperties();
        properties.setRelayCyclePeriodSeconds(1);
        properties.setStuckThresholdMultiplier(1); // threshold = 1s

        String bucket = "outbound-relay-leader-test-" + UUID.randomUUID();
        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket(bucket, Duration.ofSeconds(60), 1, natsConnection);
        SweepLeaderLease leaderLease = new SweepLeaderLease(jetStream, kvManager, natsConnection,
                bucket, "relay-leader.", "camunda", "test-node", Duration.ofSeconds(60));
        assertThat(leaderLease.tryAcquireOrRenew()).isTrue();
        OutboundMessageRelay relay = new OutboundMessageRelay(dataSource, brokenJetStream, leaderLease,
                properties, null, "camunda");

        Thread.sleep(1500); // row is now older than the 1s stuck threshold

        assertThatCode(relay::relayCycle).doesNotThrowAnyException();
        assertThat(countOutboxRows()).isEqualTo(1); // still stuck -- checkStuckRows only alarms, never drops rows
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
