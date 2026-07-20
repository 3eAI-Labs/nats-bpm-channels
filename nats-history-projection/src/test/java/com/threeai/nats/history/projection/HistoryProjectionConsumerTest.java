package com.threeai.nats.history.projection;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.threeai.nats.core.db.SqlMigrationRunner;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.headers.BpmHeaders;
import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.core.history.HistoryHeaders;
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
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.NatsJetStreamConstants;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Real NATS JetStream (Testcontainers) + real Postgres — proves the full wire → projection-row
 * path, including merge-upsert (entity-lifecycle), dedup insert (append-only), and DLQ routing
 * on schema drift.
 */
@Testcontainers
class HistoryProjectionConsumerTest {

    private static PostgreSQLContainer<?> postgres;
    private static GenericContainer<?> natsContainer;
    private static PGSimpleDataSource dataSource;
    private static io.nats.client.Connection natsConnection;
    private static JetStream jetStream;
    private static ProjectionStore projectionStore;
    private static HistoryDlqConsumer dlqConsumer;

    @BeforeAll
    static void setUp() throws Exception {
        postgres = new PostgreSQLContainer<>("postgres:16");
        postgres.start();
        dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgres.getJdbcUrl());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/projection/V1__entity_lifecycle_tables.sql");
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/projection/V2__append_log_tables.sql");
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/projection/V3__control_plane_and_compliance.sql");
        // FINDING-001 (faz-5 review): a dated partition (rather than relying on the catch-all
        // _default) so the redelivery test below can prove the row's PARTITION ANCHOR is driven
        // by the engine's real event_time header, not the consumer's ingest-time clock.
        try (Connection c = dataSource.getConnection(); java.sql.Statement stmt = c.createStatement()) {
            stmt.execute("CREATE TABLE operation_log_history_2020_01 PARTITION OF operation_log_history "
                    + "FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')");
        }
        projectionStore = new ProjectionStore(dataSource);

        natsContainer = new GenericContainer<>("nats:2.10-alpine").withCommand("--jetstream").withExposedPorts(4222);
        natsContainer.start();
        natsConnection = Nats.connect("nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222));
        jetStream = natsConnection.jetStream();

        JetStreamManagement jsm = natsConnection.jetStreamManagement();
        jsm.addStream(StreamConfiguration.builder()
                .name("HISTORY_CONSUMER_TEST")
                .subjects("history.>")
                .retentionPolicy(RetentionPolicy.Limits)
                .storageType(StorageType.File)
                .build());
        jsm.addStream(StreamConfiguration.builder()
                .name("DLQ_HISTORY_CONSUMER_TEST")
                .subjects("dlq.history.>")
                .retentionPolicy(RetentionPolicy.Limits)
                .storageType(StorageType.File)
                .build());

        DlqPublisher dlqPublisher = new DlqPublisher(jetStream, natsConnection, null);
        dlqConsumer = new HistoryDlqConsumer(dlqPublisher, null);
    }

    @AfterAll
    static void tearDown() throws Exception {
        natsConnection.close();
        natsContainer.stop();
        postgres.stop();
    }

    private HistoryProjectionConsumer newConsumer() {
        return new HistoryProjectionConsumer(3, jetStream, projectionStore, dlqConsumer, null, 4);
    }

    private Message publishAndFetch(String subject, Headers headers, String jsonBody, String durable) throws Exception {
        NatsMessage msg = NatsMessage.builder().subject(subject).headers(headers)
                .data(jsonBody.getBytes(java.nio.charset.StandardCharsets.UTF_8)).build();
        jetStream.publish(msg);

        JetStreamManagement jsm = natsConnection.jetStreamManagement();
        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(durable).filterSubject(subject).build();
        jsm.addOrUpdateConsumer("HISTORY_CONSUMER_TEST", cc);
        JetStreamSubscription sub = jetStream.subscribe(subject, PullSubscribeOptions.bind("HISTORY_CONSUMER_TEST", durable));
        List<Message> fetched = sub.fetch(1, Duration.ofSeconds(5));
        assertThat(fetched).hasSize(1);
        return fetched.get(0);
    }

    private Headers historyHeaders(String engineId, String historyClass, String eventType, String eventId,
            String processInstanceId, String businessKey) {
        return historyHeaders(engineId, historyClass, eventType, eventId, processInstanceId, businessKey, Instant.now());
    }

    private Headers historyHeaders(String engineId, String historyClass, String eventType, String eventId,
            String processInstanceId, String businessKey, Instant eventTime) {
        Headers headers = new Headers();
        headers.add(NatsJetStreamConstants.MSG_ID_HDR, eventId + ":" + eventType);
        headers.add(HistoryHeaders.ENGINE_ID, engineId);
        headers.add(HistoryHeaders.CLASS, historyClass);
        headers.add(HistoryHeaders.EVENT_TYPE, eventType);
        headers.add(HistoryHeaders.EVENT_ID, eventId);
        headers.add(HistoryHeaders.PROCESS_INSTANCE_ID, processInstanceId);
        headers.add(HistoryHeaders.EVENT_TIME, String.valueOf(eventTime.toEpochMilli()));
        if (businessKey != null) {
            headers.add(BpmHeaders.BUSINESS_KEY, businessKey);
        }
        return headers;
    }

    @Test
    void onMessage_procinstEvent_upsertsProjectionRow_andAcks() throws Exception {
        // Fork-verified (DefaultHistoryEventProducer.initProcessInstanceEvent): for PROCINST
        // events, HistoryEvent.id IS the processInstanceId -- same value, not two independent ids.
        String processInstanceId = "proc-" + UUID.randomUUID();
        String eventId = processInstanceId;
        Headers headers = historyHeaders("camunda", HistoryClassNames.PROCINST, "start", eventId, processInstanceId, "biz-1");
        Message msg = publishAndFetch("history.camunda.PROCINST." + processInstanceId, headers,
                "{\"businessKey\":\"biz-1\",\"state\":\"ACTIVE\"}", "consumer-procinst-" + UUID.randomUUID());

        newConsumer().onMessage(msg);

        assertThat(stateOf(processInstanceId)).isEqualTo("ACTIVE");
    }

    @Test
    void onMessage_opLogEvent_insertsAppendOnlyRow() throws Exception {
        String eventId = UUID.randomUUID().toString();
        String processInstanceId = "proc-" + UUID.randomUUID();
        Headers headers = historyHeaders("camunda", HistoryClassNames.OP_LOG, "create", eventId, processInstanceId, null);
        Message msg = publishAndFetch("history.camunda.OP_LOG." + processInstanceId, headers,
                "{\"operationType\":\"Complete\",\"userId\":\"user-1\"}", "consumer-oplog-" + UUID.randomUUID());

        newConsumer().onMessage(msg);

        assertThat(countOpLogRows(eventId)).isEqualTo(1);
    }

    /**
     * FINDING-001 (faz-5 review, Levent kararı 2026-07-20): redelivery e2e on an audit-critical
     * append-log class (OP_LOG). Before this fix, {@code event_time} was {@code Instant.now()} at
     * CONSUME time — two deliveries of the SAME message would carry two DIFFERENT {@code
     * event_time} values, so the append-only dedup unique key ({@code engine_id,
     * history_event_id, event_type, event_time}) would NOT match and the ON CONFLICT would MISS,
     * producing a duplicate audit row. This test proves: (1) two deliveries of the identical
     * message -> exactly ONE row (dedup held), (2) the row's {@code event_time} equals the
     * ENGINE's real event time (a fixed historic instant carried on the wire header), never the
     * consume-time clock, and (3) that value correctly anchors the row into the matching
     * range-partition (not the {@code _default} catch-all).
     */
    @Test
    void onMessage_redeliveredOpLogEvent_dedupsToSingleRow_eventTimeIsEngineTimeAndCorrectPartition() throws Exception {
        Instant engineEventTime = Instant.parse("2020-01-15T10:00:00Z"); // matches operation_log_history_2020_01
        String eventId = UUID.randomUUID().toString();
        String processInstanceId = "proc-" + UUID.randomUUID();
        Headers headers = historyHeaders("camunda", HistoryClassNames.OP_LOG, "create", eventId, processInstanceId,
                null, engineEventTime);
        Message msg = publishAndFetch("history.camunda.OP_LOG." + processInstanceId, headers,
                "{\"operationType\":\"Complete\",\"userId\":\"user-1\"}", "consumer-redelivery-" + UUID.randomUUID());

        HistoryProjectionConsumer consumer = newConsumer();
        consumer.onMessage(msg); // 1st delivery attempt
        consumer.onMessage(msg); // 2nd delivery attempt (redelivery simulation, SAME underlying message)

        assertThat(countOpLogRows(eventId)).isEqualTo(1); // ON CONFLICT held -- no duplicate audit row
        assertThat(eventTimeOfOpLogRow(eventId)).isEqualTo(engineEventTime);
        assertThat(countOpLogRowsInPartition("operation_log_history_2020_01", eventId)).isEqualTo(1);
        assertThat(countOpLogRowsInPartition("operation_log_history_default", eventId)).isZero();
    }

    @Test
    void onMessage_missingRequiredHeader_routesToDlq() throws Exception {
        Headers headers = new Headers(); // missing everything -- schema drift
        headers.add(NatsJetStreamConstants.MSG_ID_HDR, "bad:evt");
        String processInstanceId = "proc-" + UUID.randomUUID();
        Message msg = publishAndFetch("history.camunda.OP_LOG." + processInstanceId, headers, "{}",
                "consumer-drift-" + UUID.randomUUID());

        newConsumer().onMessage(msg);

        // Verify DLQ stream received the message.
        JetStreamManagement jsm = natsConnection.jetStreamManagement();
        String dlqDurable = "dlq-check-" + UUID.randomUUID();
        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(dlqDurable)
                .filterSubject("dlq.history.camunda.OP_LOG." + processInstanceId).build();
        jsm.addOrUpdateConsumer("DLQ_HISTORY_CONSUMER_TEST", cc);
        JetStreamSubscription dlqSub = jetStream.subscribe("dlq.history.camunda.OP_LOG." + processInstanceId,
                PullSubscribeOptions.bind("DLQ_HISTORY_CONSUMER_TEST", dlqDurable));
        List<Message> dlqMessages = dlqSub.fetch(1, Duration.ofSeconds(5));

        assertThat(dlqMessages).hasSize(1);
    }

    private String stateOf(String processInstanceId) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "SELECT state FROM process_instance_history WHERE process_instance_id = ?")) {
            stmt.setString(1, processInstanceId);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private long countOpLogRows(String eventId) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "SELECT count(*) FROM operation_log_history WHERE history_event_id = ?")) {
            stmt.setString(1, eventId);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Instant eventTimeOfOpLogRow(String eventId) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "SELECT event_time FROM operation_log_history WHERE history_event_id = ?")) {
            stmt.setString(1, eventId);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getTimestamp(1).toInstant();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Queries the NAMED partition table directly (not the parent) to prove partition-anchor placement. */
    private long countOpLogRowsInPartition(String partitionTableName, String eventId) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "SELECT count(*) FROM " + partitionTableName + " WHERE history_event_id = ?")) {
            stmt.setString(1, eventId);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
