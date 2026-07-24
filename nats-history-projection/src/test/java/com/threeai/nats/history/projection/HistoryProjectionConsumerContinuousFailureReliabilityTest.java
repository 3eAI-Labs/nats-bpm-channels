package com.threeai.nats.history.projection;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.threeai.nats.core.db.SqlMigrationRunner;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.headers.BpmHeaders;
import com.threeai.nats.core.headers.DlqHeaders;
import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.core.history.HistoryHeaders;
import io.nats.client.Dispatcher;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Phase 5.5 (QA) reliability suite -- two invariants proven end-to-end with REAL JetStream
 * redelivery mechanics (not a mocked {@code deliveredCount()}, unlike {@code
 * HistoryProjectionConsumerErrorPathTest}/{@code HistoryProjectionConsumerDlqRoutingTest}):
 *
 * <ol>
 *   <li>DLQ-under-continuous-failure (BR-REL-005): a downstream that fails on EVERY attempt must
 *       exhaust the real delivery-count budget via GENUINE JetStream redeliveries (real {@code
 *       nakWithDelay} backoff, real consumer {@code maxDeliver}) before landing in the real DLQ
 *       stream — never silently dropped, never stuck retrying forever.</li>
 *   <li>Sustained ingest throughput + backpressure (BR-REL-002/006): a burst of real messages
 *       (including redelivered duplicates) drains under a single sequential partition consumer
 *       with zero loss and zero duplicate rows -- measured throughput is REPORTED, not asserted
 *       as a numeric SLA (environment-dependent).</li>
 * </ol>
 *
 * <p>Heavy (real Docker/NATS/PG, real backoff wall-clock waits, burst load) --
 * {@code @Tag("reliability")}, excluded from the default {@code mvn test} run. Run explicitly via
 * {@code mvn test -Dgroups=reliability -Dreliability.excludedGroups=}.
 */
@Tag("reliability")
@Testcontainers
class HistoryProjectionConsumerContinuousFailureReliabilityTest {

    private static PostgreSQLContainer<?> postgres;
    private static GenericContainer<?> natsContainer;
    private static PGSimpleDataSource dataSource;
    private static io.nats.client.Connection natsConnection;
    private static JetStream jetStream;

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
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/projection/V4__large_payload_content_addressing.sql");

        natsContainer = new GenericContainer<>("nats:2.10-alpine").withCommand("--jetstream").withExposedPorts(4222);
        natsContainer.start();
        natsConnection = Nats.connect("nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222));
        jetStream = natsConnection.jetStream();

        JetStreamManagement jsm = natsConnection.jetStreamManagement();
        jsm.addStream(StreamConfiguration.builder()
                .name("HISTORY_CONTINUOUS_FAILURE_TEST")
                .subjects("history.>")
                .retentionPolicy(RetentionPolicy.Limits)
                .storageType(StorageType.File)
                .build());
        jsm.addStream(StreamConfiguration.builder()
                .name("DLQ_HISTORY_CONTINUOUS_FAILURE_TEST")
                .subjects("dlq.history.>")
                .retentionPolicy(RetentionPolicy.Limits)
                .storageType(StorageType.File)
                .build());
    }

    @AfterAll
    static void tearDown() throws Exception {
        natsConnection.close();
        natsContainer.stop();
        postgres.stop();
    }

    private PGSimpleDataSource unreachableDataSource() {
        PGSimpleDataSource broken = new PGSimpleDataSource();
        broken.setUrl("jdbc:postgresql://127.0.0.1:1/nonexistent");
        broken.setUser("nobody");
        broken.setPassword("nobody");
        return broken;
    }

    private Headers historyHeaders(String engineId, String historyClass, String eventType, String eventId,
            String processInstanceId, Instant eventTime) {
        Headers headers = new Headers();
        headers.add(NatsJetStreamConstants.MSG_ID_HDR, eventId + ":" + eventType);
        headers.add(HistoryHeaders.ENGINE_ID, engineId);
        headers.add(HistoryHeaders.CLASS, historyClass);
        headers.add(HistoryHeaders.EVENT_TYPE, eventType);
        headers.add(HistoryHeaders.EVENT_ID, eventId);
        headers.add(HistoryHeaders.PROCESS_INSTANCE_ID, processInstanceId);
        headers.add(HistoryHeaders.EVENT_TIME, String.valueOf(eventTime.toEpochMilli()));
        headers.add(BpmHeaders.BUSINESS_KEY, "biz-1");
        return headers;
    }

    /**
     * A downstream (projection Postgres) that fails EVERY attempt, driven by REAL JetStream
     * redelivery (a push {@link Dispatcher} subscription with production-shaped {@code
     * maxDeliver}/{@code nakWithDelay} backoff, not a single mocked {@code onMessage()} call):
     * after the app-level delivery-count budget ({@code maxDeliver=4}) is exceeded by the 5th
     * genuine redelivery, the message must land in the real DLQ stream with the correct reason,
     * and the original HISTORY consumer must have zero pending redeliveries left (custody
     * transferred, not silently stuck).
     */
    @Test
    void continuousDownstreamFailure_realJetStreamRedelivery_exhaustsBudget_routesToRealDlq() throws Exception {
        ProjectionStore alwaysFailingStore = new ProjectionStore(unreachableDataSource());
        DlqPublisher dlqPublisher = new DlqPublisher(jetStream, natsConnection, null);
        HistoryDlqConsumer dlqConsumer = new HistoryDlqConsumer(dlqPublisher, null);
        int appMaxDeliver = 4;
        HistoryProjectionConsumer consumer =
                new HistoryProjectionConsumer(0, jetStream, alwaysFailingStore, dlqConsumer, null, appMaxDeliver);

        String processInstanceId = "proc-continuous-failure-" + UUID.randomUUID();
        String subject = "history.camunda.PROCINST." + processInstanceId;
        Headers headers = historyHeaders("camunda", HistoryClassNames.PROCINST, "start", processInstanceId,
                processInstanceId, Instant.now());
        jetStream.publish(NatsMessage.builder().subject(subject).headers(headers)
                .data("{\"businessKey\":\"biz-1\",\"state\":\"ACTIVE\"}".getBytes(StandardCharsets.UTF_8)).build());

        String durable = "continuous-failure-consumer-" + UUID.randomUUID();
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable(durable)
                .filterSubject(subject)
                .ackWait(Duration.ofSeconds(5))
                .maxDeliver(appMaxDeliver + 1) // production convention (HistoryProjectionConsumerBootstrap)
                .build();
        // NOTE: do NOT pre-create the consumer via jsm.addOrUpdateConsumer() here -- that would
        // provision it as a PULL consumer (no deliver-subject), and the subsequent PUSH subscribe
        // below would then fail with SUB-90009 "already configured as a pull consumer". Matches
        // production (HistoryProjectionConsumerBootstrap.registerPartitionConsumer): jetStream.
        // subscribe(..., PushSubscribeOptions) creates the durable AS a push consumer itself.
        Dispatcher dispatcher = natsConnection.createDispatcher();
        AtomicInteger deliveryAttempts = new AtomicInteger();
        try {
            jetStream.subscribe(subject, dispatcher, msg -> {
                deliveryAttempts.incrementAndGet();
                consumer.onMessage(msg);
            }, false, io.nats.client.PushSubscribeOptions.builder().configuration(cc).build());

            // Real backoff schedule (deliveryCount 1..4 -> 1s/2s/4s/8s nakWithDelay, then the 5th
            // real redelivery exceeds appMaxDeliver and routes to DLQ) -- bounded overall wait.
            Message dlqMsg = fetchOneWithRetry("DLQ_HISTORY_CONTINUOUS_FAILURE_TEST",
                    "dlq.history.camunda.PROCINST." + processInstanceId, Duration.ofSeconds(45));

            assertThat(dlqMsg).isNotNull();
            assertThat(dlqMsg.getHeaders().getLast(DlqHeaders.REASON))
                    .isEqualTo(com.threeai.nats.core.dlq.DlqReason.HISTORY_DELIVERY_BUDGET_EXCEEDED.headerValue());
            assertThat(dlqMsg.getHeaders().getLast(DlqHeaders.ORIGINAL_SUBJECT)).isEqualTo(subject);
            // Real JetStream actually redelivered multiple times (not a single mocked call).
            assertThat(deliveryAttempts.get()).isGreaterThanOrEqualTo(appMaxDeliver + 1);
        } finally {
            dispatcher.drain(Duration.ofSeconds(5));
        }

        // The ORIGINAL history consumer must have zero pending messages left -- the DLQ-routed
        // message was genuinely ACKed (custody transferred), not left stuck for infinite retry.
        JetStreamManagement jsmCheck = natsConnection.jetStreamManagement();
        long numPending = jsmCheck.getConsumerInfo("HISTORY_CONTINUOUS_FAILURE_TEST", durable).getNumPending();
        assertThat(numPending).isZero();
    }

    /**
     * Sustained ingest under a single sequential partition consumer (matches production's
     * one-{@code Dispatcher}-per-partition serialization -- {@code
     * HistoryProjectionConsumerBootstrap}): a burst of {@code uniqueEntityCount} distinct
     * PROCINST-create events must drain with zero loss and zero duplicate rows, and a genuine
     * REDELIVERY of a subset (the SAME real {@link Message} object -- same {@code
     * msg.metaData().streamSequence()}, exactly what a lost/delayed ack would cause JetStream to
     * redeliver -- re-fed directly into {@code onMessage()} a second time, in-process, AFTER the
     * first pass has fully drained) must silently no-op via {@code STALE_DISCARDED} rather than
     * duplicating the row.
     *
     * <p>CODER-NOTE (test-design finding, not a production bug): an EARLIER version of this test
     * tried to simulate redelivery by simply re-{@code publish()}-ing the identical wire message a
     * second time. JetStream's OWN stream-level {@code Nats-Msg-Id} de-duplication window silently
     * absorbed those republishes at the BROKER before they ever reached this consumer (good news --
     * the broker-level dedup this architecture relies on for {@code Nats-Msg-Id} genuinely works
     * end-to-end), so that approach could never reach {@code HistoryProjectionConsumer} at all and
     * always measured 0 "duplicates delivered". Replaying the SAME already-delivered {@link
     * Message} object instead exercises the intended layer: {@code ProjectionStore}'s
     * stream_sequence-based {@code STALE_DISCARDED} no-op.
     *
     * <p>Measured throughput is printed for the record, never asserted as a numeric SLA
     * (BR-OBS-003 discipline).
     */
    @Test
    void sustainedIngestBurst_thenRedeliveredDuplicates_singlePartitionConsumer_zeroLossZeroDuplicateRows() throws Exception {
        int uniqueEntityCount = 300;
        int redeliveryReplayCount = 40; // ~13% of the batch re-fed a second time -- redelivery simulation

        ProjectionStore store = new ProjectionStore(dataSource);
        DlqPublisher dlqPublisher = new DlqPublisher(jetStream, natsConnection, null);
        HistoryDlqConsumer dlqConsumer = new HistoryDlqConsumer(dlqPublisher, null);
        HistoryProjectionConsumer consumer = new HistoryProjectionConsumer(1, jetStream, store, dlqConsumer, null, 4);

        String subjectFilter = "history.camunda.PROCINST-BURST.*";
        String durable = "burst-consumer-" + UUID.randomUUID();
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable(durable)
                .filterSubject(subjectFilter)
                .ackWait(Duration.ofSeconds(30))
                .maxDeliver(5)
                .build();
        // See continuousDownstreamFailure_...'s NOTE above -- no jsm.addOrUpdateConsumer()
        // pre-creation; the push subscribe below provisions the durable itself.
        Dispatcher dispatcher = natsConnection.createDispatcher();
        AtomicInteger processed = new AtomicInteger();
        java.util.List<Message> delivered = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

        Instant startPublish = Instant.now();
        try {
            for (int i = 0; i < uniqueEntityCount; i++) {
                String processInstanceId = "burst-proc-" + i + "-" + UUID.randomUUID();
                String subject = "history.camunda.PROCINST-BURST." + processInstanceId;
                Headers headers = historyHeaders("camunda", HistoryClassNames.PROCINST, "start", processInstanceId,
                        processInstanceId, Instant.now());
                NatsMessage msg = NatsMessage.builder().subject(subject).headers(headers)
                        .data("{\"businessKey\":\"biz-1\",\"state\":\"ACTIVE\"}".getBytes(StandardCharsets.UTF_8)).build();
                jetStream.publish(msg);
            }

            jetStream.subscribe(subjectFilter, dispatcher, msg -> {
                consumer.onMessage(msg);
                delivered.add(msg);
                processed.incrementAndGet();
            }, false, io.nats.client.PushSubscribeOptions.builder().configuration(cc).build());

            Instant deadline = Instant.now().plus(Duration.ofSeconds(60));
            while (processed.get() < uniqueEntityCount && Instant.now().isBefore(deadline)) {
                Thread.sleep(50);
            }
        } finally {
            dispatcher.drain(Duration.ofSeconds(5));
        }
        Duration elapsedFirstPass = Duration.between(startPublish, Instant.now());

        assertThat(processed.get()).as("every published message must be delivered exactly once to the consumer's "
                + "onMessage -- none silently dropped by the broker").isEqualTo(uniqueEntityCount);
        assertThat(countDistinctBurstRows()).isEqualTo(uniqueEntityCount); // zero loss, zero duplicates after pass 1

        // Redelivery replay: the SAME real Message objects, fed a second time directly (models a
        // lost/delayed ack causing JetStream to redeliver -- SAME stream_sequence both times).
        for (int i = 0; i < redeliveryReplayCount; i++) {
            consumer.onMessage(delivered.get(i));
        }

        long finalRowCount = countDistinctBurstRows();
        int totalDeliveries = uniqueEntityCount + redeliveryReplayCount;
        double throughputPerSecond = uniqueEntityCount / Math.max(elapsedFirstPass.toMillis() / 1000.0, 0.001);
        System.out.println("HistoryProjectionConsumer sustained-ingest burst measured: uniqueEntities=" + uniqueEntityCount
                + " firstPassElapsed=" + elapsedFirstPass + " throughputMsgsPerSec=" + String.format("%.1f", throughputPerSecond)
                + " redeliveryReplayCount=" + redeliveryReplayCount + " totalDeliveries=" + totalDeliveries
                + " finalProjectionRowCount=" + finalRowCount);

        // The structural invariant (never a numeric SLA): redelivery of already-applied events is
        // a silent no-op -- zero loss, zero duplicate rows even after the replay.
        assertThat(finalRowCount).isEqualTo(uniqueEntityCount);
    }

    private long countDistinctBurstRows() throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "SELECT count(*) FROM process_instance_history WHERE process_instance_id LIKE 'burst-proc-%'");
             ResultSet rs = stmt.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private Message fetchOneWithRetry(String streamName, String subject, Duration overallTimeout) throws Exception {
        JetStreamManagement jsm = natsConnection.jetStreamManagement();
        String durable = "dlq-fetch-" + UUID.randomUUID();
        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(durable).filterSubject(subject).build();
        jsm.addOrUpdateConsumer(streamName, cc);
        JetStreamSubscription sub = jetStream.subscribe(subject, PullSubscribeOptions.bind(streamName, durable));
        Instant deadline = Instant.now().plus(overallTimeout);
        while (Instant.now().isBefore(deadline)) {
            java.util.List<Message> fetched = sub.fetch(1, Duration.ofSeconds(2));
            if (!fetched.isEmpty()) {
                return fetched.get(0);
            }
        }
        return null;
    }
}
