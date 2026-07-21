package com.threeai.nats.cadenzaflow.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import com.threeai.nats.core.db.SqlMigrationRunner;
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
import io.nats.client.support.NatsJetStreamConstants;
import org.cadenzaflow.bpm.engine.HistoryService;
import org.cadenzaflow.bpm.engine.ProcessEngine;
import org.cadenzaflow.bpm.engine.ProcessEngineConfiguration;
import org.cadenzaflow.bpm.engine.RepositoryService;
import org.cadenzaflow.bpm.engine.RuntimeService;
import org.cadenzaflow.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.cadenzaflow.bpm.engine.impl.history.handler.DbHistoryEventHandler;
import org.cadenzaflow.bpm.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * End-to-end audit-critical slice: real embedded Camunda engine (Testcontainers Postgres — the
 * SAME physical database as {@code compact_history_outbox}, matching ADR-0010's tx-atomicity
 * requirement) + real NATS JetStream (Testcontainers). Proves: (1) {@code createIncident} writes
 * exactly one {@code compact_history_outbox} row in the SAME transaction, (2) dual-run continues
 * writing {@code ACT_HI_INCIDENT} too (class not cut over), (3) {@code HistoryOutboxRelay} then
 * relays that row onto {@code history.cadenzaflow.INCIDENT.<processInstanceId>} and deletes it after
 * PubAck (BR-REL-001).
 */
@Testcontainers
class HistoryOffloadAuditCriticalFlowIntegrationTest {

    private static PostgreSQLContainer<?> postgres;
    private static GenericContainer<?> natsContainer;
    private static PGSimpleDataSource dataSource;
    private static io.nats.client.Connection natsConnection;
    private static JetStream jetStream;
    private static ProcessEngine engine;
    private static NatsHistoryEventHandler handler;
    private static HistoryOutboxRelay outboxRelay;

    @BeforeAll
    static void setUp() throws Exception {
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
        natsConnection.jetStreamManagement().addStream(StreamConfiguration.builder()
                .name("HISTORY_E2E_TEST")
                .subjects("history.>")
                .retentionPolicy(RetentionPolicy.Limits)
                .storageType(StorageType.File)
                .build());

        HistoryClassificationProperties classification = new HistoryClassificationProperties();
        HistoryOutboxProperties outboxProperties = new HistoryOutboxProperties();
        CompactHistoryOutboxWriter outboxWriter = new CompactHistoryOutboxWriter(
                dataSource, new PseudonymTokenGenerator(), classification, null);
        HistoryPostCommitPublisher postCommitPublisher = new HistoryPostCommitPublisher(jetStream, null);

        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket("history-cutover-state", Duration.ZERO, 1, natsConnection);
        ClassCutoverStateRegistry cutoverRegistry = new ClassCutoverStateRegistry(kvManager, natsConnection, "cadenzaflow");
        cutoverRegistry.loadAtBootstrap(); // no KV entries -> every class DUAL_RUN (not cut over)

        handler = new NatsHistoryEventHandler(cutoverRegistry, classification, outboxWriter, postCommitPublisher,
                new DbHistoryEventHandler(), "cadenzaflow");

        String leaderBucket = "history-relay-leader-e2e-" + UUID.randomUUID();
        kvManager.ensureBucket(leaderBucket, Duration.ofSeconds(60), 1, natsConnection);
        SweepLeaderLease leaderLease = new SweepLeaderLease(jetStream, kvManager, natsConnection,
                leaderBucket, "relay-leader.", "cadenzaflow", "test-node", Duration.ofSeconds(60));
        assertThat(leaderLease.tryAcquireOrRenew()).isTrue();
        outboxRelay = new HistoryOutboxRelay(dataSource, jetStream, leaderLease, outboxProperties, null, "cadenzaflow");

        ProcessEngineConfigurationImpl config = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneProcessEngineConfiguration();
        config.setDataSource(dataSource);
        config.setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE);
        config.setJobExecutorActivate(false);
        config.setHistory(ProcessEngineConfiguration.HISTORY_FULL);
        config.setCustomHistoryEventHandlers(new java.util.ArrayList<>(List.of(handler)));
        config.setEnableDefaultDbHistoryEventHandler(false);
        engine = config.buildProcessEngine();

        deployProcess(engine.getRepositoryService());
    }

    private static void deployProcess(RepositoryService repositoryService) {
        repositoryService.createDeployment()
                .addString("audit-critical-flow.bpmn20.xml", """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                     xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                     targetNamespace="http://threeai.com/history-offload-test">
                          <process id="auditCriticalFlowProcess" isExecutable="true" camunda:historyTimeToLive="180">
                            <startEvent id="start" />
                            <sequenceFlow id="flow1" sourceRef="start" targetRef="waitTask" />
                            <userTask id="waitTask" name="Wait" />
                            <sequenceFlow id="flow2" sourceRef="waitTask" targetRef="end" />
                            <endEvent id="end" />
                          </process>
                        </definitions>
                        """)
                .deploy();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (engine != null) {
            engine.close();
        }
        natsConnection.close();
        natsContainer.stop();
        postgres.stop();
    }

    @Test
    void createIncident_writesOutboxRowAndActHiRow_thenRelayPublishesAndDeletes() throws Exception {
        RuntimeService runtimeService = engine.getRuntimeService();
        HistoryService historyService = engine.getHistoryService();

        ProcessInstance instance = runtimeService.startProcessInstanceByKey("auditCriticalFlowProcess");
        String processInstanceId = instance.getProcessInstanceId();

        runtimeService.createIncident("customIncident", processInstanceId, "config-1", "boom");

        // (1) compact_history_outbox got exactly one INCIDENT row for this process instance.
        long outboxRowCount = countOutboxRows(processInstanceId);
        assertThat(outboxRowCount).isEqualTo(1);

        // (2) Dual-run: ACT_HI_INCIDENT was ALSO written (class not cut over).
        long historicIncidentCount = historyService.createHistoricIncidentQuery()
                .processInstanceId(processInstanceId).count();
        assertThat(historicIncidentCount).isEqualTo(1);

        // (3) Relay picks the row up, publishes, deletes.
        outboxRelay.relayCycle();
        assertThat(countOutboxRows(processInstanceId)).isZero();

        // (4) The wire message landed on the expected subject with the correct dedup header.
        JetStreamManagement jsm = natsConnection.jetStreamManagement();
        String subject = "history.cadenzaflow.INCIDENT." + processInstanceId;
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable("test-consumer-" + UUID.randomUUID().toString().replace("-", ""))
                .filterSubject(subject)
                .build();
        jsm.addOrUpdateConsumer("HISTORY_E2E_TEST", cc);
        JetStreamSubscription sub = jetStream.subscribe(subject, PullSubscribeOptions.bind("HISTORY_E2E_TEST", cc.getDurable()));
        List<Message> messages = sub.fetch(1, Duration.ofSeconds(5));

        assertThat(messages).hasSize(1);
        Message msg = messages.get(0);
        assertThat(msg.getHeaders().getFirst(NatsJetStreamConstants.MSG_ID_HDR)).isNotBlank();
        assertThat(new String(msg.getData(), java.nio.charset.StandardCharsets.UTF_8)).contains("\"incidentMessage\":\"boom\"");
    }

    private static long countOutboxRows(String processInstanceId) throws Exception {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "SELECT count(*) FROM compact_history_outbox WHERE process_instance_id = ? AND history_class = 'INCIDENT'")) {
            stmt.setString(1, processInstanceId);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }
}
