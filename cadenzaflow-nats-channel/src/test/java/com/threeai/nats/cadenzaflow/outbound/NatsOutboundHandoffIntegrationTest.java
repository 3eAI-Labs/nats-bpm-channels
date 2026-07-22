package com.threeai.nats.cadenzaflow.outbound;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.threeai.nats.core.db.SqlMigrationRunner;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.jetstream.SweepLeaderLease;
import com.threeai.nats.core.outbound.OutboundClassificationProperties;
import com.threeai.nats.core.outbound.OutboundMessageOutboxProperties;
import com.threeai.nats.core.outbound.OutboundMessageOutboxWriter;
import com.threeai.nats.core.outbound.OutboundMessageRelay;
import com.threeai.nats.core.outbound.OutboundPostCommitPublisher;
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
import org.cadenzaflow.bpm.engine.ProcessEngine;
import org.cadenzaflow.bpm.engine.ProcessEngineConfiguration;
import org.cadenzaflow.bpm.engine.RepositoryService;
import org.cadenzaflow.bpm.engine.RuntimeService;
import org.cadenzaflow.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * End-to-end outbound-handoff slice (docs/09-outbound-handoff.md D-A'..D-F'): real embedded
 * CadenzaFlow engine (Testcontainers Postgres) + real NATS JetStream (Testcontainers). The tenant BPMN
 * attaches {@code NatsOutboundPublisher} exactly the way D-A' specifies — an {@code
 * executionListener event="end"} with {@code delegateExpression="${natsOutboundPublisher}"} —
 * resolved via {@code ProcessEngineConfigurationImpl.setBeans(...)} (the non-Spring equivalent of
 * a Spring bean registry, avoiding a full Spring Boot test context for this slice).
 *
 * <p>Proves: (1) a CRITICAL-classified message-throw event writes exactly one {@code
 * outbound_message_outbox} row in the SAME transaction as the process instance advancing past it,
 * and the relay then publishes+deletes it; (2) a BEST_EFFORT-classified send-task publishes
 * directly post-commit with ZERO {@code outbound_message_outbox} rows ever written.
 */
@Testcontainers
class NatsOutboundHandoffIntegrationTest {

    private static PostgreSQLContainer<?> postgres;
    private static GenericContainer<?> natsContainer;
    private static PGSimpleDataSource dataSource;
    private static io.nats.client.Connection natsConnection;
    private static JetStream jetStream;
    private static ProcessEngine engine;
    private static RepositoryService repositoryService;
    private static RuntimeService runtimeService;

    @BeforeAll
    static void setUp() throws Exception {
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
        natsConnection.jetStreamManagement().addStream(StreamConfiguration.builder()
                .name("OUTBOUND_E2E_TEST")
                .subjects("events.>")
                .retentionPolicy(RetentionPolicy.Limits)
                .storageType(StorageType.File)
                .build());

        OutboundClassificationProperties classification = new OutboundClassificationProperties();
        classification.setCriticalTypes(Set.of("payment_requested"));
        OutboundMessageOutboxWriter outboxWriter = new OutboundMessageOutboxWriter(null);
        OutboundPostCommitPublisher postCommitPublisher = new OutboundPostCommitPublisher(jetStream, null);
        NatsOutboundPublisher publisher = new NatsOutboundPublisher(classification, outboxWriter, postCommitPublisher, "cadenzaflow");

        ProcessEngineConfigurationImpl config = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneProcessEngineConfiguration();
        config.setDataSource(dataSource);
        config.setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE);
        config.setJobExecutorActivate(false);
        // D-A': tenant resolves the listener bean via delegateExpression="${natsOutboundPublisher}" --
        // setBeans(...) is the non-Spring equivalent of a Spring ApplicationContext bean registry.
        // "noopDelegate" drives the send-task's own (non-listener) activity behavior so it completes
        // SYNCHRONOUSLY -- camunda:type="external" would instead park it in a wait state until a
        // worker polls it, which is not what this test is exercising (D-B' attachment point only).
        config.setBeans(Map.of("natsOutboundPublisher", publisher, "noopDelegate",
                (org.cadenzaflow.bpm.engine.delegate.JavaDelegate) execution -> { }));
        engine = config.buildProcessEngine();
        repositoryService = engine.getRepositoryService();
        runtimeService = engine.getRuntimeService();

        deployProcess();
    }

    private static void deployProcess() {
        repositoryService.createDeployment()
                .addString("outbound-handoff-test.bpmn20.xml", """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                     xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                     targetNamespace="http://threeai.com/outbound-handoff-test">
                          <message id="paymentRequestedMessage" name="payment_requested" />
                          <message id="orderCreatedMessage" name="order_created" />

                          <process id="criticalOutboundProcess" isExecutable="true" camunda:historyTimeToLive="180">
                            <startEvent id="start" />
                            <sequenceFlow id="flow1" sourceRef="start" targetRef="throwPayment" />
                            <intermediateThrowEvent id="throwPayment">
                              <extensionElements>
                                <camunda:executionListener event="end" delegateExpression="${natsOutboundPublisher}" />
                              </extensionElements>
                              <messageEventDefinition messageRef="paymentRequestedMessage" />
                            </intermediateThrowEvent>
                            <sequenceFlow id="flow2" sourceRef="throwPayment" targetRef="end" />
                            <endEvent id="end" />
                          </process>

                          <process id="bestEffortOutboundProcess" isExecutable="true" camunda:historyTimeToLive="180">
                            <startEvent id="start2" />
                            <sequenceFlow id="flow3" sourceRef="start2" targetRef="sendOrder" />
                            <sendTask id="sendOrder" camunda:delegateExpression="${noopDelegate}" messageRef="orderCreatedMessage">
                              <extensionElements>
                                <camunda:executionListener event="end" delegateExpression="${natsOutboundPublisher}" />
                              </extensionElements>
                            </sendTask>
                            <sequenceFlow id="flow4" sourceRef="sendOrder" targetRef="end2" />
                            <endEvent id="end2" />
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

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement("DELETE FROM outbound_message_outbox")) {
            stmt.executeUpdate();
        }
    }

    @Test
    void criticalMessageThrow_writesOutboxRow_thenRelayPublishesAndDeletes() throws Exception {
        var instance = runtimeService.startProcessInstanceByKey("criticalOutboundProcess");
        String processInstanceId = instance.getProcessInstanceId();

        assertThat(countOutboxRows(processInstanceId)).isEqualTo(1);

        String bucket = "outbound-relay-leader-e2e-" + UUID.randomUUID();
        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket(bucket, Duration.ofSeconds(60), 1, natsConnection);
        SweepLeaderLease leaderLease = new SweepLeaderLease(jetStream, kvManager, natsConnection,
                bucket, "relay-leader.", "cadenzaflow", "test-node", Duration.ofSeconds(60));
        assertThat(leaderLease.tryAcquireOrRenew()).isTrue();
        OutboundMessageRelay relay = new OutboundMessageRelay(dataSource, jetStream, leaderLease,
                new OutboundMessageOutboxProperties(), null, "cadenzaflow");

        relay.relayCycle();

        assertThat(countOutboxRows(processInstanceId)).isZero();

        String subject = "events.cadenzaflow.payment_requested." + processInstanceId;
        List<Message> messages = fetchMessages(subject, "OUTBOUND_E2E_TEST");
        assertThat(messages).hasSize(1);
        assertThat(new String(messages.get(0).getData(), java.nio.charset.StandardCharsets.UTF_8))
                .contains("\"messageType\":\"payment_requested\"").contains("\"processInstanceId\":\"" + processInstanceId + "\"");
    }

    @Test
    void bestEffortSendTask_publishesDirectly_zeroOutboxRows() throws Exception {
        var instance = runtimeService.startProcessInstanceByKey("bestEffortOutboundProcess");
        String processInstanceId = instance.getProcessInstanceId();

        // Best-effort path: post-commit TransactionListener -- by the time startProcessInstanceByKey
        // returns, the top-level command has already committed and the listener has already run.
        assertThat(countOutboxRows(processInstanceId)).isZero();

        String subject = "events.cadenzaflow.order_created." + processInstanceId;
        List<Message> messages = fetchMessages(subject, "OUTBOUND_E2E_TEST");
        assertThat(messages).hasSize(1);
        assertThat(new String(messages.get(0).getData(), java.nio.charset.StandardCharsets.UTF_8))
                .contains("\"messageType\":\"order_created\"");
    }

    private static List<Message> fetchMessages(String subject, String streamName) throws Exception {
        JetStreamManagement jsm = natsConnection.jetStreamManagement();
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable("test-consumer-" + UUID.randomUUID().toString().replace("-", ""))
                .filterSubject(subject)
                .build();
        jsm.addOrUpdateConsumer(streamName, cc);
        JetStreamSubscription sub = jetStream.subscribe(subject, PullSubscribeOptions.bind(streamName, cc.getDurable()));
        return sub.fetch(1, Duration.ofSeconds(5));
    }

    private static long countOutboxRows(String processInstanceId) throws Exception {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "SELECT count(*) FROM outbound_message_outbox WHERE process_instance_id = ?")) {
            stmt.setString(1, processInstanceId);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }
}
