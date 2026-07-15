package com.threeai.nats.bench;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.sql.DataSource;

import com.threeai.nats.camunda.a2.A2BpmnParseListener;
import com.threeai.nats.camunda.a2.A2CompletionBridge;
import com.threeai.nats.camunda.a2.A2ConsumerConfig;
import com.threeai.nats.camunda.a2.A2PostCommitPublisher;
import com.threeai.nats.camunda.a2.A2Properties;
import com.threeai.nats.camunda.a2.A2TopicConfig;
import com.threeai.nats.camunda.a2.UmbrellaLockResolver;
import com.threeai.nats.camunda.a2.UmbrellaLockValidator;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection.Status;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.NatsJetStreamConstants;
import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.ManagementService;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.ProcessEngineConfiguration;
import org.camunda.bpm.engine.RepositoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Testcontainers PG (with {@code pg_stat_statements}) + embedded Camunda engine + JetStream +
 * one real {@link A2CompletionBridge}-backed simulated worker. Two BPMN process definitions are
 * deployed side by side in the SAME engine/DB: one classic external-task topic (native poller
 * baseline) and one A2 topic (A2ExternalTaskBehavior active) — 03_classes/5_bench.md §1.
 */
public class BenchEnvironment implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BenchEnvironment.class);

    public static final String NATIVE_TOPIC = "bench-native-topic";
    public static final String A2_TOPIC = "bench-a2-topic";
    static final String SENTINEL_WORKER_ID = "bench-a2-jetstream-bridge";

    private final PostgreSQLContainer<?> postgres;
    private final GenericContainer<?> natsContainer;
    private final DataSource dataSource;
    private final ProcessEngine engine;
    private final io.nats.client.Connection natsConnection;
    private final JetStream jetStream;
    private final A2CompletionBridge completionBridge;
    private final SimulatedA2Worker simulatedWorker;

    @SuppressWarnings("resource")
    private BenchEnvironment() throws Exception {
        this.postgres = new PostgreSQLContainer<>("postgres:16")
                .withCommand("postgres", "-c", "shared_preload_libraries=pg_stat_statements",
                        "-c", "pg_stat_statements.track=all", "-c", "pg_stat_statements.max=10000");
        postgres.start();

        this.natsContainer = new GenericContainer<>("nats:2.10-alpine")
                .withCommand("--jetstream")
                .withExposedPorts(4222);
        natsContainer.start();

        this.dataSource = buildDataSource(postgres);
        createPgStatStatementsExtension(dataSource);

        this.natsConnection = Nats.connect(
                "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222));
        this.jetStream = natsConnection.jetStream();
        ensureStreams(natsConnection);

        NatsChannelMetrics metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        DlqPublisher dlqPublisher = new DlqPublisher(jetStream, natsConnection, metrics);

        A2Properties a2Properties = new A2Properties();
        a2Properties.setSentinelWorkerId(SENTINEL_WORKER_ID);
        a2Properties.setTopics(List.of(A2_TOPIC));
        A2TopicConfig topicConfig = new A2TopicConfig(a2Properties);
        UmbrellaLockResolver lockResolver = new UmbrellaLockResolver(a2Properties);
        UmbrellaLockValidator lockValidator = new UmbrellaLockValidator(a2Properties, lockResolver);
        lockValidator.afterPropertiesSet();
        A2PostCommitPublisher postCommitPublisher = new A2PostCommitPublisher(jetStream, metrics, lockValidator);
        A2BpmnParseListener parseListener = new A2BpmnParseListener(
                topicConfig, a2Properties.getSentinelWorkerId(), lockResolver, postCommitPublisher);

        ProcessEngineConfigurationImpl config = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneProcessEngineConfiguration();
        config.setDataSource(dataSource);
        config.setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE);
        config.setJobExecutorActivate(false);
        config.setPreParseListeners(new java.util.ArrayList<>(List.of(parseListener)));
        this.engine = config.buildProcessEngine();

        deployProcesses(engine.getRepositoryService());

        A2ConsumerConfig replyConfig = new A2ConsumerConfig();
        replyConfig.setSubject("jobs." + A2_TOPIC + ".reply");
        replyConfig.setMessageName(A2_TOPIC);
        replyConfig.setDlqSubject("dlq.jobs." + A2_TOPIC);
        replyConfig.setAckWaitSeconds(30);
        replyConfig.setMaxDeliver(4);
        this.completionBridge = new A2CompletionBridge(natsConnection, jetStream, engine.getExternalTaskService(),
                SENTINEL_WORKER_ID, replyConfig, dlqPublisher, metrics);
        completionBridge.subscribe();

        this.simulatedWorker = new SimulatedA2Worker(natsConnection, jetStream, A2_TOPIC);
        simulatedWorker.start();
    }

    public static BenchEnvironment start() throws Exception {
        return new BenchEnvironment();
    }

    public ProcessEngine engine() {
        return engine;
    }

    public RuntimeService runtimeService() {
        return engine.getRuntimeService();
    }

    public RepositoryService repositoryService() {
        return engine.getRepositoryService();
    }

    public ExternalTaskService externalTaskService() {
        return engine.getExternalTaskService();
    }

    public ManagementService managementService() {
        return engine.getManagementService();
    }

    public DataSource dataSource() {
        return dataSource;
    }

    private static DataSource buildDataSource(PostgreSQLContainer<?> postgres) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(postgres.getJdbcUrl());
        ds.setUser(postgres.getUsername());
        ds.setPassword(postgres.getPassword());
        return ds;
    }

    private static void createPgStatStatementsExtension(DataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")) {
            stmt.execute();
        }
    }

    /**
     * CODER-NOTE / architectural risk found while building this bench module: JetStream
     * deduplication is STREAM-scoped, not subject-scoped. Both the job dispatch
     * ({@code jobs.<topic>}) and its reply ({@code jobs.<topic>.reply}) intentionally carry the
     * SAME {@code Nats-Msg-Id} (= externalTaskId, IR-3/asyncapi ReplyHeaders). If an operator
     * ever provisions ONE catch-all stream spanning both subjects (a natural simplification given
     * {@code jobs.*} is described as a single reserved namespace), JetStream would treat every
     * reply as a duplicate of its own job and silently drop it — replies would never arrive.
     * This bench environment therefore provisions SEPARATE streams per channel, matching the
     * asyncapi contract's per-channel {@code x-jetstream} blocks (each channel documented with
     * its own stream config) — this must be a hard deployment requirement, not just a
     * recommendation; see CODER-QUESTIONS in the phase-5 summary.
     */
    private static void ensureStreams(io.nats.client.Connection connection) throws Exception {
        JetStreamManagement jsm = connection.jetStreamManagement();
        jsm.addStream(StreamConfiguration.builder()
                .name("BENCH-JOBS")
                .subjects("jobs." + A2_TOPIC)
                .retentionPolicy(RetentionPolicy.Limits)
                .storageType(StorageType.File)
                .duplicateWindow(Duration.ofSeconds(120))
                .build());
        jsm.addStream(StreamConfiguration.builder()
                .name("BENCH-JOBS-REPLY")
                .subjects("jobs." + A2_TOPIC + ".reply")
                .retentionPolicy(RetentionPolicy.Limits)
                .storageType(StorageType.File)
                .duplicateWindow(Duration.ofSeconds(120))
                .build());
        jsm.addStream(StreamConfiguration.builder()
                .name("BENCH-DLQ")
                .subjects("dlq.>")
                .retentionPolicy(RetentionPolicy.Limits)
                .storageType(StorageType.File)
                .duplicateWindow(Duration.ofSeconds(120))
                .build());
    }

    private void deployProcesses(RepositoryService repositoryService) {
        repositoryService.createDeployment()
                .addString("bench-native.bpmn20.xml", processXml("benchNativeProcess", NATIVE_TOPIC))
                .deploy();
        repositoryService.createDeployment()
                .addString("bench-a2.bpmn20.xml", processXml("benchA2Process", A2_TOPIC))
                .deploy();
    }

    private static String processXml(String processId, String topic) {
        return """
                <?xml version="1.0" encoding="UTF-8"?>
                <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                             xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                             targetNamespace="http://threeai.com/bench">
                  <process id="%s" isExecutable="true" camunda:historyTimeToLive="1">
                    <startEvent id="start" />
                    <sequenceFlow id="flow1" sourceRef="start" targetRef="benchTask" />
                    <serviceTask id="benchTask" camunda:type="external" camunda:topic="%s" />
                    <sequenceFlow id="flow2" sourceRef="benchTask" targetRef="end" />
                    <endEvent id="end" />
                  </process>
                </definitions>
                """.formatted(processId, topic);
    }

    @Override
    public void close() {
        try {
            simulatedWorker.close();
        } catch (Exception e) {
            log.warn("Error closing simulated worker", e);
        }
        try {
            completionBridge.unsubscribe();
        } catch (Exception e) {
            log.warn("Error unsubscribing completion bridge", e);
        }
        try {
            if (engine != null) {
                engine.close();
            }
        } catch (Exception e) {
            log.warn("Error closing process engine", e);
        }
        try {
            if (natsConnection != null && natsConnection.getStatus() != Status.CLOSED) {
                natsConnection.close();
            }
        } catch (Exception e) {
            log.warn("Error closing NATS connection", e);
        }
        natsContainer.stop();
        postgres.stop();
    }

    /**
     * Minimal simulated external worker: consumes {@code jobs.bench-a2-topic} and replies with a
     * SUCCESS (opaque, no {@code Content-Type} header) reply carrying the same
     * {@code Nats-Msg-Id} — {@link A2CompletionBridge} then completes the task for real, so the
     * bench measures the actual A2 completion path, not a shortcut.
     */
    private static final class SimulatedA2Worker implements AutoCloseable {

        private final io.nats.client.Connection connection;
        private final JetStream jetStream;
        private final String topic;
        private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        private Dispatcher dispatcher;

        SimulatedA2Worker(io.nats.client.Connection connection, JetStream jetStream, String topic) {
            this.connection = connection;
            this.jetStream = jetStream;
            this.topic = topic;
        }

        void start() throws Exception {
            this.dispatcher = connection.createDispatcher();
            jetStream.subscribe("jobs." + topic, dispatcher, this::handle, false);
        }

        private void handle(Message msg) {
            executor.submit(() -> {
                try {
                    String externalTaskId = msg.getHeaders() != null
                            ? msg.getHeaders().getLast(NatsJetStreamConstants.MSG_ID_HDR) : null;
                    io.nats.client.impl.Headers replyHeaders = new io.nats.client.impl.Headers();
                    if (externalTaskId != null) {
                        replyHeaders.add(NatsJetStreamConstants.MSG_ID_HDR, externalTaskId);
                    }
                    NatsMessage reply = NatsMessage.builder()
                            .subject("jobs." + topic + ".reply")
                            .headers(replyHeaders)
                            .data("{}".getBytes(StandardCharsets.UTF_8))
                            .build();
                    jetStream.publish(reply);
                    msg.ack();
                } catch (Exception e) {
                    log.error("Simulated worker failed to process job", e);
                    msg.nak();
                }
            });
        }

        @Override
        public void close() {
            if (dispatcher != null) {
                try {
                    dispatcher.drain(Duration.ofSeconds(5));
                } catch (Exception e) {
                    log.warn("Error draining simulated worker dispatcher", e);
                }
            }
            executor.shutdown();
        }
    }
}
