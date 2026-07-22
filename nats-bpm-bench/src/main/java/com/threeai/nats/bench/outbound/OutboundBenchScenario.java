package com.threeai.nats.bench.outbound;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;

import com.threeai.nats.bench.BenchEnvironment;
import com.threeai.nats.bench.BenchMode;
import com.threeai.nats.bench.PgStatStatementsSnapshotter;
import com.threeai.nats.bench.QueryStat;
import com.threeai.nats.bench.Snapshot;
import com.threeai.nats.camunda.outbound.NatsOutboundPublisher;
import com.threeai.nats.core.outbound.OutboundClassificationProperties;
import com.threeai.nats.core.outbound.OutboundMessageOutboxWriter;
import com.threeai.nats.core.outbound.OutboundPostCommitPublisher;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.ProcessEngineConfiguration;
import org.camunda.bpm.engine.RepositoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * Basamak-4 outbound-handoff DB-write-op profile (docs/09-outbound-handoff.md §3): one shared
 * {@link ProcessEngine}, two modes ({@link BenchMode#OUTBOUND_CRITICAL}/{@link
 * BenchMode#OUTBOUND_BEST_EFFORT}) toggled by mutating {@link OutboundClassificationProperties}
 * BETWEEN runs — unlike {@code HistoryBenchScenario} (which needs one dedicated engine PER mode
 * because Camunda's {@code HistoryEventHandler} chain is boot-time-fixed), {@code
 * NatsOutboundPublisher} re-reads its classification config on every {@code notify()} call, so a
 * single engine/publisher pair safely serves both modes.
 *
 * <p><b>CODER-NOTE (own sibling Postgres database, {@code HistoryBenchScenario} precedent):</b>
 * {@code env.dataSource()}'s schema is already claimed by {@code BenchEnvironment}'s own A2
 * fixture engine (different {@code historyLevel} pinning) — this scenario provisions its own
 * sibling database on the SAME container the first time it is needed, exactly like {@code
 * HistoryBenchScenario} does.
 */
public class OutboundBenchScenario implements AutoCloseable {

    private static final String PROCESS_ID = "outboundBenchProcess";
    private static final String ENGINE_ID = "camunda";
    private static final String OUTBOUND_BENCH_DATABASE = "outboundbench";
    // Phase-review FINDING-003: underscore-only -- a dotted messageType would fail
    // OutboundSubjectBuilder's subject-token safety validation.
    private static final String MESSAGE_NAME = "bench_outbound_message";

    private final BenchEnvironment env;
    private final PgStatStatementsSnapshotter snapshotter;
    private final OutboundClassificationProperties classification = new OutboundClassificationProperties();

    private DataSource outboundBenchDataSource;
    private ProcessEngine engine;

    public OutboundBenchScenario(BenchEnvironment env, PgStatStatementsSnapshotter snapshotter) {
        this.env = env;
        this.snapshotter = snapshotter;
    }

    public OutboundDbWriteOpReport run(BenchMode mode, int instanceCount) throws Exception {
        DataSource dataSource = outboundBenchDataSource();
        ProcessEngine processEngine = engineSafely();
        applyClassification(mode);

        snapshotter.reset(dataSource);
        driveInstances(processEngine, instanceCount);

        Snapshot snapshot = snapshotter.capture(dataSource, "%outbound_message_outbox%");
        long outboxInsertCount = countOutboxInserts(snapshot);
        long outboxRowCount = countRows("outbound_message_outbox");

        return new OutboundDbWriteOpReport(outboxInsertCount, outboxRowCount, instanceCount, mode);
    }

    private void applyClassification(BenchMode mode) {
        classification.setCriticalTypes(mode == BenchMode.OUTBOUND_CRITICAL ? Set.of(MESSAGE_NAME) : Set.of());
    }

    private DataSource outboundBenchDataSource() throws Exception {
        if (outboundBenchDataSource != null) {
            return outboundBenchDataSource;
        }
        try (Connection connection = env.dataSource().getConnection();
             java.sql.Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE DATABASE " + OUTBOUND_BENCH_DATABASE);
        }
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[] {env.postgresContainer().getHost()});
        ds.setPortNumbers(new int[] {env.postgresContainer().getMappedPort(5432)});
        ds.setDatabaseName(OUTBOUND_BENCH_DATABASE);
        ds.setUser(env.postgresContainer().getUsername());
        ds.setPassword(env.postgresContainer().getPassword());
        try (Connection connection = ds.getConnection();
             java.sql.Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        }
        com.threeai.nats.core.db.SqlMigrationRunner.applyClasspathScript(
                ds, "db/migration/outbound/V1__outbound_message_outbox.sql");
        outboundBenchDataSource = ds;
        return outboundBenchDataSource;
    }

    private ProcessEngine engineSafely() throws Exception {
        if (engine != null) {
            return engine;
        }
        DataSource dataSource = outboundBenchDataSource();
        ensureOutboundStream();
        OutboundMessageOutboxWriter outboxWriter = new OutboundMessageOutboxWriter(null);
        OutboundPostCommitPublisher postCommitPublisher = new OutboundPostCommitPublisher(env.jetStream(), null);
        NatsOutboundPublisher publisher = new NatsOutboundPublisher(classification, outboxWriter, postCommitPublisher, ENGINE_ID);

        ProcessEngineConfigurationImpl config = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneProcessEngineConfiguration();
        config.setDataSource(dataSource);
        config.setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE);
        config.setJobExecutorActivate(false);
        config.setEnforceHistoryTimeToLive(false);
        // D-A': tenant resolves the listener bean via delegateExpression="${natsOutboundPublisher}".
        config.setBeans(Map.of("natsOutboundPublisher", publisher));
        engine = config.buildProcessEngine();
        deployProcess(engine.getRepositoryService());
        return engine;
    }

    /** JetStream {@code publish()} needs a stream whose subject filter covers the target subject
     *  ("no responders" otherwise) — mirrors {@code HistoryStreamProvisioner}'s idempotent {@code
     *  ensureStream}, scoped to the D-E' {@code events.>} namespace this scenario publishes into. */
    private void ensureOutboundStream() {
        new com.threeai.nats.core.jetstream.JetStreamStreamManager()
                .ensureStream("BENCH-OUTBOUND-EVENTS", "events.>", env.natsConnection());
    }

    private void deployProcess(RepositoryService repositoryService) {
        repositoryService.createDeployment()
                .addString("outbound-bench.bpmn20.xml", """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                     xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                     targetNamespace="http://threeai.com/outbound-bench">
                          <message id="benchOutboundMessage" name="%s" />
                          <process id="%s" isExecutable="true">
                            <startEvent id="start" />
                            <sequenceFlow id="flow1" sourceRef="start" targetRef="throwOutbound" />
                            <intermediateThrowEvent id="throwOutbound">
                              <extensionElements>
                                <camunda:executionListener event="end" delegateExpression="${natsOutboundPublisher}" />
                              </extensionElements>
                              <messageEventDefinition messageRef="benchOutboundMessage" />
                            </intermediateThrowEvent>
                            <sequenceFlow id="flow2" sourceRef="throwOutbound" targetRef="end" />
                            <endEvent id="end" />
                          </process>
                        </definitions>
                        """.formatted(MESSAGE_NAME, PROCESS_ID))
                .deploy();
    }

    private void driveInstances(ProcessEngine processEngine, int instanceCount) {
        RuntimeService runtimeService = processEngine.getRuntimeService();
        for (int i = 0; i < instanceCount; i++) {
            runtimeService.startProcessInstanceByKey(PROCESS_ID);
        }
    }

    private long countOutboxInserts(Snapshot snapshot) {
        long total = 0;
        for (QueryStat stat : snapshot.queryStats()) {
            String normalized = stat.query() == null ? "" : stat.query().trim().toLowerCase(java.util.Locale.ROOT);
            if (normalized.startsWith("insert") && normalized.contains("outbound_message_outbox")) {
                total += stat.calls();
            }
        }
        return total;
    }

    private long countRows(String tableName) throws Exception {
        try (Connection connection = outboundBenchDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT count(*) FROM " + tableName);
             ResultSet rs = stmt.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Override
    public void close() {
        if (engine != null) {
            engine.close();
        }
    }
}
