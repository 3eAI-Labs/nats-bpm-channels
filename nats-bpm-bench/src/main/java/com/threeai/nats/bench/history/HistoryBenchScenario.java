package com.threeai.nats.bench.history;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

import com.threeai.nats.bench.BenchEnvironment;
import com.threeai.nats.bench.BenchMode;
import com.threeai.nats.bench.PgStatStatementsSnapshotter;
import com.threeai.nats.bench.QueryStat;
import com.threeai.nats.bench.Snapshot;
import com.threeai.nats.camunda.history.ClassCutoverStateRegistry;
import com.threeai.nats.camunda.history.CompactHistoryOutboxWriter;
import com.threeai.nats.camunda.history.HistoryClassificationProperties;
import com.threeai.nats.camunda.history.HistoryPostCommitPublisher;
import com.threeai.nats.camunda.history.NatsHistoryEventHandler;
import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.core.history.PseudonymTokenGenerator;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.ProcessEngineConfiguration;
import org.camunda.bpm.engine.RepositoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.TaskService;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.impl.history.handler.DbHistoryEventHandler;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.task.Task;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * Two modes, same scenario — {@link BenchMode#DB_HISTORY_BASELINE} (default {@code
 * DbHistoryEventHandler}, no offload) vs {@link BenchMode#HISTORY_OFFLOAD} (audit-critical +
 * bulk classes all cut over) — `03_classes/5_bench.md` §1.
 *
 * <p><b>CODER-NOTE (own ProcessEngine per mode, not the shared {@code BenchEnvironment} engine):</b>
 * the LLD sketch is {@code HistoryBenchScenario(BenchEnvironment env, ...)}, reusing {@code env}
 * for its Testcontainers Postgres/NATS — this class does exactly that. What it does NOT reuse is
 * {@code env}'s own embedded {@code ProcessEngine} (that engine is dedicated to basamak-1's
 * A2/external-task bench fixture and never installs a history handler). Camunda's {@code
 * HistoryEventHandler} chain is fixed at {@code ProcessEngineConfigurationImpl.buildProcessEngine()}
 * time — fork-verified (see {@code ClassCutoverStateRegistry}'s own boot-read-only CODER-NOTE) —
 * so a single shared engine object cannot flip between "default DB handler" and "NATS handler,
 * classes cut over" between calls. This class therefore builds one dedicated {@code ProcessEngine}
 * per {@link BenchMode}, lazily, sharing {@code env}'s NATS connection/JetStream but NOT its
 * Postgres schema (see the {@code historyBenchDataSource()} CODER-NOTE below) — the physical
 * containers are shared, the engine objects and their schemas are not.
 */
public class HistoryBenchScenario implements AutoCloseable {

    private static final String PROCESS_ID = "historyBenchProcess";
    private static final String CUTOVER_BUCKET = "history-cutover-state";
    private static final String ENGINE_ID = "camunda";
    private static final String HISTORY_BENCH_DATABASE = "historybench";

    private final BenchEnvironment env;
    private final PgStatStatementsSnapshotter snapshotter;
    private final Map<BenchMode, ProcessEngine> engines = new ConcurrentHashMap<>();
    private DataSource historyBenchDataSource;

    public HistoryBenchScenario(BenchEnvironment env, PgStatStatementsSnapshotter snapshotter) {
        this.env = env;
        this.snapshotter = snapshotter;
    }

    public HistoryDbWriteOpReport run(BenchMode mode, int instanceCount) throws Exception {
        DataSource dataSource = historyBenchDataSource();
        ProcessEngine engine = engines.computeIfAbsent(mode, this::buildEngineSafely);

        snapshotter.reset(dataSource);
        driveInstances(engine, instanceCount);

        Snapshot snapshot = snapshotter.capture(dataSource, "%ACT_HI_%");
        long actHiWriteOpCount = countActHiWrites(snapshot);
        long outboxRowCount = countRows("compact_history_outbox");
        long outboxPayloadRowCount = countRows("compact_history_outbox_payload");

        return new HistoryDbWriteOpReport(actHiWriteOpCount, outboxRowCount, outboxPayloadRowCount, mode);
    }

    /**
     * CODER-NOTE (production bug found via {@code DB_HISTORY_BASELINE}/{@code HISTORY_OFFLOAD}
     * sharing {@code env.dataSource()}): Camunda pins a schema's {@code historyLevel} into {@code
     * ACT_GE_PROPERTY} on FIRST engine build and REJECTS a later engine build against that SAME
     * schema if its configured level differs ({@code ProcessEngineException: historyLevel
     * mismatch}). {@code env.dataSource()}'s schema is already claimed by {@code BenchEnvironment}
     * itself (its own A2 fixture engine, built at a different level) BEFORE this scenario ever
     * runs — so this class provisions its OWN sibling Postgres database on the SAME container
     * (via {@code CREATE DATABASE}, same host/port/credentials) the first time it is needed,
     * fully isolated from the A2 fixture's schema and reused across BOTH bench modes.
     */
    private DataSource historyBenchDataSource() throws Exception {
        if (historyBenchDataSource != null) {
            return historyBenchDataSource;
        }
        try (Connection connection = env.dataSource().getConnection();
             java.sql.Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE DATABASE " + HISTORY_BENCH_DATABASE);
        }
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[] {env.postgresContainer().getHost()});
        ds.setPortNumbers(new int[] {env.postgresContainer().getMappedPort(5432)});
        ds.setDatabaseName(HISTORY_BENCH_DATABASE);
        ds.setUser(env.postgresContainer().getUsername());
        ds.setPassword(env.postgresContainer().getPassword());
        try (Connection connection = ds.getConnection();
             java.sql.Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        }
        com.threeai.nats.core.db.SqlMigrationRunner.applyClasspathScript(
                ds, "db/migration/history/V1__compact_history_outbox.sql");
        historyBenchDataSource = ds;
        return historyBenchDataSource;
    }

    private ProcessEngine buildEngineSafely(BenchMode mode) {
        try {
            return mode == BenchMode.HISTORY_OFFLOAD ? buildOffloadEngine() : buildBaselineEngine();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build history-bench ProcessEngine for mode " + mode, e);
        }
    }

    private ProcessEngine buildBaselineEngine() {
        ProcessEngineConfigurationImpl config = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneProcessEngineConfiguration();
        config.setDataSource(historyBenchDataSource);
        config.setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE);
        config.setJobExecutorActivate(false);
        // CODER-NOTE (production bug found via this scenario's own createIncident() assertion):
        // HISTORY_AUDIT's HistoryLevel (HistoryLevelAudit -> HistoryLevelActivity, fork-verified
        // via javap on HistoryLevelActivity.isHistoryEventProduced) does NOT include any
        // INCIDENT_* HistoryEventType in its produced-event allow-list -- only PROCESS_INSTANCE_*/
        // TASK_INSTANCE_*/ACTIVITY_INSTANCE_*/CASE_*/VARIABLE_INSTANCE_*/FORM_PROPERTY_UPDATE.
        // INCIDENT is PO-Q5's DEFAULT audit-critical class, so a scenario meant to exercise the
        // compact-outbox path via createIncident(...) needs HISTORY_FULL (this basamak's own
        // VAL_HISTORY_LEVEL_AUDIT_CRITICAL_MISMATCH bootstrap guard exists PRECISELY to catch a
        // tenant misconfiguring this combination in production). Both engines MUST agree on this
        // (see historyBenchDataSource()'s own CODER-NOTE on why they share ONE schema).
        config.setHistory(ProcessEngineConfiguration.HISTORY_FULL);
        // Companion to the deployProcess() CODER-NOTE: the BPMN carries no historyTimeToLive, so
        // parsing would otherwise fail with ENGINE-12018 (TTL enforcement) -- this scenario has no
        // interest in Camunda's own history-cleanup subsystem at all.
        config.setEnforceHistoryTimeToLive(false);
        ProcessEngine engine = config.buildProcessEngine();
        deployProcess(engine.getRepositoryService());
        return engine;
    }

    private ProcessEngine buildOffloadEngine() throws Exception {
        HistoryClassificationProperties classification = new HistoryClassificationProperties();
        CompactHistoryOutboxWriter outboxWriter = new CompactHistoryOutboxWriter(
                historyBenchDataSource, new PseudonymTokenGenerator(), classification, null);
        HistoryPostCommitPublisher postCommitPublisher = new HistoryPostCommitPublisher(env.jetStream(), null);

        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket(CUTOVER_BUCKET, Duration.ZERO, 1, env.natsConnection());
        seedAllClassesCutOver();
        ClassCutoverStateRegistry cutoverRegistry =
                new ClassCutoverStateRegistry(kvManager, env.natsConnection(), ENGINE_ID);
        cutoverRegistry.loadAtBootstrap();

        NatsHistoryEventHandler handler = new NatsHistoryEventHandler(cutoverRegistry, classification,
                outboxWriter, postCommitPublisher, new DbHistoryEventHandler(), ENGINE_ID);

        ProcessEngineConfigurationImpl config = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneProcessEngineConfiguration();
        config.setDataSource(historyBenchDataSource);
        config.setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE);
        config.setJobExecutorActivate(false);
        // See buildBaselineEngine()'s identical CODER-NOTE re: HISTORY_FULL + enforceHistoryTimeToLive=false.
        config.setHistory(ProcessEngineConfiguration.HISTORY_FULL);
        config.setEnforceHistoryTimeToLive(false);
        config.setCustomHistoryEventHandlers(new java.util.ArrayList<>(List.of(handler)));
        config.setEnableDefaultDbHistoryEventHandler(false);
        ProcessEngine engine = config.buildProcessEngine();
        deployProcess(engine.getRepositoryService());
        return engine;
    }

    /** Every class cut over — this scenario measures the fully-offloaded steady state (D-F). */
    private void seedAllClassesCutOver() throws Exception {
        io.nats.client.KeyValue kv = env.natsConnection().keyValue(CUTOVER_BUCKET);
        for (String historyClass : HistoryClassNames.ALL_CLASSES) {
            kv.put("cutover." + ENGINE_ID + "." + historyClass, "true".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    /**
     * CODER-NOTE (production bug found via this scenario's own D-F hard-gate assertion): the
     * BPMN deliberately carries NO {@code camunda:historyTimeToLive} attribute. Setting one (as
     * {@code BenchEnvironment}'s OWN unrelated A2 bench processes do, and as this class originally
     * copied from {@code HistoryOffloadAuditCriticalFlowIntegrationTest}) activates Camunda's
     * built-in history-cleanup "removal time" propagation: at process-instance end, the engine
     * issues a BLIND {@code UPDATE ACT_HI_<X> SET REMOVAL_TIME_ = ?} against EVERY {@code ACT_HI_*}
     * table for that root instance (0-row-affected if nothing was ever inserted there) — a
     * completely separate built-in mechanism, NOT routed through {@code HistoryEventHandler} at
     * all, so {@link NatsHistoryEventHandler}'s cutover check cannot intercept it. Verified
     * directly: with {@code historyTimeToLive} set, {@code actHiWriteOpCount} in {@link
     * com.threeai.nats.bench.BenchMode#HISTORY_OFFLOAD} mode was nonzero (all {@code
     * REMOVAL_TIME_} UPDATEs, zero real INSERTs) even with every class correctly cut over; without
     * it, {@code actHiWriteOpCount} is genuinely zero. This is a property of Camunda's cleanup
     * subsystem, not a gap in the offload routing itself — flagged in the phase-5 return report.
     */
    private void deployProcess(RepositoryService repositoryService) {
        repositoryService.createDeployment()
                .addString("history-bench.bpmn20.xml", """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                     xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                     targetNamespace="http://threeai.com/history-bench">
                          <process id="%s" isExecutable="true">
                            <startEvent id="start" />
                            <sequenceFlow id="flow1" sourceRef="start" targetRef="waitTask" />
                            <userTask id="waitTask" name="Wait" />
                            <sequenceFlow id="flow2" sourceRef="waitTask" targetRef="end" />
                            <endEvent id="end" />
                          </process>
                        </definitions>
                        """.formatted(PROCESS_ID))
                .deploy();
    }

    /** Also raises one {@code INCIDENT} per instance (PO-Q5 default audit-critical class) so the
     *  scenario exercises BOTH the compact-outbox (audit-critical) and post-commit (bulk) paths,
     *  not just the bulk one -- a plain start/complete flow alone produces only bulk-classified
     *  events (PROCINST/ACTINST/TASKINST/DETAIL), none of which are audit-critical by default. */
    private void driveInstances(ProcessEngine engine, int instanceCount) {
        RuntimeService runtimeService = engine.getRuntimeService();
        TaskService taskService = engine.getTaskService();
        for (int i = 0; i < instanceCount; i++) {
            ProcessInstance instance = runtimeService.startProcessInstanceByKey(PROCESS_ID);
            runtimeService.createIncident("benchIncident", instance.getProcessInstanceId(), "bench-config", "bench-induced incident");
            Task task = taskService.createTaskQuery().processInstanceId(instance.getProcessInstanceId()).singleResult();
            Map<String, Object> vars = new HashMap<>();
            vars.put("benchVariable", "value-" + i);
            taskService.complete(task.getId(), vars);
        }
    }

    /**
     * CODER-NOTE (production bug found via this scenario's own D-F hard-gate assertion): the
     * {@code "%ACT_HI_%"} ILIKE pre-filter passed to {@link PgStatStatementsSnapshotter#capture}
     * is a SUBSTRING match, and {@code compact_history_outbox} literally contains {@code
     * act_hi_} as a substring ({@code comp-ACT_HI-story_outbox}) — the exact table this basamak
     * offloads TO, which must never itself count against the "zero ACT_HI writes" hard gate. This
     * method re-checks each candidate for a REAL {@code act_hi_<table>} token (space-bounded, as
     * every genuine {@code ACT_HI_*} INSERT/UPDATE statement is shaped) to exclude that false
     * positive.
     */
    private long countActHiWrites(Snapshot snapshot) {
        long total = 0;
        for (QueryStat stat : snapshot.queryStats()) {
            String normalized = stat.query() == null ? "" : stat.query().trim().toLowerCase(java.util.Locale.ROOT);
            boolean isWrite = normalized.startsWith("insert") || normalized.startsWith("update");
            if (isWrite && normalized.contains(" act_hi_")) {
                total += stat.calls();
            }
        }
        return total;
    }

    private long countRows(String tableName) throws Exception {
        try (Connection connection = historyBenchDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT count(*) FROM " + tableName);
             ResultSet rs = stmt.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Override
    public void close() {
        for (ProcessEngine engine : engines.values()) {
            engine.close();
        }
        engines.clear();
    }
}
