package com.threeai.nats.bench.largevariable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

import com.threeai.nats.bench.BenchEnvironment;
import com.threeai.nats.bench.BenchMode;
import com.threeai.nats.camunda.variable.LargeVariablePostCommitExternalizer;
import com.threeai.nats.camunda.variable.LargeVariableSerializer;
import com.threeai.nats.core.largepayload.ContentAddressedLargePayloadStore;
import com.threeai.nats.core.largepayload.LargeVariableExternalizationProperties;
import com.threeai.nats.core.largepayload.LargeVariableSerializerNames;
import com.threeai.nats.core.db.SqlMigrationRunner;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.ProcessEngineConfiguration;
import org.camunda.bpm.engine.RepositoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.impl.variable.serializer.ByteArrayValueSerializer;
import org.camunda.bpm.engine.impl.variable.serializer.FileValueSerializer;
import org.camunda.bpm.engine.impl.variable.serializer.JavaObjectSerializer;
import org.camunda.bpm.engine.impl.variable.serializer.TypedValueSerializer;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.variable.Variables;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * D-C' threshold-calibration scenario (`docs/08-large-variable-externalization.md` §3 bench item).
 * {@link BenchMode#LARGE_VARIABLE_BASELINE} (built-in {@code ByteArrayValueSerializer} only) vs
 * {@link BenchMode#LARGE_VARIABLE_EXTERNALIZED} ({@code LargeVariableSerializer} + deferred
 * externalization active) — measures total {@code ACT_GE_BYTEARRAY} bytes stored for a batch of
 * over-threshold BYTES variables, at a caller-supplied threshold, so a calibration sweep across
 * several {@code (payloadBytes, thresholdBytes)} pairs can find a sensible default.
 *
 * <p><b>CODER-NOTE (one sibling database, engine schema + content-addressed store co-located):</b>
 * unlike {@code HistoryBenchScenario} (which needs a genuinely SEPARATE
 * {@code compact_history_outbox} table identity per historyLevel-pinned engine build), this
 * scenario's {@code projection_large_payload} table has no naming collision with any {@code ACT_*}
 * table, so both live in the SAME sibling Postgres database — one pool, one migration application,
 * simpler than provisioning yet another physical database purely to mirror production's separate-
 * instance topology, which this bench has no need to simulate.
 */
public class LargeVariableBenchScenario implements AutoCloseable {

    private static final String PROCESS_ID = "largeVariableBenchProcess";
    private static final String VARIABLE_NAME = "benchPayload";
    private static final String LARGE_VARIABLE_BENCH_DATABASE = "largevariablebench";

    private final BenchEnvironment env;
    private final Map<BenchMode, ProcessEngine> engines = new ConcurrentHashMap<>();
    private final Map<BenchMode, LargeVariableExternalizationProperties> propertiesByMode = new ConcurrentHashMap<>();
    private DataSource benchDataSource;
    private ContentAddressedLargePayloadStore payloadStore;

    public LargeVariableBenchScenario(BenchEnvironment env) {
        this.env = env;
    }

    public LargeVariableDbWriteSizeReport run(BenchMode mode, int instanceCount, int payloadBytes, int thresholdBytes)
            throws Exception {
        benchDataSource();
        // computeIfAbsent (lazy, first-call-per-mode) is what populates propertiesByMode for
        // LARGE_VARIABLE_EXTERNALIZED -- the threshold can only be adjusted AFTER that.
        ProcessEngine engine = engines.computeIfAbsent(mode, this::buildEngineSafely);
        if (mode == BenchMode.LARGE_VARIABLE_EXTERNALIZED) {
            propertiesByMode.get(mode).setThresholdBytes(thresholdBytes);
        }

        List<String> processInstanceIds = driveInstances(engine, instanceCount, payloadBytes);

        if (mode == BenchMode.LARGE_VARIABLE_EXTERNALIZED) {
            awaitExternalizationSettled(processInstanceIds, payloadBytes, thresholdBytes);
        }

        long totalBytes = totalByteArrayBytes(processInstanceIds);
        return new LargeVariableDbWriteSizeReport(totalBytes, instanceCount, payloadBytes, thresholdBytes, mode);
    }

    /** See {@code HistoryBenchScenario}'s identical CODER-NOTE — {@code env.dataSource()}'s schema
     *  is already claimed by {@code BenchEnvironment}'s own A2 fixture engine. */
    private DataSource benchDataSource() throws Exception {
        if (benchDataSource != null) {
            return benchDataSource;
        }
        try (Connection connection = env.dataSource().getConnection();
             java.sql.Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE DATABASE " + LARGE_VARIABLE_BENCH_DATABASE);
        }
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[] {env.postgresContainer().getHost()});
        ds.setPortNumbers(new int[] {env.postgresContainer().getMappedPort(5432)});
        ds.setDatabaseName(LARGE_VARIABLE_BENCH_DATABASE);
        ds.setUser(env.postgresContainer().getUsername());
        ds.setPassword(env.postgresContainer().getPassword());
        SqlMigrationRunner.applyClasspathScript(ds, "db/migration/projection/V1__entity_lifecycle_tables.sql");
        SqlMigrationRunner.applyClasspathScript(ds, "db/migration/projection/V2__append_log_tables.sql");
        SqlMigrationRunner.applyClasspathScript(ds, "db/migration/projection/V3__control_plane_and_compliance.sql");
        SqlMigrationRunner.applyClasspathScript(ds, "db/migration/projection/V4__large_payload_content_addressing.sql");
        benchDataSource = ds;
        payloadStore = new ContentAddressedLargePayloadStore(ds);
        return benchDataSource;
    }

    private ProcessEngine buildEngineSafely(BenchMode mode) {
        try {
            return mode == BenchMode.LARGE_VARIABLE_EXTERNALIZED ? buildExternalizedEngine() : buildBaselineEngine();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build large-variable-bench ProcessEngine for mode " + mode, e);
        }
    }

    private ProcessEngine buildBaselineEngine() {
        ProcessEngineConfigurationImpl config = newBaseConfig();
        ProcessEngine engine = config.buildProcessEngine();
        deployProcess(engine.getRepositoryService());
        return engine;
    }

    private ProcessEngine buildExternalizedEngine() {
        LargeVariableExternalizationProperties properties = new LargeVariableExternalizationProperties();
        propertiesByMode.put(BenchMode.LARGE_VARIABLE_EXTERNALIZED, properties);
        LargeVariablePostCommitExternalizer postCommitExternalizer =
                new LargeVariablePostCommitExternalizer(payloadStore, properties, null, "camunda-bench");

        ProcessEngineConfigurationImpl config = newBaseConfig();
        postCommitExternalizer.bindConfiguration(config);
        List<TypedValueSerializer> customSerializers = new ArrayList<>();
        customSerializers.add(new LargeVariableSerializer<>(new ByteArrayValueSerializer(),
                LargeVariableSerializerNames.BYTES, properties, payloadStore, postCommitExternalizer));
        customSerializers.add(new LargeVariableSerializer<>(new JavaObjectSerializer(),
                LargeVariableSerializerNames.OBJECT, properties, payloadStore, postCommitExternalizer));
        customSerializers.add(new LargeVariableSerializer<>(new FileValueSerializer(),
                LargeVariableSerializerNames.FILE, properties, payloadStore, postCommitExternalizer));
        config.setCustomPreVariableSerializers(customSerializers);

        ProcessEngine engine = config.buildProcessEngine();
        deployProcess(engine.getRepositoryService());
        return engine;
    }

    private ProcessEngineConfigurationImpl newBaseConfig() {
        ProcessEngineConfigurationImpl config = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneProcessEngineConfiguration();
        config.setDataSource(benchDataSource);
        config.setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE);
        config.setJobExecutorActivate(false);
        // BASELINE and EXTERNALIZED share ONE physical schema (see class Javadoc) -- both engine
        // builds MUST agree on historyLevel (Camunda pins it on first build), and this scenario has
        // no interest in history at all, so HISTORY_NONE for both sidesteps the mismatch entirely
        // (same gotcha HistoryBenchScenario's own CODER-NOTE documents for a DIFFERENT axis).
        config.setHistory(ProcessEngineConfiguration.HISTORY_NONE);
        config.setEnforceHistoryTimeToLive(false);
        return config;
    }

    private void deployProcess(RepositoryService repositoryService) {
        repositoryService.createDeployment()
                .addString("large-variable-bench.bpmn20.xml", """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                     targetNamespace="http://threeai.com/large-variable-bench">
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

    private List<String> driveInstances(ProcessEngine engine, int instanceCount, int payloadBytes) {
        RuntimeService runtimeService = engine.getRuntimeService();
        byte[] payload = new byte[payloadBytes];
        java.util.Arrays.fill(payload, (byte) 'x');
        List<String> processInstanceIds = new ArrayList<>(instanceCount);
        for (int i = 0; i < instanceCount; i++) {
            ProcessInstance instance = runtimeService.startProcessInstanceByKey(PROCESS_ID);
            runtimeService.setVariable(instance.getProcessInstanceId(), VARIABLE_NAME, Variables.byteArrayValue(payload));
            processInstanceIds.add(instance.getProcessInstanceId());
        }
        return processInstanceIds;
    }

    /** Bench-only bounded settle wait (not a correctness assertion — {@code
     *  LargeVariableExternalizationE2eTest} already proves the mechanism); a run where nothing was
     *  eligible for externalization returns immediately (nothing to wait for). */
    private void awaitExternalizationSettled(List<String> processInstanceIds, int payloadBytes, int thresholdBytes)
            throws Exception {
        if (payloadBytes <= thresholdBytes) {
            return;
        }
        Instant deadline = Instant.now().plus(Duration.ofSeconds(15));
        while (Instant.now().isBefore(deadline)) {
            if (totalByteArrayBytes(processInstanceIds) < (long) processInstanceIds.size() * payloadBytes) {
                return; // at least some externalization has landed -- good enough for a bench signal
            }
            Thread.sleep(200);
        }
    }

    private long totalByteArrayBytes(List<String> processInstanceIds) throws Exception {
        if (processInstanceIds.isEmpty()) {
            return 0;
        }
        // Unquoted identifiers -- the fork's DDL creates these tables/columns unquoted too, so both
        // sides fold to lowercase consistently (matching LargeVariableExternalizationSweep's SQL).
        String sql = "SELECT COALESCE(SUM(octet_length(b.BYTES_)), 0) "
                + "FROM ACT_RU_VARIABLE v JOIN ACT_GE_BYTEARRAY b ON v.BYTEARRAY_ID_ = b.ID_ "
                + "WHERE v.NAME_ = ? AND v.PROC_INST_ID_ = ANY(?)";
        try (Connection connection = benchDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, VARIABLE_NAME);
            stmt.setArray(2, connection.createArrayOf("VARCHAR", processInstanceIds.toArray()));
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
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
