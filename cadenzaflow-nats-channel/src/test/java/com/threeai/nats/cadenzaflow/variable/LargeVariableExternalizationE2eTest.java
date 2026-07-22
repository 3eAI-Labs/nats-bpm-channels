package com.threeai.nats.cadenzaflow.variable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import com.threeai.nats.core.db.SqlMigrationRunner;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.jetstream.SweepLeaderLease;
import com.threeai.nats.core.largepayload.ContentAddressedLargePayloadStore;
import com.threeai.nats.core.largepayload.ContentHash;
import com.threeai.nats.core.largepayload.LargeVariableExternalizationProperties;
import com.threeai.nats.core.largepayload.LargeVariableSerializerNames;
import io.nats.client.JetStream;
import io.nats.client.Nats;
import org.cadenzaflow.bpm.engine.ProcessEngine;
import org.cadenzaflow.bpm.engine.ProcessEngineConfiguration;
import org.cadenzaflow.bpm.engine.RepositoryService;
import org.cadenzaflow.bpm.engine.RuntimeService;
import org.cadenzaflow.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.cadenzaflow.bpm.engine.impl.variable.serializer.ByteArrayValueSerializer;
import org.cadenzaflow.bpm.engine.impl.variable.serializer.FileValueSerializer;
import org.cadenzaflow.bpm.engine.impl.variable.serializer.JavaObjectSerializer;
import org.cadenzaflow.bpm.engine.impl.variable.serializer.TypedValueSerializer;
import org.cadenzaflow.bpm.engine.runtime.ProcessInstance;
import org.cadenzaflow.bpm.engine.variable.Variables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Real embedded CadenzaFlow (camunda fork) engine (Testcontainers Postgres) + real content-addressed projection store
 * (Testcontainers Postgres, migration V1+V4) + real NATS JetStream (leader lease) — proves the full
 * write -> deferred-externalize -> read round trip through the ACTUAL fork engine, not mocks
 * (`docs/08-large-variable-externalization.md` D-A'/D-C'/D-E'/D-F').
 */
@Testcontainers
class LargeVariableExternalizationE2eTest {

    private static final String PROCESS_ID = "largeVariableProcess";

    private static PostgreSQLContainer<?> postgres;
    private static GenericContainer<?> natsContainer;
    private static PGSimpleDataSource engineDataSource;
    private static PGSimpleDataSource projectionDataSource;
    private static io.nats.client.Connection natsConnection;
    private static JetStream jetStream;
    private static ProcessEngine engine;
    private static LargeVariablePostCommitExternalizer postCommitExternalizer;
    private static LargeVariableExternalizationSweep sweep;
    private static LargeVariableExternalizationProperties properties;
    private static ContentAddressedLargePayloadStore payloadStore;

    @BeforeAll
    static void setUp() throws Exception {
        postgres = new PostgreSQLContainer<>("postgres:16");
        postgres.start();
        engineDataSource = new PGSimpleDataSource();
        engineDataSource.setUrl(postgres.getJdbcUrl());
        engineDataSource.setUser(postgres.getUsername());
        engineDataSource.setPassword(postgres.getPassword());

        try (Connection c = engineDataSource.getConnection(); java.sql.Statement stmt = c.createStatement()) {
            stmt.execute("CREATE DATABASE large_variable_projection");
        }
        projectionDataSource = new PGSimpleDataSource();
        projectionDataSource.setServerNames(new String[] {postgres.getHost()});
        projectionDataSource.setPortNumbers(new int[] {postgres.getMappedPort(5432)});
        projectionDataSource.setDatabaseName("large_variable_projection");
        projectionDataSource.setUser(postgres.getUsername());
        projectionDataSource.setPassword(postgres.getPassword());
        SqlMigrationRunner.applyClasspathScript(projectionDataSource,
                "db/migration/projection/V1__entity_lifecycle_tables.sql");
        SqlMigrationRunner.applyClasspathScript(projectionDataSource,
                "db/migration/projection/V2__append_log_tables.sql");
        SqlMigrationRunner.applyClasspathScript(projectionDataSource,
                "db/migration/projection/V3__control_plane_and_compliance.sql");
        SqlMigrationRunner.applyClasspathScript(projectionDataSource,
                "db/migration/projection/V4__large_payload_content_addressing.sql");
        SqlMigrationRunner.applyClasspathScript(projectionDataSource,
                "db/migration/projection/V5__runtime_large_variable_reference.sql");

        natsContainer = new GenericContainer<>("nats:2.10-alpine").withCommand("--jetstream").withExposedPorts(4222);
        natsContainer.start();
        natsConnection = Nats.connect("nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222));
        jetStream = natsConnection.jetStream();

        properties = new LargeVariableExternalizationProperties();
        properties.setThresholdBytes(64);

        payloadStore = new ContentAddressedLargePayloadStore(projectionDataSource);
        postCommitExternalizer = new LargeVariablePostCommitExternalizer(payloadStore, properties, null, "cadenzaflow");

        JetStreamKvManager kvManager = new JetStreamKvManager();
        String leaderBucket = "large-variable-sweep-leader-e2e-" + UUID.randomUUID();
        kvManager.ensureBucket(leaderBucket, Duration.ofSeconds(60), 1, natsConnection);
        SweepLeaderLease leaderLease = new SweepLeaderLease(jetStream, kvManager, natsConnection,
                leaderBucket, "sweep-leader.", "cadenzaflow", "test-node", Duration.ofSeconds(60));
        sweep = new LargeVariableExternalizationSweep(
                engineDataSource, leaderLease, postCommitExternalizer, payloadStore, properties, "cadenzaflow");

        ProcessEngineConfigurationImpl config = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneProcessEngineConfiguration();
        config.setDataSource(engineDataSource);
        config.setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE);
        config.setJobExecutorActivate(false);
        config.setEnforceHistoryTimeToLive(false); // BPMN carries no historyTimeToLive -- not this test's concern

        // Mirrors CadenzaFlowNatsAutoConfiguration's largeVariableProcessEnginePlugin bean, without Spring.
        postCommitExternalizer.bindConfiguration(config);
        List<TypedValueSerializer> customSerializers = new java.util.ArrayList<>();
        customSerializers.add(new LargeVariableSerializer<>(new ByteArrayValueSerializer(),
                LargeVariableSerializerNames.BYTES, properties, payloadStore, postCommitExternalizer));
        customSerializers.add(new LargeVariableSerializer<>(new JavaObjectSerializer(),
                LargeVariableSerializerNames.OBJECT, properties, payloadStore, postCommitExternalizer));
        customSerializers.add(new LargeVariableSerializer<>(new FileValueSerializer(),
                LargeVariableSerializerNames.FILE, properties, payloadStore, postCommitExternalizer));
        config.setCustomPreVariableSerializers(customSerializers);

        engine = config.buildProcessEngine();
        deployProcess(engine.getRepositoryService());
    }

    private static void deployProcess(RepositoryService repositoryService) {
        repositoryService.createDeployment()
                .addString("large-variable-process.bpmn20.xml", """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                     targetNamespace="http://threeai.com/large-variable-test">
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

    @AfterAll
    static void tearDown() throws Exception {
        if (engine != null) {
            engine.close();
        }
        natsConnection.close();
        natsContainer.stop();
        postgres.stop();
    }

    private String startInstance() {
        RuntimeService runtimeService = engine.getRuntimeService();
        return runtimeService.startProcessInstanceByKey(PROCESS_ID).getProcessInstanceId();
    }

    @Test
    void bytesVariable_overThreshold_externalizedThenReadBackCorrectly() {
        String processInstanceId = startInstance();
        byte[] largeContent = "x".repeat(200).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        RuntimeService runtimeService = engine.getRuntimeService();

        runtimeService.setVariable(processInstanceId, "largeBytes", Variables.byteArrayValue(largeContent));
        String variableId = variableInstanceId(processInstanceId, "largeBytes");

        await().atMost(Duration.ofSeconds(10)).until(() -> byteArrayLengthOf(variableId) < largeContent.length);

        assertThat(byteArrayLengthOf(variableId)).isLessThan(100); // shrunk to the tiny marker
        assertThat((byte[]) runtimeService.getVariable(processInstanceId, "largeBytes")).isEqualTo(largeContent);
    }

    @Test
    void bytesVariable_underThreshold_neverExternalized_staysInlineImmediately() throws Exception {
        String processInstanceId = startInstance();
        byte[] smallContent = "small".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        RuntimeService runtimeService = engine.getRuntimeService();

        runtimeService.setVariable(processInstanceId, "smallBytes", Variables.byteArrayValue(smallContent));

        assertThat((byte[]) runtimeService.getVariable(processInstanceId, "smallBytes")).isEqualTo(smallContent);
        String variableId = variableInstanceId(processInstanceId, "smallBytes");
        // No externalization is even scheduled for an under-threshold value -- length stays exactly
        // as written from the very first read, no need to await anything.
        assertThat(byteArrayLengthOf(variableId)).isEqualTo(smallContent.length);
    }

    @Test
    void objectVariable_overThreshold_externalizedThenDeserializedCorrectly() {
        String processInstanceId = startInstance();
        RuntimeService runtimeService = engine.getRuntimeService();
        SamplePayload payload = new SamplePayload("x".repeat(300));

        runtimeService.setVariable(processInstanceId, "largeObject", Variables.objectValue(payload).create());
        String variableId = variableInstanceId(processInstanceId, "largeObject");

        await().atMost(Duration.ofSeconds(10)).until(() -> isExternalized(variableId));

        Object result = runtimeService.getVariable(processInstanceId, "largeObject");
        assertThat(result).isInstanceOf(SamplePayload.class);
        assertThat(((SamplePayload) result).text()).isEqualTo(payload.text());
    }

    /** Proves the leader-elected catch-all sweep independently finds and externalizes a variable
     *  the fast path did not (simulated here by disabling {@code properties.enabled} for the write,
     *  then re-enabling it before running the sweep — the write itself is otherwise identical). */
    @Test
    void sweepCycle_findsAndExternalizesVariable_fastPathDidNotHandle() {
        properties.setEnabled(false); // fast-path scheduling suppressed for this one write
        String processInstanceId = startInstance();
        byte[] largeContent = "y".repeat(200).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        RuntimeService runtimeService = engine.getRuntimeService();
        runtimeService.setVariable(processInstanceId, "sweptBytes", Variables.byteArrayValue(largeContent));
        String variableId = variableInstanceId(processInstanceId, "sweptBytes");
        assertThat(byteArrayLengthOf(variableId)).isEqualTo(largeContent.length); // still inline

        properties.setEnabled(true);
        sweep.sweepCycle();

        await().atMost(Duration.ofSeconds(10)).until(() -> isExternalized(variableId));
        assertThat((byte[]) runtimeService.getVariable(processInstanceId, "sweptBytes")).isEqualTo(largeContent);
    }

    /**
     * FINDING-001 probe (Sentinel review, ship-blocker): hard-deleting the OWNING PROCESS of a
     * sole-referenced externalized variable must eventually (via reconciliation) bring the
     * content-addressed object's ref_count to 0 and delete the row — otherwise the externalized
     * PII payload survives the deletion forever (a KVKK/D-F' violation the fork's delete path
     * cannot be synchronously hooked to prevent — see {@code LargeVariableExternalizationSweep}
     * class Javadoc).
     */
    @Test
    void hardDeleteProcess_soleReference_reconciliationReleasesAndDeletesPayload() {
        String processInstanceId = startInstance();
        byte[] piiContent = "z".repeat(200).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        RuntimeService runtimeService = engine.getRuntimeService();
        runtimeService.setVariable(processInstanceId, "piiBytes", Variables.byteArrayValue(piiContent));
        String variableId = variableInstanceId(processInstanceId, "piiBytes");
        await().atMost(Duration.ofSeconds(10)).until(() -> isExternalized(variableId));
        String contentHash = ContentHash.sha256Hex(piiContent);
        assertThat(payloadStore.fetchByContentHash(contentHash)).isPresent(); // sanity: really externalized

        runtimeService.deleteProcessInstance(processInstanceId, "test hard delete — FINDING-001 probe");
        sweep.sweepCycle(); // leader-elected cycle now also reconciles runtime references

        assertThat(payloadStore.fetchByContentHash(contentHash)).isEmpty(); // PII genuinely gone
        assertThat(payloadStore.currentRuntimeReference("cadenzaflow", variableId)).isEmpty(); // ledger row cleaned up too
    }

    /**
     * FINDING-001 probe, shared-content variant: two INDEPENDENT variables externalize to the SAME
     * content (dedup, D-B'/D-D') — deleting one referrer's process must NOT delete the object (the
     * other referrer still needs it); deleting BOTH must.
     */
    @Test
    void hardDeleteProcess_sharedReference_survivesUntilLastReferrerDeleted() {
        byte[] sharedPiiContent = "shared-pii-".repeat(30).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String contentHash = ContentHash.sha256Hex(sharedPiiContent);
        RuntimeService runtimeService = engine.getRuntimeService();

        String processInstanceIdA = startInstance();
        runtimeService.setVariable(processInstanceIdA, "sharedBytesA", Variables.byteArrayValue(sharedPiiContent));
        String variableIdA = variableInstanceId(processInstanceIdA, "sharedBytesA");

        String processInstanceIdB = startInstance();
        runtimeService.setVariable(processInstanceIdB, "sharedBytesB", Variables.byteArrayValue(sharedPiiContent));
        String variableIdB = variableInstanceId(processInstanceIdB, "sharedBytesB");

        await().atMost(Duration.ofSeconds(10)).until(() -> isExternalized(variableIdA) && isExternalized(variableIdB));
        assertThat(payloadStore.fetchByContentHash(contentHash)).isPresent();

        runtimeService.deleteProcessInstance(processInstanceIdA, "test hard delete A — FINDING-001 probe");
        sweep.sweepCycle();

        // referrer B still holds a live reference -- the shared object must survive.
        assertThat(payloadStore.fetchByContentHash(contentHash)).isPresent();
        assertThat(payloadStore.currentRuntimeReference("cadenzaflow", variableIdA)).isEmpty();
        assertThat(payloadStore.currentRuntimeReference("cadenzaflow", variableIdB)).isPresent();

        runtimeService.deleteProcessInstance(processInstanceIdB, "test hard delete B — FINDING-001 probe");
        sweep.sweepCycle();

        // last referrer gone -- the object is now genuinely deleted.
        assertThat(payloadStore.fetchByContentHash(contentHash)).isEmpty();
        assertThat(payloadStore.currentRuntimeReference("cadenzaflow", variableIdB)).isEmpty();
    }

    /**
     * FINDING-001 probe, overwrite variant: re-externalizing the SAME variable to DIFFERENT content
     * releases the OLD payload (once no longer shared) without waiting for reconciliation — the
     * fast/sweep path itself performs this release eagerly (see {@code
     * LargeVariablePostCommitExternalizer#externalizeNow}), since the overwrite IS observable at
     * write time (unlike a hard delete).
     */
    @Test
    void overwriteWithDifferentContent_releasesOldPayloadEagerly() {
        String processInstanceId = startInstance();
        RuntimeService runtimeService = engine.getRuntimeService();
        byte[] oldContent = "old-".repeat(60).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] newContent = "new-".repeat(60).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String oldHash = ContentHash.sha256Hex(oldContent);
        String newHash = ContentHash.sha256Hex(newContent);

        runtimeService.setVariable(processInstanceId, "overwritten", Variables.byteArrayValue(oldContent));
        String variableId = variableInstanceId(processInstanceId, "overwritten");
        await().atMost(Duration.ofSeconds(10)).until(() -> isExternalized(variableId));
        assertThat(payloadStore.fetchByContentHash(oldHash)).isPresent();

        runtimeService.setVariable(processInstanceId, "overwritten", Variables.byteArrayValue(newContent));
        await().atMost(Duration.ofSeconds(10)).until(() -> payloadStore.fetchByContentHash(newHash).isPresent());

        assertThat(payloadStore.fetchByContentHash(oldHash)).isEmpty(); // released without needing reconciliation
        assertThat((byte[]) runtimeService.getVariable(processInstanceId, "overwritten")).isEqualTo(newContent);
    }

    private String variableInstanceId(String processInstanceId, String variableName) {
        return engine.getRuntimeService().createVariableInstanceQuery()
                .processInstanceIdIn(processInstanceId).variableName(variableName)
                .singleResult().getId();
    }

    private boolean isExternalized(String variableId) throws Exception {
        try (Connection c = engineDataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "SELECT b.BYTES_ FROM ACT_RU_VARIABLE v JOIN ACT_GE_BYTEARRAY b ON v.BYTEARRAY_ID_ = b.ID_ "
                   + "WHERE v.ID_ = ?")) {
            stmt.setString(1, variableId);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getBytes(1).length < 100;
            }
        }
    }

    private int byteArrayLengthOf(String variableId) {
        try (Connection c = engineDataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "SELECT b.BYTES_ FROM ACT_RU_VARIABLE v JOIN ACT_GE_BYTEARRAY b ON v.BYTEARRAY_ID_ = b.ID_ "
                   + "WHERE v.ID_ = ?")) {
            stmt.setString(1, variableId);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getBytes(1).length;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Simple {@link Serializable} test payload for the OBJECT-type externalization scenario. */
    public record SamplePayload(String text) implements Serializable {
    }
}
