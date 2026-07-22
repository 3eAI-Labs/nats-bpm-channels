package com.threeai.nats.history.cutover;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;

import com.threeai.nats.core.db.SqlMigrationRunner;
import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.history.cutover.ClassCutoverState.ConsistencyPath;
import com.threeai.nats.history.cutover.ClassCutoverState.CutoverState;
import io.nats.client.KeyValue;
import io.nats.client.Nats;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Real Postgres + real NATS JetStream KV (Testcontainers) — proves the KV + DB dual-write. */
@Testcontainers
class CutoverControlPlaneTest {

    private static PostgreSQLContainer<?> postgres;
    private static GenericContainer<?> natsContainer;
    private static PGSimpleDataSource dataSource;
    private static io.nats.client.Connection natsConnection;
    private static ClassCutoverStateStore stateStore;
    private static JetStreamKvManager kvManager;

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
        stateStore = new ClassCutoverStateStore(dataSource);

        natsContainer = new GenericContainer<>("nats:2.10-alpine").withCommand("--jetstream").withExposedPorts(4222);
        natsContainer.start();
        natsConnection = Nats.connect("nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222));
        kvManager = new JetStreamKvManager();
        kvManager.ensureBucket("history-cutover-state", Duration.ZERO, 1, natsConnection);
    }

    @AfterAll
    static void tearDown() throws Exception {
        natsConnection.close();
        natsContainer.stop();
        postgres.stop();
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection c = dataSource.getConnection(); java.sql.Statement stmt = c.createStatement()) {
            stmt.execute("TRUNCATE class_cutover_state");
        }
    }

    private CutoverControlPlane newControlPlane() {
        return new CutoverControlPlane(stateStore, kvManager, new HistoryCutoverProperties(), natsConnection);
    }

    @Test
    void requestCutover_gateNotMet_rejectsWithoutKvWrite() {
        stateStore.findOrCreate("camunda", HistoryClassNames.OP_LOG, ConsistencyPath.AUDIT_CRITICAL, 7); // DUAL_RUN

        CutoverOutcome outcome = newControlPlane().requestCutover("camunda", HistoryClassNames.OP_LOG);

        assertThat(outcome).isEqualTo(CutoverOutcome.GATE_NOT_MET);
        assertThat(stateStore.find("camunda", HistoryClassNames.OP_LOG).orElseThrow().state())
                .isEqualTo(CutoverState.DUAL_RUN);
    }

    @Test
    void requestCutover_gateOpen_writesKvAndTransitionsState() throws Exception {
        stateStore.findOrCreate("camunda", HistoryClassNames.OP_LOG, ConsistencyPath.AUDIT_CRITICAL, 7);
        stateStore.recordCleanCycle("camunda", HistoryClassNames.OP_LOG, 7, 0, CutoverState.N_GUN_TEMIZ);

        CutoverOutcome outcome = newControlPlane().requestCutover("camunda", HistoryClassNames.OP_LOG);

        assertThat(outcome).isEqualTo(CutoverOutcome.REQUESTED);
        assertThat(stateStore.find("camunda", HistoryClassNames.OP_LOG).orElseThrow().state())
                .isEqualTo(CutoverState.CUTOVER_TALEP);

        KeyValue kv = natsConnection.keyValue("history-cutover-state");
        assertThat(new String(kv.get("cutover.camunda.OP_LOG").getValue(), StandardCharsets.UTF_8)).isEqualTo("true");
    }

    @Test
    void confirmCutoverApplied_transitionsToCutoverlanmis() {
        stateStore.findOrCreate("camunda", HistoryClassNames.OP_LOG, ConsistencyPath.AUDIT_CRITICAL, 7);
        stateStore.recordCleanCycle("camunda", HistoryClassNames.OP_LOG, 7, 0, CutoverState.N_GUN_TEMIZ);
        newControlPlane().requestCutover("camunda", HistoryClassNames.OP_LOG);

        newControlPlane().confirmCutoverApplied("camunda", HistoryClassNames.OP_LOG);

        ClassCutoverState state = stateStore.find("camunda", HistoryClassNames.OP_LOG).orElseThrow();
        assertThat(state.state()).isEqualTo(CutoverState.CUTOVERLANMIS);
        assertThat(state.cutoverAppliedAt()).isNotNull();
    }

    @Test
    void volumePriorityOrder_returnsConfiguredDefault() {
        assertThat(newControlPlane().volumePriorityOrder()).contains("DETAIL", "OP_LOG");
    }

    /**
     * Phase 5.5 (QA) — {@code SYS_CUTOVER_CONFIG_APPLY_FAILED} (KV-write branch) had zero test
     * coverage: a genuinely unreachable NATS connection must yield {@code APPLY_FAILED} and leave
     * the DB state untouched (fail-safe — dual-run continues, `CutoverControlPlane` class Javadoc).
     */
    @Test
    void requestCutover_kvConnectionUnreachable_returnsApplyFailed_stateUnchanged() throws Exception {
        stateStore.findOrCreate("camunda", HistoryClassNames.OP_LOG, ConsistencyPath.AUDIT_CRITICAL, 7);
        stateStore.recordCleanCycle("camunda", HistoryClassNames.OP_LOG, 7, 0, CutoverState.N_GUN_TEMIZ);

        io.nats.client.Connection deadConnection =
                Nats.connect("nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222));
        deadConnection.close(); // simulates a broker/connection outage at the moment of the KV write

        CutoverControlPlane controlPlane =
                new CutoverControlPlane(stateStore, kvManager, new HistoryCutoverProperties(), deadConnection);
        CutoverOutcome outcome = controlPlane.requestCutover("camunda", HistoryClassNames.OP_LOG);

        assertThat(outcome).isEqualTo(CutoverOutcome.APPLY_FAILED);
        assertThat(stateStore.find("camunda", HistoryClassNames.OP_LOG).orElseThrow().state())
                .isEqualTo(CutoverState.N_GUN_TEMIZ); // never transitioned -- dual-run continues
    }

    /**
     * {@code SYS_CUTOVER_CONFIG_APPLY_FAILED} (state-store branch, AFTER the KV write already
     * succeeded) — the class Javadoc documents this as a distinct failure mode from the KV-write
     * branch above; the KV flag is left {@code true} even though the outcome is APPLY_FAILED
     * (the DB write, not the KV write, is what failed).
     */
    @Test
    void requestCutover_stateStoreWriteFailsAfterKvSucceeded_returnsApplyFailed_kvStillWritten() {
        ClassCutoverState gateOpenState = new ClassCutoverState("camunda", HistoryClassNames.OP_LOG,
                ConsistencyPath.AUDIT_CRITICAL, CutoverState.N_GUN_TEMIZ, 7, 7, Instant.now(), 0, null, 0, null);
        ClassCutoverStateStore failingStore = org.mockito.Mockito.mock(ClassCutoverStateStore.class);
        org.mockito.Mockito.when(failingStore.find("camunda", HistoryClassNames.OP_LOG))
                .thenReturn(java.util.Optional.of(gateOpenState));
        org.mockito.Mockito.doThrow(new RuntimeException("db write failed"))
                .when(failingStore).markCutoverRequested("camunda", HistoryClassNames.OP_LOG);

        CutoverControlPlane controlPlane =
                new CutoverControlPlane(failingStore, kvManager, new HistoryCutoverProperties(), natsConnection);
        CutoverOutcome outcome = controlPlane.requestCutover("camunda", HistoryClassNames.OP_LOG);

        assertThat(outcome).isEqualTo(CutoverOutcome.APPLY_FAILED);
        try {
            KeyValue kv = natsConnection.keyValue("history-cutover-state");
            assertThat(new String(kv.get("cutover.camunda.OP_LOG").getValue(), StandardCharsets.UTF_8)).isEqualTo("true");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
