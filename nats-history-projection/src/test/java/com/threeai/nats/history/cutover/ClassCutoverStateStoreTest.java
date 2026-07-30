package com.threeai.nats.history.cutover;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.time.Instant;
import java.util.Optional;

import com.threeai.nats.core.db.SqlMigrationRunner;
import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.history.cutover.ClassCutoverState.ConsistencyPath;
import com.threeai.nats.history.cutover.ClassCutoverState.CutoverState;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Real Postgres (Testcontainers) — the bundled projection migration is applied verbatim. */
@Testcontainers
class ClassCutoverStateStoreTest {

    private static PostgreSQLContainer<?> postgres;
    private static PGSimpleDataSource dataSource;
    private static ClassCutoverStateStore store;

    @BeforeAll
    static void startContainer() {
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
        store = new ClassCutoverStateStore(dataSource);
    }

    @AfterAll
    static void stopContainer() {
        postgres.stop();
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection c = dataSource.getConnection(); java.sql.Statement stmt = c.createStatement()) {
            stmt.execute("TRUNCATE class_cutover_state");
        }
    }

    @Test
    void findOrCreate_newRow_defaultsToDualRun() {
        ClassCutoverState state = store.findOrCreate("camunda", HistoryClassNames.OP_LOG, ConsistencyPath.AUDIT_CRITICAL, 7);

        assertThat(state.state()).isEqualTo(CutoverState.DUAL_RUN);
        assertThat(state.consistencyPath()).isEqualTo(ConsistencyPath.AUDIT_CRITICAL);
        assertThat(state.cleanStreakTarget()).isEqualTo(7);
        assertThat(state.cleanStreakDays()).isZero();
    }

    @Test
    void findOrCreate_idempotent_doesNotOverwriteExistingRow() {
        store.findOrCreate("camunda", HistoryClassNames.OP_LOG, ConsistencyPath.AUDIT_CRITICAL, 7);
        store.recordCleanCycle("camunda", HistoryClassNames.OP_LOG, 3, 0, CutoverState.RECONCILING);

        // Second findOrCreate should NOT reset the streak already recorded.
        ClassCutoverState state = store.findOrCreate("camunda", HistoryClassNames.OP_LOG, ConsistencyPath.AUDIT_CRITICAL, 7);

        assertThat(state.cleanStreakDays()).isEqualTo(3);
        assertThat(state.state()).isEqualTo(CutoverState.RECONCILING);
    }

    @Test
    void recordCleanCycle_advancesStreakAndState() {
        store.findOrCreate("camunda", HistoryClassNames.INCIDENT, ConsistencyPath.AUDIT_CRITICAL, 7);

        store.recordCleanCycle("camunda", HistoryClassNames.INCIDENT, 7, 0, CutoverState.N_GUN_TEMIZ);

        ClassCutoverState state = store.find("camunda", HistoryClassNames.INCIDENT).orElseThrow();
        assertThat(state.cleanStreakDays()).isEqualTo(7);
        assertThat(state.state()).isEqualTo(CutoverState.N_GUN_TEMIZ);
        assertThat(state.lastReconciledAt()).isNotNull();
    }

    @Test
    void resetStreak_backToDualRun_zeroesStreak() {
        store.findOrCreate("camunda", HistoryClassNames.INCIDENT, ConsistencyPath.AUDIT_CRITICAL, 7);
        store.recordCleanCycle("camunda", HistoryClassNames.INCIDENT, 5, 0, CutoverState.RECONCILING);

        store.resetStreak("camunda", HistoryClassNames.INCIDENT, 3);

        ClassCutoverState state = store.find("camunda", HistoryClassNames.INCIDENT).orElseThrow();
        assertThat(state.cleanStreakDays()).isZero();
        assertThat(state.state()).isEqualTo(CutoverState.DUAL_RUN);
        assertThat(state.lastDiffCount()).isEqualTo(3);
    }

    @Test
    void cutoverLifecycle_requestApplyRollback() {
        store.findOrCreate("camunda", HistoryClassNames.OP_LOG, ConsistencyPath.AUDIT_CRITICAL, 7);
        store.recordCleanCycle("camunda", HistoryClassNames.OP_LOG, 7, 0, CutoverState.N_GUN_TEMIZ);

        store.markCutoverRequested("camunda", HistoryClassNames.OP_LOG);
        assertThat(store.find("camunda", HistoryClassNames.OP_LOG).orElseThrow().state())
                .isEqualTo(CutoverState.CUTOVER_TALEP);

        Instant appliedAt = Instant.now();
        store.markCutoverApplied("camunda", HistoryClassNames.OP_LOG, appliedAt);
        ClassCutoverState applied = store.find("camunda", HistoryClassNames.OP_LOG).orElseThrow();
        assertThat(applied.state()).isEqualTo(CutoverState.CUTOVERLANMIS);
        assertThat(applied.cutoverAppliedAt()).isNotNull();

        store.rollback("camunda", HistoryClassNames.OP_LOG, Instant.now());
        ClassCutoverState rolledBack = store.find("camunda", HistoryClassNames.OP_LOG).orElseThrow();
        assertThat(rolledBack.state()).isEqualTo(CutoverState.DUAL_RUN);
        assertThat(rolledBack.cleanStreakDays()).isZero();
        assertThat(rolledBack.rollbackCount()).isEqualTo(1);
        assertThat(rolledBack.lastRollbackAt()).isNotNull();
    }

    @Test
    void revertCutoverRequest_backToNGunTemiz() {
        store.findOrCreate("camunda", HistoryClassNames.OP_LOG, ConsistencyPath.AUDIT_CRITICAL, 7);
        store.recordCleanCycle("camunda", HistoryClassNames.OP_LOG, 7, 0, CutoverState.N_GUN_TEMIZ);
        store.markCutoverRequested("camunda", HistoryClassNames.OP_LOG);

        store.revertCutoverRequest("camunda", HistoryClassNames.OP_LOG);

        assertThat(store.find("camunda", HistoryClassNames.OP_LOG).orElseThrow().state())
                .isEqualTo(CutoverState.N_GUN_TEMIZ);
    }

    @Test
    void find_missingRow_returnsEmpty() {
        Optional<ClassCutoverState> result = store.find("camunda", "NEVER_SEEN_CLASS");

        assertThat(result).isEmpty();
    }

    @Test
    void findAllByEngine_returnsOnlyMatchingEngine() {
        store.findOrCreate("camunda", HistoryClassNames.OP_LOG, ConsistencyPath.AUDIT_CRITICAL, 7);
        store.findOrCreate("cadenzaflow", HistoryClassNames.OP_LOG, ConsistencyPath.AUDIT_CRITICAL, 7);

        assertThat(store.findAllByEngine("camunda")).hasSize(1);
        assertThat(store.findAllByEngine("cadenzaflow")).hasSize(1);
    }
}
