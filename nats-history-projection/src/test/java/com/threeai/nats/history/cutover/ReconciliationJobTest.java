package com.threeai.nats.history.cutover;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.sql.Connection;

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

/** Real Postgres (Testcontainers), TWO separate databases (projection + a minimal fake engine DB). */
@Testcontainers
class ReconciliationJobTest {

    private static PostgreSQLContainer<?> projectionPostgres;
    private static PostgreSQLContainer<?> enginePostgres;
    private static PGSimpleDataSource projectionDataSource;
    private static PGSimpleDataSource engineDataSource;
    private static ClassCutoverStateStore stateStore;

    @BeforeAll
    static void startContainers() throws Exception {
        projectionPostgres = new PostgreSQLContainer<>("postgres:16");
        projectionPostgres.start();
        projectionDataSource = new PGSimpleDataSource();
        projectionDataSource.setUrl(projectionPostgres.getJdbcUrl());
        projectionDataSource.setUser(projectionPostgres.getUsername());
        projectionDataSource.setPassword(projectionPostgres.getPassword());
        SqlMigrationRunner.applyClasspathScript(projectionDataSource, "db/migration/projection/V1__entity_lifecycle_tables.sql");
        SqlMigrationRunner.applyClasspathScript(projectionDataSource, "db/migration/projection/V2__append_log_tables.sql");
        SqlMigrationRunner.applyClasspathScript(projectionDataSource, "db/migration/projection/V3__control_plane_and_compliance.sql");
        stateStore = new ClassCutoverStateStore(projectionDataSource);

        enginePostgres = new PostgreSQLContainer<>("postgres:16");
        enginePostgres.start();
        engineDataSource = new PGSimpleDataSource();
        engineDataSource.setUrl(enginePostgres.getJdbcUrl());
        engineDataSource.setUser(enginePostgres.getUsername());
        engineDataSource.setPassword(enginePostgres.getPassword());
        // Minimal fake ACT_HI_OP_LOG table -- only the ONE class this test's reconcileAllClasses()
        // scenario exercises end-to-end (the other 14 ACT_HI_* tables intentionally do NOT exist,
        // exercising the per-class SYS_RECONCILIATION_JOB_FAILED catch-and-skip path).
        try (Connection c = engineDataSource.getConnection(); java.sql.Statement stmt = c.createStatement()) {
            stmt.execute("CREATE TABLE ACT_HI_OP_LOG (ID_ VARCHAR(64) PRIMARY KEY)");
        }
    }

    @AfterAll
    static void stopContainers() {
        enginePostgres.stop();
        projectionPostgres.stop();
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection c = projectionDataSource.getConnection(); java.sql.Statement stmt = c.createStatement()) {
            stmt.execute("TRUNCATE class_cutover_state, operation_log_history");
        }
        try (Connection c = engineDataSource.getConnection(); java.sql.Statement stmt = c.createStatement()) {
            stmt.execute("TRUNCATE ACT_HI_OP_LOG");
        }
    }

    private ReconciliationJob newJob() {
        return new ReconciliationJob(projectionDataSource, engineDataSource, stateStore, null,
                new ReconciliationProperties(), "camunda");
    }

    @Test
    void reconcileAllClasses_matchingCounts_advancesStreak_forOpLogOnly() {
        ReconciliationJob job = newJob();

        job.reconcileAllClasses();

        ClassCutoverState state = stateStore.find("camunda", HistoryClassNames.OP_LOG).orElseThrow();
        assertThat(state.cleanStreakDays()).isEqualTo(1);
        assertThat(state.state()).isEqualTo(CutoverState.RECONCILING);
    }

    @Test
    void reconcileAllClasses_missingEngineTables_otherClassesSkippedSafely_noException() {
        assertThatCode(() -> newJob().reconcileAllClasses()).doesNotThrowAnyException();

        // SYS_RECONCILIATION_JOB_FAILED contract: "streak untouched, cycle skipped" -- the
        // class_cutover_state row IS created (findOrCreate happens before the count queries that
        // fail against the missing ACT_HI_PROCINST table), but its streak stays at the DUAL_RUN
        // default (0), never advanced, because the count comparison itself never completed.
        ClassCutoverState state = stateStore.find("camunda", HistoryClassNames.PROCINST).orElseThrow();
        assertThat(state.cleanStreakDays()).isZero();
        assertThat(state.state()).isEqualTo(CutoverState.DUAL_RUN);
    }

    @Test
    void evaluateGate_auditCritical_diffZero_advancesStreak() {
        ReconciliationJob job = newJob();
        stateStore.findOrCreate("camunda", HistoryClassNames.INCIDENT, ConsistencyPath.AUDIT_CRITICAL, 3);
        ClassCutoverState state = stateStore.find("camunda", HistoryClassNames.INCIDENT).orElseThrow();

        job.evaluateGate(state, 0);

        ClassCutoverState after = stateStore.find("camunda", HistoryClassNames.INCIDENT).orElseThrow();
        assertThat(after.cleanStreakDays()).isEqualTo(1);
        assertThat(after.state()).isEqualTo(CutoverState.RECONCILING);
    }

    @Test
    void evaluateGate_auditCritical_diffNonZero_resetsStreak() {
        ReconciliationJob job = newJob();
        stateStore.findOrCreate("camunda", HistoryClassNames.INCIDENT, ConsistencyPath.AUDIT_CRITICAL, 3);
        stateStore.recordCleanCycle("camunda", HistoryClassNames.INCIDENT, 2, 0, CutoverState.RECONCILING);
        ClassCutoverState state = stateStore.find("camunda", HistoryClassNames.INCIDENT).orElseThrow();

        job.evaluateGate(state, 5);

        ClassCutoverState after = stateStore.find("camunda", HistoryClassNames.INCIDENT).orElseThrow();
        assertThat(after.cleanStreakDays()).isZero();
        assertThat(after.state()).isEqualTo(CutoverState.DUAL_RUN);
        assertThat(after.lastDiffCount()).isEqualTo(5);
    }

    @Test
    void evaluateGate_reachesCleanStreakTarget_transitionsToNGunTemiz() {
        ReconciliationJob job = newJob();
        stateStore.findOrCreate("camunda", HistoryClassNames.INCIDENT, ConsistencyPath.AUDIT_CRITICAL, 2);
        stateStore.recordCleanCycle("camunda", HistoryClassNames.INCIDENT, 1, 0, CutoverState.RECONCILING);
        ClassCutoverState state = stateStore.find("camunda", HistoryClassNames.INCIDENT).orElseThrow();

        job.evaluateGate(state, 0); // 2nd consecutive clean cycle, target=2

        ClassCutoverState after = stateStore.find("camunda", HistoryClassNames.INCIDENT).orElseThrow();
        assertThat(after.cleanStreakDays()).isEqualTo(2);
        assertThat(after.state()).isEqualTo(CutoverState.N_GUN_TEMIZ);
    }

    @Test
    void evaluateGate_bulkClass_withinEpsilonAndNotIncreasing_isClean() {
        ReconciliationJob job = newJob();
        ReconciliationProperties props = new ReconciliationProperties();
        props.setBulkEpsilonOverrides(java.util.Map.of(HistoryClassNames.DETAIL, 5L));
        ReconciliationJob bulkAwareJob = new ReconciliationJob(projectionDataSource, engineDataSource, stateStore,
                null, props, "camunda");
        stateStore.findOrCreate("camunda", HistoryClassNames.DETAIL, ConsistencyPath.BULK, 3);
        stateStore.recordCleanCycle("camunda", HistoryClassNames.DETAIL, 1, 10, CutoverState.RECONCILING);
        ClassCutoverState state = stateStore.find("camunda", HistoryClassNames.DETAIL).orElseThrow();

        bulkAwareJob.evaluateGate(state, 3); // <=epsilon(5) AND <= lastDiffCount(10) -> clean

        ClassCutoverState after = stateStore.find("camunda", HistoryClassNames.DETAIL).orElseThrow();
        assertThat(after.cleanStreakDays()).isEqualTo(2);
    }

    @Test
    void evaluateGate_bulkClass_increasingTrend_notClean_evenWithinEpsilon() {
        ReconciliationJob job = newJob();
        ReconciliationProperties props = new ReconciliationProperties();
        props.setBulkEpsilonOverrides(java.util.Map.of(HistoryClassNames.DETAIL, 100L));
        ReconciliationJob bulkAwareJob = new ReconciliationJob(projectionDataSource, engineDataSource, stateStore,
                null, props, "camunda");
        stateStore.findOrCreate("camunda", HistoryClassNames.DETAIL, ConsistencyPath.BULK, 3);
        stateStore.recordCleanCycle("camunda", HistoryClassNames.DETAIL, 1, 2, CutoverState.RECONCILING);
        ClassCutoverState state = stateStore.find("camunda", HistoryClassNames.DETAIL).orElseThrow();

        bulkAwareJob.evaluateGate(state, 10); // within epsilon(100) but INCREASING vs lastDiffCount(2)

        ClassCutoverState after = stateStore.find("camunda", HistoryClassNames.DETAIL).orElseThrow();
        assertThat(after.cleanStreakDays()).isZero(); // reset -- not clean
    }
}
