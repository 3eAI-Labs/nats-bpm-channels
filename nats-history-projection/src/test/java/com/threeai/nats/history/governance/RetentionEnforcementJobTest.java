package com.threeai.nats.history.governance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Map;

import com.threeai.nats.core.db.SqlMigrationRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Real Postgres (Testcontainers) — mirrors DB_SCHEMA.md §5 item 2's docker-proof: a manually
 * created dated partition on {@code variable_detail_history}, DROPped by retention enforcement.
 */
@Testcontainers
class RetentionEnforcementJobTest {

    private static PostgreSQLContainer<?> postgres;
    private static PGSimpleDataSource dataSource;
    private static RetentionAuditLogger auditLogger;

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
        auditLogger = new RetentionAuditLogger(dataSource);
    }

    @AfterAll
    static void stopContainer() {
        postgres.stop();
    }

    @BeforeEach
    void createDatedPartition() throws Exception {
        try (Connection c = dataSource.getConnection(); java.sql.Statement stmt = c.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS variable_detail_history_2020_01");
            stmt.execute("CREATE TABLE variable_detail_history_2020_01 PARTITION OF variable_detail_history "
                    + "FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-02-01 00:00:00+00')");
            stmt.execute("INSERT INTO variable_detail_history (engine_id, process_instance_id, history_event_id, "
                    + "event_type, variable_name, stream_sequence, event_time) "
                    + "VALUES ('camunda', 'proc-old', 'evt-old', 'update', 'oldVar', 1, '2020-01-15 00:00:00+00')");
        }
    }

    private RetentionEnforcementJob newJob(RetentionProperties properties) {
        return new RetentionEnforcementJob(dataSource, auditLogger, properties, "camunda");
    }

    @Test
    void enforceRetention_partitionOlderThanWindow_dropped_auditLogWritten() throws Exception {
        RetentionProperties properties = new RetentionProperties();
        properties.setBulkDefaultDays(90); // 2020 partition is FAR older than 90 days

        newJob(properties).enforceRetention();

        assertThat(partitionExists("variable_detail_history_2020_01")).isFalse();
        assertThat(auditLogCount("variable_detail_history_2020_01")).isEqualTo(1);
    }

    @Test
    void enforceRetention_partitionWithinWindow_notDropped() throws Exception {
        // A retention window long enough that the 2020 partition's upper bound is still "recent".
        RetentionProperties properties = new RetentionProperties();
        properties.setBulkDefaultDays(365 * 50); // 50 years -- 2020 well within window

        newJob(properties).enforceRetention();

        assertThat(partitionExists("variable_detail_history_2020_01")).isTrue();
    }

    @Test
    void enforceRetention_defaultPartition_neverDropped() throws Exception {
        RetentionProperties properties = new RetentionProperties();
        properties.setBulkDefaultDays(0);

        newJob(properties).enforceRetention();

        assertThat(partitionExists("variable_detail_history_default")).isTrue();
    }

    @Test
    void validateRetentionOverrides_belowLegalMinimum_rejected() {
        RetentionProperties properties = new RetentionProperties();
        properties.setAuditCriticalDefaultWindow("P7Y");
        properties.setPerClassOverrides(Map.of("OP_LOG", "P1Y")); // below 7-year legal minimum

        assertThatThrownBy(() -> newJob(properties).validateRetentionOverrides(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("VAL_RETENTION_OVERRIDE_BELOW_LEGAL_MINIMUM");
    }

    @Test
    void validateRetentionOverrides_aboveLegalMinimum_accepted() {
        RetentionProperties properties = new RetentionProperties();
        properties.setAuditCriticalDefaultWindow("P7Y");
        properties.setPerClassOverrides(Map.of("OP_LOG", "P10Y"));

        assertThatCode(() -> newJob(properties).validateRetentionOverrides(properties)).doesNotThrowAnyException();
    }

    /**
     * Phase 5.5 QA (phase-review FINDING-001) — {@code SYS_RETENTION_AUDIT_LOG_WRITE_FAILED} had
     * no test proving the DATA_GOVERNANCE.md §4.4 atomicity invariant ("every retention deletion
     * MUST be audit-logged — an orphan delete is a compliance violation") actually holds under a
     * REAL audit-log write failure, as opposed to being assumed from the
     * {@link RetentionEnforcementJob} class Javadoc's design argument alone.
     *
     * <p><b>Fault injection:</b> {@link RetentionAuditLogger} is backed by a deliberately
     * UNREACHABLE {@code DataSource} (connection-refused, fast/deterministic — same technique as
     * {@code PseudonymizationVaultClientTest}), while {@link RetentionEnforcementJob} itself keeps
     * the REAL, healthy Testcontainers {@code dataSource} for the {@code DROP TABLE} side.
     * Production wiring shares ONE {@code projectionDataSource} bean between both
     * ({@code NatsHistoryProjectionAutoConfiguration}) — splitting them here isolates exactly
     * which side fails, the same technique {@code HistoryProjectionConsumerDlqRoutingTest} uses
     * for a dual-failure scenario a single real dependency cannot easily force on its own.
     *
     * <p><b>Result of this empirical run: the atomicity invariant HOLDS.</b> {@code
     * dropPartitionWithAudit}'s {@code DROP TABLE} runs on the job's OWN connection with
     * autocommit disabled; when {@code auditLogger.record(...)} throws (its own SEPARATE
     * connection failing to even connect), the surrounding catch block calls {@code
     * connection.rollback()} BEFORE re-throwing — Postgres's transactional DDL means the
     * uncommitted {@code DROP TABLE} is undone. The partition survives, zero audit rows are
     * written, and {@link RetentionAuditLogWriteFailedException} propagates uncaught through
     * {@code enforceRetention()} (its own {@code catch (RetentionAuditLogWriteFailedException)}
     * re-throws rather than swallowing it, unlike the generic {@code SYS_RETENTION_JOB_FAILED}
     * log-and-continue path for other exception types) -- confirmed CRITICAL-page behavior, not
     * silently downgraded to a WARN/retry.
     */
    @Test
    void enforceRetention_auditLogWriteFails_dropRolledBack_noOrphanDeletion_throwsCriticalException() throws Exception {
        RetentionProperties properties = new RetentionProperties();
        properties.setBulkDefaultDays(90); // 2020 partition is eligible for drop

        // Baseline BEFORE this test's own attempt -- `retention_audit_log` is not truncated
        // between test methods in this class (unlike the partition table itself, recreated fresh
        // by @BeforeEach), so other passing tests in this class that also legitimately drop+audit
        // a "variable_detail_history_2020_01"-named partition leave rows behind. The correct
        // atomicity assertion is therefore "this fault-injected attempt added zero rows", not
        // "the table is empty" -- an absolute-zero assertion would be a test-isolation artifact,
        // not evidence of (or against) the atomicity property under test.
        long auditRowsBefore = auditLogCount("variable_detail_history_2020_01");

        RetentionAuditLogger failingAuditLogger = new RetentionAuditLogger(unreachableDataSource());
        RetentionEnforcementJob job = new RetentionEnforcementJob(dataSource, failingAuditLogger, properties, "camunda");

        assertThatThrownBy(job::enforceRetention)
                .isInstanceOf(com.threeai.nats.core.history.exception.RetentionAuditLogWriteFailedException.class);

        // CRITICAL atomicity assertion (FINDING-001): no orphan delete -- the partition must
        // still exist, and THIS attempt must not have added any audit row (DATA_GOVERNANCE.md
        // §4.4 -- a delete without a matching audit entry is a compliance violation).
        assertThat(partitionExists("variable_detail_history_2020_01")).isTrue();
        assertThat(auditLogCount("variable_detail_history_2020_01")).isEqualTo(auditRowsBefore);
    }

    private PGSimpleDataSource unreachableDataSource() {
        PGSimpleDataSource broken = new PGSimpleDataSource();
        broken.setUrl("jdbc:postgresql://127.0.0.1:1/nonexistent"); // port 1 -- connection refused, fast/deterministic
        broken.setUser("nobody");
        broken.setPassword("nobody");
        return broken;
    }

    private boolean partitionExists(String partitionName) throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "SELECT 1 FROM information_schema.tables WHERE table_name = ?")) {
            stmt.setString(1, partitionName);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    private long auditLogCount(String partitionName) throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "SELECT count(*) FROM retention_audit_log WHERE partition_name = ?")) {
            stmt.setString(1, partitionName);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }
}
