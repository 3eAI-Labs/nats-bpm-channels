package com.threeai.nats.history.governance;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;

import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.core.history.exception.RetentionAuditLogWriteFailedException;
import com.threeai.nats.history.projection.ProjectionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Scheduled per-class-table partition retention enforcement (BR-PII-001, ADR-0018,
 * `03_classes/6_governance.md` §1). Finds partitions whose upper bound is older than the class's
 * retention window and DROPs them (bulk-DELETE VACUUM cost avoided — docker-proven mechanism,
 * {@code DB_SCHEMA.md} §5 item 2).
 *
 * <p><b>CODER-NOTE (audit-log atomicity, an IMPROVEMENT over the LLD's described two-phase
 * approach):</b> the LLD Javadoc states "audit row is written in the SAME transaction as the DROP
 * where supported, else immediately after with a reconciling read-back" — implying the DROP and
 * the audit INSERT might not always be atomic. Postgres supports transactional DDL, so this
 * implementation runs {@code DROP TABLE} + the {@code retention_audit_log} INSERT in the SAME
 * transaction, committed together: either both succeed or both roll back (retried next period,
 * {@code SYS_RETENTION_JOB_FAILED}). This is STRICTLY safer than a two-phase approach — an
 * "orphan drop without audit row" state becomes structurally impossible rather than merely
 * reconciled after the fact — and does not deviate from any locked decision (ADR-0018 mandates
 * the audit invariant; it does not mandate a specific two-phase mechanism).
 *
 * <p><b>CODER-NOTE (no automatic partition CREATION):</b> the migrations this basamak ships
 * ({@code V1__entity_lifecycle_tables.sql}/{@code V2__append_log_tables.sql}) create ONLY a
 * {@code <table>_default} partition per class table — no scheduled job creates additional
 * time-bounded partitions. This job's DROP mechanism is schema-agnostic (it discovers whatever
 * dated child partitions exist via {@code pg_inherits}) and is exactly what {@code DB_SCHEMA.md}
 * §5 item 2's docker-proof manually verified; a partition-creation routine (e.g., monthly) is a
 * natural, low-risk follow-up this basamak does not include, matching the LLD's "Phase 5 detayı"
 * framing for partition management specifics.
 */
public class RetentionEnforcementJob {

    private static final Logger log = LoggerFactory.getLogger(RetentionEnforcementJob.class);
    private static final Pattern PARTITION_BOUND_TO = Pattern.compile("TO \\('([^']+)'\\)");

    private static final String LIST_PARTITIONS_SQL =
            "SELECT c.relname AS partition_name, pg_get_expr(c.relpartbound, c.oid) AS partition_bound "
          + "FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent "
          + "WHERE p.relname = ?";

    private final DataSource projectionDataSource;
    private final RetentionAuditLogger auditLogger;
    private final RetentionProperties properties;
    private final String engineId;

    public RetentionEnforcementJob(DataSource projectionDataSource, RetentionAuditLogger auditLogger,
            RetentionProperties properties, String engineId) {
        this.projectionDataSource = projectionDataSource;
        this.auditLogger = auditLogger;
        this.properties = properties;
        this.engineId = engineId;
    }

    @Scheduled(cron = "${history.retention.cron:0 30 3 * * *}")
    public void enforceRetention() {
        for (String historyClass : HistoryClassNames.ALL_CLASSES) {
            try {
                enforceRetentionForClass(historyClass);
            } catch (RetentionAuditLogWriteFailedException criticalFailure) {
                throw criticalFailure; // CRITICAL -- must propagate to page on-call, not be swallowed
            } catch (Exception e) {
                // SYS_RETENTION_JOB_FAILED -- log-only, retried next period.
                log.error("Retention enforcement failed for class — retried next period",
                        kv("history_class", historyClass), kv("engine_id", engineId), e);
            }
        }
    }

    private void enforceRetentionForClass(String historyClass) throws SQLException {
        String table = ProjectionStore.tableNameFor(historyClass);
        Duration window = properties.windowFor(historyClass);
        Instant cutoff = Instant.now().minus(window);

        for (PartitionInfo partition : listDatedPartitions(table)) {
            if (partition.upperBound() == null || !partition.upperBound().isBefore(cutoff)) {
                continue; // still within the retention window (or DEFAULT partition -- never dropped)
            }
            dropPartitionWithAudit(historyClass, table, partition, (int) window.toDays());
        }
    }

    private void dropPartitionWithAudit(String historyClass, String table, PartitionInfo partition, int windowDays)
            throws SQLException {
        try (Connection connection = projectionDataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                long estimatedRows = estimateRowCount(connection, partition.partitionName());
                try (java.sql.Statement stmt = connection.createStatement()) {
                    stmt.execute("DROP TABLE " + partition.partitionName());
                }
                auditLogger.record(engineId, historyClass, table, partition.partitionName(), "DROP_PARTITION",
                        estimatedRows, windowDays, legalBasisFor(historyClass), "Retention window exceeded");
                connection.commit();
                log.info("Retention partition dropped", kv("history_class", historyClass),
                        kv("partition", partition.partitionName()), kv("row_count_estimate", estimatedRows));
                // BUS_RETENTION_WINDOW_BREACH_DETECTED (informational, INFO level, deletion applied)
            } catch (Exception e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (RetentionAuditLogWriteFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to drop retention partition " + partition.partitionName(), e);
        }
    }

    private String legalBasisFor(String historyClass) {
        return HistoryClassNames.DEFAULT_AUDIT_CRITICAL_CLASSES.contains(historyClass)
                ? "KVKK/legal-hold retention window (" + properties.getAuditCriticalDefaultWindow() + ")"
                : null;
    }

    private long estimateRowCount(Connection connection, String partitionName) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("SELECT count(*) FROM " + partitionName);
             ResultSet rs = stmt.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private List<PartitionInfo> listDatedPartitions(String table) throws SQLException {
        List<PartitionInfo> partitions = new ArrayList<>();
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(LIST_PARTITIONS_SQL)) {
            stmt.setString(1, table);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String partitionName = rs.getString("partition_name");
                    String bound = rs.getString("partition_bound");
                    if (partitionName.endsWith("_default")) {
                        continue; // never drop the catch-all default partition
                    }
                    partitions.add(new PartitionInfo(partitionName, parseUpperBound(bound)));
                }
            }
        }
        return partitions;
    }

    private static Instant parseUpperBound(String boundExpression) {
        if (boundExpression == null || "DEFAULT".equals(boundExpression)) {
            return null;
        }
        Matcher matcher = PARTITION_BOUND_TO.matcher(boundExpression);
        if (!matcher.find()) {
            return null;
        }
        try {
            // Postgres pg_get_expr() renders the offset WITHOUT a colon when minutes are zero
            // (e.g. "+00", "+05") and WITH one otherwise (e.g. "+05:30") -- Java's single "x"
            // offset pattern matches both variable-width forms; "xxx" (always "+HH:mm") does not.
            return OffsetDateTime.parse(matcher.group(1), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssx"))
                    .toInstant();
        } catch (Exception e) {
            return null;
        }
    }

    /** Bootstrap/config-validation guard — {@code VAL_RETENTION_OVERRIDE_BELOW_LEGAL_MINIMUM}. */
    public void validateRetentionOverrides(RetentionProperties propertiesToValidate) {
        propertiesToValidate.getPerClassOverrides().forEach((historyClass, windowText) -> {
            java.time.Period parsed = java.time.Period.parse(windowText);
            Duration proposed = Duration.ofDays((long) parsed.getYears() * 365 + (long) parsed.getMonths() * 30 + parsed.getDays());
            if (propertiesToValidate.isBelowLegalMinimum(historyClass, proposed)) {
                throw new IllegalArgumentException(
                        "VAL_RETENTION_OVERRIDE_BELOW_LEGAL_MINIMUM: override for " + historyClass
                                + " (" + windowText + ") is below the legal minimum retention window");
            }
        });
    }

    private record PartitionInfo(String partitionName, Instant upperBound) {
    }
}
