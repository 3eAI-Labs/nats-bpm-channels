package com.threeai.nats.history.governance;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;

import com.threeai.nats.core.history.exception.RetentionAuditLogWriteFailedException;

/**
 * Writes {@code retention_audit_log} rows (`DATA_GOVERNANCE v4.0 §4.4` audit-log-per-deletion
 * invariant). A write failure AFTER a successful partition drop is
 * {@code SYS_RETENTION_AUDIT_LOG_WRITE_FAILED} (CRITICAL page) — see
 * {@link RetentionAuditLogWriteFailedException}.
 */
public class RetentionAuditLogger {

    private static final String INSERT_SQL =
            "INSERT INTO retention_audit_log (engine_id, history_class, target_table, partition_name, action, "
          + "row_count_estimate, retention_window_days, legal_basis, reason) VALUES (?,?,?,?,?,?,?,?,?)";

    private final DataSource projectionDataSource;

    public RetentionAuditLogger(DataSource projectionDataSource) {
        this.projectionDataSource = projectionDataSource;
    }

    public void record(String engineId, String historyClass, String targetTable, String partitionName, String action,
            long rowCountEstimate, int retentionWindowDays, String legalBasis, String reason) {
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(INSERT_SQL)) {
            stmt.setString(1, engineId);
            stmt.setString(2, historyClass);
            stmt.setString(3, targetTable);
            stmt.setString(4, partitionName);
            stmt.setString(5, action);
            stmt.setLong(6, rowCountEstimate);
            stmt.setInt(7, retentionWindowDays);
            stmt.setString(8, legalBasis);
            stmt.setString(9, reason);
            stmt.executeUpdate();
        } catch (SQLException e) {
            // SYS_RETENTION_AUDIT_LOG_WRITE_FAILED -- CRITICAL, compliance-invariant violation:
            // the partition was already dropped by the time this is called (caller's contract).
            throw new RetentionAuditLogWriteFailedException(
                    "Retention audit log write failed after partition drop for " + targetTable + "/" + partitionName, e);
        }
    }
}
