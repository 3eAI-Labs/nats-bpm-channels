package com.threeai.nats.history.governance;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import javax.sql.DataSource;

/** Writes {@code erasure_audit_log} rows (ADR-0017 erasure audit trail). */
public class ErasureAuditLogger {

    private final DataSource projectionDataSource;

    public ErasureAuditLogger(DataSource projectionDataSource) {
        this.projectionDataSource = projectionDataSource;
    }

    public void record(UUID requestId, String subjectKey, String targetTable, String action,
            long affectedRowCount, String reason) {
        String sql = "INSERT INTO erasure_audit_log (request_id, subject_key, target_table, action, "
                + "affected_row_count, reason) VALUES (?,?,?,?,?,?)";
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, requestId);
            stmt.setString(2, subjectKey);
            stmt.setString(3, targetTable);
            stmt.setString(4, action);
            stmt.setLong(5, affectedRowCount);
            stmt.setString(6, reason);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to write erasure_audit_log row for request " + requestId, e);
        }
    }

    public void recordVerification(UUID requestId, String verificationStatus, Instant verifiedAt) {
        String sql = "UPDATE erasure_audit_log SET verification_status = ?, verified_at = ? WHERE request_id = ?";
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, verificationStatus);
            stmt.setTimestamp(2, Timestamp.from(verifiedAt));
            stmt.setObject(3, requestId);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to record erasure verification status for request " + requestId, e);
        }
    }
}
