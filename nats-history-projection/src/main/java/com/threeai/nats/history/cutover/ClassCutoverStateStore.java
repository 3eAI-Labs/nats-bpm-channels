package com.threeai.nats.history.cutover;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;

import com.threeai.nats.history.cutover.ClassCutoverState.ConsistencyPath;
import com.threeai.nats.history.cutover.ClassCutoverState.CutoverState;

/**
 * JDBC access layer for {@code class_cutover_state} (`03_classes/4_cutover_reconciliation.md`,
 * `DB_SCHEMA.md §2.7`). Package-private-adjacent DAO shared by {@link ReconciliationJob},
 * {@link CutoverControlPlane}, and {@link CutoverRollback}.
 */
public class ClassCutoverStateStore {

    private final DataSource projectionDataSource;

    public ClassCutoverStateStore(DataSource projectionDataSource) {
        this.projectionDataSource = projectionDataSource;
    }

    /** {@code [*] -> DUAL_RUN} bootstrap — idempotent (ON CONFLICT keeps the existing row). */
    public ClassCutoverState findOrCreate(String engineId, String historyClass, ConsistencyPath consistencyPath,
            int cleanStreakTargetDefault) {
        String upsert = "INSERT INTO class_cutover_state (engine_id, history_class, consistency_path, clean_streak_target) "
                + "VALUES (?,?,?,?) ON CONFLICT (engine_id, history_class) DO NOTHING";
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(upsert)) {
            stmt.setString(1, engineId);
            stmt.setString(2, historyClass);
            stmt.setString(3, consistencyPath.name());
            stmt.setInt(4, cleanStreakTargetDefault);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialize class_cutover_state row", e);
        }
        return find(engineId, historyClass).orElseThrow(() ->
                new IllegalStateException("class_cutover_state row missing immediately after findOrCreate"));
    }

    public Optional<ClassCutoverState> find(String engineId, String historyClass) {
        String sql = "SELECT * FROM class_cutover_state WHERE engine_id = ? AND history_class = ?";
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, engineId);
            stmt.setString(2, historyClass);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? Optional.of(mapRow(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read class_cutover_state row", e);
        }
    }

    public List<ClassCutoverState> findAllByEngine(String engineId) {
        String sql = "SELECT * FROM class_cutover_state WHERE engine_id = ?";
        List<ClassCutoverState> results = new ArrayList<>();
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, engineId);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(mapRow(rs));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to list class_cutover_state rows", e);
        }
        return results;
    }

    /** {@code RECONCILING -> N_GUN_TEMIZ} progress (or continued RECONCILING) — clean cycle. */
    public void recordCleanCycle(String engineId, String historyClass, int newStreakDays, long diffCount,
            CutoverState resultingState) {
        String sql = "UPDATE class_cutover_state SET clean_streak_days = ?, last_diff_count = ?, "
                + "last_reconciled_at = now(), state = ?, updated_at = now() WHERE engine_id = ? AND history_class = ?";
        execute(sql, newStreakDays, diffCount, resultingState.name(), engineId, historyClass);
    }

    /** {@code RECONCILING -> DUAL_RUN} (streak reset, diff detected) — {@code BUS_RECONCILIATION_DIFF_DETECTED}. */
    public void resetStreak(String engineId, String historyClass, long diffCount) {
        String sql = "UPDATE class_cutover_state SET clean_streak_days = 0, last_diff_count = ?, "
                + "last_reconciled_at = now(), state = 'DUAL_RUN', updated_at = now() "
                + "WHERE engine_id = ? AND history_class = ?";
        execute(sql, diffCount, engineId, historyClass);
    }

    /** {@code N_GUN_TEMIZ -> CUTOVER_TALEP}. */
    public void markCutoverRequested(String engineId, String historyClass) {
        String sql = "UPDATE class_cutover_state SET state = 'CUTOVER_TALEP', updated_at = now() "
                + "WHERE engine_id = ? AND history_class = ?";
        execute(sql, engineId, historyClass);
    }

    /** {@code CUTOVER_TALEP -> CUTOVERLANMIS}. */
    public void markCutoverApplied(String engineId, String historyClass, Instant appliedAt) {
        String sql = "UPDATE class_cutover_state SET state = 'CUTOVERLANMIS', cutover_applied_at = ?, updated_at = now() "
                + "WHERE engine_id = ? AND history_class = ?";
        execute(sql, Timestamp.from(appliedAt), engineId, historyClass);
    }

    /** {@code CUTOVER_TALEP -> N_GUN_TEMIZ} (apply failed, fail-safe — dual-run continues). */
    public void revertCutoverRequest(String engineId, String historyClass) {
        String sql = "UPDATE class_cutover_state SET state = 'N_GUN_TEMIZ', updated_at = now() "
                + "WHERE engine_id = ? AND history_class = ?";
        execute(sql, engineId, historyClass);
    }

    /** {@code CUTOVERLANMIS -> DUAL_RUN} (operator-triggered rollback). */
    public void rollback(String engineId, String historyClass, Instant rolledBackAt) {
        String sql = "UPDATE class_cutover_state SET state = 'DUAL_RUN', clean_streak_days = 0, "
                + "rollback_count = rollback_count + 1, last_rollback_at = ?, updated_at = now() "
                + "WHERE engine_id = ? AND history_class = ?";
        execute(sql, Timestamp.from(rolledBackAt), engineId, historyClass);
    }

    private void execute(String sql, Object... params) {
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to update class_cutover_state", e);
        }
    }

    private ClassCutoverState mapRow(ResultSet rs) throws SQLException {
        Timestamp lastReconciledAt = rs.getTimestamp("last_reconciled_at");
        Timestamp cutoverAppliedAt = rs.getTimestamp("cutover_applied_at");
        Timestamp lastRollbackAt = rs.getTimestamp("last_rollback_at");
        return new ClassCutoverState(
                rs.getString("engine_id"),
                rs.getString("history_class"),
                ConsistencyPath.valueOf(rs.getString("consistency_path")),
                CutoverState.valueOf(rs.getString("state")),
                rs.getInt("clean_streak_days"),
                rs.getInt("clean_streak_target"),
                lastReconciledAt != null ? lastReconciledAt.toInstant() : null,
                rs.getLong("last_diff_count"),
                cutoverAppliedAt != null ? cutoverAppliedAt.toInstant() : null,
                rs.getInt("rollback_count"),
                lastRollbackAt != null ? lastRollbackAt.toInstant() : null);
    }
}
