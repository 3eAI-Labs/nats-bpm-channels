package com.threeai.nats.history.query;

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

import com.threeai.nats.history.projection.ProjectionStore;

/**
 * çekirdek-4, read-only history query service (BR-QRY-001/003, ADR-0014,
 * `03_classes/3_query_api.md` §1). Embeddable service bean (ARCH-Q4 "gömülebilir" mod) AND,
 * when {@code nats-history-projection} runs standalone, exposed via {@code HistoryQueryController}
 * (REST). Authz is delegated to {@link HistoryQueryAuthzSpi} (pluggable). Kontrat: {@code
 * api/openapi.yaml} (tek doğruluk kaynağı, inline şema burada TEKRARLANMAZ).
 *
 * <p><b>CODER-NOTE (no explicit engineId parameter):</b> the openapi contract's çekirdek-4
 * endpoints carry no {@code engineId} query parameter, and the LLD method signatures mirror that
 * (`String processInstanceId`, not {@code (engineId, processInstanceId)}). Queries here are
 * therefore NOT filtered by {@code engine_id} — correct/unambiguous for the common single-engine
 * deployment this basamak targets; a genuinely multi-engine deployment sharing one projection
 * store could see id collisions across engines. This is a fixed-contract limitation (the openapi
 * spec cannot be changed per task instructions), not something this class can unilaterally fix —
 * flagged in the phase-5 return report CODER-QUESTIONS.
 *
 * <p><b>CODER-NOTE (constructor takes DataSource, not ProjectionStore):</b> the LLD sketch is
 * {@code HistoryQueryApi(ProjectionStore projectionStore, ...)}, but {@code ProjectionStore}'s own
 * definition (`03_classes/2_relay_projection.md` §3) exposes ONLY write methods
 * ({@code upsertEntity}/{@code insertLogEvent}/{@code storeLargePayload}) — no read/list
 * capability exists to route through. Rather than bloat the write-side DAO with unrelated
 * read-side query methods (a separation-of-concerns violation), this class takes the projection
 * {@code DataSource} directly, matching the read-only, SELECT-only nature of every method here.
 */
public class HistoryQueryApi {

    private final DataSource projectionDataSource;
    private final PiiMaskingService maskingService;
    private final HistoryQueryAuthzSpi authzSpi;

    public HistoryQueryApi(DataSource projectionDataSource, PiiMaskingService maskingService, HistoryQueryAuthzSpi authzSpi) {
        this.projectionDataSource = projectionDataSource;
        this.maskingService = maskingService;
        this.authzSpi = authzSpi;
    }

    /** çekirdek-4 desen 1: processInstanceId -> full history summary. */
    public Optional<ProcessInstanceSummary> getProcessInstanceHistory(String processInstanceId, QueryContext ctx) {
        requireAuthorized(ctx, QueryOperation.GET_PROCESS_INSTANCE_HISTORY);
        String sql = "SELECT p.process_instance_id, p.process_definition_key, p.business_key, p.start_user_id, "
                + "p.start_time, p.end_time, p.state, c.state AS cutover_state "
                + "FROM process_instance_history p "
                + "LEFT JOIN class_cutover_state c ON c.history_class = 'PROCINST' "
                + "WHERE p.process_instance_id = ? AND p.deleted_at IS NULL LIMIT 1";
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, processInstanceId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty(); // RES_HISTORY_INSTANCE_NOT_FOUND (caller maps to 404)
                }
                ProcessInstanceSummary summary = new ProcessInstanceSummary(
                        rs.getString("process_instance_id"), rs.getString("process_definition_key"),
                        rs.getString("business_key"), rs.getString("start_user_id"),
                        toInstant(rs.getTimestamp("start_time")), toInstant(rs.getTimestamp("end_time")),
                        rs.getString("state"), rs.getString("cutover_state"));
                return Optional.of(maskingService.mask(summary, ctx));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query process instance history for " + processInstanceId, e);
        }
    }

    /** çekirdek-4 desen 2 (businessKey) / desen 3 (zaman-aralığı+definition). */
    public PagedResponse<ProcessInstanceSummary> listProcessInstanceHistory(ProcessInstanceListQuery query, QueryContext ctx) {
        requireAuthorized(ctx, QueryOperation.LIST_PROCESS_INSTANCE_HISTORY);
        if (!query.hasAnyFilter()) {
            // VAL_QUERY_UNSUPPORTED_PATTERN -- filterless full-scan is out of scope (PO-Q3).
            throw new IllegalArgumentException("At least one filter (businessKey or processDefinitionKey+range) is required");
        }

        StringBuilder where = new StringBuilder(" WHERE deleted_at IS NULL");
        List<Object> params = new ArrayList<>();
        if (query.businessKey() != null && !query.businessKey().isBlank()) {
            where.append(" AND business_key = ?");
            params.add(query.businessKey());
        }
        if (query.processDefinitionKey() != null && !query.processDefinitionKey().isBlank()) {
            where.append(" AND process_definition_key = ?");
            params.add(query.processDefinitionKey());
        }
        if (query.startedAfter() != null) {
            where.append(" AND start_time >= ?");
            params.add(Timestamp.from(query.startedAfter()));
        }
        if (query.startedBefore() != null) {
            where.append(" AND start_time <= ?");
            params.add(Timestamp.from(query.startedBefore()));
        }

        long total = count("process_instance_history", where.toString(), params);
        String sql = "SELECT process_instance_id, process_definition_key, business_key, start_user_id, "
                + "start_time, end_time, state FROM process_instance_history" + where
                + " ORDER BY start_time DESC LIMIT ? OFFSET ?";
        List<ProcessInstanceSummary> results = new ArrayList<>();
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            int idx = bindParams(stmt, params);
            stmt.setInt(++idx, query.page().size());
            stmt.setInt(++idx, query.page().offset());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    ProcessInstanceSummary summary = new ProcessInstanceSummary(
                            rs.getString("process_instance_id"), rs.getString("process_definition_key"),
                            rs.getString("business_key"), rs.getString("start_user_id"),
                            toInstant(rs.getTimestamp("start_time")), toInstant(rs.getTimestamp("end_time")),
                            rs.getString("state"), null);
                    results.add(maskingService.mask(summary, ctx));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to list process instance history", e);
        }
        return new PagedResponse<>(results, query.page().page(), query.page().size(), total);
    }

    /** çekirdek-4 desen 4a: instance -> activity geçmişi. */
    public Optional<PagedResponse<ActivityHistoryEntry>> listActivityHistory(String processInstanceId, PageRequest page, QueryContext ctx) {
        requireAuthorized(ctx, QueryOperation.LIST_ACTIVITY_HISTORY);
        if (!processInstanceExists(processInstanceId)) {
            return Optional.empty();
        }
        String where = " WHERE process_instance_id = ? AND deleted_at IS NULL";
        long total = count("activity_instance_history", where, List.of(processInstanceId));
        String sql = "SELECT activity_id, activity_type, task_assignee, start_time, end_time "
                + "FROM activity_instance_history" + where + " ORDER BY start_time ASC LIMIT ? OFFSET ?";
        List<ActivityHistoryEntry> results = new ArrayList<>();
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, processInstanceId);
            stmt.setInt(2, page.size());
            stmt.setInt(3, page.offset());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    ActivityHistoryEntry entry = new ActivityHistoryEntry(rs.getString("activity_id"),
                            rs.getString("activity_type"), rs.getString("task_assignee"),
                            toInstant(rs.getTimestamp("start_time")), toInstant(rs.getTimestamp("end_time")));
                    results.add(maskingService.mask(entry, ctx));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to list activity history for " + processInstanceId, e);
        }
        return Optional.of(new PagedResponse<>(results, page.page(), page.size(), total));
    }

    /** çekirdek-4 desen 4b: instance -> task geçmişi. */
    public Optional<PagedResponse<TaskHistoryEntry>> listTaskHistory(String processInstanceId, PageRequest page, QueryContext ctx) {
        requireAuthorized(ctx, QueryOperation.LIST_TASK_HISTORY);
        if (!processInstanceExists(processInstanceId)) {
            return Optional.empty();
        }
        String where = " WHERE process_instance_id = ? AND deleted_at IS NULL";
        long total = count("task_instance_history", where, List.of(processInstanceId));
        String sql = "SELECT task_id, task_name, assignee, owner, start_time, end_time "
                + "FROM task_instance_history" + where + " ORDER BY start_time ASC LIMIT ? OFFSET ?";
        List<TaskHistoryEntry> results = new ArrayList<>();
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, processInstanceId);
            stmt.setInt(2, page.size());
            stmt.setInt(3, page.offset());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    TaskHistoryEntry entry = new TaskHistoryEntry(rs.getString("task_id"), rs.getString("task_name"),
                            rs.getString("assignee"), rs.getString("owner"),
                            toInstant(rs.getTimestamp("start_time")), toInstant(rs.getTimestamp("end_time")));
                    results.add(maskingService.mask(entry, ctx));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to list task history for " + processInstanceId, e);
        }
        return Optional.of(new PagedResponse<>(results, page.page(), page.size(), total));
    }

    /**
     * çekirdek-4 desen 4c: instance -> variable geçmişi. Reads {@code variable_detail_history}
     * (this basamak's DETAIL-classified append-only stream — VARINST current-value projection is
     * a documented, bounded follow-up per the engine-side {@code HistoryEventClassResolver}
     * CODER-NOTE; the openapi schema itself is agnostic to which source table backs it).
     */
    public Optional<PagedResponse<VariableHistoryEntry>> listVariableHistory(String processInstanceId, PageRequest page, QueryContext ctx) {
        requireAuthorized(ctx, QueryOperation.LIST_VARIABLE_HISTORY);
        if (!processInstanceExists(processInstanceId)) {
            return Optional.empty();
        }
        String where = " WHERE process_instance_id = ? AND deleted_at IS NULL";
        long total = count("variable_detail_history", where, List.of(processInstanceId));
        String sql = "SELECT variable_name, variable_type, variable_value_text, event_time "
                + "FROM variable_detail_history" + where + " ORDER BY event_time ASC LIMIT ? OFFSET ?";
        List<VariableHistoryEntry> results = new ArrayList<>();
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, processInstanceId);
            stmt.setInt(2, page.size());
            stmt.setInt(3, page.offset());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    VariableHistoryEntry entry = new VariableHistoryEntry(rs.getString("variable_name"),
                            rs.getString("variable_type"), rs.getString("variable_value_text"), false,
                            toInstant(rs.getTimestamp("event_time")));
                    results.add(maskingService.mask(entry, ctx));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to list variable history for " + processInstanceId, e);
        }
        return Optional.of(new PagedResponse<>(results, page.page(), page.size(), total));
    }

    private void requireAuthorized(QueryContext ctx, QueryOperation operation) {
        if (!authzSpi.isAuthorized(ctx, operation)) {
            throw new SecurityException("AUTH_QUERY_ACCESS_DENIED: " + operation); // AUTH_QUERY_ACCESS_DENIED
        }
    }

    private boolean processInstanceExists(String processInstanceId) {
        String sql = "SELECT 1 FROM process_instance_history WHERE process_instance_id = ? AND deleted_at IS NULL LIMIT 1";
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, processInstanceId);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to check process instance existence for " + processInstanceId, e);
        }
    }

    private long count(String table, String where, List<Object> params) {
        String sql = "SELECT count(*) FROM " + table + where;
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            bindParams(stmt, params);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to count rows in " + table, e);
        }
    }

    private int bindParams(PreparedStatement stmt, List<Object> params) throws SQLException {
        int idx = 0;
        for (Object param : params) {
            stmt.setObject(++idx, param);
        }
        return idx;
    }

    private static Instant toInstant(Timestamp timestamp) {
        return timestamp != null ? timestamp.toInstant() : null;
    }

    /** Exposed for {@code ErasurePipeline}'s post-erasure verification query. */
    public static String projectionTableNameForVerification(String historyClass) {
        return ProjectionStore.tableNameFor(historyClass);
    }
}
