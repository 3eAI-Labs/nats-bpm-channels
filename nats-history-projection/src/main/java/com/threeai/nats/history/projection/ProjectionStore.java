package com.threeai.nats.history.projection;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;

import com.threeai.nats.history.projection.HistoryClassColumnMapping.TableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC erişim katmanı (BR-REL-003, ADR-0011, `03_classes/2_relay_projection.md` §3). Implements
 * the 3-step partition-safe merge-upsert protocol from {@code DB_SCHEMA.md §2.3} / migration
 * {@code V1__entity_lifecycle_tables.sql} header (single source of truth for the protocol text).
 */
public class ProjectionStore {

    private static final Logger log = LoggerFactory.getLogger(ProjectionStore.class);

    private final DataSource projectionDataSource;

    public ProjectionStore(DataSource projectionDataSource) {
        this.projectionDataSource = projectionDataSource;
    }

    /** Table name for a given ACT_HI class — used by {@code ReconciliationJob}'s row-count comparison. */
    public static String tableNameFor(String historyClass) {
        return HistoryClassColumnMapping.tableFor(historyClass).tableName();
    }

    /**
     * The exact set of non-structural DB columns a class's table exposes (`04_interfaces/2
     * _projection_dtos.md` §2 allowlist — same [BLOCKING] SQL-injection defense-in-depth source
     * of truth {@link #upsertEntity}/{@link #insertLogEvent} already use via {@code
     * appendMappedFields}). Exposed for {@code ErasurePipeline} (CQ-3) so its anonymization
     * column list is validated against the SAME allowlist rather than a second, independently
     * hand-maintained set that could silently drift out of sync with the schema.
     */
    public static java.util.Set<String> allowedColumnsFor(String historyClass) {
        return HistoryClassColumnMapping.tableFor(historyClass).allowedColumns();
    }

    /**
     * Entity-lifecycle merge-upsert (process_instance_history, activity_instance_history,
     * variable_instance_history, task_instance_history, incident_history, case_instance_history).
     */
    public UpsertOutcome upsertEntity(String historyClass, EntityHistoryRecord record) {
        TableMeta meta = HistoryClassColumnMapping.tableFor(historyClass);
        if (!meta.isEntityLifecycle()) {
            throw new IllegalArgumentException(historyClass + " is not an entity-lifecycle class");
        }
        try (Connection connection = projectionDataSource.getConnection()) {
            Optional<ExistingRow> existing = selectExisting(connection, meta, record);
            if (existing.isEmpty()) {
                insertNew(connection, meta, historyClass, record);
                return UpsertOutcome.APPLIED;
            }
            long existingSeq = existing.get().streamSequence();
            if (record.streamSequence() > existingSeq) {
                updateExisting(connection, meta, historyClass, record, existing.get().partitionAnchorAt());
                return UpsertOutcome.APPLIED;
            }
            if (record.streamSequence() == existingSeq) {
                // BUS_MERGE_UPSERT_CONFLICT_AMBIGUOUS -- theoretical (duplicate delivery at the
                // SAME stream sequence); safe tie-break: discard, no-op (idempotent).
                log.warn("Merge-upsert conflict: incoming stream_sequence equals existing (ambiguous, discarding)",
                        kv("history_class", historyClass), kv("entity_id", record.entityId()),
                        kv("stream_sequence", record.streamSequence()));
                return UpsertOutcome.STALE_DISCARDED;
            }
            log.debug("Stale event discarded (incoming stream_sequence older than stored)",
                    kv("history_class", historyClass), kv("entity_id", record.entityId()));
            return UpsertOutcome.STALE_DISCARDED;
        } catch (SQLException e) {
            throw new IllegalStateException("Projection upsertEntity failed for class " + historyClass, e);
        }
    }

    /**
     * Append-only dedup insert (variable_detail_history, identity_link_history,
     * operation_log_history, ext_task_log_history, job_log_history, comment_history,
     * attachment_history, decision_evaluation_history, batch_history).
     */
    public UpsertOutcome insertLogEvent(String historyClass, LogHistoryRecord record) {
        TableMeta meta = HistoryClassColumnMapping.tableFor(historyClass);
        if (meta.isEntityLifecycle()) {
            throw new IllegalArgumentException(historyClass + " is not an append-only log class");
        }
        List<String> columns = new ArrayList<>(List.of("engine_id", "process_instance_id",
                "history_event_id", "event_type", "stream_sequence", "event_time"));
        List<Object> values = new ArrayList<>(List.of(record.engineId(), record.processInstanceId(),
                record.historyEventId(), record.eventType(), record.streamSequence(),
                Timestamp.from(record.eventTime())));
        appendMappedFields(historyClass, record.fields(), columns, values);

        String columnList = String.join(", ", columns);
        String placeholders = String.join(", ", columns.stream().map(c -> "?").toList());
        String sql = "INSERT INTO " + meta.tableName() + " (" + columnList + ") VALUES (" + placeholders + ") "
                + "ON CONFLICT (engine_id, history_event_id, event_type, event_time) DO NOTHING";

        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            bindValues(stmt, values);
            int affected = stmt.executeUpdate();
            return affected > 0 ? UpsertOutcome.APPLIED : UpsertOutcome.DEDUP_SKIPPED;
        } catch (SQLException e) {
            throw new IllegalStateException("Projection insertLogEvent failed for class " + historyClass, e);
        }
    }

    /**
     * Writes a large byte-array payload into {@code projection_large_payload} for callers to
     * populate {@code variable_value_ref}/{@code error_details_ref}/{@code content_ref}
     * (ARCH-Q1 referans pattern reused inside the projection store).
     */
    public UUID storeLargePayload(byte[] payload, String sourceTable) {
        UUID id = UUID.randomUUID();
        String sql = "INSERT INTO projection_large_payload (id, source_table, payload_bytes) VALUES (?,?,?)";
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, id);
            stmt.setString(2, sourceTable);
            stmt.setBytes(3, payload);
            stmt.executeUpdate();
            return id;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to store large payload for source table " + sourceTable, e);
        }
    }

    private Optional<ExistingRow> selectExisting(Connection connection, TableMeta meta, EntityHistoryRecord record)
            throws SQLException {
        String sql = "SELECT partition_anchor_at, stream_sequence FROM " + meta.tableName()
                + " WHERE engine_id = ? AND " + meta.entityIdColumn() + " = ? LIMIT 1";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, record.engineId());
            stmt.setString(2, record.entityId());
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(new ExistingRow(rs.getTimestamp("partition_anchor_at").toInstant(),
                        rs.getLong("stream_sequence")));
            }
        }
    }

    private void insertNew(Connection connection, TableMeta meta, String historyClass, EntityHistoryRecord record)
            throws SQLException {
        List<String> columns = new ArrayList<>(List.of("engine_id", meta.entityIdColumn(),
                "stream_sequence", "event_time", "partition_anchor_at"));
        List<Object> values = new ArrayList<>(List.of(record.engineId(), record.entityId(),
                record.streamSequence(), Timestamp.from(record.eventTime()), Timestamp.from(record.eventTime())));
        if (meta.hasProcessInstanceIdColumn()) {
            columns.add("process_instance_id");
            values.add(record.processInstanceId());
        }
        appendMappedFields(historyClass, record.fields(), columns, values);

        String columnList = String.join(", ", columns);
        String placeholders = String.join(", ", columns.stream().map(c -> "?").toList());
        String sql = "INSERT INTO " + meta.tableName() + " (" + columnList + ") VALUES (" + placeholders + ")";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            bindValues(stmt, values);
            stmt.executeUpdate();
        }
    }

    private void updateExisting(Connection connection, TableMeta meta, String historyClass,
            EntityHistoryRecord record, Instant partitionAnchorAt) throws SQLException {
        List<String> setColumns = new ArrayList<>(List.of("stream_sequence", "event_time", "updated_at"));
        List<Object> setValues = new ArrayList<>(List.of(record.streamSequence(),
                Timestamp.from(record.eventTime()), Timestamp.from(Instant.now())));
        appendMappedFields(historyClass, record.fields(), setColumns, setValues);

        StringBuilder sql = new StringBuilder("UPDATE ").append(meta.tableName()).append(" SET ");
        for (int i = 0; i < setColumns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(setColumns.get(i)).append(" = ?");
        }
        sql.append(" WHERE engine_id = ? AND ").append(meta.entityIdColumn()).append(" = ? AND partition_anchor_at = ?");

        try (PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
            bindValues(stmt, setValues);
            int idx = setValues.size();
            stmt.setString(++idx, record.engineId());
            stmt.setString(++idx, record.entityId());
            stmt.setTimestamp(++idx, Timestamp.from(partitionAnchorAt));
            stmt.executeUpdate();
        }
    }

    private void appendMappedFields(String historyClass, Map<String, Object> fields,
            List<String> columns, List<Object> values) {
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            String column;
            try {
                column = HistoryClassColumnMapping.columnFor(historyClass, entry.getKey());
            } catch (Exception e) {
                log.debug("Skipping unmappable field", kv("history_class", historyClass), kv("field", entry.getKey()));
                continue;
            }
            columns.add(column);
            values.add(entry.getValue());
        }
    }

    private void bindValues(PreparedStatement stmt, List<Object> values) throws SQLException {
        for (int i = 0; i < values.size(); i++) {
            stmt.setObject(i + 1, values.get(i));
        }
    }

    private record ExistingRow(Instant partitionAnchorAt, long streamSequence) {
    }
}
