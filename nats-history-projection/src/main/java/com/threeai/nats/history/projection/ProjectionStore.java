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

import com.threeai.nats.core.largepayload.ContentAddressedLargePayloadStore;
import com.threeai.nats.core.largepayload.LargePayloadReference;
import com.threeai.nats.history.projection.HistoryClassColumnMapping.TableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC erişim katmanı (BR-REL-003, ADR-0011, `03_classes/2_relay_projection.md` §3). Implements
 * the 3-step partition-safe merge-upsert protocol from {@code DB_SCHEMA.md §2.3} / migration
 * {@code V1__entity_lifecycle_tables.sql} header (single source of truth for the protocol text).
 *
 * <p><b>CODER-NOTE (basamak-3, D-B'/D-D'/D-F' — content-addressed {@code projection_large_payload}):</b>
 * {@link #storeLargePayload}/{@link #releaseLargePayloadReference} now delegate to the shared
 * {@link ContentAddressedLargePayloadStore} (migration {@code
 * V4__large_payload_content_addressing.sql}) instead of a private, always-insert/always-delete
 * table — the SAME store the engine-side deferred-externalization worker
 * (`camunda-nats-channel`/`cadenzaflow-nats-channel`) writes into for RUNTIME payloads, so
 * byte-identical content from either side dedups onto one row (D-D' "3-copy -> 1 object"). Public
 * signatures are unchanged (basamak-2 compatibility, `ProjectionStoreTest`/`ErasurePipelineTest`
 * pass unmodified) — the dedup/refcount behavior is purely additive underneath.
 *
 * <p><b>CODER-NOTE (reliability hardening, QA-FINDING-1/QA-FINDING-2 —
 * {@code ProjectionStoreConcurrencyReliabilityTest}):</b> {@link #upsertEntity}'s 3-step
 * partition-safe merge-upsert protocol (SELECT then INSERT-or-UPDATE, DB_SCHEMA.md §2.3,
 * ADR-0011/0012/0018 — the protocol's SQL shape, the range-partition scheme, and
 * {@code partition_anchor_at} semantics are all UNCHANGED/LOCKED by this fix) is now wrapped in a
 * single transaction guarded by a Postgres session-level advisory lock scoped to that transaction
 * ({@code pg_advisory_xact_lock}, see {@link #acquireEntityLock}), keyed per
 * (historyClass, engineId, entityId). This serializes the check-then-act sequence across
 * concurrent callers for the SAME entity — closing both the first-insert race (QA-FINDING-1:
 * concurrent distinct-timestamp first-events for a new entity could each observe "not found" and
 * each insert their own row, splitting the entity across partitions) and the lost-update race
 * (QA-FINDING-2: a lower-{@code stream_sequence} racer's UPDATE could physically commit after a
 * higher-sequence racer's, silently reverting to older state). {@link #updateExisting} additionally
 * carries an independent {@code AND stream_sequence < ?} CAS guard in its WHERE clause (defense in
 * depth, ADR-0012 tie-break authority enforced at the SQL layer itself, not only by the caller's
 * earlier read) — the same CAS discipline {@link com.threeai.nats.core.jetstream.SweepLeaderLease}
 * and {@link ContentAddressedLargePayloadStore} already use elsewhere in this codebase. Concurrent
 * upserts for DIFFERENT entities never contend (distinct lock keys) — no cross-entity throughput
 * cost. The sequential (single-caller) call shape and outcomes are byte-for-byte unchanged
 * ({@link ProjectionStoreTest} passes unmodified); only the concurrent-caller behavior is fixed.
 */
public class ProjectionStore {

    private static final Logger log = LoggerFactory.getLogger(ProjectionStore.class);

    /** {@code hashtextextended(text, seed)} folds the composite lock key into the {@code bigint}
     *  {@code pg_advisory_xact_lock} requires; auto-released at COMMIT/ROLLBACK of the calling
     *  transaction (see {@link #acquireEntityLock}). */
    private static final String ACQUIRE_ENTITY_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtextextended(?, 0))";

    private final DataSource projectionDataSource;
    private final ContentAddressedLargePayloadStore largePayloadStore;

    public ProjectionStore(DataSource projectionDataSource) {
        this.projectionDataSource = projectionDataSource;
        this.largePayloadStore = new ContentAddressedLargePayloadStore(projectionDataSource);
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
            connection.setAutoCommit(false);
            try {
                acquireEntityLock(connection, historyClass, record.engineId(), record.entityId());
                UpsertOutcome outcome = applyMergeUpsert(connection, meta, historyClass, record);
                connection.commit();
                return outcome;
            } catch (SQLException e) {
                rollbackQuietly(connection, historyClass, record.entityId());
                throw e;
            } catch (RuntimeException e) {
                rollbackQuietly(connection, historyClass, record.entityId());
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Projection upsertEntity failed for class " + historyClass, e);
        }
    }

    /**
     * Serializes the check-then-act merge-upsert protocol (§2.3, class Javadoc) per
     * (historyClass, engineId, entityId) using a Postgres advisory lock scoped to the CURRENT
     * transaction ({@code pg_advisory_xact_lock}) -- auto-released at COMMIT or ROLLBACK, so there
     * is no explicit unlock call to forget and no leak risk even on an exception path. {@code
     * hashtextextended} folds the composite key into the single {@code bigint} the function
     * requires; a hash collision between two DIFFERENT entities only costs a spurious extra wait
     * (never a correctness issue -- advisory locks are pure mutual exclusion, not identity
     * assertions). {@code historyClass} is included in the key so two different entity-lifecycle
     * classes can never coincidentally collide on the SAME (engineId, entityId) string.
     */
    private void acquireEntityLock(Connection connection, String historyClass, String engineId, String entityId)
            throws SQLException {
        String lockKey = historyClass + ':' + engineId + ':' + entityId;
        try (PreparedStatement stmt = connection.prepareStatement(ACQUIRE_ENTITY_LOCK_SQL)) {
            stmt.setString(1, lockKey);
            stmt.execute();
        }
    }

    private UpsertOutcome applyMergeUpsert(Connection connection, TableMeta meta, String historyClass,
            EntityHistoryRecord record) throws SQLException {
        Optional<ExistingRow> existing = selectExisting(connection, meta, record);
        if (existing.isEmpty()) {
            insertNew(connection, meta, historyClass, record);
            return UpsertOutcome.APPLIED;
        }
        long existingSeq = existing.get().streamSequence();
        if (record.streamSequence() > existingSeq) {
            boolean updated = updateExisting(connection, meta, historyClass, record, existing.get().partitionAnchorAt());
            if (updated) {
                return UpsertOutcome.APPLIED;
            }
            // Defense-in-depth only -- should not happen while acquireEntityLock() correctly
            // serializes every caller for this entity: updateExisting's own stream_sequence CAS
            // guard (ADR-0012 tie-break authority) rejected the write anyway. Fail safe: no-op.
            log.warn("Merge-upsert update rejected by stream_sequence CAS guard despite advisory-lock "
                            + "serialization (unexpected -- discarding as stale)",
                    kv("history_class", historyClass), kv("entity_id", record.entityId()),
                    kv("stream_sequence", record.streamSequence()));
            return UpsertOutcome.STALE_DISCARDED;
        }
        if (record.streamSequence() == existingSeq) {
            // BUS_MERGE_UPSERT_CONFLICT_AMBIGUOUS -- theoretical (duplicate delivery at the
            // SAME stream sequence); safe tie-break: discard, no-op (idempotent).
            log.warn("Merge-upsert conflict: incoming stream_sequence equals existing (ambiguous, discarding)",
                    kv("history_class", historyClass), kv("entity_id", record.entityId()),
                    kv("stream_sequence", record.streamSequence()));
            return UpsertOutcome.STALE_DISCARDED;
        }
        // BUS_PROJECTION_STALE_EVENT_DISCARDED -- expected, no-op (FINDING-004, faz-5 review:
        // this was ALREADY the emit point, just missing the registry code comment).
        log.debug("Stale event discarded (incoming stream_sequence older than stored)",
                kv("history_class", historyClass), kv("entity_id", record.entityId()));
        return UpsertOutcome.STALE_DISCARDED;
    }

    private void rollbackQuietly(Connection connection, String historyClass, String entityId) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            log.warn("Failed to roll back projection upsertEntity transaction after error",
                    kv("history_class", historyClass), kv("entity_id", entityId), rollbackFailure);
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
     * Writes (or, on content-hash dedup hit, acquires an additional reference to) a large
     * byte-array payload for callers to populate {@code variable_value_ref}/
     * {@code error_details_ref}/{@code content_ref} (ARCH-Q1 referans pattern reused inside the
     * projection store; basamak-3 D-B'/D-D' content-addressing — see class Javadoc).
     */
    public UUID storeLargePayload(byte[] payload, String sourceTable) {
        LargePayloadReference ref = largePayloadStore.storeAndAcquireReference(payload, sourceTable);
        return ref.id();
    }

    /**
     * D-F' refcount/GC: releases ONE reference previously acquired via {@link #storeLargePayload}
     * for {@code payloadId} — the row is physically deleted only once every reference has been
     * released (never a direct unconditional DELETE, which would corrupt state for content shared
     * with another still-live referrer). Called by {@link
     * com.threeai.nats.history.governance.ErasurePipeline} (erasure removes a reference) and {@link
     * com.threeai.nats.history.governance.RetentionEnforcementJob} (a dropped partition's rows stop
     * referencing their large-payload companions).
     */
    public void releaseLargePayloadReference(UUID payloadId) {
        largePayloadStore.releaseReference(payloadId);
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

    /**
     * @return {@code true} if the row was actually updated (CAS guard passed), {@code false} if
     *         the {@code stream_sequence} guard rejected the write (see caller for the
     *         defense-in-depth fallback -- QA-FINDING-2 fix, class Javadoc).
     */
    private boolean updateExisting(Connection connection, TableMeta meta, String historyClass,
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
        // ADR-0012 tie-break authority CAS guard (QA-FINDING-2 fix): a lower-or-equal incoming
        // stream_sequence can never overwrite what is currently stored, regardless of what the
        // caller's earlier selectExisting() observed -- independent of, and in addition to,
        // acquireEntityLock()'s serialization above.
        sql.append(" WHERE engine_id = ? AND ").append(meta.entityIdColumn())
                .append(" = ? AND partition_anchor_at = ? AND stream_sequence < ?");

        try (PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
            bindValues(stmt, setValues);
            int idx = setValues.size();
            stmt.setString(++idx, record.engineId());
            stmt.setString(++idx, record.entityId());
            stmt.setTimestamp(++idx, Timestamp.from(partitionAnchorAt));
            stmt.setLong(++idx, record.streamSequence());
            return stmt.executeUpdate() > 0;
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
