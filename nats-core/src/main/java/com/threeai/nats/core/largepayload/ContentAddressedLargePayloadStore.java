package com.threeai.nats.core.largepayload;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC client for the basamak-3 unified, content-addressed {@code projection_large_payload} store
 * (`docs/08-large-variable-externalization.md` D-B'/D-D'/D-F', migrations
 * {@code V4__large_payload_content_addressing.sql} + {@code V5__runtime_large_variable_reference.sql}).
 * Engine-neutral, framework-agnostic — takes any
 * {@link DataSource} pointed at the projection Postgres instance, so it can be instantiated both by
 * {@code nats-history-projection}'s {@code ProjectionStore} (HISTORY writer, existing basamak-2
 * pool) and by the engine-side deferred externalization worker in {@code camunda-nats-channel}/
 * {@code cadenzaflow-nats-channel} (RUNTIME writer, its OWN separate pool against the SAME physical
 * database) — the SAME Java class implementing the SAME SQL against the SAME table is what makes
 * "one store, two consumers" (D-D') a genuine single dedup domain rather than two independently
 * re-implemented ones that could drift.
 *
 * <p><b>Concurrency (dedup race safety):</b> {@link #storeAndAcquireReference} uses a single atomic
 * {@code INSERT ... ON CONFLICT (content_hash) DO UPDATE ... RETURNING} statement — two concurrent
 * writers racing to store byte-identical content never both insert (Postgres serializes on the
 * unique index), and the loser's call still correctly observes/increments the winner's row. {@link
 * #releaseReference} decrements with a single atomic {@code UPDATE ... RETURNING}, then re-checks
 * {@code ref_count = 0} at DELETE time (not against a stale value read earlier) — a concurrent
 * {@link #storeAndAcquireReference} that re-acquires the SAME row between the decrement and the
 * delete safely prevents the delete (the row is no longer at 0 by the time DELETE's own WHERE
 * clause evaluates it).
 */
public class ContentAddressedLargePayloadStore {

    private static final Logger log = LoggerFactory.getLogger(ContentAddressedLargePayloadStore.class);

    private static final String STORE_AND_ACQUIRE_SQL =
            "INSERT INTO projection_large_payload (id, source_table, payload_bytes, content_hash, ref_count) "
          + "VALUES (?, ?, ?, ?, 1) "
          + "ON CONFLICT (content_hash) DO UPDATE SET ref_count = projection_large_payload.ref_count + 1 "
          + "RETURNING id, ref_count, (xmax = 0) AS newly_inserted";

    private static final String FETCH_BY_HASH_SQL =
            "SELECT payload_bytes FROM projection_large_payload WHERE content_hash = ?";

    private static final String FETCH_BY_ID_SQL =
            "SELECT payload_bytes FROM projection_large_payload WHERE id = ?";

    private static final String DECREMENT_SQL =
            "UPDATE projection_large_payload SET ref_count = ref_count - 1 "
          + "WHERE id = ? AND ref_count > 0 RETURNING ref_count";

    private static final String DELETE_IF_ZERO_SQL =
            "DELETE FROM projection_large_payload WHERE id = ? AND ref_count = 0";

    private static final String CURRENT_RUNTIME_REFERENCE_SQL =
            "SELECT payload_id FROM runtime_large_variable_ref WHERE engine_id = ? AND variable_id = ?";

    private static final String RECORD_RUNTIME_REFERENCE_SQL =
            "INSERT INTO runtime_large_variable_ref (engine_id, variable_id, payload_id, content_hash) "
          + "VALUES (?, ?, ?, ?) "
          + "ON CONFLICT (engine_id, variable_id) DO UPDATE SET "
          + "payload_id = EXCLUDED.payload_id, content_hash = EXCLUDED.content_hash, updated_at = now()";

    private static final String DELETE_RUNTIME_REFERENCE_SQL =
            "DELETE FROM runtime_large_variable_ref WHERE engine_id = ? AND variable_id = ?";

    private static final String LIST_RUNTIME_REFERENCES_SQL =
            "SELECT variable_id, payload_id, content_hash FROM runtime_large_variable_ref WHERE engine_id = ?";

    private final DataSource projectionDataSource;

    public ContentAddressedLargePayloadStore(DataSource projectionDataSource) {
        this.projectionDataSource = projectionDataSource;
    }

    /**
     * Stores {@code payload} under its SHA-256 content hash, or — if a row with that hash already
     * exists (byte-identical content previously stored by ANY caller, RUNTIME or HISTORY) —
     * increments its {@code ref_count} instead of writing a second copy (D-B'/D-D' dedup).
     *
     * @param payload    the bytes to store (never {@code null} — callers gate on the externalization
     *                   threshold before calling this; an empty/tiny payload is still valid content)
     * @param sourceTable free-text provenance tag (`ext_task_log_history`, `variable_instance_history`,
     *                   `runtime.<engineId>`, ... ) — informational only, NOT part of the dedup key
     *                   (identical bytes from two different sources still collapse to one row).
     */
    public LargePayloadReference storeAndAcquireReference(byte[] payload, String sourceTable) {
        String hash = ContentHash.sha256Hex(payload);
        UUID candidateId = UUID.randomUUID();
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(STORE_AND_ACQUIRE_SQL)) {
            stmt.setObject(1, candidateId);
            stmt.setString(2, sourceTable);
            stmt.setBytes(3, payload);
            stmt.setString(4, hash);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException(
                            "storeAndAcquireReference INSERT ... RETURNING produced no row for hash " + hash);
                }
                UUID id = (UUID) rs.getObject("id");
                int refCount = rs.getInt("ref_count");
                boolean newlyInserted = rs.getBoolean("newly_inserted");
                log.debug("Large payload reference acquired", kv("content_hash", hash),
                        kv("source_table", sourceTable), kv("ref_count", refCount), kv("newly_stored", newlyInserted));
                return new LargePayloadReference(id, hash, refCount, newlyInserted);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "SYS_LARGE_PAYLOAD_STORE_FAILED: failed to store/acquire large payload for source " + sourceTable, e);
        }
    }

    /** @return the payload bytes for {@code contentHash}, or empty if no row currently references it. */
    public Optional<byte[]> fetchByContentHash(String contentHash) {
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(FETCH_BY_HASH_SQL)) {
            stmt.setString(1, contentHash);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? Optional.ofNullable(rs.getBytes(1)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "SYS_LARGE_PAYLOAD_FETCH_FAILED: failed to fetch large payload by content_hash " + contentHash, e);
        }
    }

    /** @return the payload bytes for {@code id} (basamak-2 {@code *_ref} column compatibility). */
    public Optional<byte[]> fetchById(UUID id) {
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(FETCH_BY_ID_SQL)) {
            stmt.setObject(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? Optional.ofNullable(rs.getBytes(1)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "SYS_LARGE_PAYLOAD_FETCH_FAILED: failed to fetch large payload by id " + id, e);
        }
    }

    /**
     * D-F' refcount/GC: decrements {@code ref_count} for {@code id} and deletes the row only if
     * that decrement reaches zero (never a direct unconditional DELETE — dedup means another caller
     * may still be referencing the SAME content). A no-op (WARN, not thrown) if {@code id} does not
     * exist or is already at {@code ref_count = 0} (double-release — defensive, should not happen
     * under correct caller discipline, but must never corrupt state if it does).
     */
    public void releaseReference(UUID id) {
        try (Connection connection = projectionDataSource.getConnection()) {
            Integer refCountAfter = decrement(connection, id);
            if (refCountAfter == null) {
                log.warn("Large payload reference release found no decrementable row — already released?",
                        kv("payload_id", id));
                return;
            }
            if (refCountAfter == 0) {
                deleteIfZero(connection, id);
                log.debug("Large payload row deleted — last reference released", kv("payload_id", id));
            } else {
                log.debug("Large payload reference released", kv("payload_id", id), kv("ref_count", refCountAfter));
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "SYS_LARGE_PAYLOAD_RELEASE_FAILED: failed to release large payload reference " + id, e);
        }
    }

    private Integer decrement(Connection connection, UUID id) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(DECREMENT_SQL)) {
            stmt.setObject(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? rs.getInt(1) : null;
            }
        }
    }

    private void deleteIfZero(Connection connection, UUID id) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(DELETE_IF_ZERO_SQL)) {
            stmt.setObject(1, id);
            stmt.executeUpdate();
        }
    }

    // --- RUNTIME reference ledger (D-F' FINDING-001 fix, migration V5) ---
    //
    // The fork's variable/process HARD-delete path never re-enters the custom serializer (no
    // synchronous release-on-delete hook is possible — docs/08 evidence), so a RUNTIME reference
    // cannot be released at delete time the way a HISTORY reference is released by ErasurePipeline/
    // RetentionEnforcementJob (both of which DO see the row being removed). This ledger instead
    // records "this RUNTIME variable is why this payload's ref_count includes +1"; a periodic
    // reconciliation sweep (LargeVariableExternalizationSweep, engine-side) compares ledger rows
    // against ACT_RU_VARIABLE's CURRENT externalization marker and releases any row whose variable
    // is gone or whose marker no longer matches — see that class for the actual reconciliation loop.

    /** @return the {@code payload_id} this variable is CURRENTLY on record as referencing, if any —
     *          used by {@code LargeVariablePostCommitExternalizer} to release a STALE reference when
     *          the SAME variable is re-externalized to different content (overwrite case). */
    public Optional<UUID> currentRuntimeReference(String engineId, String variableId) {
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(CURRENT_RUNTIME_REFERENCE_SQL)) {
            stmt.setString(1, engineId);
            stmt.setString(2, variableId);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? Optional.ofNullable((UUID) rs.getObject(1)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "SYS_LARGE_PAYLOAD_RUNTIME_REF_READ_FAILED: failed to read runtime reference for variable "
                            + variableId, e);
        }
    }

    /**
     * Upserts the ledger row for (engineId, variableId) to point at {@code payloadId} — called
     * AFTER {@link #storeAndAcquireReference} has already acquired the NEW reference, so the
     * payload this ledger row is about to stop pointing at (if different) is never left
     * under-referenced during the transition.
     */
    public void recordRuntimeReference(String engineId, String variableId, UUID payloadId, String contentHash) {
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(RECORD_RUNTIME_REFERENCE_SQL)) {
            stmt.setString(1, engineId);
            stmt.setString(2, variableId);
            stmt.setObject(3, payloadId);
            stmt.setString(4, contentHash);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "SYS_LARGE_PAYLOAD_RUNTIME_REF_WRITE_FAILED: failed to record runtime reference for variable "
                            + variableId, e);
        }
    }

    /** Called by the reconciliation sweep once the corresponding reference has been released. */
    public void deleteRuntimeReferenceRecord(String engineId, String variableId) {
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(DELETE_RUNTIME_REFERENCE_SQL)) {
            stmt.setString(1, engineId);
            stmt.setString(2, variableId);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "SYS_LARGE_PAYLOAD_RUNTIME_REF_DELETE_FAILED: failed to delete runtime reference record for variable "
                            + variableId, e);
        }
    }

    /** @return every RUNTIME reference ledger row for {@code engineId} — the reconciliation sweep's input set. */
    public List<RuntimeVariableReference> listRuntimeReferences(String engineId) {
        List<RuntimeVariableReference> refs = new ArrayList<>();
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(LIST_RUNTIME_REFERENCES_SQL)) {
            stmt.setString(1, engineId);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    refs.add(new RuntimeVariableReference(engineId, rs.getString("variable_id"),
                            (UUID) rs.getObject("payload_id"), rs.getString("content_hash")));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "SYS_LARGE_PAYLOAD_RUNTIME_REF_LIST_FAILED: failed to list runtime references for engine "
                            + engineId, e);
        }
        return refs;
    }
}
