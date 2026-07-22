package com.threeai.nats.core.largepayload;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC client for the basamak-3 unified, content-addressed {@code projection_large_payload} store
 * (`docs/08-large-variable-externalization.md` D-B'/D-D'/D-F', migration
 * {@code V4__large_payload_content_addressing.sql}). Engine-neutral, framework-agnostic — takes any
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
}
