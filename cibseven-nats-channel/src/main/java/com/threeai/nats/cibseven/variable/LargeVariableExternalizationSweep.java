package com.threeai.nats.cibseven.variable;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;

import com.threeai.nats.core.jetstream.SweepLeaderLease;
import com.threeai.nats.core.largepayload.ContentAddressedLargePayloadStore;
import com.threeai.nats.core.largepayload.ExternalizationMarker;
import com.threeai.nats.core.largepayload.LargeVariableExternalizationProperties;
import com.threeai.nats.core.largepayload.LargeVariableSerializerNames;
import com.threeai.nats.core.largepayload.RuntimeVariableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Leader-elected, catch-all externalization sweep (`docs/08-large-variable-externalization.md`
 * D-A' "post-commit/downstream" — the SLOW path, mirroring {@code HistoryOutboxRelay}'s scheduled-
 * leader pattern). Picks up any variable {@link LargeVariableSerializer#writeValue}'s fast-path
 * post-commit listener missed (node crash between commit and dispatch, a WARN-logged externalization
 * failure, ...): the fast path is best-effort by design (D-A'/docs/08 §5), so this sweep is what
 * makes eventual offload a GUARANTEE rather than a hope.
 *
 * <p><b>CODER-NOTE (raw SQL against {@code ACT_RU_VARIABLE}/{@code ACT_GE_BYTEARRAY}, not the
 * command/query API):</b> there is no {@code VariableInstanceQuery} filter for "byte-array length"
 * or "not yet externalized" — finding CANDIDATES is a genuinely bulk, cross-cutting scan a
 * per-row command-API walk cannot do efficiently. This is READ-ONLY (candidate discovery only —
 * the actual mutation still goes through {@link LargeVariablePostCommitExternalizer#externalizeNow},
 * the engine's own {@code DbEntityManager}/OCC path) and depends only on the stable, well-known
 * cibseven 7.x physical column names ({@code ID_}, {@code TYPE_}, {@code BYTEARRAY_ID_}, {@code
 * BYTES_}) — the SAME physical-schema stability D-E' already requires for "zero fork schema
 * change" to be possible at all. {@code TYPE_ IN (nats-ext-bytes, nats-ext-object, nats-ext-file)}
 * (never the built-in serializer names) is deliberate — see {@code LargeVariableSerializerNames}'
 * own CODER-NOTE: only rows THIS module's serializer wrote can ever be legitimate "staged, not yet
 * externalized" candidates.
 *
 * <p><b>FINDING-001 fix (Sentinel review) — {@link #reconcileRuntimeReferences}:</b> D-F' refcount/GC
 * was previously only wired on the HISTORY side (ErasurePipeline/RetentionEnforcementJob); RUNTIME
 * references were never released, so a hard-deleted variable/process's externalized PII payload
 * survived forever. The fork's byte-array delete path does not re-enter the custom serializer (no
 * synchronous release-on-delete hook is possible), so this reconciliation runs in the SAME
 * leader-elected cycle as candidate externalization: it compares every {@code
 * runtime_large_variable_ref} ledger row for this engine against {@code ACT_RU_VARIABLE}'s CURRENT
 * externalization marker for that variable id (batch-read, one query) and releases (ref_count--;
 * object deleted at 0) any row whose variable is gone or whose marker no longer matches — the
 * SAME basamak-2 {@code ReconciliationJob} idiom (compare authoritative source vs. derived state,
 * repair drift) applied to a ledger instead of two projection tables.
 */
public class LargeVariableExternalizationSweep {

    private static final Logger log = LoggerFactory.getLogger(LargeVariableExternalizationSweep.class);
    private static final int BATCH_SIZE = 500;

    private static final String FIND_CANDIDATES_SQL =
            "SELECT v.ID_ FROM ACT_RU_VARIABLE v JOIN ACT_GE_BYTEARRAY b ON v.BYTEARRAY_ID_ = b.ID_ "
          + "WHERE v.TYPE_ IN (?, ?, ?) AND octet_length(b.BYTES_) > ? LIMIT " + BATCH_SIZE;

    private static final String CURRENT_MARKERS_SQL =
            "SELECT v.ID_, b.BYTES_ FROM ACT_RU_VARIABLE v LEFT JOIN ACT_GE_BYTEARRAY b ON v.BYTEARRAY_ID_ = b.ID_ "
          + "WHERE v.ID_ = ANY(?)";

    private final DataSource engineDataSource;
    private final SweepLeaderLease leaderLease;
    private final LargeVariablePostCommitExternalizer externalizer;
    private final ContentAddressedLargePayloadStore largePayloadStore;
    private final LargeVariableExternalizationProperties properties;
    private final String engineId;

    public LargeVariableExternalizationSweep(DataSource engineDataSource, SweepLeaderLease leaderLease,
            LargeVariablePostCommitExternalizer externalizer, ContentAddressedLargePayloadStore largePayloadStore,
            LargeVariableExternalizationProperties properties, String engineId) {
        this.engineDataSource = engineDataSource;
        this.leaderLease = leaderLease;
        this.externalizer = externalizer;
        this.largePayloadStore = largePayloadStore;
        this.properties = properties;
        this.engineId = engineId;
    }

    @Scheduled(fixedDelayString = "${history.large-variable.sweep-cycle-period-seconds:60}000")
    public void sweepCycle() {
        if (!properties.isEnabled()) {
            return;
        }
        if (!leaderLease.tryAcquireOrRenew()) {
            return; // not the leader -- zero DB reads (basamak-1/2 parity, ADR-0002)
        }
        List<String> candidateVariableIds;
        try {
            candidateVariableIds = findCandidates();
        } catch (SQLException e) {
            // SYS_LARGE_VARIABLE_SWEEP_QUERY_FAILED -- cycle skipped, retried next cycle.
            log.error("Large-variable sweep candidate query failed — cycle skipped, retried next cycle",
                    kv("engine_id", engineId), e);
            return;
        }
        for (String variableId : candidateVariableIds) {
            externalizer.externalizeNow(variableId);
        }
        reconcileRuntimeReferences();
    }

    /**
     * FINDING-001 fix: releases every RUNTIME reference ledger row whose variable is gone or whose
     * current marker no longer matches the content this row recorded. Runs in the SAME leader-gated
     * cycle as candidate externalization (no separate lease needed).
     */
    void reconcileRuntimeReferences() {
        List<RuntimeVariableReference> ledgerRows;
        try {
            ledgerRows = largePayloadStore.listRuntimeReferences(engineId);
        } catch (Exception e) {
            // SYS_LARGE_VARIABLE_RECONCILIATION_LIST_FAILED -- cycle skipped, retried next cycle.
            log.error("Large-variable runtime-reference ledger read failed — reconciliation skipped, retried next cycle",
                    kv("engine_id", engineId), e);
            return;
        }
        if (ledgerRows.isEmpty()) {
            return;
        }
        Map<String, Optional<String>> currentMarkerHashByVariableId;
        try {
            currentMarkerHashByVariableId = currentMarkerHashes(ledgerRows);
        } catch (SQLException e) {
            // SYS_LARGE_VARIABLE_RECONCILIATION_QUERY_FAILED -- cycle skipped, retried next cycle.
            log.error("Large-variable reconciliation marker query failed — cycle skipped, retried next cycle",
                    kv("engine_id", engineId), e);
            return;
        }
        for (RuntimeVariableReference row : ledgerRows) {
            Optional<String> currentHash = currentMarkerHashByVariableId.getOrDefault(row.variableId(), Optional.empty());
            if (currentHash.isPresent() && currentHash.get().equals(row.contentHash())) {
                continue; // still live, still referencing the SAME content -- nothing to do
            }
            releaseStaleRuntimeReference(row, currentHash.isEmpty());
        }
    }

    /**
     * CODER-NOTE (production bug found via this class's own E2E probe test, FINDING-001 fix
     * verification): {@code runtime_large_variable_ref.payload_id} has an FK to {@code
     * projection_large_payload(id)} (migration V5) — deleting the LEDGER row must happen BEFORE
     * {@link ContentAddressedLargePayloadStore#releaseReference} attempts its own conditional
     * {@code DELETE FROM projection_large_payload} (at ref_count 0), or Postgres rejects that
     * DELETE with a foreign-key violation while the ledger row still references it. The reverse
     * order (delete ledger row first, THEN release) leaves only a benign crash window: if the
     * process dies between the two statements, the payload's {@code ref_count} is left one higher
     * than it should be (a storage-efficiency leak, picked up again only if this SAME variable id
     * ever reappears with the SAME content — otherwise permanent) — never a premature delete while
     * still referenced, which is the property that actually matters for D-F'/KVKK correctness.
     */
    private void releaseStaleRuntimeReference(RuntimeVariableReference row, boolean variableGone) {
        try {
            largePayloadStore.deleteRuntimeReferenceRecord(row.engineId(), row.variableId());
            largePayloadStore.releaseReference(row.payloadId());
            // BUS_LARGE_VARIABLE_RUNTIME_REFERENCE_RECONCILED -- DP-1: never logs the payload content.
            log.info("Runtime large-variable reference reconciled (released)", kv("variable_id", row.variableId()),
                    kv("engine_id", engineId), kv("payload_id", row.payloadId()),
                    kv("reason", variableGone ? "variable_deleted" : "content_changed"));
        } catch (Exception e) {
            // SYS_LARGE_VARIABLE_RECONCILIATION_RELEASE_FAILED -- this ONE row is retried next cycle
            // ONLY IF the ledger-row delete itself failed (still present -> reconciliation sees it
            // again); if the ledger delete succeeded but releaseReference then failed, the ledger
            // row is already gone -- see class Javadoc CODER-NOTE for why that is an accepted,
            // bounded trade-off (a leak, never a premature/incorrect delete).
            log.warn("Failed to release stale runtime large-variable reference — may be retried next cycle",
                    kv("variable_id", row.variableId()), kv("engine_id", engineId), e);
        }
    }

    /** @return CURRENT externalization-marker content hash per variable id in {@code ledgerRows}
     *          (absent if the variable's byte array is null/not-a-marker; entry MISSING entirely if
     *          the variable itself no longer exists). */
    private Map<String, Optional<String>> currentMarkerHashes(List<RuntimeVariableReference> ledgerRows) throws SQLException {
        String[] variableIds = ledgerRows.stream().map(RuntimeVariableReference::variableId).toArray(String[]::new);
        Map<String, Optional<String>> result = new HashMap<>();
        try (Connection connection = engineDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(CURRENT_MARKERS_SQL)) {
            stmt.setArray(1, connection.createArrayOf("VARCHAR", variableIds));
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String variableId = rs.getString(1);
                    byte[] bytes = rs.getBytes(2);
                    result.put(variableId, ExternalizationMarker.decode(bytes));
                }
            }
        }
        return result;
    }

    private List<String> findCandidates() throws SQLException {
        List<String> ids = new ArrayList<>();
        try (Connection connection = engineDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(FIND_CANDIDATES_SQL)) {
            stmt.setString(1, LargeVariableSerializerNames.BYTES);
            stmt.setString(2, LargeVariableSerializerNames.OBJECT);
            stmt.setString(3, LargeVariableSerializerNames.FILE);
            stmt.setInt(4, properties.getThresholdBytes());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getString(1));
                }
            }
        }
        return ids;
    }
}
