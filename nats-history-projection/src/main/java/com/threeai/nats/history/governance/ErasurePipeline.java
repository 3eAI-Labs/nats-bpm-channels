package com.threeai.nats.history.governance;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.sql.DataSource;

import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.core.history.exception.ErasureVerificationFailedException;
import com.threeai.nats.core.largepayload.ContentAddressedLargePayloadStore;
import com.threeai.nats.history.governance.ScopeResolution.CandidateInstance;
import com.threeai.nats.history.projection.ProjectionStore;
import com.threeai.nats.history.query.HistoryQueryApi;
import com.threeai.nats.history.query.PageRequest;
import com.threeai.nats.history.query.PiiMaskingService;
import com.threeai.nats.history.query.QueryContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for a data-subject erasure request (BR-PII-002/005, ADR-0017,
 * `03_classes/6_governance.md` §2). Routes by class: audit-critical target →
 * {@code BUS_ERASURE_REQUEST_LEGAL_HOLD_BLOCKED} (rejected, pseudonymization alternative
 * surfaced). Bulk target → delegates to {@link ErasureScopeResolver#resolve} first (BA-Q6).
 *
 * <p><b>CQ-3 (Levent, önerilen — DP-10/FR-G2 primary bulk-PII erasure targets):</b> {@code
 * ANONYMIZATION_COLUMNS} covers the value-bearing columns DP-10/FR-G2 name explicitly: VARINST/
 * DETAIL variable VALUES ({@code variable_value_text}, plus VARINST's large-payload companion —
 * see {@link #anonymizeVarinstLargePayloads}), TASKINST {@code name}/{@code description}, and
 * COMMENT's free-text {@code message} — on top of the identity columns already covered
 * (PROCINST business_key/start_user_id, ACTINST task_assignee, TASKINST assignee/owner). Every
 * configured column name is validated at anonymization time against {@link
 * ProjectionStore#allowedColumnsFor} — the SAME SQL-injection allowlist {@code
 * HistoryClassColumnMapping} enforces for the projection WRITE path (commit 17099d4) — so this
 * class can never interpolate a column name the schema does not actually expose. Remaining
 * lower-priority bulk PII (e.g. {@code attachment_history.attachment_name}/{@code url}) is a
 * bounded follow-up, flagged in CODER-QUESTIONS.
 */
public class ErasurePipeline {

    private static final Logger log = LoggerFactory.getLogger(ErasurePipeline.class);
    private static final java.util.regex.Pattern SAFE_IDENTIFIER = java.util.regex.Pattern.compile("^[a-z][a-z0-9_]*$");

    /** Keyed by ACT_HI class (not table name) so each column list can be validated against
     *  {@link ProjectionStore#allowedColumnsFor(String)} for that SAME class. */
    private static final Map<String, List<String>> ANONYMIZATION_COLUMNS = Map.of(
            HistoryClassNames.PROCINST, List.of("business_key", "start_user_id"),
            HistoryClassNames.ACTINST, List.of("task_assignee"),
            HistoryClassNames.VARINST, List.of("variable_value_text"), // variable_value_ref handled separately (large-payload cascade)
            HistoryClassNames.TASKINST, List.of("assignee", "owner", "task_name", "task_description"),
            HistoryClassNames.DETAIL, List.of("variable_value_text"),
            HistoryClassNames.COMMENT, List.of("message"));

    private final DataSource projectionDataSource;
    private final ErasureScopeResolver scopeResolver;
    private final ErasureAuditLogger auditLogger;
    private final HistoryQueryApi verificationQuery;
    private final ContentAddressedLargePayloadStore largePayloadStore;

    public ErasurePipeline(DataSource projectionDataSource, ErasureScopeResolver scopeResolver,
            ErasureAuditLogger auditLogger, HistoryQueryApi verificationQuery) {
        this.projectionDataSource = projectionDataSource;
        this.scopeResolver = scopeResolver;
        this.auditLogger = auditLogger;
        this.verificationQuery = verificationQuery;
        this.largePayloadStore = new ContentAddressedLargePayloadStore(projectionDataSource);
    }

    public ErasureRequestOutcome requestErasure(String subjectKey, ErasureTargetScope scope) {
        Set<String> bulkClasses = scope.historyClasses().stream()
                .filter(cls -> !HistoryClassNames.DEFAULT_AUDIT_CRITICAL_CLASSES.contains(cls))
                .collect(java.util.stream.Collectors.toSet());

        if (bulkClasses.isEmpty()) {
            log.warn("Erasure request rejected — all targeted classes are audit-critical (legal_hold)",
                    kv("subject_key_present", true)); // BUS_ERASURE_REQUEST_LEGAL_HOLD_BLOCKED (DP-1: no raw key logged)
            return new ErasureRequestOutcome.LegalHoldBlocked(
                    "Pseudonymization opt-in (BA-Q5) is available for OP_LOG.userId as an alternative to erasure");
        }

        ScopeResolution resolution = scopeResolver.resolve(subjectKey);
        if (resolution instanceof ScopeResolution.Ambiguous ambiguous) {
            return new ErasureRequestOutcome.ScopeConfirmationRequired(ambiguous.requestId());
        }
        ScopeResolution.Resolved resolved = (ScopeResolution.Resolved) resolution;
        log.info("Erasure request accepted", kv("request_id", resolved.requestId())); // BUS_ERASURE_REQUEST_ACCEPTED
        executeAnonymization(resolved.requestId(), resolved.confirmedScope(), bulkClasses, subjectKey);
        return new ErasureRequestOutcome.Accepted(resolved.requestId());
    }

    /**
     * Soft-delete → anonymize on the resolved (confirmed) scope, per bulk class table
     * ({@code deleted_at}/{@code anonymized_at} columns, {@code DB_SCHEMA.md §2.6}). Writes
     * {@code erasure_audit_log}. On SQL failure → {@code SYS_ERASURE_PIPELINE_FAILED} (retry +
     * alert). After completion, calls {@link #verifyErasure} to confirm every column actually
     * anonymized for THIS request is genuinely clean (FINDING-002: not just ACTINST assignee) —
     * failure → {@code RES_ERASURE_VERIFICATION_FAILED} (CRITICAL, KVKK 30d SLA).
     */
    protected void executeAnonymization(UUID requestId, List<CandidateInstance> confirmedScope,
            Set<String> bulkClasses, String subjectKey) {
        for (String historyClass : bulkClasses) {
            String table = ProjectionStore.tableNameFor(historyClass);
            try {
                List<UUID> orphanedPayloadIds = HistoryClassNames.VARINST.equals(historyClass)
                        ? findVarinstLargePayloadIds(confirmedScope)
                        : List.of();
                long affected = anonymizeTable(historyClass, table, confirmedScope);
                if (!orphanedPayloadIds.isEmpty()) {
                    deleteLargePayloads(orphanedPayloadIds);
                }
                auditLogger.record(requestId, subjectKey, table, "ANONYMIZE", affected, "Erasure request processed");
            } catch (SQLException e) {
                log.error("Erasure pipeline failed for table", kv("request_id", requestId), kv("table", table), e);
                throw new IllegalStateException("SYS_ERASURE_PIPELINE_FAILED: failed to anonymize " + table, e);
            }
        }
        verifyErasure(requestId, confirmedScope, bulkClasses);
    }

    /**
     * CQ-3: nulling {@code variable_instance_history.variable_value_ref} alone would leave the
     * REFERENCED {@code projection_large_payload} row (the actual byte-array variable value —
     * e.g. a serialized object/file, ARCH-Q1 "referans" pattern) orphaned but intact — a residual
     * PII leak {@code HistoryQueryApi}'s verification path does not surface either (it never
     * dereferences large payloads). The FK ({@code variable_instance_history_variable_value_ref_fkey})
     * is NOT deferrable, so the referenced ids must be captured BEFORE {@link #anonymizeTable}
     * nulls the column (breaking the reference) and deleted AFTER — deleting first (while the FK
     * still points at them) is rejected by Postgres.
     */
    private List<UUID> findVarinstLargePayloadIds(List<CandidateInstance> confirmedScope) throws SQLException {
        if (confirmedScope.isEmpty()) {
            return List.of();
        }
        String sql = "SELECT variable_value_ref FROM variable_instance_history "
                + "WHERE process_instance_id = ANY(?) AND deleted_at IS NULL AND variable_value_ref IS NOT NULL";
        String[] instanceIds = confirmedScope.stream().map(CandidateInstance::processInstanceId).toArray(String[]::new);
        List<UUID> ids = new java.util.ArrayList<>();
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setArray(1, connection.createArrayOf("VARCHAR", instanceIds));
            try (var rs = stmt.executeQuery()) {
                while (rs.next()) {
                    ids.add((UUID) rs.getObject(1));
                }
            }
        }
        return ids;
    }

    /**
     * basamak-3 D-F': releases (rather than unconditionally hard-deletes) each large-payload
     * reference — {@code projection_large_payload} is now content-addressed/dedup'd (D-B'/D-D'),
     * so ANOTHER still-live referrer (e.g. a byte-identical RUNTIME variable value elsewhere, or
     * the SAME content attached to a different {@code EXT_TASK_LOG}/{@code ATTACHMENT} row) may
     * legitimately still need the row; {@link ContentAddressedLargePayloadStore#releaseReference}
     * only physically deletes once the reference count reaches zero.
     */
    private void deleteLargePayloads(List<UUID> payloadIds) {
        for (UUID payloadId : payloadIds) {
            largePayloadStore.releaseReference(payloadId);
        }
    }

    private long anonymizeTable(String historyClass, String table, List<CandidateInstance> confirmedScope) throws SQLException {
        if (confirmedScope.isEmpty()) {
            return 0;
        }
        List<String> piiColumns = allowlistedAnonymizationColumns(historyClass);
        StringBuilder sql = new StringBuilder("UPDATE ").append(table).append(" SET deleted_at = now(), anonymized_at = now()");
        for (String column : piiColumns) {
            sql.append(", ").append(column).append(" = NULL");
        }
        if (HistoryClassNames.VARINST.equals(historyClass)) {
            sql.append(", variable_value_ref = NULL");
        }
        sql.append(" WHERE process_instance_id = ANY(?) AND deleted_at IS NULL");

        String[] instanceIds = confirmedScope.stream().map(CandidateInstance::processInstanceId).toArray(String[]::new);
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
            stmt.setArray(1, connection.createArrayOf("VARCHAR", instanceIds));
            return stmt.executeUpdate();
        }
    }

    /**
     * CQ-3: re-validates every configured {@code ANONYMIZATION_COLUMNS} entry against {@link
     * ProjectionStore#allowedColumnsFor} (the SAME allowlist {@code HistoryClassColumnMapping}
     * enforces for the projection write path, commit 17099d4) plus the identical {@code
     * SAFE_IDENTIFIER} shape check, before it is ever interpolated into SQL. These entries are
     * curated compile-time literals (never attacker-influenceable), so this is defense-in-depth
     * consistency with the write-path discipline rather than a live injection fix — but it means
     * a future edit to this map that typos a column name, or a schema change that drops one,
     * fails LOUD (IllegalStateException) instead of silently building invalid/unsafe SQL.
     */
    private List<String> allowlistedAnonymizationColumns(String historyClass) {
        List<String> configured = ANONYMIZATION_COLUMNS.getOrDefault(historyClass, List.of());
        if (configured.isEmpty()) {
            return configured;
        }
        Set<String> allowed = ProjectionStore.allowedColumnsFor(historyClass);
        for (String column : configured) {
            if (!SAFE_IDENTIFIER.matcher(column).matches() || !allowed.contains(column)) {
                throw new IllegalStateException(
                        "ANONYMIZATION_COLUMNS entry '" + column + "' for " + historyClass
                                + " is not on the projection allowlist — refusing to build erasure SQL");
            }
        }
        return configured;
    }

    /**
     * FINDING-002 (faz-5 review, MINOR): previously only re-checked {@code
     * activity_instance_history.task_assignee} via {@link HistoryQueryApi} — the OTHER CQ-3
     * primary surfaces (VARINST/DETAIL value, TASKINST name/description, COMMENT message) were
     * anonymized but never re-verified, so {@code RES_ERASURE_VERIFICATION_FAILED} could not
     * actually catch a regression in any of them. Two complementary checks per confirmed
     * instance now run: (1) the EXISTING {@code HistoryQueryApi}-facing check for ACTINST
     * assignee (proves the tenant-facing QUERY surface — not just the raw column — no longer
     * leaks, when ACTINST was in scope), and (2) a NEW raw-column check, direct against the
     * projection DB, for every {@code ANONYMIZATION_COLUMNS} entry of every class actually
     * targeted by THIS request ({@code bulkClasses} — classes NOT in scope are correctly left
     * unchecked, since their data was never anonymized and a real value there is not a
     * regression). The raw-column check exists because {@code HistoryQueryApi} has no query
     * surface at all for COMMENT or VARINST, and {@code listTaskHistory}'s {@code
     * TaskInstanceHistory} openapi schema does not carry {@code description} — this basamak does
     * not add columns to a locked contract merely to widen an internal verification check.
     */
    /** {@code protected} — same test-seam rationale as {@link #executeAnonymization}: lets
     *  {@code ErasurePipelineTest} exercise the FAILURE path directly (seed a still-populated
     *  column, bypassing the normal anonymize step) without needing a mid-pipeline injection hook. */
    protected void verifyErasure(UUID requestId, List<CandidateInstance> confirmedScope, Set<String> bulkClasses) {
        QueryContext systemCtx = new QueryContext("system:erasure-pipeline", Set.of(), true);
        for (CandidateInstance instance : confirmedScope) {
            if (bulkClasses.contains(HistoryClassNames.ACTINST) && actinstAssigneeStillVisible(instance, systemCtx)) {
                failVerification(requestId, instance, "activity_instance_history.task_assignee still visible via HistoryQueryApi");
            }
            for (String historyClass : bulkClasses) {
                String populatedColumn = firstStillPopulatedColumn(historyClass, instance.processInstanceId());
                if (populatedColumn != null) {
                    failVerification(requestId, instance, historyClass + "." + populatedColumn + " still populated");
                }
            }
        }
        auditLogger.recordVerification(requestId, "PASSED", Instant.now());
    }

    private boolean actinstAssigneeStillVisible(CandidateInstance instance, QueryContext systemCtx) {
        var activities = verificationQuery.listActivityHistory(instance.processInstanceId(), new PageRequest(0, 1), systemCtx);
        return activities.map(page -> !page.data().isEmpty()).orElse(false)
                && activities.get().data().stream().anyMatch(a -> a.assignee() != null && !a.assignee().equals(PiiMaskingService.MASK_PLACEHOLDER));
    }

    /**
     * @return the name of the first {@code ANONYMIZATION_COLUMNS} column (or, for VARINST, the
     *         large-payload {@code variable_value_ref} FK) still non-NULL for this instance in
     *         this class's table, or {@code null} if fully clean.
     */
    private String firstStillPopulatedColumn(String historyClass, String processInstanceId) {
        List<String> columns = allowlistedAnonymizationColumns(historyClass);
        List<String> checkedColumns = HistoryClassNames.VARINST.equals(historyClass)
                ? concatWithVariableValueRef(columns)
                : columns;
        if (checkedColumns.isEmpty()) {
            return null;
        }
        String table = ProjectionStore.tableNameFor(historyClass);
        // No LIMIT: scoped to ONE instance via the SAME idx_*_process_instance index every other
        // per-instance query in this module already relies on -- an artificial cap here would
        // risk a false-negative PASS on a high-volume append-only class (e.g. DETAIL) if any row
        // beyond the cap were still populated.
        StringBuilder sql = new StringBuilder("SELECT ").append(String.join(", ", checkedColumns))
                .append(" FROM ").append(table).append(" WHERE process_instance_id = ?");
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
            stmt.setString(1, processInstanceId);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    for (int i = 0; i < checkedColumns.size(); i++) {
                        if (rs.getObject(i + 1) != null) {
                            return checkedColumns.get(i);
                        }
                    }
                }
            }
            return null;
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to verify erasure for " + historyClass + " / " + processInstanceId, e);
        }
    }

    private List<String> concatWithVariableValueRef(List<String> columns) {
        List<String> withRef = new java.util.ArrayList<>(columns);
        withRef.add("variable_value_ref");
        return withRef;
    }

    private void failVerification(UUID requestId, CandidateInstance instance, String reason) {
        auditLogger.recordVerification(requestId, "FAILED", Instant.now());
        log.error("Erasure verification failed", kv("request_id", requestId),
                kv("process_instance_id", instance.processInstanceId()), kv("reason", reason));
        throw new ErasureVerificationFailedException(
                "Erasure completed but PII still surfaces for instance " + instance.processInstanceId()
                        + " (" + reason + ")");
    }
}
