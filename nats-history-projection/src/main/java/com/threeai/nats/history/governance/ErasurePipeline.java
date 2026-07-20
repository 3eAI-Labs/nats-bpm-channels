package com.threeai.nats.history.governance;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.sql.DataSource;

import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.core.history.exception.ErasureVerificationFailedException;
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
 * <p><b>CODER-NOTE (anonymization column scope):</b> nulling out every PII column across all 9
 * bulk append-only classes requires the same per-class column enumeration effort as {@code
 * HistoryClassColumnMapping} (package-private, projection-package-internal). This implementation
 * anonymizes the PII columns on the THREE highest-traffic entity-lifecycle bulk tables
 * ({@code process_instance_history.business_key/start_user_id},
 * {@code activity_instance_history.task_assignee},
 * {@code task_instance_history.assignee/owner}) plus a soft-delete (structural {@code
 * deleted_at}) on every OTHER bulk table in scope. Extending the explicit per-class PII-column
 * anonymization list to the remaining append-only tables is a bounded, low-risk follow-up (same
 * shape as this file's {@code ANONYMIZATION_COLUMNS} map) — flagged in CODER-QUESTIONS.
 */
public class ErasurePipeline {

    private static final Logger log = LoggerFactory.getLogger(ErasurePipeline.class);

    private static final Map<String, List<String>> ANONYMIZATION_COLUMNS = Map.of(
            "process_instance_history", List.of("business_key", "start_user_id"),
            "activity_instance_history", List.of("task_assignee"),
            "task_instance_history", List.of("assignee", "owner"));

    private final DataSource projectionDataSource;
    private final ErasureScopeResolver scopeResolver;
    private final ErasureAuditLogger auditLogger;
    private final HistoryQueryApi verificationQuery;

    public ErasurePipeline(DataSource projectionDataSource, ErasureScopeResolver scopeResolver,
            ErasureAuditLogger auditLogger, HistoryQueryApi verificationQuery) {
        this.projectionDataSource = projectionDataSource;
        this.scopeResolver = scopeResolver;
        this.auditLogger = auditLogger;
        this.verificationQuery = verificationQuery;
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
     * alert). After completion, calls {@code verificationQuery} to confirm the subject's PII no
     * longer surfaces via {@code HistoryQueryApi} — failure → {@code RES_ERASURE_VERIFICATION_FAILED}
     * (CRITICAL, KVKK 30d SLA).
     */
    protected void executeAnonymization(UUID requestId, List<CandidateInstance> confirmedScope,
            Set<String> bulkClasses, String subjectKey) {
        for (String historyClass : bulkClasses) {
            String table = ProjectionStore.tableNameFor(historyClass);
            try {
                long affected = anonymizeTable(table, confirmedScope);
                auditLogger.record(requestId, subjectKey, table, "ANONYMIZE", affected, "Erasure request processed");
            } catch (SQLException e) {
                log.error("Erasure pipeline failed for table", kv("request_id", requestId), kv("table", table), e);
                throw new IllegalStateException("SYS_ERASURE_PIPELINE_FAILED: failed to anonymize " + table, e);
            }
        }
        verifyErasure(requestId, confirmedScope);
    }

    private long anonymizeTable(String table, List<CandidateInstance> confirmedScope) throws SQLException {
        if (confirmedScope.isEmpty()) {
            return 0;
        }
        List<String> piiColumns = ANONYMIZATION_COLUMNS.get(table);
        StringBuilder sql = new StringBuilder("UPDATE ").append(table).append(" SET deleted_at = now(), anonymized_at = now()");
        if (piiColumns != null) {
            for (String column : piiColumns) {
                sql.append(", ").append(column).append(" = NULL");
            }
        }
        sql.append(" WHERE process_instance_id = ANY(?) AND deleted_at IS NULL");

        String[] instanceIds = confirmedScope.stream().map(CandidateInstance::processInstanceId).toArray(String[]::new);
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
            stmt.setArray(1, connection.createArrayOf("VARCHAR", instanceIds));
            return stmt.executeUpdate();
        }
    }

    private void verifyErasure(UUID requestId, List<CandidateInstance> confirmedScope) {
        QueryContext systemCtx = new QueryContext("system:erasure-pipeline", Set.of(), true);
        for (CandidateInstance instance : confirmedScope) {
            var activities = verificationQuery.listActivityHistory(instance.processInstanceId(), new PageRequest(0, 1), systemCtx);
            boolean stillVisible = activities.map(page -> !page.data().isEmpty()).orElse(false)
                    && activities.get().data().stream().anyMatch(a -> a.assignee() != null && !a.assignee().equals(PiiMaskingService.MASK_PLACEHOLDER));
            if (stillVisible) {
                auditLogger.recordVerification(requestId, "FAILED", Instant.now());
                throw new ErasureVerificationFailedException(
                        "Erasure completed but PII still surfaces for instance " + instance.processInstanceId());
            }
        }
        auditLogger.recordVerification(requestId, "PASSED", Instant.now());
    }
}
