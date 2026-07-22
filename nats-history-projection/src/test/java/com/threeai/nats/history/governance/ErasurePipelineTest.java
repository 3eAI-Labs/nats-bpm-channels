package com.threeai.nats.history.governance;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.threeai.nats.core.db.SqlMigrationRunner;
import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.history.projection.EntityHistoryRecord;
import com.threeai.nats.history.projection.LogHistoryRecord;
import com.threeai.nats.history.projection.ProjectionStore;
import com.threeai.nats.history.query.HistoryQueryApi;
import com.threeai.nats.history.query.HistoryQueryAuthzSpi;
import com.threeai.nats.history.query.PiiMaskingService;
import com.threeai.nats.history.query.QueryContext;
import com.threeai.nats.history.query.QueryOperation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class ErasurePipelineTest {

    private static PostgreSQLContainer<?> postgres;
    private static PGSimpleDataSource dataSource;
    private static ProjectionStore projectionStore;
    private static ErasurePipeline pipeline;
    private static ErasureAuditLogger auditLogger;

    private static final HistoryQueryAuthzSpi ALLOW_ALL = new HistoryQueryAuthzSpi() {
        public boolean isAuthorized(QueryContext ctx, QueryOperation operation) {
            return true;
        }

        public boolean hasPiiViewPermission(QueryContext ctx) {
            return true;
        }
    };

    @BeforeAll
    static void startContainer() {
        postgres = new PostgreSQLContainer<>("postgres:16");
        postgres.start();
        dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgres.getJdbcUrl());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/projection/V1__entity_lifecycle_tables.sql");
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/projection/V2__append_log_tables.sql");
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/projection/V3__control_plane_and_compliance.sql");
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/projection/V4__large_payload_content_addressing.sql");
        projectionStore = new ProjectionStore(dataSource);
        ErasureScopeResolver scopeResolver = new ErasureScopeResolver(dataSource);
        auditLogger = new ErasureAuditLogger(dataSource);
        HistoryQueryApi verificationQuery = new HistoryQueryApi(dataSource, new PiiMaskingService(), ALLOW_ALL);
        pipeline = new ErasurePipeline(dataSource, scopeResolver, auditLogger, verificationQuery);
    }

    @AfterAll
    static void stopContainer() {
        postgres.stop();
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection c = dataSource.getConnection(); java.sql.Statement stmt = c.createStatement()) {
            stmt.execute("TRUNCATE process_instance_history, activity_instance_history, "
                    + "variable_instance_history, projection_large_payload, task_instance_history, "
                    + "comment_history, variable_detail_history, "
                    + "erasure_audit_log, erasure_scope_confirmation CASCADE");
        }
    }

    private void insertProcessInstance(String processInstanceId, String businessKey, Instant startTime) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("businessKey", businessKey);
        fields.put("startUserId", "real-user-42");
        fields.put("startTime", java.sql.Timestamp.from(startTime));
        projectionStore.upsertEntity(HistoryClassNames.PROCINST,
                new EntityHistoryRecord("camunda", processInstanceId, processInstanceId, 1, startTime, fields));
    }

    @Test
    void requestErasure_onlyAuditCriticalScope_legalHoldBlocked() {
        ErasureTargetScope scope = new ErasureTargetScope(Set.of(HistoryClassNames.OP_LOG, HistoryClassNames.INCIDENT));

        ErasureRequestOutcome outcome = pipeline.requestErasure("msisdn-1", scope);

        assertThat(outcome).isInstanceOf(ErasureRequestOutcome.LegalHoldBlocked.class);
    }

    @Test
    void requestErasure_bulkScope_unambiguous_accepted_anonymizesRow() throws Exception {
        insertProcessInstance("proc-1", "msisdn-2", Instant.now());
        ErasureTargetScope scope = new ErasureTargetScope(Set.of(HistoryClassNames.PROCINST));

        ErasureRequestOutcome outcome = pipeline.requestErasure("msisdn-2", scope);

        assertThat(outcome).isInstanceOf(ErasureRequestOutcome.Accepted.class);
        assertThat(businessKeyOf("proc-1")).isNull(); // anonymized
        assertThat(deletedAtSet("proc-1")).isTrue();
    }

    @Test
    void requestErasure_bulkScope_ambiguous_returnsScopeConfirmationRequired() throws Exception {
        insertProcessInstance("proc-old", "msisdn-3", Instant.now().minus(java.time.Duration.ofDays(400)));
        insertProcessInstance("proc-new", "msisdn-3", Instant.now());
        ErasureTargetScope scope = new ErasureTargetScope(Set.of(HistoryClassNames.PROCINST));

        ErasureRequestOutcome outcome = pipeline.requestErasure("msisdn-3", scope);

        assertThat(outcome).isInstanceOf(ErasureRequestOutcome.ScopeConfirmationRequired.class);
        // Neither instance touched yet -- awaiting explicit confirmation.
        assertThat(businessKeyOf("proc-old")).isEqualTo("msisdn-3");
        assertThat(businessKeyOf("proc-new")).isEqualTo("msisdn-3");
    }

    @Test
    void requestErasure_mixedScope_onlyBulkClassesConsidered() throws Exception {
        insertProcessInstance("proc-4", "msisdn-4", Instant.now());
        ErasureTargetScope scope = new ErasureTargetScope(Set.of(HistoryClassNames.PROCINST, HistoryClassNames.OP_LOG));

        ErasureRequestOutcome outcome = pipeline.requestErasure("msisdn-4", scope);

        // PROCINST (bulk) still processed even though OP_LOG (audit-critical) is also in scope.
        assertThat(outcome).isInstanceOf(ErasureRequestOutcome.Accepted.class);
        assertThat(businessKeyOf("proc-4")).isNull();
    }

    /**
     * CQ-3 (Levent, önerilen): DP-10/FR-G2 primary bulk-PII erasure targets — VARINST/DETAIL
     * variable VALUES (including the large-payload companion), TASKINST name/description, and
     * COMMENT free-text message — must all be anonymized, and doing so must NOT spuriously trip
     * {@code RES_ERASURE_VERIFICATION_FAILED} (verification only inspects {@code
     * ActivityHistoryEntry.assignee} via {@code HistoryQueryApi}, untouched by this scenario).
     */
    @Test
    void requestErasure_bulkScope_anonymizesVarinstDetailTaskinstCommentValues() throws Exception {
        String processInstanceId = "proc-5";
        insertProcessInstance(processInstanceId, "msisdn-5", Instant.now());

        // VARINST: scalar value + a large-payload companion (byte-array variable value).
        Map<String, Object> varinstFields = new LinkedHashMap<>();
        varinstFields.put("variableName", "ssn");
        varinstFields.put("variableValueText", "real-secret-value");
        projectionStore.upsertEntity(HistoryClassNames.VARINST,
                new EntityHistoryRecord("camunda", "var-1", processInstanceId, 1, Instant.now(), varinstFields));
        java.util.UUID payloadId = projectionStore.storeLargePayload(
                "byte-array-pii-payload".getBytes(java.nio.charset.StandardCharsets.UTF_8), "variable_instance_history");
        linkVarinstToLargePayload("var-1", payloadId);

        // TASKINST: name via the normal field-map path; description seeded directly (see CODER-NOTE
        // in ErasurePipeline -- HistoryEventFieldExtractor does not currently emit a description field).
        Map<String, Object> taskFields = new LinkedHashMap<>();
        taskFields.put("name", "Approve refund");
        projectionStore.upsertEntity(HistoryClassNames.TASKINST,
                new EntityHistoryRecord("camunda", "task-1", processInstanceId, 1, Instant.now(), taskFields));
        seedTaskDescription("task-1", "Sensitive free-text description");

        // COMMENT (append-only): free-text message.
        Map<String, Object> commentFields = new LinkedHashMap<>();
        commentFields.put("message", "Called the customer at their real phone number");
        projectionStore.insertLogEvent(HistoryClassNames.COMMENT,
                new LogHistoryRecord("camunda", processInstanceId, "evt-comment-1", "COMMENT_CREATE", 1, Instant.now(), commentFields));

        // DETAIL (append-only): variable value (textValue -> variable_value_text override).
        Map<String, Object> detailFields = new LinkedHashMap<>();
        detailFields.put("textValue", "another-real-secret-value");
        projectionStore.insertLogEvent(HistoryClassNames.DETAIL,
                new LogHistoryRecord("camunda", processInstanceId, "evt-detail-1", "DETAIL_CREATE", 1, Instant.now(), detailFields));

        ErasureTargetScope scope = new ErasureTargetScope(
                Set.of(HistoryClassNames.VARINST, HistoryClassNames.TASKINST, HistoryClassNames.COMMENT, HistoryClassNames.DETAIL));

        ErasureRequestOutcome outcome = pipeline.requestErasure("msisdn-5", scope);

        assertThat(outcome).isInstanceOf(ErasureRequestOutcome.Accepted.class); // RES_ERASURE_VERIFICATION_FAILED NOT thrown
        assertThat(varinstValueTextOf("var-1")).isNull();
        assertThat(varinstValueRefOf("var-1")).isNull();
        assertThat(largePayloadExists(payloadId)).isFalse(); // residual-PII cascade: companion row deleted, not just un-referenced
        assertThat(taskNameOf("task-1")).isNull();
        assertThat(taskDescriptionOf("task-1")).isNull();
        assertThat(commentMessageOf(processInstanceId)).isNull();
        assertThat(detailValueTextOf(processInstanceId)).isNull();
    }

    /**
     * FINDING-002 (faz-5 review, MINOR): proves {@code verifyErasure} actually DETECTS a still-
     * populated CQ-3 surface, one class at a time, not just ACTINST assignee. Seeds a row whose
     * anonymization column is still populated (bypassing the normal anonymize step, simulating a
     * regression), calls {@code verifyErasure} directly (test-seam, {@code protected}), and
     * asserts {@code ErasureVerificationFailedException} fires + {@code erasure_audit_log}
     * records a FAILED verification.
     */
    @Test
    void verifyErasure_varinstValueStillPopulated_throwsAndRecordsFailure() throws Exception {
        String processInstanceId = "proc-verify-varinst";
        Map<String, Object> varinstFields = new LinkedHashMap<>();
        varinstFields.put("variableName", "ssn");
        varinstFields.put("variableValueText", "still-here-real-value"); // NOT anonymized -- regression simulation
        projectionStore.upsertEntity(HistoryClassNames.VARINST,
                new EntityHistoryRecord("camunda", "var-verify-1", processInstanceId, 1, Instant.now(), varinstFields));

        java.util.UUID requestId = java.util.UUID.randomUUID();
        auditLogger.record(requestId, "msisdn-verify-varinst", "variable_instance_history", "ANONYMIZE", 1, "test seed");
        var scope = java.util.List.of(new ScopeResolution.CandidateInstance(processInstanceId, Instant.now(), Instant.now()));

        assertThatThrownByVerification(requestId, scope, Set.of(HistoryClassNames.VARINST));
        assertThat(verificationStatusOf(requestId)).isEqualTo("FAILED");
    }

    @Test
    void verifyErasure_commentMessageStillPopulated_throwsAndRecordsFailure() throws Exception {
        String processInstanceId = "proc-verify-comment";
        Map<String, Object> commentFields = new LinkedHashMap<>();
        commentFields.put("message", "still-here-real-message"); // NOT anonymized -- regression simulation
        projectionStore.insertLogEvent(HistoryClassNames.COMMENT,
                new LogHistoryRecord("camunda", processInstanceId, "evt-verify-comment", "COMMENT_CREATE", 1, Instant.now(), commentFields));

        java.util.UUID requestId = java.util.UUID.randomUUID();
        auditLogger.record(requestId, "msisdn-verify-comment", "comment_history", "ANONYMIZE", 1, "test seed");
        var scope = java.util.List.of(new ScopeResolution.CandidateInstance(processInstanceId, Instant.now(), Instant.now()));

        assertThatThrownByVerification(requestId, scope, Set.of(HistoryClassNames.COMMENT));
        assertThat(verificationStatusOf(requestId)).isEqualTo("FAILED");
    }

    /** {@code bulkClasses} deliberately does NOT include VARINST here -- an un-anonymized VARINST
     *  row for the SAME instance must NOT fail verification when VARINST was never targeted
     *  (TASKINST is the in-scope class instead, with no rows at all for this instance -- an
     *  in-scope class with nothing to find is the OTHER trivially-passing case this proves). */
    @Test
    void verifyErasure_columnOutOfRequestedScope_notChecked_passes() throws Exception {
        String processInstanceId = "proc-verify-out-of-scope";
        Map<String, Object> varinstFields = new LinkedHashMap<>();
        varinstFields.put("variableValueText", "irrelevant-untouched-value");
        projectionStore.upsertEntity(HistoryClassNames.VARINST,
                new EntityHistoryRecord("camunda", "var-out-of-scope", processInstanceId, 1, Instant.now(), varinstFields));

        java.util.UUID requestId = java.util.UUID.randomUUID();
        auditLogger.record(requestId, "msisdn-out-of-scope", "task_instance_history", "ANONYMIZE", 0, "test seed");
        var scope = java.util.List.of(new ScopeResolution.CandidateInstance(processInstanceId, Instant.now(), Instant.now()));

        pipeline.verifyErasure(requestId, scope, Set.of(HistoryClassNames.TASKINST)); // does not throw
        assertThat(verificationStatusOf(requestId)).isEqualTo("PASSED");
    }

    private void assertThatThrownByVerification(java.util.UUID requestId,
            java.util.List<ScopeResolution.CandidateInstance> scope, Set<String> bulkClasses) {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> pipeline.verifyErasure(requestId, scope, bulkClasses))
                .isInstanceOf(com.threeai.nats.core.history.exception.ErasureVerificationFailedException.class);
    }

    private String verificationStatusOf(java.util.UUID requestId) throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "SELECT verification_status FROM erasure_audit_log WHERE request_id = ? "
                   + "ORDER BY performed_at DESC LIMIT 1")) {
            stmt.setObject(1, requestId);
            try (ResultSet rs = stmt.executeQuery()) {
                assertThat(rs.next()).isTrue();
                return rs.getString(1);
            }
        }
    }

    private void linkVarinstToLargePayload(String variableInstanceId, java.util.UUID payloadId) throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "UPDATE variable_instance_history SET variable_value_ref = ? WHERE variable_instance_id = ?")) {
            stmt.setObject(1, payloadId);
            stmt.setString(2, variableInstanceId);
            stmt.executeUpdate();
        }
    }

    private void seedTaskDescription(String taskId, String description) throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "UPDATE task_instance_history SET task_description = ? WHERE task_id = ?")) {
            stmt.setString(1, description);
            stmt.setString(2, taskId);
            stmt.executeUpdate();
        }
    }

    private String varinstValueTextOf(String variableInstanceId) throws Exception {
        return singleStringColumn("SELECT variable_value_text FROM variable_instance_history WHERE variable_instance_id = ?", variableInstanceId);
    }

    private String varinstValueRefOf(String variableInstanceId) throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "SELECT variable_value_ref FROM variable_instance_history WHERE variable_instance_id = ?")) {
            stmt.setString(1, variableInstanceId);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return (String) rs.getObject(1);
            }
        }
    }

    private boolean largePayloadExists(java.util.UUID payloadId) throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement("SELECT count(*) FROM projection_large_payload WHERE id = ?")) {
            stmt.setObject(1, payloadId);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getLong(1) > 0;
            }
        }
    }

    private String taskNameOf(String taskId) throws Exception {
        return singleStringColumn("SELECT task_name FROM task_instance_history WHERE task_id = ?", taskId);
    }

    private String taskDescriptionOf(String taskId) throws Exception {
        return singleStringColumn("SELECT task_description FROM task_instance_history WHERE task_id = ?", taskId);
    }

    private String commentMessageOf(String processInstanceId) throws Exception {
        return singleStringColumn("SELECT message FROM comment_history WHERE process_instance_id = ?", processInstanceId);
    }

    private String detailValueTextOf(String processInstanceId) throws Exception {
        return singleStringColumn("SELECT variable_value_text FROM variable_detail_history WHERE process_instance_id = ?", processInstanceId);
    }

    private String singleStringColumn(String sql, String param) throws Exception {
        try (Connection c = dataSource.getConnection(); PreparedStatement stmt = c.prepareStatement(sql)) {
            stmt.setString(1, param);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }

    private String businessKeyOf(String processInstanceId) throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "SELECT business_key FROM process_instance_history WHERE process_instance_id = ?")) {
            stmt.setString(1, processInstanceId);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }

    private boolean deletedAtSet(String processInstanceId) throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "SELECT deleted_at FROM process_instance_history WHERE process_instance_id = ?")) {
            stmt.setString(1, processInstanceId);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getTimestamp(1) != null;
            }
        }
    }
}
