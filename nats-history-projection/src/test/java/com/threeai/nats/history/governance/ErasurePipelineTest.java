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
        projectionStore = new ProjectionStore(dataSource);
        ErasureScopeResolver scopeResolver = new ErasureScopeResolver(dataSource);
        ErasureAuditLogger auditLogger = new ErasureAuditLogger(dataSource);
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
                    + "erasure_audit_log, erasure_scope_confirmation");
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
