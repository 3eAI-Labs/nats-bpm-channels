package com.threeai.nats.history.governance;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import com.threeai.nats.core.db.SqlMigrationRunner;
import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.history.projection.EntityHistoryRecord;
import com.threeai.nats.history.projection.ProjectionStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class ErasureScopeResolverTest {

    private static PostgreSQLContainer<?> postgres;
    private static PGSimpleDataSource dataSource;
    private static ProjectionStore projectionStore;
    private static ErasureScopeResolver resolver;

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
        resolver = new ErasureScopeResolver(dataSource);
    }

    @AfterAll
    static void stopContainer() {
        postgres.stop();
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection c = dataSource.getConnection(); java.sql.Statement stmt = c.createStatement()) {
            stmt.execute("TRUNCATE process_instance_history, erasure_scope_confirmation");
        }
    }

    private void insertProcessInstance(String processInstanceId, String businessKey, Instant startTime) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("businessKey", businessKey);
        fields.put("startTime", java.sql.Timestamp.from(startTime));
        projectionStore.upsertEntity(HistoryClassNames.PROCINST,
                new EntityHistoryRecord("camunda", processInstanceId, processInstanceId, 1, startTime, fields));
    }

    @Test
    void resolve_singleTimeCluster_resolvedUnambiguously() {
        Instant now = Instant.now();
        insertProcessInstance("proc-1", "msisdn-1", now);
        insertProcessInstance("proc-2", "msisdn-1", now.plusSeconds(3600));

        ScopeResolution result = resolver.resolve("msisdn-1");

        assertThat(result).isInstanceOf(ScopeResolution.Resolved.class);
        assertThat(((ScopeResolution.Resolved) result).confirmedScope()).hasSize(2);
    }

    @Test
    void resolve_timeDisjointGroups_returnsAmbiguous() {
        Instant longAgo = Instant.now().minus(java.time.Duration.ofDays(400));
        Instant recent = Instant.now();
        insertProcessInstance("proc-old", "msisdn-reused", longAgo);
        insertProcessInstance("proc-new", "msisdn-reused", recent);

        ScopeResolution result = resolver.resolve("msisdn-reused");

        assertThat(result).isInstanceOf(ScopeResolution.Ambiguous.class);
        assertThat(((ScopeResolution.Ambiguous) result).candidateInstances()).hasSize(2);
    }

    @Test
    void resolve_noInstances_resolvedWithEmptyScope() {
        ScopeResolution result = resolver.resolve("never-seen-key");

        assertThat(result).isInstanceOf(ScopeResolution.Resolved.class);
        assertThat(((ScopeResolution.Resolved) result).confirmedScope()).isEmpty();
    }

    @Test
    void confirmScope_updatesConfirmationRow() throws Exception {
        Instant longAgo = Instant.now().minus(java.time.Duration.ofDays(400));
        insertProcessInstance("proc-a", "msisdn-x", longAgo);
        insertProcessInstance("proc-b", "msisdn-x", Instant.now());
        ScopeResolution.Ambiguous ambiguous = (ScopeResolution.Ambiguous) resolver.resolve("msisdn-x");

        resolver.confirmScope(ambiguous.requestId(), java.util.List.of("proc-b"), "compliance-officer-1");

        try (Connection c = dataSource.getConnection();
             var stmt = c.prepareStatement("SELECT status, confirmed_by FROM erasure_scope_confirmation WHERE request_id = ?")) {
            stmt.setObject(1, ambiguous.requestId());
            try (var rs = stmt.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("status")).isEqualTo("CONFIRMED");
                assertThat(rs.getString("confirmed_by")).isEqualTo("compliance-officer-1");
            }
        }
    }
}
