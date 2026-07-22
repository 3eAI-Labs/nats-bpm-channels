package com.threeai.nats.history.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

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

/** Real Postgres (Testcontainers) — proves çekirdek-4 read patterns against real projection rows. */
@Testcontainers
class HistoryQueryApiTest {

    private static PostgreSQLContainer<?> postgres;
    private static PGSimpleDataSource dataSource;
    private static ProjectionStore projectionStore;
    private static HistoryQueryApi queryApi;

    private static final HistoryQueryAuthzSpi ALLOW_ALL = new HistoryQueryAuthzSpi() {
        public boolean isAuthorized(QueryContext ctx, QueryOperation operation) {
            return true;
        }

        public boolean hasPiiViewPermission(QueryContext ctx) {
            return true;
        }
    };

    private static final HistoryQueryAuthzSpi DENY_ALL = new HistoryQueryAuthzSpi() {
        public boolean isAuthorized(QueryContext ctx, QueryOperation operation) {
            return false;
        }

        public boolean hasPiiViewPermission(QueryContext ctx) {
            return false;
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
        queryApi = new HistoryQueryApi(dataSource, new PiiMaskingService(), ALLOW_ALL);
    }

    @AfterAll
    static void stopContainer() {
        postgres.stop();
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection c = dataSource.getConnection(); java.sql.Statement stmt = c.createStatement()) {
            stmt.execute("TRUNCATE process_instance_history, activity_instance_history, task_instance_history, "
                    + "variable_detail_history CASCADE");
        }
    }

    private QueryContext ctx() {
        return new QueryContext("caller-1", Set.of(), true);
    }

    private void insertProcessInstance(String processInstanceId, String businessKey, String defKey, Instant startTime) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("businessKey", businessKey);
        fields.put("processDefinitionKey", defKey);
        fields.put("state", "ACTIVE");
        fields.put("startTime", java.sql.Timestamp.from(startTime));
        projectionStore.upsertEntity(HistoryClassNames.PROCINST,
                new EntityHistoryRecord("camunda", processInstanceId, processInstanceId, 1, startTime, fields));
    }

    @Test
    void getProcessInstanceHistory_found_returnsSummary() {
        insertProcessInstance("proc-1", "biz-1", "myDef", Instant.now());

        var result = queryApi.getProcessInstanceHistory("proc-1", ctx());

        assertThat(result).isPresent();
        assertThat(result.get().businessKey()).isEqualTo("biz-1");
    }

    @Test
    void getProcessInstanceHistory_notFound_returnsEmpty() {
        var result = queryApi.getProcessInstanceHistory("does-not-exist", ctx());

        assertThat(result).isEmpty();
    }

    @Test
    void getProcessInstanceHistory_notAuthorized_throwsSecurityException() {
        HistoryQueryApi deniedApi = new HistoryQueryApi(dataSource, new PiiMaskingService(), DENY_ALL);

        assertThatThrownBy(() -> deniedApi.getProcessInstanceHistory("proc-1", ctx()))
                .isInstanceOf(SecurityException.class);
    }

    @Test
    void listProcessInstanceHistory_noFilter_rejectedAsUnsupportedPattern() {
        ProcessInstanceListQuery query = new ProcessInstanceListQuery(null, null, null, null, new PageRequest(0, 20));

        assertThatThrownBy(() -> queryApi.listProcessInstanceHistory(query, ctx()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void listProcessInstanceHistory_byBusinessKey_returnsMatches() {
        insertProcessInstance("proc-2", "biz-shared", "myDef", Instant.now());
        insertProcessInstance("proc-3", "biz-shared", "myDef", Instant.now());
        insertProcessInstance("proc-4", "biz-other", "myDef", Instant.now());

        ProcessInstanceListQuery query = new ProcessInstanceListQuery("biz-shared", null, null, null, new PageRequest(0, 20));
        PagedResponse<ProcessInstanceSummary> result = queryApi.listProcessInstanceHistory(query, ctx());

        assertThat(result.data()).hasSize(2);
        assertThat(result.total()).isEqualTo(2);
    }

    @Test
    void listProcessInstanceHistory_byDefinitionAndTimeRange_returnsMatches() {
        Instant now = Instant.now();
        insertProcessInstance("proc-5", "biz-x", "targetDef", now);
        insertProcessInstance("proc-6", "biz-y", "otherDef", now);

        ProcessInstanceListQuery query = new ProcessInstanceListQuery(null, "targetDef",
                now.minusSeconds(60), now.plusSeconds(60), new PageRequest(0, 20));
        PagedResponse<ProcessInstanceSummary> result = queryApi.listProcessInstanceHistory(query, ctx());

        assertThat(result.data()).hasSize(1);
        assertThat(result.data().get(0).processInstanceId()).isEqualTo("proc-5");
    }

    @Test
    void listActivityHistory_instanceNotFound_returnsEmpty() {
        var result = queryApi.listActivityHistory("no-such-instance", new PageRequest(0, 20), ctx());

        assertThat(result).isEmpty();
    }

    @Test
    void listActivityHistory_instanceExists_returnsActivities() {
        insertProcessInstance("proc-7", "biz-7", "def7", Instant.now());
        Map<String, Object> actFields = new LinkedHashMap<>();
        actFields.put("activityId", "task1");
        actFields.put("activityType", "userTask");
        projectionStore.upsertEntity(HistoryClassNames.ACTINST,
                new EntityHistoryRecord("camunda", "act-1", "proc-7", 1, Instant.now(), actFields));

        var result = queryApi.listActivityHistory("proc-7", new PageRequest(0, 20), ctx());

        assertThat(result).isPresent();
        assertThat(result.get().data()).hasSize(1);
        assertThat(result.get().data().get(0).activityId()).isEqualTo("task1");
    }

    @Test
    void listVariableHistory_piiNotPermitted_masksValue() {
        insertProcessInstance("proc-8", "biz-8", "def8", Instant.now());
        Map<String, Object> varFields = new LinkedHashMap<>();
        varFields.put("variableName", "amount");
        varFields.put("variableType", "String");
        varFields.put("textValue", "1000-secret");
        projectionStore.insertLogEvent(HistoryClassNames.DETAIL,
                new com.threeai.nats.history.projection.LogHistoryRecord("camunda", "proc-8", "evt-1", "update",
                        1, Instant.now(), varFields));

        HistoryQueryApi noPiiApi = new HistoryQueryApi(dataSource, new PiiMaskingService(), DENY_PII_ONLY);
        QueryContext noPiiCtx = new QueryContext("caller-1", Set.of(), false);
        var result = noPiiApi.listVariableHistory("proc-8", new PageRequest(0, 20), noPiiCtx);

        assertThat(result).isPresent();
        assertThat(result.get().data()).hasSize(1);
        assertThat(result.get().data().get(0).value()).isEqualTo("***");
        assertThat(result.get().data().get(0).masked()).isTrue();
    }

    private static final HistoryQueryAuthzSpi DENY_PII_ONLY = new HistoryQueryAuthzSpi() {
        public boolean isAuthorized(QueryContext ctx, QueryOperation operation) {
            return true; // authorized to query, just no PII-view permission
        }

        public boolean hasPiiViewPermission(QueryContext ctx) {
            return false;
        }
    };
}
