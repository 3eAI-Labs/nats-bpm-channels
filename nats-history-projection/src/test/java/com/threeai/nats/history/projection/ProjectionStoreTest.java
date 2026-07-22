package com.threeai.nats.history.projection;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import com.threeai.nats.core.db.SqlMigrationRunner;
import com.threeai.nats.core.history.HistoryClassNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Real Postgres (Testcontainers) — the bundled projection migration is applied verbatim. */
@Testcontainers
class ProjectionStoreTest {

    private static PostgreSQLContainer<?> postgres;
    private static PGSimpleDataSource dataSource;
    private static ProjectionStore store;

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
        store = new ProjectionStore(dataSource);
    }

    @AfterAll
    static void stopContainer() {
        postgres.stop();
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection c = dataSource.getConnection(); java.sql.Statement stmt = c.createStatement()) {
            stmt.execute("TRUNCATE process_instance_history, activity_instance_history, "
                    + "operation_log_history, incident_history");
        }
    }

    private EntityHistoryRecord procInstRecord(String entityId, long streamSequence, String state) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("businessKey", "biz-1");
        fields.put("state", state);
        return new EntityHistoryRecord("camunda", entityId, entityId, streamSequence, Instant.now(), fields);
    }

    @Test
    void upsertEntity_newEntity_insertsRow() {
        UpsertOutcome outcome = store.upsertEntity(HistoryClassNames.PROCINST, procInstRecord("proc-1", 1, "ACTIVE"));

        assertThat(outcome).isEqualTo(UpsertOutcome.APPLIED);
        assertThat(countRows("process_instance_history", "proc-1")).isEqualTo(1);
        assertThat(stateOf("proc-1")).isEqualTo("ACTIVE");
    }

    @Test
    void upsertEntity_newerSequence_updatesInPlace_sameRow() {
        store.upsertEntity(HistoryClassNames.PROCINST, procInstRecord("proc-2", 1, "ACTIVE"));

        UpsertOutcome outcome = store.upsertEntity(HistoryClassNames.PROCINST, procInstRecord("proc-2", 2, "COMPLETED"));

        assertThat(outcome).isEqualTo(UpsertOutcome.APPLIED);
        assertThat(countRows("process_instance_history", "proc-2")).isEqualTo(1); // still ONE row (updated, not inserted)
        assertThat(stateOf("proc-2")).isEqualTo("COMPLETED");
    }

    @Test
    void upsertEntity_staleOlderSequence_discardedNoOp() {
        store.upsertEntity(HistoryClassNames.PROCINST, procInstRecord("proc-3", 5, "COMPLETED"));

        UpsertOutcome outcome = store.upsertEntity(HistoryClassNames.PROCINST, procInstRecord("proc-3", 2, "ACTIVE"));

        assertThat(outcome).isEqualTo(UpsertOutcome.STALE_DISCARDED);
        assertThat(stateOf("proc-3")).isEqualTo("COMPLETED"); // unchanged
    }

    @Test
    void upsertEntity_equalSequence_ambiguousDiscarded() {
        store.upsertEntity(HistoryClassNames.PROCINST, procInstRecord("proc-4", 3, "ACTIVE"));

        UpsertOutcome outcome = store.upsertEntity(HistoryClassNames.PROCINST, procInstRecord("proc-4", 3, "COMPLETED"));

        assertThat(outcome).isEqualTo(UpsertOutcome.STALE_DISCARDED);
        assertThat(stateOf("proc-4")).isEqualTo("ACTIVE"); // unchanged
    }

    @Test
    void upsertEntity_maliciousFieldKey_sqlInjectionAttempt_columnDroppedRowStillInsertedSafely() {
        // [BLOCKING] security-review regression (commit 03439e1 follow-up): a wire-payload field
        // key shaped like a SQL-injection payload must be silently dropped (not interpolated into
        // the dynamic column list) -- the row is still written using only the legitimate fields.
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("businessKey", "biz-safe");
        fields.put("state) VALUES ('x'); DROP TABLE process_instance_history; --", "ACTIVE");
        EntityHistoryRecord record = new EntityHistoryRecord("camunda", "proc-injection", "proc-injection",
                1, Instant.now(), fields);

        UpsertOutcome outcome = store.upsertEntity(HistoryClassNames.PROCINST, record);

        assertThat(outcome).isEqualTo(UpsertOutcome.APPLIED);
        assertThat(countRows("process_instance_history", "proc-injection")).isEqualTo(1); // table still exists, exactly 1 row
        // The legitimate field was still written; the malicious "column" was dropped, not applied.
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "SELECT business_key FROM process_instance_history WHERE process_instance_id = ?")) {
            stmt.setString(1, "proc-injection");
            try (ResultSet rs = stmt.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(1)).isEqualTo("biz-safe");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void upsertEntity_actinstClass_mapsToCorrectTable() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("activityId", "task1");
        fields.put("activityType", "userTask");
        EntityHistoryRecord record = new EntityHistoryRecord("camunda", "act-1", "proc-5", 1, Instant.now(), fields);

        UpsertOutcome outcome = store.upsertEntity(HistoryClassNames.ACTINST, record);

        assertThat(outcome).isEqualTo(UpsertOutcome.APPLIED);
        assertThat(countRows("activity_instance_history", "act-1")).isEqualTo(1);
    }

    @Test
    void insertLogEvent_newEvent_inserted() {
        LogHistoryRecord record = new LogHistoryRecord("camunda", "proc-6", "evt-1", "create",
                1, Instant.now(), Map.of("operationType", "Complete", "userId", "user-1"));

        UpsertOutcome outcome = store.insertLogEvent(HistoryClassNames.OP_LOG, record);

        assertThat(outcome).isEqualTo(UpsertOutcome.APPLIED);
    }

    @Test
    void insertLogEvent_duplicateDedupKey_skipped() {
        LogHistoryRecord record = new LogHistoryRecord("camunda", "proc-7", "evt-2", "create",
                1, Instant.parse("2026-01-01T00:00:00Z"), Map.of("operationType", "Complete"));

        UpsertOutcome first = store.insertLogEvent(HistoryClassNames.OP_LOG, record);
        UpsertOutcome second = store.insertLogEvent(HistoryClassNames.OP_LOG, record); // exact same dedup key

        assertThat(first).isEqualTo(UpsertOutcome.APPLIED);
        assertThat(second).isEqualTo(UpsertOutcome.DEDUP_SKIPPED);
    }

    @Test
    void storeLargePayload_returnsRetrievableId() throws Exception {
        byte[] payload = "large content".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        UUID id = store.storeLargePayload(payload, "ext_task_log_history");

        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement("SELECT payload_bytes FROM projection_large_payload WHERE id = ?")) {
            stmt.setObject(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getBytes(1)).isEqualTo(payload);
            }
        }
    }

    private long countRows(String table, String entityId) {
        String col = table.equals("process_instance_history") ? "process_instance_id" : "activity_instance_id";
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement("SELECT count(*) FROM " + table + " WHERE " + col + " = ?")) {
            stmt.setString(1, entityId);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String stateOf(String processInstanceId) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "SELECT state FROM process_instance_history WHERE process_instance_id = ?")) {
            stmt.setString(1, processInstanceId);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
