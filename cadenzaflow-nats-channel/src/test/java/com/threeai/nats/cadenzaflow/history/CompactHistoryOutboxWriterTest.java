package com.threeai.nats.cadenzaflow.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.threeai.nats.core.db.SqlMigrationRunner;
import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.core.history.PseudonymTokenGenerator;
import org.cadenzaflow.bpm.engine.impl.history.event.HistoricExternalTaskLogEntity;
import org.cadenzaflow.bpm.engine.impl.history.event.HistoricIncidentEventEntity;
import org.cadenzaflow.bpm.engine.impl.history.event.UserOperationLogEntryEventEntity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Real Postgres (Testcontainers) — the SAME migration bundled at
 * {@code db/migration/history/V1__compact_history_outbox.sql} is applied verbatim. */
@Testcontainers
class CompactHistoryOutboxWriterTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static PostgreSQLContainer<?> postgres;
    private static PGSimpleDataSource dataSource;

    @BeforeAll
    static void startContainer() {
        postgres = new PostgreSQLContainer<>("postgres:16");
        postgres.start();
        dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgres.getJdbcUrl());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/history/V1__compact_history_outbox.sql");
    }

    @AfterAll
    static void stopContainer() {
        postgres.stop();
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection c = dataSource.getConnection(); PreparedStatement stmt =
                c.prepareStatement("DELETE FROM compact_history_outbox")) {
            stmt.executeUpdate();
        }
    }

    private CompactHistoryOutboxWriter newWriter(HistoryClassificationProperties classification) {
        return new CompactHistoryOutboxWriter(dataSource, new PseudonymTokenGenerator(), classification, null);
    }

    @Test
    void write_opLogEvent_insertsOneRowWithScalarPayload() throws Exception {
        UserOperationLogEntryEventEntity event = new UserOperationLogEntryEventEntity();
        event.setId(UUID.randomUUID().toString());
        event.setEventType("UserOperationLog");
        event.setProcessInstanceId("proc-1");
        event.setUserId("user-42");
        event.setOperationType("Complete");

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            newWriter(new HistoryClassificationProperties())
                    .write(event, HistoryClassNames.OP_LOG, "cadenzaflow", connection);
            connection.commit();
        }

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "SELECT history_class, engine_id, process_instance_id, payload_scalar, payload_large_ref "
                   + "FROM compact_history_outbox WHERE history_event_id = ?")) {
            stmt.setString(1, event.getId());
            try (ResultSet rs = stmt.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("history_class")).isEqualTo("OP_LOG");
                assertThat(rs.getString("engine_id")).isEqualTo("cadenzaflow");
                assertThat(rs.getString("process_instance_id")).isEqualTo("proc-1");
                Map<String, Object> payloadScalar = JSON.readValue(rs.getString("payload_scalar"), Map.class);
                assertThat(payloadScalar).containsEntry("userId", "user-42");
                assertThat(rs.getObject("payload_large_ref")).isNull();
                assertThat(rs.next()).isFalse(); // exactly one row (NFR-P2)
            }
        }
    }

    @Test
    void write_pseudonymizationOptIn_replacesRawUserIdWithToken() throws Exception {
        HistoryClassificationProperties classification = new HistoryClassificationProperties();
        classification.setPseudonymizationOptIn(true);
        classification.setTenantKeyId("tenant-key-1");
        classification.setTenantKeyVersion(1);

        UserOperationLogEntryEventEntity event = new UserOperationLogEntryEventEntity();
        event.setId(UUID.randomUUID().toString());
        event.setEventType("UserOperationLog");
        event.setProcessInstanceId("proc-2");
        event.setUserId("secret-user-42");

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            newWriter(classification).write(event, HistoryClassNames.OP_LOG, "cadenzaflow", connection);
            connection.commit();
        }

        String expectedToken = new PseudonymTokenGenerator().generate("secret-user-42", "tenant-key-1", 1);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "SELECT payload_scalar FROM compact_history_outbox WHERE history_event_id = ?")) {
            stmt.setString(1, event.getId());
            try (ResultSet rs = stmt.executeQuery()) {
                assertThat(rs.next()).isTrue();
                String payloadScalarRaw = rs.getString("payload_scalar");
                assertThat(payloadScalarRaw).doesNotContain("secret-user-42");
                Map<String, Object> payloadScalar = JSON.readValue(payloadScalarRaw, Map.class);
                assertThat(payloadScalar).containsEntry("userIdPseudonymized", true);
                assertThat(payloadScalar).containsEntry("pseudonymToken", expectedToken);
                assertThat(payloadScalar).doesNotContainKey("userId");
            }
        }
    }

    @Test
    void write_extTaskLogWithErrorDetails_writesCompanionPayloadRow() throws Exception {
        HistoricExternalTaskLogEntity event = new HistoricExternalTaskLogEntity() {
            @Override
            public String getErrorDetails() {
                return "full stack trace here";
            }
        };
        event.setId(UUID.randomUUID().toString());
        event.setEventType("EXTERNAL_TASK_FAIL");
        event.setProcessInstanceId("proc-3");

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            newWriter(new HistoryClassificationProperties())
                    .write(event, HistoryClassNames.EXT_TASK_LOG, "cadenzaflow", connection);
            connection.commit();
        }

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "SELECT o.payload_large_ref, p.payload_bytes FROM compact_history_outbox o "
                   + "JOIN compact_history_outbox_payload p ON p.id = o.payload_large_ref "
                   + "WHERE o.history_event_id = ?")) {
            stmt.setString(1, event.getId());
            try (ResultSet rs = stmt.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getObject("payload_large_ref")).isNotNull();
                assertThat(new String(rs.getBytes("payload_bytes"), java.nio.charset.StandardCharsets.UTF_8))
                        .isEqualTo("full stack trace here");
            }
        }
    }

    @Test
    void write_incidentEvent_noDuplicateOnUniqueEventKey() throws Exception {
        HistoricIncidentEventEntity event = new HistoricIncidentEventEntity();
        event.setId(UUID.randomUUID().toString());
        event.setEventType("create");
        event.setProcessInstanceId("proc-4");
        event.setIncidentMessage("boom");

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            newWriter(new HistoryClassificationProperties())
                    .write(event, HistoryClassNames.INCIDENT, "cadenzaflow", connection);
            connection.commit();
        }

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "SELECT count(*) FROM compact_history_outbox WHERE history_event_id = ? AND event_type = ?")) {
            stmt.setString(1, event.getId());
            stmt.setString(2, "create");
            try (ResultSet rs = stmt.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(1L);
            }
        }
    }
}
