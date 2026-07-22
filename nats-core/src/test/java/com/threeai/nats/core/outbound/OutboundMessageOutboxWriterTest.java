package com.threeai.nats.core.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;

import com.threeai.nats.core.db.SqlMigrationRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Real Postgres (Testcontainers) — the SAME migration bundled at
 * {@code db/migration/outbound/V1__outbound_message_outbox.sql} is applied verbatim. */
@Testcontainers
class OutboundMessageOutboxWriterTest {

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
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/outbound/V1__outbound_message_outbox.sql");
    }

    @AfterAll
    static void stopContainer() {
        postgres.stop();
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement("DELETE FROM outbound_message_outbox")) {
            stmt.executeUpdate();
        }
    }

    private OutboundMessageDraft draft(String processInstanceId) {
        return new OutboundMessageDraft("camunda", "order.created", processInstanceId, "biz-1", "trace-1",
                "events.camunda.order.created." + processInstanceId, "{\"foo\":1}".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void write_insertsOneRow_returnsGeneratedId() throws Exception {
        UUID id;
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            id = new OutboundMessageOutboxWriter(null).write(connection, draft("proc-1"));
            connection.commit();
        }

        assertThat(id).isNotNull();
        assertThat(countRows()).isEqualTo(1);
    }

    @Test
    void write_persistsAllColumnsVerbatim() throws Exception {
        OutboundMessageDraft draft = draft("proc-2");
        UUID id;
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            id = new OutboundMessageOutboxWriter(null).write(connection, draft);
            connection.commit();
        }

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "SELECT engine_id, message_type, process_instance_id, business_key, trace_id, subject, payload "
                     + "FROM outbound_message_outbox WHERE id = ?")) {
            stmt.setObject(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("engine_id")).isEqualTo("camunda");
                assertThat(rs.getString("message_type")).isEqualTo("order.created");
                assertThat(rs.getString("process_instance_id")).isEqualTo("proc-2");
                assertThat(rs.getString("business_key")).isEqualTo("biz-1");
                assertThat(rs.getString("trace_id")).isEqualTo("trace-1");
                assertThat(rs.getString("subject")).isEqualTo("events.camunda.order.created.proc-2");
                assertThat(new String(rs.getBytes("payload"), StandardCharsets.UTF_8)).isEqualTo("{\"foo\":1}");
            }
        }
    }

    @Test
    void write_rolledBackTransaction_rowNotPersisted() throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            new OutboundMessageOutboxWriter(null).write(connection, draft("proc-3"));
            connection.rollback();
        }

        assertThat(countRows()).isZero();
    }

    @Test
    void write_sqlFailure_throwsIllegalStateException() throws Exception {
        OutboundMessageDraft draftWithNullPayload = new OutboundMessageDraft("camunda", "order.created", "proc-4",
                null, "trace-1", "events.camunda.order.created.proc-4", null); // payload NOT NULL -> SQL failure

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            assertThatThrownBy(() -> new OutboundMessageOutboxWriter(null).write(connection, draftWithNullPayload))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("proc-4");
            connection.rollback();
        }
    }

    private long countRows() throws Exception {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT count(*) FROM outbound_message_outbox");
             ResultSet rs = stmt.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
