package com.threeai.nats.core.vault;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Optional;
import java.util.UUID;

import com.threeai.nats.core.db.SqlMigrationRunner;
import com.threeai.nats.core.history.exception.PseudonymVaultAccessDeniedException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Real Postgres (Testcontainers) — proves pgcrypto encrypt/decrypt round-trip and hard-delete. */
@Testcontainers
class PseudonymizationVaultClientTest {

    private static PostgreSQLContainer<?> postgres;
    private static PGSimpleDataSource dataSource;
    private static PseudonymizationVaultClient client;
    private static VaultAccessAuditor auditor;

    @BeforeAll
    static void startContainer() {
        postgres = new PostgreSQLContainer<>("postgres:16");
        postgres.start();
        dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgres.getJdbcUrl());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/vault/V1__pseudonym_map.sql");
        auditor = new VaultAccessAuditor(dataSource);
        client = new PseudonymizationVaultClient(dataSource, auditor, "test-column-key");
    }

    @AfterAll
    static void stopContainer() {
        postgres.stop();
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection c = dataSource.getConnection(); java.sql.Statement stmt = c.createStatement()) {
            stmt.execute("TRUNCATE pseudonym_map, vault_access_audit");
        }
    }

    @Test
    void persistMapping_thenReidentify_authorized_decryptsRealValue() {
        String token = "tok-" + UUID.randomUUID();
        client.persistMapping(token, "camunda", "user-42", 1, "OP_LOG");

        Optional<String> reidentified = client.reidentify(token, "compliance-officer-1", "KVKK subject access request", true);

        assertThat(reidentified).contains("user-42");
    }

    @Test
    void persistMapping_isIdempotent_onConflictDoNothing() {
        String token = "tok-" + UUID.randomUUID();
        client.persistMapping(token, "camunda", "user-1", 1, "OP_LOG");
        client.persistMapping(token, "camunda", "user-1", 1, "OP_LOG"); // same token, repeat write

        assertThat(countMapRows(token)).isEqualTo(1);
    }

    @Test
    void reidentify_unauthorized_throwsAndAudits() {
        String token = "tok-" + UUID.randomUUID();
        client.persistMapping(token, "camunda", "user-99", 1, "OP_LOG");

        assertThatThrownBy(() -> client.reidentify(token, "attacker", "no reason", false))
                .isInstanceOf(PseudonymVaultAccessDeniedException.class);

        assertThat(deniedAuditCount(token)).isEqualTo(1);
    }

    @Test
    void deleteMapping_hardDeletes_rowIsGone() {
        String token = "tok-" + UUID.randomUUID();
        client.persistMapping(token, "camunda", "user-7", 1, "OP_LOG");

        client.deleteMapping(token, "erasure-pipeline");

        assertThat(countMapRows(token)).isZero();
    }

    @Test
    void reidentify_unknownToken_returnsEmpty() {
        Optional<String> result = client.reidentify("nonexistent-token", "ops", "check", true);

        assertThat(result).isEmpty();
    }

    private long countMapRows(String token) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement("SELECT count(*) FROM pseudonym_map WHERE pseudonym_token = ?")) {
            stmt.setString(1, token);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private long deniedAuditCount(String token) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "SELECT count(*) FROM vault_access_audit WHERE pseudonym_token = ? AND granted = FALSE")) {
            stmt.setString(1, token);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
