package com.threeai.nats.core.vault;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.threeai.nats.core.db.SqlMigrationRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Real Postgres (Testcontainers) — direct unit coverage of {@link VaultAccessAuditor#record},
 *  independent of {@link PseudonymizationVaultClientTest}'s indirect exercise of it. */
@Testcontainers
class VaultAccessAuditorTest {

    private static PostgreSQLContainer<?> postgres;
    private static PGSimpleDataSource dataSource;
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
    }

    @AfterAll
    static void stopContainer() {
        postgres.stop();
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection c = dataSource.getConnection(); java.sql.Statement stmt = c.createStatement()) {
            stmt.execute("TRUNCATE vault_access_audit");
        }
    }

    @Test
    void record_grantedAccess_insertsAuditRow() throws Exception {
        auditor.record("tok-1", "READ", "svc-history-projection", "operator lookup", true);

        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "SELECT operation, accessor_identity, access_reason, granted FROM vault_access_audit "
                   + "WHERE pseudonym_token = ?")) {
            stmt.setString(1, "tok-1");
            try (ResultSet rs = stmt.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("operation")).isEqualTo("READ");
                assertThat(rs.getString("accessor_identity")).isEqualTo("svc-history-projection");
                assertThat(rs.getString("access_reason")).isEqualTo("operator lookup");
                assertThat(rs.getBoolean("granted")).isTrue();
            }
        }
    }

    @Test
    void record_deniedAccess_insertsAuditRowWithGrantedFalse() throws Exception {
        auditor.record("tok-2", "REIDENTIFY_ATTEMPT", "unknown-caller", null, false);

        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement(
                     "SELECT granted FROM vault_access_audit WHERE pseudonym_token = ?")) {
            stmt.setString(1, "tok-2");
            try (ResultSet rs = stmt.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getBoolean("granted")).isFalse();
            }
        }
    }

    @Test
    void record_unreachableDataSource_throwsIllegalStateException() {
        VaultAccessAuditor unreachable = new VaultAccessAuditor(unreachableDataSource());

        assertThatThrownBy(() -> unreachable.record("tok-3", "WRITE", "svc", "reason", true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("WRITE");
    }

    private PGSimpleDataSource unreachableDataSource() {
        PGSimpleDataSource broken = new PGSimpleDataSource();
        broken.setUrl("jdbc:postgresql://127.0.0.1:1/nonexistent");
        broken.setUser("nobody");
        broken.setPassword("nobody");
        return broken;
    }
}
