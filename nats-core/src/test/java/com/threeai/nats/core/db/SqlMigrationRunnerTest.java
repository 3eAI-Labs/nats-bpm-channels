package com.threeai.nats.core.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Real Postgres (Testcontainers) — verifies the runner against an actual JDBC driver, not mocks. */
@Testcontainers
class SqlMigrationRunnerTest {

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
    }

    @AfterAll
    static void stopContainer() {
        postgres.stop();
    }

    @Test
    void applyClasspathScript_createsTableFromResource() throws Exception {
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/test-fixture/V1__widget.sql");

        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement("INSERT INTO widget (name) VALUES (?)")) {
            stmt.setString(1, "test-widget");
            assertThat(stmt.executeUpdate()).isEqualTo(1);
        }

        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement("SELECT count(*) FROM widget");
             ResultSet rs = stmt.executeQuery()) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).isEqualTo(1L);
        }
    }

    @Test
    void applyClasspathScript_missingResource_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/does-not-exist.sql"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does-not-exist.sql");
    }

    /**
     * Idempotency contract (class Javadoc): a SECOND application of the SAME script against a
     * schema where it was already applied hits Postgres SQLState {@code 42P07} ("relation already
     * exists") and must be swallowed (DEBUG log, no throw) -- never re-thrown as a failure.
     */
    @Test
    void applyClasspathScript_alreadyApplied_isIdempotent_doesNotThrow() {
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/test-fixture/V1__widget.sql");

        assertThatCode(() -> SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/test-fixture/V1__widget.sql"))
                .doesNotThrowAnyException();
    }

    @Test
    void applyClasspathScript_genuineSqlFailure_wrapsAsIllegalStateException() {
        assertThatThrownBy(() -> SqlMigrationRunner.applyClasspathScript(dataSource,
                "db/migration/test-fixture/V4__broken_syntax.sql"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("V4__broken_syntax.sql")
                .hasCauseInstanceOf(java.sql.SQLException.class);
    }
}
