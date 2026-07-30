package com.threeai.nats.core.db;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal, dependency-free classpath SQL script runner (increment 2, reusing the migration-proof
 * discipline programmatically). Applies a {@code db/migration/**}
 * resource verbatim via plain JDBC — the schema DDL itself lives ONLY in the canonical migration
 * scripts (copied byte-for-byte into each owning
 * module's {@code src/main/resources}) and is never re-typed here.
 *
 * <p><b>CODER-NOTE (why not Flyway/Liquibase):</b> neither is a dependency of this repo today, and
 * the DB-schema design itself states the engine-outbox migration is applied by the TENANT alongside
 * their own engine-DB migration chain ("Flyway/Liquibase changelog integration" is explicitly
 * called out as a deploy-time concern, not this repo's). This class exists so
 * Testcontainers-backed slice tests (and, optionally, a tenant's own bootstrap code) can apply the
 * bundled scripts without hand-rolling JDBC boilerplate per test — it is intentionally NOT a
 * general-purpose migration framework (no version tracking table, no rollback). Idempotent-safe
 * for repeated invocation against the SAME schema: a "relation already exists" (SQLState
 * {@code 42P07}) is treated as already-applied and logged at DEBUG, not re-thrown.
 */
public final class SqlMigrationRunner {

    private static final Logger log = LoggerFactory.getLogger(SqlMigrationRunner.class);
    private static final String ALREADY_EXISTS_SQLSTATE = "42P07";

    private SqlMigrationRunner() {
    }

    /**
     * @param dataSource       target database
     * @param classpathResource e.g. {@code "db/migration/history/V1__compact_history_outbox.sql"}
     *                          (resolved via this class's own classloader against the CALLING
     *                          module's classpath — pass a resource that module actually bundles)
     */
    public static void applyClasspathScript(DataSource dataSource, String classpathResource) {
        String sql = readClasspathResource(classpathResource);
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
            log.info("Applied SQL migration script", kv("resource", classpathResource));
        } catch (SQLException e) {
            if (ALREADY_EXISTS_SQLSTATE.equals(e.getSQLState())) {
                log.debug("SQL migration script already applied — skipping",
                        kv("resource", classpathResource), kv("sql_state", e.getSQLState()));
                return;
            }
            throw new IllegalStateException("Failed to apply SQL migration script '" + classpathResource + "'", e);
        }
    }

    private static String readClasspathResource(String classpathResource) {
        try (InputStream in = SqlMigrationRunner.class.getClassLoader().getResourceAsStream(classpathResource)) {
            if (in == null) {
                throw new IllegalArgumentException("Classpath resource not found: " + classpathResource);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read classpath resource: " + classpathResource, e);
        }
    }
}
