package com.threeai.nats.core.largepayload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Optional;
import java.util.UUID;

import com.threeai.nats.core.db.SqlMigrationRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Real Postgres (Testcontainers) — proves the content-addressed dedup/refcount/release protocol
 * (`docs/08-large-variable-externalization.md` D-B'/D-D'/D-F') against an actual JDBC driver.
 */
@Testcontainers
class ContentAddressedLargePayloadStoreTest {

    private static PostgreSQLContainer<?> postgres;
    private static PGSimpleDataSource dataSource;
    private static ContentAddressedLargePayloadStore store;

    @BeforeAll
    static void startContainer() {
        postgres = new PostgreSQLContainer<>("postgres:16");
        postgres.start();
        dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgres.getJdbcUrl());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/test-fixture/V2__large_payload_store.sql");
        store = new ContentAddressedLargePayloadStore(dataSource);
    }

    @AfterAll
    static void stopContainer() {
        postgres.stop();
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection c = dataSource.getConnection(); java.sql.Statement stmt = c.createStatement()) {
            stmt.execute("TRUNCATE projection_large_payload");
        }
    }

    @Test
    void storeAndAcquireReference_firstWriter_insertsNewRow_refCountOne() {
        byte[] payload = bytesOf("first-content");

        LargePayloadReference ref = store.storeAndAcquireReference(payload, "variable_instance_history");

        assertThat(ref.newlyStored()).isTrue();
        assertThat(ref.refCountAfter()).isEqualTo(1);
        assertThat(ref.contentHash()).isEqualTo(ContentHash.sha256Hex(payload));
    }

    @Test
    void storeAndAcquireReference_duplicateContent_dedupsOntoSameRow_incrementsRefCount() {
        byte[] payload = bytesOf("shared-content");

        LargePayloadReference first = store.storeAndAcquireReference(payload, "variable_instance_history");
        LargePayloadReference second = store.storeAndAcquireReference(payload, "ext_task_log_history");

        assertThat(second.newlyStored()).isFalse();
        assertThat(second.id()).isEqualTo(first.id());
        assertThat(second.refCountAfter()).isEqualTo(2);
        assertThat(countRows()).isEqualTo(1); // ONE physical row for both "consumers"
    }

    @Test
    void storeAndAcquireReference_differentContent_distinctRows() {
        LargePayloadReference a = store.storeAndAcquireReference(bytesOf("content-a"), "src");
        LargePayloadReference b = store.storeAndAcquireReference(bytesOf("content-b"), "src");

        assertThat(a.id()).isNotEqualTo(b.id());
        assertThat(countRows()).isEqualTo(2);
    }

    @Test
    void fetchByContentHash_returnsStoredBytes() {
        byte[] payload = bytesOf("dereference-me");
        LargePayloadReference ref = store.storeAndAcquireReference(payload, "src");

        Optional<byte[]> fetched = store.fetchByContentHash(ref.contentHash());

        assertThat(fetched).isPresent();
        assertThat(fetched.get()).isEqualTo(payload);
    }

    @Test
    void fetchByContentHash_unknownHash_empty() {
        Optional<byte[]> fetched = store.fetchByContentHash("f".repeat(64));

        assertThat(fetched).isEmpty();
    }

    @Test
    void fetchById_returnsStoredBytes() {
        byte[] payload = bytesOf("by-id-lookup");
        LargePayloadReference ref = store.storeAndAcquireReference(payload, "src");

        assertThat(store.fetchById(ref.id())).contains(payload);
    }

    @Test
    void fetchById_unknownId_empty() {
        assertThat(store.fetchById(UUID.randomUUID())).isEmpty();
    }

    @Test
    void releaseReference_lastReference_deletesRow() {
        LargePayloadReference ref = store.storeAndAcquireReference(bytesOf("solo-owner"), "src");

        store.releaseReference(ref.id());

        assertThat(countRows()).isZero();
    }

    @Test
    void releaseReference_sharedReference_decrementsButKeepsRow() {
        byte[] payload = bytesOf("shared-owner");
        LargePayloadReference first = store.storeAndAcquireReference(payload, "src-1");
        store.storeAndAcquireReference(payload, "src-2"); // ref_count now 2

        store.releaseReference(first.id());

        assertThat(countRows()).isEqualTo(1); // still referenced by src-2
        assertThat(refCountOf(first.id())).isEqualTo(1);
    }

    @Test
    void releaseReference_thenReacquire_freshRowRecreated() {
        byte[] payload = bytesOf("recreate-me");
        LargePayloadReference first = store.storeAndAcquireReference(payload, "src");
        store.releaseReference(first.id());
        assertThat(countRows()).isZero();

        LargePayloadReference second = store.storeAndAcquireReference(payload, "src");

        assertThat(second.newlyStored()).isTrue();
        assertThat(second.refCountAfter()).isEqualTo(1);
    }

    @Test
    void releaseReference_unknownId_isNoOp_doesNotThrow() {
        store.releaseReference(UUID.randomUUID()); // no exception, just a WARN log
    }

    @Test
    void storeAndAcquireReference_unreachableStore_throwsIllegalStateException() {
        ContentAddressedLargePayloadStore unreachable = new ContentAddressedLargePayloadStore(unreachableDataSource());

        assertThatThrownBy(() -> unreachable.storeAndAcquireReference(bytesOf("x"), "src"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SYS_LARGE_PAYLOAD_STORE_FAILED");
    }

    private byte[] bytesOf(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private long countRows() {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement("SELECT count(*) FROM projection_large_payload");
             ResultSet rs = stmt.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int refCountOf(UUID id) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement("SELECT ref_count FROM projection_large_payload WHERE id = ?")) {
            stmt.setObject(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private PGSimpleDataSource unreachableDataSource() {
        PGSimpleDataSource broken = new PGSimpleDataSource();
        broken.setUrl("jdbc:postgresql://127.0.0.1:1/nonexistent");
        broken.setUser("nobody");
        broken.setPassword("nobody");
        return broken;
    }
}
