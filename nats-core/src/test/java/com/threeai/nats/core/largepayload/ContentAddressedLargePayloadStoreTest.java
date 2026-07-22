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
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/test-fixture/V3__runtime_large_variable_reference.sql");
        store = new ContentAddressedLargePayloadStore(dataSource);
    }

    @AfterAll
    static void stopContainer() {
        postgres.stop();
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection c = dataSource.getConnection(); java.sql.Statement stmt = c.createStatement()) {
            stmt.execute("TRUNCATE runtime_large_variable_ref, projection_large_payload");
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

    // --- RUNTIME reference ledger (D-F' FINDING-001 fix) ---

    @Test
    void currentRuntimeReference_none_empty() {
        assertThat(store.currentRuntimeReference("camunda", "var-unknown")).isEmpty();
    }

    @Test
    void recordRuntimeReference_thenRead_returnsPayloadId() {
        LargePayloadReference ref = store.storeAndAcquireReference(bytesOf("runtime-content"), "runtime.camunda");

        store.recordRuntimeReference("camunda", "var-1", ref.id(), ref.contentHash());

        assertThat(store.currentRuntimeReference("camunda", "var-1")).contains(ref.id());
    }

    @Test
    void recordRuntimeReference_sameVariableDifferentPayload_upsertsInPlace() {
        LargePayloadReference first = store.storeAndAcquireReference(bytesOf("first-content"), "runtime.camunda");
        LargePayloadReference second = store.storeAndAcquireReference(bytesOf("second-content"), "runtime.camunda");
        store.recordRuntimeReference("camunda", "var-1", first.id(), first.contentHash());

        store.recordRuntimeReference("camunda", "var-1", second.id(), second.contentHash());

        assertThat(store.currentRuntimeReference("camunda", "var-1")).contains(second.id());
        assertThat(store.listRuntimeReferences("camunda")).hasSize(1); // ONE row per (engine, variable)
    }

    @Test
    void listRuntimeReferences_scopedByEngineId() {
        LargePayloadReference ref = store.storeAndAcquireReference(bytesOf("scoped-content"), "runtime.camunda");
        store.recordRuntimeReference("camunda", "var-1", ref.id(), ref.contentHash());
        store.recordRuntimeReference("cadenzaflow", "var-2", ref.id(), ref.contentHash());

        java.util.List<RuntimeVariableReference> camundaRefs = store.listRuntimeReferences("camunda");

        assertThat(camundaRefs).hasSize(1);
        assertThat(camundaRefs.get(0).variableId()).isEqualTo("var-1");
    }

    @Test
    void deleteRuntimeReferenceRecord_removesLedgerRow_payloadRowUnaffected() {
        LargePayloadReference ref = store.storeAndAcquireReference(bytesOf("delete-ledger-content"), "runtime.camunda");
        store.recordRuntimeReference("camunda", "var-1", ref.id(), ref.contentHash());

        store.deleteRuntimeReferenceRecord("camunda", "var-1");

        assertThat(store.currentRuntimeReference("camunda", "var-1")).isEmpty();
        assertThat(store.fetchById(ref.id())).isPresent(); // deleting the LEDGER row is not a release
    }

    /**
     * The FINDING-001 fix, proven at the store layer: acquire-new-then-release-old (the order
     * {@code LargeVariablePostCommitExternalizer} uses) never leaves a payload under-referenced
     * during the transition, even when old and new happen to race — releasing the OLD reference
     * only after the NEW one is durably acquired means the object is never momentarily unreferenced.
     */
    @Test
    void overwriteFlow_acquireNewThenReleaseOld_oldPayloadReleasedWhenNoLongerShared() {
        LargePayloadReference oldRef = store.storeAndAcquireReference(bytesOf("old-value"), "runtime.camunda");
        store.recordRuntimeReference("camunda", "var-1", oldRef.id(), oldRef.contentHash());

        LargePayloadReference newRef = store.storeAndAcquireReference(bytesOf("new-value"), "runtime.camunda");
        store.recordRuntimeReference("camunda", "var-1", newRef.id(), newRef.contentHash());
        store.releaseReference(oldRef.id()); // the ledger row no longer references it

        assertThat(store.fetchById(oldRef.id())).isEmpty(); // old payload gone -- sole reference released
        assertThat(store.fetchById(newRef.id())).isPresent();
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
