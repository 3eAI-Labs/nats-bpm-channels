package com.threeai.nats.core.largepayload;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.threeai.nats.core.db.SqlMigrationRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Phase 5.5 (QA) reliability suite -- idempotency-at-scale for the D-F' content-addressed
 * refcount/GC protocol (docs/08-large-variable-externalization.md), under REAL concurrent load
 * (real Postgres, real threads racing the SAME content hash) rather than
 * {@link ContentAddressedLargePayloadStoreTest}'s sequential-call proof of the SQL statements'
 * own atomicity. Proves the architectural invariant end-to-end: no matter how many
 * writers/releasers race the SAME content, (a) exactly ONE physical row ever exists for that
 * content, (b) the final {@code ref_count} exactly matches (acquires - releases), and (c) the row
 * is deleted if-and-only-if the count reaches zero -- never a premature delete under a
 * concurrent re-acquire, never a leaked row, never a negative count.
 *
 * <p>Heavy (real Postgres, concurrent load) -- {@code @Tag("reliability")}, excluded from the
 * default {@code mvn test} run. Run explicitly via
 * {@code mvn test -Dgroups=reliability -Dreliability.excludedGroups=}.
 */
@Tag("reliability")
@Testcontainers
class ContentAddressedLargePayloadStoreConcurrencyReliabilityTest {

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

    /**
     * N concurrent "acquire" calls (barrier-synchronized -- genuine simultaneous contention, real
     * Postgres serializing on the {@code content_hash} unique index) for BYTE-IDENTICAL content
     * must collapse onto exactly ONE physical row with {@code ref_count == N} -- never N separate
     * rows (dedup lost under contention), never a count that undercounts a racing acquirer.
     */
    @Test
    void concurrentAcquire_sameContent_collapsesToOneRow_refCountEqualsAcquireCount() throws Exception {
        int concurrency = 16;
        byte[] payload = "duplicate-storm-content".getBytes(StandardCharsets.UTF_8);

        List<LargePayloadReference> results = runConcurrently(concurrency,
                () -> store.storeAndAcquireReference(payload, "concurrency-test"));

        assertThat(countRows()).isEqualTo(1); // ONE physical row despite N concurrent acquirers
        UUID rowId = results.get(0).id();
        assertThat(results).allSatisfy(ref -> assertThat(ref.id()).isEqualTo(rowId));
        assertThat(refCountOf(rowId)).isEqualTo(concurrency);
        // Exactly one caller must observe newlyStored()==true (the winner of the race); every
        // other caller observed the dedup hit and incremented instead.
        long newlyStoredCount = results.stream().filter(LargePayloadReference::newlyStored).count();
        assertThat(newlyStoredCount).isEqualTo(1);
    }

    /**
     * Acquire N references concurrently, then release all N concurrently. The row must survive
     * every release except the LAST one (ref_count never observed negative, never deleted while
     * still referenced) and must be gone after the final release — proving
     * {@code releaseReference}'s "re-check ref_count=0 at DELETE time" design (class Javadoc) holds
     * under genuine concurrent decrement/delete races, not just sequential calls.
     */
    @Test
    void concurrentAcquireThenConcurrentRelease_rowSurvivesUntilLastRelease_thenDeleted() throws Exception {
        int concurrency = 12;
        byte[] payload = "acquire-then-release-storm".getBytes(StandardCharsets.UTF_8);

        List<LargePayloadReference> acquired = runConcurrently(concurrency,
                () -> store.storeAndAcquireReference(payload, "concurrency-test"));
        UUID rowId = acquired.get(0).id();
        assertThat(refCountOf(rowId)).isEqualTo(concurrency);

        runConcurrently(concurrency, () -> {
            store.releaseReference(rowId);
            return null;
        });

        // Every reference was released -- the row must be gone, not left with a stale/negative count.
        assertThat(countRows()).isZero();
    }

    /**
     * A "reader" thread repeatedly re-acquiring (incrementing) the SAME content WHILE a "releaser"
     * thread concurrently releases a DIFFERENT pre-existing reference to it must never let the row
     * disappear while the reader still holds (or is acquiring) a live reference -- the dedup/GC
     * protocol's core promise: content in active use is never garbage-collected out from under a
     * concurrent acquirer.
     */
    @Test
    void concurrentReacquireDuringRelease_rowNeverDisappearsWhileStillReferenced() throws Exception {
        byte[] payload = "reacquire-during-release".getBytes(StandardCharsets.UTF_8);
        LargePayloadReference original = store.storeAndAcquireReference(payload, "concurrency-test");
        // Pre-seed a SECOND, independent reference so ref_count starts at 2 -- the releaser below
        // releases the ORIGINAL reference while a swarm of reacquirers races to hold their own.
        LargePayloadReference seedRef = store.storeAndAcquireReference(payload, "concurrency-test-seed");
        assertThat(seedRef.id()).isEqualTo(original.id());
        assertThat(refCountOf(original.id())).isEqualTo(2);

        int reacquirerCount = 10;
        CyclicBarrier barrier = new CyclicBarrier(reacquirerCount + 1);
        ExecutorService pool = Executors.newFixedThreadPool(reacquirerCount + 1);
        try {
            List<Future<LargePayloadReference>> reacquireFutures = new ArrayList<>();
            for (int i = 0; i < reacquirerCount; i++) {
                reacquireFutures.add(pool.submit(() -> {
                    await(barrier);
                    return store.storeAndAcquireReference(payload, "concurrency-test-reacquirer");
                }));
            }
            Future<Void> releaseFuture = pool.submit(() -> {
                await(barrier);
                store.releaseReference(original.id()); // releases ONE of the pre-existing 2 references
                return null;
            });

            List<LargePayloadReference> reacquired = new ArrayList<>();
            for (Future<LargePayloadReference> f : reacquireFutures) {
                reacquired.add(f.get(30, TimeUnit.SECONDS));
            }
            releaseFuture.get(30, TimeUnit.SECONDS);

            // Every reacquirer must have observed the SAME row -- the row was never deleted out
            // from under them (had it been, a reacquirer would have created a FRESH row instead).
            assertThat(reacquired).allSatisfy(ref -> assertThat(ref.id()).isEqualTo(original.id()));
            assertThat(countRows()).isEqualTo(1);
            // Net refcount: started at 2, +reacquirerCount acquires, -1 release.
            assertThat(refCountOf(original.id())).isEqualTo(2 + reacquirerCount - 1);
        } finally {
            pool.shutdownNow();
        }
    }

    private <T> List<T> runConcurrently(int concurrency, java.util.concurrent.Callable<T> task) throws Exception {
        AtomicReferenceArray<T> results = new AtomicReferenceArray<>(concurrency);
        CyclicBarrier barrier = new CyclicBarrier(concurrency);
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < concurrency; i++) {
                int idx = i;
                futures.add(pool.submit(() -> {
                    await(barrier);
                    results.set(idx, task.call());
                    return null;
                }));
            }
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        List<T> out = new ArrayList<>(concurrency);
        for (int i = 0; i < concurrency; i++) {
            out.add(results.get(i));
        }
        return out;
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Interrupted/failed awaiting concurrency barrier", e);
        }
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
}
