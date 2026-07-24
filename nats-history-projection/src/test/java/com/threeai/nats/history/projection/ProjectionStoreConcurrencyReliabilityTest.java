package com.threeai.nats.history.projection;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.threeai.nats.core.db.SqlMigrationRunner;
import com.threeai.nats.core.history.HistoryClassNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Phase 5.5 (QA) reliability suite -- idempotency-at-scale for {@link ProjectionStore}'s
 * merge-upsert (BR-REL-003, ADR-0011) and append-only dedup (BR-REL-006) protocols under REAL
 * concurrent duplicate delivery (real Postgres, real threads), complementing {@link
 * ProjectionStoreTest}'s sequential-call proof of the SAME-thread merge-upsert tie-break logic.
 *
 * <p>Production reachability: {@link HistoryProjectionConsumerBootstrap} normally serializes
 * delivery per partition (one NATS {@code Dispatcher} thread per partition), but JetStream's
 * at-least-once contract plus rolling-deploy partition-ownership overlap windows mean the SAME
 * entity's events can genuinely be processed concurrently by two overlapping consumer instances --
 * this test proves what the store does when that happens, not merely what a single thread does.
 *
 * <p>Two of the four tests below are DELIBERATE, LABELED QA-FINDINGS: they assert the intended
 * architectural invariant and are EXPECTED TO FAIL against the current implementation, reproducing
 * genuine (non-mock) concurrency defects with real Postgres. This is intentional evidence, not a
 * broken test -- see each method's Javadoc for root cause + recommended fix direction (production
 * code is out of scope for the Tester phase; these are handed to the coder phase as findings).
 *
 * <p>Heavy (real Postgres, concurrent load) -- {@code @Tag("reliability")}, excluded from the
 * default {@code mvn test} run. Run explicitly via
 * {@code mvn test -Dgroups=reliability -Dreliability.excludedGroups=}.
 */
@Tag("reliability")
@Testcontainers
class ProjectionStoreConcurrencyReliabilityTest {

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
            stmt.execute("TRUNCATE process_instance_history, operation_log_history");
        }
    }

    /**
     * TRUE redelivery/overlap duplicate: N concurrent racers all process the LITERAL SAME wire
     * message (identical {@link EntityHistoryRecord}, so identical {@code event_time} hence
     * identical {@code partition_anchor_at}) -- the shape a genuine JetStream at-least-once
     * redelivery, or a rolling-deploy partition-ownership overlap, actually produces (the consumer
     * never invents a new timestamp per delivery attempt; it re-parses the SAME message). Here the
     * unique index DOES collide across every racer's potential insert, so exactly ONE physical row
     * must survive -- and it does (positive control, contrasts with the two QA-FINDING tests below).
     */
    @Test
    void concurrentDuplicateRedelivery_identicalMessage_neverDuplicatesRow() throws Exception {
        int concurrency = 10;
        String entityId = "concurrent-redelivery-proc";
        EntityHistoryRecord sameRecord = procInstRecord(entityId, 1, "ACTIVE"); // ONE fixed eventTime for all racers

        List<Object> outcomes = runConcurrently(concurrency, idx -> {
            try {
                return store.upsertEntity(HistoryClassNames.PROCINST, sameRecord);
            } catch (IllegalStateException conflict) {
                return conflict; // acceptable: DB-level unique-index rejection, safely retried upstream
            }
        });

        assertThat(countRows("process_instance_history", "process_instance_id", entityId)).isEqualTo(1);
        long applied = outcomes.stream().filter(o -> o == UpsertOutcome.APPLIED).count();
        long conflicts = outcomes.stream().filter(o -> o instanceof IllegalStateException).count();
        long staleDiscarded = outcomes.stream().filter(o -> o == UpsertOutcome.STALE_DISCARDED).count();
        assertThat(applied + conflicts + staleDiscarded).isEqualTo(concurrency);
        assertThat(applied).isGreaterThanOrEqualTo(1);
    }

    /**
     * QA-FINDING (real, reproduced with real Postgres -- NOT a mock artifact): {@link
     * ProjectionStore#upsertEntity}'s {@code selectExisting()}-then-{@code insertNew()} is a
     * classic check-then-act race with NO atomicity guard between the two steps. When N DISTINCT
     * events (distinct {@code event_time}, hence distinct {@code partition_anchor_at} -- e.g. two
     * genuinely different lifecycle events for the SAME not-yet-projected entity arriving close
     * together under overlapping consumers) all run {@code selectExisting()} before ANY of them has
     * committed its {@code insertNew()}, EVERY racer observes "not found" and EVERY racer inserts
     * its OWN row -- the {@code unq_process_instance_history_entity} unique index (engine_id,
     * process_instance_id, partition_anchor_at) does NOT collide across them because {@code
     * partition_anchor_at} differs per racer. Result: the SAME logical entity is silently SPLIT
     * across multiple physical rows/partitions, with no self-healing merge -- a later {@code
     * selectExisting()} (unordered {@code SELECT ... LIMIT 1}) picks ONE of them arbitrarily, so
     * subsequent updates and {@code HistoryQueryApi} reads can non-deterministically see stale or
     * incomplete state depending on which duplicate row is returned. Root cause: {@code insertNew}
     * has no {@code ON CONFLICT} guard and {@code selectExisting}+{@code insertNew} do not run
     * inside a single serializable/locking unit. Recommended fix direction (NOT applied here --
     * test phase, production code is out of scope): make the entity-lifecycle path atomic the same
     * way {@link ProjectionStore#insertLogEvent} already is (a single {@code INSERT ... ON CONFLICT
     * (engine_id, process_instance_id) DO UPDATE ... WHERE incoming.stream_sequence >
     * existing.stream_sequence}, keyed on a partition-INDEPENDENT unique constraint), rather than
     * two separate statements racing across a network round-trip.
     */
    @Test
    void concurrentFirstUpsert_distinctEventsForSameNewEntity_QA_FINDING_canSplitEntityAcrossMultipleRows()
            throws Exception {
        int concurrency = 10;
        String entityId = "concurrent-new-proc";

        runConcurrently(concurrency, idx -> {
            try {
                return store.upsertEntity(HistoryClassNames.PROCINST, procInstRecord(entityId, idx + 1, "ACTIVE"));
            } catch (IllegalStateException conflict) {
                return conflict;
            }
        });

        // Intended architectural invariant (ADR-0011/0012, merge-upsert "one row per entity"): this
        // assertion is EXPECTED TO FAIL today -- the failure IS the QA-FINDING evidence, not a
        // broken test. See this method's Javadoc for root cause + recommended fix direction.
        assertThat(countRows("process_instance_history", "process_instance_id", entityId))
                .as("QA-FINDING: concurrent distinct-timestamp first-events for a new entity can silently split "
                        + "it across multiple physical rows (partition_anchor_at differs per racer, so the "
                        + "unique index never collides) -- see this test's Javadoc for root cause")
                .isEqualTo(1);
    }

    /**
     * QA-FINDING (real, reproduced with real Postgres): {@link ProjectionStore#upsertEntity}'s
     * {@code updateExisting()} UPDATE statement has NO {@code AND stream_sequence < ?} (or
     * equivalent CAS) guard in its WHERE clause -- it unconditionally overwrites whatever the
     * CALLER already decided ("incoming.streamSequence > existingSeq", read via a SEPARATE, earlier
     * {@code selectExisting()} call) without re-checking that decision at write time. Under genuine
     * concurrent updates (barrier-synchronized racers, all reading the SAME stale {@code
     * existingSeq} before any of them writes), the LAST physical UPDATE to commit wins --
     * REGARDLESS of which racer actually carried the highest {@code stream_sequence}. A
     * lower-sequence (older/stale) event can silently overwrite a higher-sequence (newer/correct)
     * one: a genuine lost-update, not merely a benign race. Root cause: the read-decide-write
     * sequence spans two round-trips with no optimistic-concurrency guard on the write. Recommended
     * fix direction (NOT applied here): add {@code AND stream_sequence < ?} to {@code
     * updateExisting}'s WHERE clause (checking the affected-row count to detect a lost race) -- the
     * same CAS discipline {@code SweepLeaderLease}/{@code ContentAddressedLargePayloadStore} already
     * use elsewhere in this codebase.
     */
    @Test
    void concurrentUpdate_existingEntity_QA_FINDING_updateNotGuardedByStreamSequence_canLoseNewerState()
            throws Exception {
        String entityId = "concurrent-update-proc";
        store.upsertEntity(HistoryClassNames.PROCINST, procInstRecord(entityId, 1, "ACTIVE"));
        int concurrency = 10;
        long highestSequence = concurrency + 1;

        runConcurrently(concurrency, idx ->
                store.upsertEntity(HistoryClassNames.PROCINST, procInstRecord(entityId, idx + 2,
                        idx + 2 == highestSequence ? "COMPLETED" : "ACTIVE")));

        assertThat(countRows("process_instance_history", "process_instance_id", entityId)).isEqualTo(1);
        // Intended invariant (ADR-0012 tie-break authority): EXPECTED TO FAIL today under real
        // concurrency -- see this method's Javadoc for root cause + recommended fix direction.
        assertThat(stateOf(entityId))
                .as("QA-FINDING: updateExisting()'s UPDATE has no stream_sequence CAS guard -- the LAST physical "
                        + "writer wins, not the highest-stream_sequence writer, under real concurrency")
                .isEqualTo("COMPLETED");
    }

    /**
     * N concurrent {@code insertLogEvent} calls (barrier-synchronized) carrying the EXACT SAME
     * dedup key (engine_id, history_event_id, event_type, event_time) -- the append-only
     * counterpart of the entity-lifecycle duplicate-storm above. Unlike {@code upsertEntity}, this
     * path IS built on a single atomic {@code ON CONFLICT ... DO NOTHING} statement (class
     * Javadoc): exactly one racer must observe {@code APPLIED}, every other racer must observe a
     * graceful {@code DEDUP_SKIPPED} -- NO thrown exceptions, unlike the entity-lifecycle path
     * (positive control).
     */
    @Test
    void concurrentInsertLogEvent_sameDedupKey_exactlyOneApplied_restGracefullyDeduped_noExceptions() throws Exception {
        int concurrency = 10;
        Instant fixedEventTime = Instant.parse("2026-01-01T00:00:00Z");
        LogHistoryRecord record = new LogHistoryRecord("camunda", "concurrent-log-proc", "evt-storm-1", "create",
                1, fixedEventTime, Map.of("operationType", "Complete"));

        List<Object> outcomes = runConcurrently(concurrency, idx -> store.insertLogEvent(HistoryClassNames.OP_LOG, record));

        long applied = outcomes.stream().filter(o -> o == UpsertOutcome.APPLIED).count();
        long dedupSkipped = outcomes.stream().filter(o -> o == UpsertOutcome.DEDUP_SKIPPED).count();
        assertThat(applied).isEqualTo(1);
        assertThat(dedupSkipped).isEqualTo(concurrency - 1);
        assertThat(countRows("operation_log_history", "history_event_id", "evt-storm-1")).isEqualTo(1);
    }

    private EntityHistoryRecord procInstRecord(String entityId, long streamSequence, String state) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("businessKey", "biz-1");
        fields.put("state", state);
        return new EntityHistoryRecord("camunda", entityId, entityId, streamSequence, Instant.now(), fields);
    }

    private List<Object> runConcurrently(int concurrency, java.util.function.IntFunction<Object> task) throws Exception {
        AtomicReferenceArray<Object> results = new AtomicReferenceArray<>(concurrency);
        CyclicBarrier barrier = new CyclicBarrier(concurrency);
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < concurrency; i++) {
                int idx = i;
                Callable<Void> callable = () -> {
                    awaitBarrier(barrier);
                    results.set(idx, task.apply(idx));
                    return null;
                };
                futures.add(pool.submit(callable));
            }
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        List<Object> out = new ArrayList<>(concurrency);
        for (int i = 0; i < concurrency; i++) {
            out.add(results.get(i));
        }
        return out;
    }

    private static void awaitBarrier(CyclicBarrier barrier) {
        try {
            barrier.await(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Interrupted/failed awaiting concurrency barrier", e);
        }
    }

    private long countRows(String table, String idColumn, String idValue) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement("SELECT count(*) FROM " + table + " WHERE " + idColumn + " = ?")) {
            stmt.setString(1, idValue);
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
