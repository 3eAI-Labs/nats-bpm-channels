package com.threeai.nats.bench;

import java.util.List;
import java.util.Locale;

/**
 * A {@code pg_stat_statements} capture, taken immediately after
 * {@code SELECT pg_stat_statements_reset()} + one full scenario run — so every {@code calls}
 * value is already scenario-relative (no separate before/after diff needed).
 */
public record Snapshot(List<QueryStat> queryStats) {

    private static final String EXT_TASK_TABLE = "act_ru_ext_task";

    /**
     * fetchable-parity predicate columns (DB_ACCESS_MAP.md §2 SQL) — the native poller's
     * candidate-selection SELECT is the only {@code ACT_RU_EXT_TASK} query referencing this
     * combination; simple by-id/by-execution lookups (used by {@code complete()},
     * {@code handleFailure()}, etc. in BOTH modes) reference none of these.
     */
    private static final String[] FETCHABLE_PARITY_COLUMNS = {
            "lock_exp_time_", "suspension_state_", "retries_", "topic_name_"
    };

    /**
     * Classifies the captured statements into BR-OBS-001's acceptance-table shape
     * (03_classes/5_bench.md §3 — {@code fetchAndLock} is a SQL-signature FAMILY, since IN-list
     * arity can split it across multiple {@code queryid}s, not a single fingerprint).
     *
     * <p>Only SELECT/UPDATE statements matching the native poller's OWN signature (the
     * fetchable-parity predicate) count toward {@code pollQueryCount}/{@code fetchAndLockCount}
     * — generic by-id lookups used by {@code complete()}/{@code handleFailure()} in BOTH modes
     * (e.g. {@code where ID_ = ?}) are deliberately excluded; otherwise every mode would show a
     * false-positive poll/lock count and the BUS_BENCH_METRIC_REGRESSION gate could never pass.
     */
    public DbRoundTripReport classify(BenchMode mode) {
        long inserts = 0;
        long polls = 0;
        long fetchAndLocks = 0;
        long deletes = 0;
        for (QueryStat stat : queryStats) {
            String normalized = normalize(stat.query());
            if (!normalized.contains(EXT_TASK_TABLE)) {
                continue;
            }
            if (normalized.startsWith("insert")) {
                inserts += stat.calls();
            } else if (normalized.startsWith("delete")) {
                deletes += stat.calls();
            } else if (normalized.startsWith("select") && matchesFetchableParityFamily(normalized)) {
                polls += stat.calls();
            } else if (normalized.startsWith("update") && matchesFetchableParityFamily(normalized)) {
                fetchAndLocks += stat.calls();
            }
        }
        // Sweep read-count is amortized (<=1 read / S / cluster, DB_ACCESS_MAP.md §4) — a single
        // short-lived scenario run never triggers a full sweep cycle (S=120s), so it is always 0
        // here by construction, not measured from this snapshot.
        return new DbRoundTripReport(inserts, polls, fetchAndLocks, deletes, 0, mode);
    }

    private boolean matchesFetchableParityFamily(String normalizedQuery) {
        int matches = 0;
        for (String column : FETCHABLE_PARITY_COLUMNS) {
            if (normalizedQuery.contains(column)) {
                matches++;
            }
        }
        return matches >= 2; // a plain by-id/by-execution lookup never references more than one
    }

    private String normalize(String query) {
        return query == null ? "" : query.trim().toLowerCase(Locale.ROOT);
    }
}
