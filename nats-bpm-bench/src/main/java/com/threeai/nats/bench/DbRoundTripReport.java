package com.threeai.nats.bench;

/**
 * BR-OBS-001's acceptance table (Task INSERT, Poll, fetchAndLock UPDATE, complete tx, sweep
 * read) — the bench module's primary output (03_classes/5_bench.md §1, DB_ACCESS_MAP.md §4).
 */
public record DbRoundTripReport(
        long taskInsertCount, long pollQueryCount, long fetchAndLockCount,
        long completeTxCount, long sweepReadCount, BenchMode mode) {

    /** BUS_BENCH_METRIC_REGRESSION — the ONE hard gate (Q7). */
    public boolean passesHardGate() {
        if (mode == BenchMode.A2_PUSH) {
            return pollQueryCount == 0 && fetchAndLockCount == 0; // NFR-P1 verification
        }
        return true; // no gate in baseline mode — it only produces the comparison reference
    }
}
