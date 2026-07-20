package com.threeai.nats.bench.history;

import com.threeai.nats.bench.BenchMode;

/**
 * D-F fingerprint report for {@link HistoryBenchScenario} (`03_classes/5_bench.md` §1).
 *
 * @param actHiWriteOpCount           {@code ACT_HI_*}-touching INSERT/UPDATE calls
 *                                    (pg_stat_statements) — MUST be 0 in {@link
 *                                    BenchMode#HISTORY_OFFLOAD} for cut-over classes.
 * @param compactOutboxRowCount       {@code compact_history_outbox} row count (audit-critical:
 *                                    &lt;=1/tx per instance, LLD-Q1 — companion rows tracked
 *                                    separately).
 * @param compactOutboxPayloadRowCount large-payload companion row count — reported but NOT part
 *                                    of the hard-gate count (LLD-Q1).
 */
public record HistoryDbWriteOpReport(
        long actHiWriteOpCount, long compactOutboxRowCount, long compactOutboxPayloadRowCount, BenchMode mode) {

    /** BUS_BENCH_HISTORY_METRIC_REGRESSION — the ONE hard gate (D-F). */
    public boolean passesHardGate() {
        if (mode == BenchMode.HISTORY_OFFLOAD) {
            return actHiWriteOpCount == 0;
        }
        return true; // DB_HISTORY_BASELINE only produces the comparison reference, no gate.
    }
}
