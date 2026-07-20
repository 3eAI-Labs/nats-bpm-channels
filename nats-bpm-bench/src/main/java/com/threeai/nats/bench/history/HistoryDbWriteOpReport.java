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

    /**
     * BUS_BENCH_HISTORY_METRIC_REGRESSION — the ONE hard gate (D-F).
     *
     * <p><b>CODER-NOTE (FINDING-004, faz-5 review — BUS_BENCH_BASELINE_MISSING /
     * SYS_BENCH_HISTORY_SLI_DRIFT not yet emitted):</b> the ERROR_REGISTRY also names two
     * SUPPORTING (non-hard-gate) signals this report does not currently produce: {@code
     * BUS_BENCH_BASELINE_MISSING} (the {@code DB_HISTORY_BASELINE} reference measurement could
     * not be captured for comparison) and {@code SYS_BENCH_HISTORY_SLI_DRIFT} (a secondary SLI
     * comparison, beyond this ONE hard gate, drifting outside an expected band). Neither is
     * fabricated here: {@link HistoryBenchScenario#run} always runs baseline synchronously before
     * offload within the SAME scenario instance (a genuinely "missing" baseline is not a state
     * the current single-scenario design can reach — it would require a caller invoking {@code
     * HISTORY_OFFLOAD} without ever having called {@code DB_HISTORY_BASELINE} first, which no
     * current caller does), and no supporting-SLI comparison beyond the hard gate exists yet to
     * drift. Both are bounded follow-ups (same design-only-deferral discipline as {@link
     * RelayFailoverBenchScenario}), not implemented merely to produce a log line.
     */
    public boolean passesHardGate() {
        if (mode == BenchMode.HISTORY_OFFLOAD) {
            return actHiWriteOpCount == 0;
        }
        return true; // DB_HISTORY_BASELINE only produces the comparison reference, no gate.
    }
}
