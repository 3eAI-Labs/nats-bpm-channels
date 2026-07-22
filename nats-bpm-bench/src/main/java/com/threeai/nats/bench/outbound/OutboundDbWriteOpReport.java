package com.threeai.nats.bench.outbound;

import com.threeai.nats.bench.BenchMode;

/**
 * D-F-equivalent fingerprint report for {@link OutboundBenchScenario} (docs/09-outbound-handoff.md
 * §3 "outbound publish DB-yazım-op profili").
 *
 * @param outboxInsertCount {@code outbound_message_outbox}-touching INSERT calls (pg_stat_statements).
 * @param outboxRowCount    current {@code outbound_message_outbox} row count (informational — the
 *                          relay is never driven by this scenario, so it always equals {@code
 *                          outboxInsertCount} for {@link BenchMode#OUTBOUND_CRITICAL}).
 * @param instanceCount     number of process instances driven this run.
 */
public record OutboundDbWriteOpReport(long outboxInsertCount, long outboxRowCount, int instanceCount, BenchMode mode) {

    /**
     * The TWO hard gates docs/09 §3 specifies: {@code BEST_EFFORT} publish incurs ZERO additional
     * DB writes (post-commit path never touches {@code outbound_message_outbox}); {@code CRITICAL}
     * incurs AT MOST ONE {@code outbound_message_outbox} row per transaction (one INSERT per
     * process instance driven — the bench BPMN attaches exactly one message-throw listener per
     * instance, so {@code outboxInsertCount <= instanceCount} is the "<=1/tx" gate).
     */
    public boolean passesHardGate() {
        return switch (mode) {
            case OUTBOUND_BEST_EFFORT -> outboxInsertCount == 0;
            case OUTBOUND_CRITICAL -> outboxInsertCount <= instanceCount;
            default -> true;
        };
    }
}
