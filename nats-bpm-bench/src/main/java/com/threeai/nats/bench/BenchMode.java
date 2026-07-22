package com.threeai.nats.bench;

/**
 * Which dispatch/history idiom a scenario exercises (basamak-1 03_classes/5_bench.md §1;
 * basamak-2 history-offload/03_classes/5_bench.md §1).
 */
public enum BenchMode {
    /** Classic {@code camunda:type="external"} (not A2-swapped) + a simulated fetchAndLock poll-loop. */
    NATIVE_POLL_BASELINE,
    /** Same BPMN model, topic in the A2 topic list — {@code A2ExternalTaskBehavior} active. */
    A2_PUSH,
    /** Basamak-2: default {@code DbHistoryEventHandler}, no offload — {@code HistoryBenchScenario}'s reference mode. */
    DB_HISTORY_BASELINE,
    /** Basamak-2: {@code NatsHistoryEventHandler} installed, classes cut over — offload active. */
    HISTORY_OFFLOAD,
    /** Basamak-3: built-in {@code ByteArrayValueSerializer} only, no externalization — {@code
     *  LargeVariableBenchScenario}'s reference mode. */
    LARGE_VARIABLE_BASELINE,
    /** Basamak-3: {@code LargeVariableSerializer} + deferred externalization active. */
    LARGE_VARIABLE_EXTERNALIZED,
    /** Basamak-4: {@code NatsOutboundPublisher} classifies the bench message type CRITICAL —
     *  tx-in {@code outbound_message_outbox} write active — {@code OutboundBenchScenario}. */
    OUTBOUND_CRITICAL,
    /** Basamak-4: {@code NatsOutboundPublisher} classifies the bench message type BEST_EFFORT —
     *  post-commit publish only, zero {@code outbound_message_outbox} writes. */
    OUTBOUND_BEST_EFFORT
}
