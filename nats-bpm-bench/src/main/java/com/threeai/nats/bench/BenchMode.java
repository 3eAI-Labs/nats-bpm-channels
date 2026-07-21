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
    HISTORY_OFFLOAD
}
