package com.threeai.nats.bench;

/** Which dispatch idiom a {@link BenchScenario} exercises (03_classes/5_bench.md §1). */
public enum BenchMode {
    /** Classic {@code camunda:type="external"} (not A2-swapped) + a simulated fetchAndLock poll-loop. */
    NATIVE_POLL_BASELINE,
    /** Same BPMN model, topic in the A2 topic list — {@code A2ExternalTaskBehavior} active. */
    A2_PUSH
}
