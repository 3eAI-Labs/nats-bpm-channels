package com.threeai.nats.bench;

/** One benchmark scenario, run in either {@link BenchMode} against the same {@link BenchEnvironment}. */
public interface BenchScenario {

    DbRoundTripReport run(BenchEnvironment env, BenchMode mode, int taskCount) throws Exception;
}
