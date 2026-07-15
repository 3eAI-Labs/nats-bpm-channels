package com.threeai.nats.bench;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.externaltask.LockedExternalTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BR-OBS-003's single scenario, run in both modes: start {@code taskCount} instances, drive
 * every external task to completion, and let the caller measure the resulting
 * {@code pg_stat_statements} deltas (03_classes/5_bench.md §1).
 */
public class ExternalTaskLifecycleScenario implements BenchScenario {

    private static final Logger log = LoggerFactory.getLogger(ExternalTaskLifecycleScenario.class);
    private static final long LOCK_DURATION_MILLIS = 60_000L;
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(60);

    @Override
    public DbRoundTripReport run(BenchEnvironment env, BenchMode mode, int taskCount) throws Exception {
        PgStatStatementsSnapshotter snapshotter = new PgStatStatementsSnapshotter();
        snapshotter.reset(env.dataSource());

        if (mode == BenchMode.NATIVE_POLL_BASELINE) {
            runNativePollBaseline(env, taskCount);
        } else {
            runA2Push(env, taskCount);
        }

        Snapshot snapshot = snapshotter.capture(env.dataSource());
        if (System.getenv("BENCH_DEBUG") != null) {
            for (QueryStat stat : snapshot.queryStats()) {
                log.info("DEBUG[{}] calls={} query={}", mode, stat.calls(), stat.query());
            }
        }
        return snapshot.classify(mode);
    }

    private void runNativePollBaseline(BenchEnvironment env, int taskCount) {
        RuntimeService runtimeService = env.runtimeService();
        ExternalTaskService externalTaskService = env.externalTaskService();

        for (int i = 0; i < taskCount; i++) {
            runtimeService.startProcessInstanceByKey("benchNativeProcess");
        }

        int completed = 0;
        Instant deadline = Instant.now().plus(AWAIT_TIMEOUT);
        while (completed < taskCount && Instant.now().isBefore(deadline)) {
            List<LockedExternalTask> locked = externalTaskService.fetchAndLock(taskCount, "bench-native-worker")
                    .topic(BenchEnvironment.NATIVE_TOPIC, LOCK_DURATION_MILLIS)
                    .execute();
            for (LockedExternalTask task : locked) {
                externalTaskService.complete(task.getId(), "bench-native-worker");
                completed++;
            }
            if (locked.isEmpty()) {
                sleepBriefly();
            }
        }
        if (completed < taskCount) {
            log.warn("NATIVE_POLL_BASELINE: only {}/{} tasks completed before timeout", completed, taskCount);
        }
    }

    private void runA2Push(BenchEnvironment env, int taskCount) throws InterruptedException {
        RuntimeService runtimeService = env.runtimeService();
        for (int i = 0; i < taskCount; i++) {
            runtimeService.startProcessInstanceByKey("benchA2Process");
        }

        Instant deadline = Instant.now().plus(AWAIT_TIMEOUT);
        long remaining;
        do {
            remaining = runtimeService.createProcessInstanceQuery()
                    .processDefinitionKey("benchA2Process")
                    .count();
            if (remaining > 0) {
                Thread.sleep(100);
            }
        } while (remaining > 0 && Instant.now().isBefore(deadline));

        if (remaining > 0) {
            log.warn("A2_PUSH: {} process instances still incomplete after timeout", remaining);
        }
    }

    private void sleepBriefly() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
