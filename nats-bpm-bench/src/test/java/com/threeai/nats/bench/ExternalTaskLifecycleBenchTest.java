package com.threeai.nats.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ContainerLaunchException;

/**
 * BR-OBS-001's single hard gate: A2-push must produce ZERO poll / fetchAndLock queries, and must
 * not increase task-insert or complete-tx counts versus the native-poll baseline
 * (03_classes/5_bench.md §2). {@code @Tag("bench")} — nightly/manual only, excluded from the
 * main CI run (see {@code nats-bpm-bench/pom.xml} surefire {@code excludedGroups}).
 */
@Tag("bench")
class ExternalTaskLifecycleBenchTest {

    private static final int TASK_COUNT = 20;

    @Test
    void a2PushProducesZeroPollAndZeroFetchAndLock() {
        try (BenchEnvironment env = BenchEnvironment.start()) {
            DbRoundTripReport baseline = new ExternalTaskLifecycleScenario()
                    .run(env, BenchMode.NATIVE_POLL_BASELINE, TASK_COUNT);
            DbRoundTripReport target = new ExternalTaskLifecycleScenario()
                    .run(env, BenchMode.A2_PUSH, TASK_COUNT);

            assertThat(target.passesHardGate()).isTrue(); // BUS_BENCH_METRIC_REGRESSION -> build-fail
            assertThat(target.taskInsertCount()).isEqualTo(baseline.taskInsertCount()); // does not increase
            assertThat(target.completeTxCount()).isEqualTo(baseline.completeTxCount()); // does not increase
            assertThat(target.pollQueryCount()).isZero();
            assertThat(target.fetchAndLockCount()).isZero();
        } catch (ContainerLaunchException dockerUnavailable) {
            abort("SYS_BENCH_ENVIRONMENT_UNAVAILABLE — Docker/Testcontainers unavailable, main CI is not blocked");
        } catch (Exception e) {
            throw new RuntimeException("Bench scenario failed", e);
        }
    }
}
