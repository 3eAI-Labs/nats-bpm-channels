package com.threeai.nats.bench.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

import com.threeai.nats.bench.BenchEnvironment;
import com.threeai.nats.bench.BenchMode;
import com.threeai.nats.bench.PgStatStatementsSnapshotter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ContainerLaunchException;

/**
 * D-F hard gate: {@link BenchMode#HISTORY_OFFLOAD} must produce ZERO {@code ACT_HI_*} write ops
 * once every class is cut over, while the {@code compact_history_outbox} table absorbs those same
 * writes instead (`03_classes/5_bench.md` §1). {@code @Tag("bench")} — nightly/manual only.
 */
@Tag("bench")
class HistoryBenchScenarioTest {

    private static final int INSTANCE_COUNT = 5;

    @Test
    void historyOffloadProducesZeroActHiWritesWhileOutboxAbsorbsThem() {
        try (BenchEnvironment env = BenchEnvironment.start();
             HistoryBenchScenario scenario = new HistoryBenchScenario(env, new PgStatStatementsSnapshotter())) {
            HistoryDbWriteOpReport baseline = scenario.run(BenchMode.DB_HISTORY_BASELINE, INSTANCE_COUNT);
            HistoryDbWriteOpReport offload = scenario.run(BenchMode.HISTORY_OFFLOAD, INSTANCE_COUNT);

            // Baseline mode is the comparison reference -- it DID write ACT_HI_* rows for real.
            assertThat(baseline.actHiWriteOpCount()).isGreaterThan(0);
            assertThat(baseline.passesHardGate()).isTrue(); // no gate in baseline mode

            // Offload mode: hard gate (BUS_BENCH_HISTORY_METRIC_REGRESSION).
            assertThat(offload.passesHardGate()).isTrue();
            assertThat(offload.actHiWriteOpCount()).isZero();
            assertThat(offload.compactOutboxRowCount()).isGreaterThan(0);
        } catch (ContainerLaunchException dockerUnavailable) {
            // SYS_BENCH_HISTORY_ENVIRONMENT_UNAVAILABLE (FINDING-004, faz-5 review) -- the
            // basamak-2-specific registry code (distinct from basamak-1's generic
            // SYS_BENCH_ENVIRONMENT_UNAVAILABLE, e.g. ExternalTaskLifecycleBenchTest): this
            // scenario needs BOTH Testcontainers Postgres AND NATS, so its own history-scoped
            // abort signal is meaningful for ops triage.
            abort("SYS_BENCH_HISTORY_ENVIRONMENT_UNAVAILABLE — Docker/Testcontainers unavailable, main CI is not blocked");
        } catch (Exception e) {
            throw new RuntimeException("History bench scenario failed", e);
        }
    }
}
