package com.threeai.nats.bench.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

import com.threeai.nats.bench.BenchEnvironment;
import com.threeai.nats.bench.BenchMode;
import com.threeai.nats.bench.PgStatStatementsSnapshotter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ContainerLaunchException;

/**
 * Basamak-4 (docs/09-outbound-handoff.md §3) hard gates: {@link BenchMode#OUTBOUND_BEST_EFFORT}
 * must produce ZERO {@code outbound_message_outbox} writes (post-commit path never touches the
 * table); {@link BenchMode#OUTBOUND_CRITICAL} must produce AT MOST ONE row per process instance
 * driven (&lt;=1/tx). {@code @Tag("bench")} — nightly/manual only, mirrors {@code
 * HistoryBenchScenarioTest}.
 */
@Tag("bench")
class OutboundBenchScenarioTest {

    private static final int INSTANCE_COUNT = 5;

    @Test
    void bestEffortProducesZeroOutboxWrites_criticalProducesAtMostOnePerInstance() {
        try (BenchEnvironment env = BenchEnvironment.start();
             OutboundBenchScenario scenario = new OutboundBenchScenario(env, new PgStatStatementsSnapshotter())) {
            OutboundDbWriteOpReport bestEffort = scenario.run(BenchMode.OUTBOUND_BEST_EFFORT, INSTANCE_COUNT);
            OutboundDbWriteOpReport critical = scenario.run(BenchMode.OUTBOUND_CRITICAL, INSTANCE_COUNT);

            // Best-effort hard gate: zero additional DB writes (post-commit path only).
            assertThat(bestEffort.passesHardGate()).isTrue();
            assertThat(bestEffort.outboxInsertCount()).isZero();
            assertThat(bestEffort.outboxRowCount()).isZero();

            // Critical hard gate: <=1 outbound_message_outbox row per transaction/instance.
            assertThat(critical.passesHardGate()).isTrue();
            assertThat(critical.outboxInsertCount()).isEqualTo(INSTANCE_COUNT);
            assertThat(critical.outboxRowCount()).isEqualTo(INSTANCE_COUNT);
        } catch (ContainerLaunchException dockerUnavailable) {
            abort("SYS_BENCH_OUTBOUND_ENVIRONMENT_UNAVAILABLE — Docker/Testcontainers unavailable, main CI is not blocked");
        } catch (Exception e) {
            throw new RuntimeException("Outbound bench scenario failed", e);
        }
    }
}
