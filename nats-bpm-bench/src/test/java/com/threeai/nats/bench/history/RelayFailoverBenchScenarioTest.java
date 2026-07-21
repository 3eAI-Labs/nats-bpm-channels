package com.threeai.nats.bench.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

import java.time.Duration;

import com.threeai.nats.bench.BenchEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ContainerLaunchException;

/**
 * Real relay-failover RTO/RPO measurement (`TEST_SPECIFICATIONS.md` (h), `01_overview.md`
 * "Phase3'ün devrettiği doğrulamalar #5") — supersedes the Phase 5 design-only placeholder
 * (`RelayFailoverBenchScenario`'s CODER-NOTE) now that Phase 5.5 (Testing) is in scope.
 *
 * <p>Wall-clock cost: this test genuinely waits out (a fraction of, via fast standby polling —
 * see {@code RelayFailoverBenchScenario#POLL_INTERVAL}) the real 60s production lease TTL, so it
 * is {@code @Tag("bench")} — nightly/manual only, matching {@code HistoryBenchScenarioTest} /
 * {@code ExternalTaskLifecycleBenchTest} (BR-OBS-003, does not gate the main CI pipeline).
 */
@Tag("bench")
class RelayFailoverBenchScenarioTest {

    private static final int ENGINE_NODE_REPLICA_COUNT = 3;

    @Test
    void leaderCrash_standbyRecoversWithinLeaseTtl_zeroAuditLoss() {
        try (BenchEnvironment env = BenchEnvironment.start()) {
            RelayFailoverBenchScenario scenario = new RelayFailoverBenchScenario();

            RelayFailoverReport report = scenario.run(env, ENGINE_NODE_REPLICA_COUNT);

            System.out.println("RelayFailoverBenchScenario measured: timeToRecover=" + report.timeToRecover()
                    + " leaseTtl=" + report.leaseTtl() + " recoveredWithinLeaseTtl=" + report.recoveredWithinLeaseTtl()
                    + " outboxRowsSeeded=" + report.outboxRowsSeeded()
                    + " outboxRowsUnrecoveredAfterFailover=" + report.outboxRowsUnrecoveredAfterFailover());

            // RTO: a standby node acquires the lease and resumes relaying at (never meaningfully
            // before -- see RelayFailoverBenchScenario class Javadoc QA-FINDING on TTL-expiry
            // recovery) the 60s (2 * 30s relayCyclePeriod) production lease-TTL bound (LLD "#3").
            assertThat(report.leaseTtl()).isEqualTo(Duration.ofSeconds(60));
            assertThat(report.recoveredWithinLeaseTtl()).isTrue();
            // Recovery cannot happen strictly before the TTL elapses (KV-expiry-driven) -- the
            // honest upper bound this harness can assert is leaseTtl + its own poll granularity.
            assertThat(report.timeToRecover()).isLessThanOrEqualTo(report.leaseTtl().plusSeconds(1));

            // RPO: every audit-critical row seeded while the crashed leader held the lease is
            // durably relayed by the new leader post-failover -- zero loss, only delay.
            assertThat(report.outboxRowsSeeded()).isEqualTo(5);
            assertThat(report.outboxRowsUnrecoveredAfterFailover()).isZero();
            assertThat(report.zeroAuditLoss()).isTrue();
        } catch (ContainerLaunchException dockerUnavailable) {
            // SYS_BENCH_HISTORY_ENVIRONMENT_UNAVAILABLE (same abort contract as
            // HistoryBenchScenarioTest) -- Docker/Testcontainers unavailable, main CI is not
            // blocked.
            abort("SYS_BENCH_HISTORY_ENVIRONMENT_UNAVAILABLE — Docker/Testcontainers unavailable, main CI is not blocked");
        } catch (Exception e) {
            throw new RuntimeException("Relay failover bench scenario failed", e);
        }
    }

    @Test
    void run_lessThanTwoReplicas_rejected() {
        try (BenchEnvironment env = BenchEnvironment.start()) {
            RelayFailoverBenchScenario scenario = new RelayFailoverBenchScenario();

            org.assertj.core.api.Assertions.assertThatThrownBy(() -> scenario.run(env, 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(">=2 engine-node replicas");
        } catch (ContainerLaunchException dockerUnavailable) {
            abort("SYS_BENCH_HISTORY_ENVIRONMENT_UNAVAILABLE — Docker/Testcontainers unavailable, main CI is not blocked");
        } catch (Exception e) {
            throw new RuntimeException("Relay failover bench scenario failed", e);
        }
    }
}
