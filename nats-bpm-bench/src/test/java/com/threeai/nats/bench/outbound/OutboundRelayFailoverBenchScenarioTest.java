package com.threeai.nats.bench.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

import java.time.Duration;

import com.threeai.nats.bench.BenchEnvironment;
import com.threeai.nats.bench.history.RelayFailoverReport;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ContainerLaunchException;

/**
 * Real outbound-relay-failover RTO/RPO measurement — basamak-4 mirror of {@code
 * RelayFailoverBenchScenarioTest} (basamak-2's real-measurement precedent), applied to {@code
 * OutboundMessageRelay} (docs/09-outbound-handoff.md D-A'/D-F'). Phase 5.5 (QA) reliability suite.
 *
 * <p>Wall-clock cost: like its history-relay sibling, this test genuinely waits out (a fraction
 * of, via fast standby polling) the real 60s production lease TTL — {@code @Tag("bench")}
 * (matches the established nats-bpm-bench convention/exclusion mechanism, {@code
 * bench.excludedGroups}) AND {@code @Tag("reliability")} (so the module-wide reliability-suite
 * invocation, {@code -Dgroups=reliability -Dbench.excludedGroups=}, also picks it up). Nightly/
 * manual only, does not gate the main CI pipeline (BR-OBS-003).
 */
@Tag("bench")
@Tag("reliability")
class OutboundRelayFailoverBenchScenarioTest {

    private static final int ENGINE_NODE_REPLICA_COUNT = 3;

    @Test
    void leaderCrash_standbyRecoversWithinLeaseTtl_zeroAuditLoss() {
        try (BenchEnvironment env = BenchEnvironment.start()) {
            OutboundRelayFailoverBenchScenario scenario = new OutboundRelayFailoverBenchScenario();

            RelayFailoverReport report = scenario.run(env, ENGINE_NODE_REPLICA_COUNT);

            System.out.println("OutboundRelayFailoverBenchScenario measured: timeToRecover=" + report.timeToRecover()
                    + " leaseTtl=" + report.leaseTtl() + " recoveredWithinLeaseTtl=" + report.recoveredWithinLeaseTtl()
                    + " outboxRowsSeeded=" + report.outboxRowsSeeded()
                    + " outboxRowsUnrecoveredAfterFailover=" + report.outboxRowsUnrecoveredAfterFailover());

            // RTO: a standby node acquires the lease and resumes relaying at (never meaningfully
            // before -- TTL-expiry-driven recovery, same QA-FINDING as RelayFailoverBenchScenario)
            // the 60s (2 * 30s relayCyclePeriod) production lease-TTL bound.
            assertThat(report.leaseTtl()).isEqualTo(Duration.ofSeconds(60));
            assertThat(report.recoveredWithinLeaseTtl()).isTrue();
            assertThat(report.timeToRecover()).isLessThanOrEqualTo(report.leaseTtl().plusSeconds(1));

            // RPO: every audit-critical row seeded while the crashed leader held the lease is
            // durably relayed by the new leader post-failover -- zero loss, only delay.
            assertThat(report.outboxRowsSeeded()).isEqualTo(5);
            assertThat(report.outboxRowsUnrecoveredAfterFailover()).isZero();
            assertThat(report.zeroAuditLoss()).isTrue();
        } catch (ContainerLaunchException dockerUnavailable) {
            abort("SYS_BENCH_OUTBOUND_ENVIRONMENT_UNAVAILABLE — Docker/Testcontainers unavailable, main CI is not blocked");
        } catch (Exception e) {
            throw new RuntimeException("Outbound relay failover bench scenario failed", e);
        }
    }

    @Test
    void run_lessThanTwoReplicas_rejected() {
        try (BenchEnvironment env = BenchEnvironment.start()) {
            OutboundRelayFailoverBenchScenario scenario = new OutboundRelayFailoverBenchScenario();

            org.assertj.core.api.Assertions.assertThatThrownBy(() -> scenario.run(env, 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(">=2 engine-node replicas");
        } catch (ContainerLaunchException dockerUnavailable) {
            abort("SYS_BENCH_OUTBOUND_ENVIRONMENT_UNAVAILABLE — Docker/Testcontainers unavailable, main CI is not blocked");
        } catch (Exception e) {
            throw new RuntimeException("Outbound relay failover bench scenario failed", e);
        }
    }
}
