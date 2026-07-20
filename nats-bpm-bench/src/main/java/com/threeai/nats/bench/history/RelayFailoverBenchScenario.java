package com.threeai.nats.bench.history;

import com.threeai.nats.bench.BenchEnvironment;

/**
 * {@code history-relay-leader} failover RTO measurement (`03_classes/5_bench.md` §3,
 * `01_overview.md` "Phase3'ün devrettiği doğrulamalar #5"): kills the current lease holder
 * mid-cycle, measures time-to-recover (a new node acquires the {@code SweepLeaderLease} and
 * resumes {@code HistoryOutboxRelay.relayCycle()}) against the lease TTL bound
 * ({@code 2 * relayCyclePeriodSeconds}, `08_config.md` §3).
 *
 * <p><b>CODER-NOTE (design-only, per explicit phase-5 task instruction):</b> the LLD itself
 * states {@code 03_classes/5_bench.md} §3: "Design only in this LLD — actual measurement is
 * phase5.5 scope". {@link #run} therefore documents the intended measurement PROCEDURE (below)
 * but deliberately does not implement it — implementing a real multi-node kill/failover harness
 * (spinning up &gt;=2 competing {@code SweepLeaderLease} holders across separate simulated engine
 * nodes, forcibly terminating the leader, and timing recovery) is nontrivial bench-infrastructure
 * work explicitly scoped to Phase 5.5 (Testing), not Phase 5 (Implementation). This is a
 * deliberate, documented placeholder — not a TODO/FIXME — verified by {@code
 * RelayFailoverBenchScenarioTest} asserting the exact deferral contract.
 *
 * <h2>Intended measurement procedure (Phase 5.5)</h2>
 * <ol>
 *   <li>Start {@code engineNodeReplicaCount} competing {@link
 *       com.threeai.nats.core.jetstream.SweepLeaderLease} holders against the SAME {@code
 *       history-relay-leader} bucket/key (one per simulated engine node), each driving its own
 *       {@code HistoryOutboxRelay.relayCycle()} loop gated on {@code isLeader()}.</li>
 *   <li>Seed {@code compact_history_outbox} with rows so the CURRENT leader has active relay
 *       work in flight.</li>
 *   <li>Forcibly terminate the current leader's lease-renewal loop (simulating a node crash —
 *       NOT a graceful {@code close()}, which would release the lease immediately and understate
 *       real-world recovery time).</li>
 *   <li>Poll the remaining nodes' {@code isLeader()} until exactly one flips {@code true} AND
 *       observably resumes draining {@code compact_history_outbox} (row count decreasing).</li>
 *   <li>Report {@link RelayFailoverReport#timeToRecover()} against the lease TTL bound.</li>
 * </ol>
 */
public class RelayFailoverBenchScenario {

    public RelayFailoverReport run(BenchEnvironment env, int engineNodeReplicaCount) {
        throw new UnsupportedOperationException(
                "RelayFailoverBenchScenario is design-only in Phase 5 (03_classes/5_bench.md §3) "
                        + "-- actual multi-node kill/failover measurement is explicit Phase 5.5 scope.");
    }
}
