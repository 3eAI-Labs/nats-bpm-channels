package com.threeai.nats.bench.history;

import java.time.Duration;

/**
 * {@link RelayFailoverBenchScenario}'s measurement output (`03_classes/5_bench.md` §3,
 * `01_overview.md` "Phase3'ün devrettiği doğrulamalar #5" RTO/RPO verification,
 * `TEST_SPECIFICATIONS.md` (h) — real measurement, Phase 5.5).
 *
 * @param timeToRecover              wall-clock time from killing the current {@code
 *                                   history-relay-leader} holder to a new node acquiring the
 *                                   lease AND resuming {@code relayCycle()}
 * @param leaseTtl                   the configured lease TTL this run measured against
 *                                   ({@code 2 * relayCyclePeriodSeconds}, `08_config.md` §3) —
 *                                   this basamak's RTO bound
 * @param recoveredWithinLeaseTtl    whether {@code timeToRecover} stayed within {@code leaseTtl}
 * @param outboxRowsSeeded           audit-critical {@code compact_history_outbox} rows seeded
 *                                   while the (about to be killed) leader held the lease but
 *                                   before it ever drained them — models "active relay work in
 *                                   flight" at the moment of the simulated crash
 * @param outboxRowsUnrecoveredAfterFailover rows still present in {@code compact_history_outbox}
 *                                   after the new leader had a bounded number of drain attempts —
 *                                   0 means every seeded row was durably relayed post-failover
 *                                   (RPO=0, no audit loss, only delay)
 */
public record RelayFailoverReport(
        Duration timeToRecover,
        Duration leaseTtl,
        boolean recoveredWithinLeaseTtl,
        int outboxRowsSeeded,
        int outboxRowsUnrecoveredAfterFailover) {

    /** RPO=0 for this run: every seeded audit-critical row was relayed, none left stranded. */
    public boolean zeroAuditLoss() {
        return outboxRowsUnrecoveredAfterFailover == 0;
    }
}
