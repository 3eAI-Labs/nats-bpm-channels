package com.threeai.nats.bench.history;

import java.time.Duration;

/**
 * {@link RelayFailoverBenchScenario}'s measurement output (`03_classes/5_bench.md` §3,
 * `01_overview.md` "Phase3'ün devrettiği doğrulamalar #5" RTO verification).
 *
 * @param timeToRecover           wall-clock time from killing the current {@code
 *                                history-relay-leader} holder to a new node acquiring the lease
 *                                AND resuming {@code relayCycle()}
 * @param leaseTtl                the configured lease TTL this run measured against
 *                                ({@code 2 * relayCyclePeriodSeconds}, `08_config.md` §3)
 * @param recoveredWithinLeaseTtl whether {@code timeToRecover} stayed within {@code leaseTtl} —
 *                                the RTO bound this basamak's design relies on
 */
public record RelayFailoverReport(Duration timeToRecover, Duration leaseTtl, boolean recoveredWithinLeaseTtl) {
}
