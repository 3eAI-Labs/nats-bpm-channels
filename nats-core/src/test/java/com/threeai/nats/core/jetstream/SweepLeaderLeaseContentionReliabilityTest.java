package com.threeai.nats.core.jetstream;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import io.nats.client.JetStream;
import io.nats.client.Nats;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Phase 5.5 (QA) reliability suite -- BR-REL-001 / ADR-0002 single-writer invariant: real NATS
 * JetStream KV (Testcontainers, NOT the mocked {@link SweepLeaderLeaseTest}) drives genuine
 * concurrent lease contention. Proves the architectural guarantee the mocked unit test cannot:
 * with N candidates racing the SAME real KV key, {@code kv.create}'s server-side atomicity means
 * split-brain (two simultaneous leaders) is IMPOSSIBLE, not merely "not observed under mocking".
 *
 * <p>Heavy (real Docker/NATS container, TTL-driven wall-clock wait for the handover test) --
 * {@code @Tag("reliability")}, excluded from the default {@code mvn test} run (mirrors {@code
 * nats-bpm-bench}'s {@code @Tag("bench")} convention, {@code BR-OBS-003}). Run explicitly via
 * {@code mvn test -Dgroups=reliability -Dreliability.excludedGroups=} (or from the module:
 * {@code mvn test -Dreliability.excludedGroups=}).
 */
@Tag("reliability")
@Testcontainers
class SweepLeaderLeaseContentionReliabilityTest {

    private static GenericContainer<?> natsContainer;
    private static io.nats.client.Connection natsConnection;
    private static JetStream jetStream;

    @BeforeAll
    static void startContainer() throws Exception {
        natsContainer = new GenericContainer<>("nats:2.10-alpine").withCommand("--jetstream").withExposedPorts(4222);
        natsContainer.start();
        natsConnection = Nats.connect("nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222));
        jetStream = natsConnection.jetStream();
    }

    @AfterAll
    static void stopContainer() throws Exception {
        natsConnection.close();
        natsContainer.stop();
    }

    /**
     * N candidates call {@code tryAcquireOrRenew()} for the SAME brand-new key at the SAME instant
     * (barrier-synchronized -- the real contention scenario a mocked KV can never reproduce: every
     * mocked call is trivially sequential). Real JetStream KV {@code create} is a server-side
     * atomic compare-and-swap on the underlying stream -- exactly one candidate's create succeeds,
     * every other candidate's create throws and correctly falls back to "someone else holds it".
     */
    @Test
    void concurrentFirstAcquire_exactlyOneCandidateBecomesLeader() throws Exception {
        int candidateCount = 8;
        String bucket = "sweep-leader-contention-" + UUID.randomUUID();
        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket(bucket, Duration.ofSeconds(60), 1, natsConnection);

        List<SweepLeaderLease> candidates = new ArrayList<>(candidateCount);
        for (int i = 0; i < candidateCount; i++) {
            candidates.add(new SweepLeaderLease(jetStream, kvManager, natsConnection,
                    bucket, "relay-leader.", "camunda", "candidate-" + i, Duration.ofSeconds(60)));
        }

        AtomicReferenceArray<Boolean> results = new AtomicReferenceArray<>(candidateCount);
        CyclicBarrier startBarrier = new CyclicBarrier(candidateCount);
        ExecutorService pool = Executors.newFixedThreadPool(candidateCount);
        try {
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < candidateCount; i++) {
                int idx = i;
                futures.add(pool.submit(() -> {
                    awaitBarrier(startBarrier);
                    results.set(idx, candidates.get(idx).tryAcquireOrRenew());
                }));
            }
            for (java.util.concurrent.Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        int winners = 0;
        int leadersReportingTrue = 0;
        for (int i = 0; i < candidateCount; i++) {
            if (Boolean.TRUE.equals(results.get(i))) {
                winners++;
            }
            if (candidates.get(i).isLeader()) {
                leadersReportingTrue++;
            }
        }
        // The split-brain invariant, proven against a REAL server-side atomic KV create: no matter
        // how many candidates race the SAME key at the SAME instant, exactly one wins.
        assertThat(winners).isEqualTo(1);
        assertThat(leadersReportingTrue).isEqualTo(1);
    }

    /**
     * Leader-crash handover under a SHORT real TTL: while the current leader holds the lease, N
     * standbys racing it concurrently every poll round NEVER see more than one of themselves (nor
     * the incumbent) simultaneously report {@code isLeader()==true}. Once the leader stops
     * renewing (simulated hard crash, matching {@code RelayFailoverBenchScenario}'s convention),
     * exactly one standby takes over the TTL-expired key -- proven via barrier-synchronized
     * concurrent polling, not sequential calls, so a real simultaneous double-acquire (had one been
     * possible) could not hide behind ordering.
     */
    @Test
    void leaderCrash_concurrentStandbyPolling_exactlyOneHandoverWinner_neverSplitBrain() throws Exception {
        Duration ttl = Duration.ofSeconds(3);
        int standbyCount = 3;
        String bucket = "sweep-leader-handover-" + UUID.randomUUID();
        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket(bucket, ttl, 1, natsConnection);

        SweepLeaderLease leader = new SweepLeaderLease(jetStream, kvManager, natsConnection,
                bucket, "relay-leader.", "camunda", "leader-0", ttl);
        List<SweepLeaderLease> standbys = new ArrayList<>(standbyCount);
        for (int i = 0; i < standbyCount; i++) {
            standbys.add(new SweepLeaderLease(jetStream, kvManager, natsConnection,
                    bucket, "relay-leader.", "camunda", "standby-" + i, ttl));
        }

        assertThat(leader.tryAcquireOrRenew()).isTrue();

        // Snapshot BEFORE crash: standbys racing the incumbent concurrently must all lose, and the
        // incumbent is the sole leader.
        int preCrashWinners = concurrentAttempt(standbys);
        assertThat(preCrashWinners).isZero();
        assertThat(leader.isLeader()).isTrue();
        assertThat(standbys.stream().filter(SweepLeaderLease::isLeader).count()).isZero();

        // "Kill" the leader: never call tryAcquireOrRenew()/renew it again -- its last-written KV
        // revision now ages toward the bucket's real TTL with no renewal (hard-crash simulation,
        // same convention as RelayFailoverBenchScenario).
        Instant killedAt = Instant.now();
        Duration pollInterval = Duration.ofMillis(300);
        Duration safetyMargin = Duration.ofSeconds(5);
        Instant deadline = killedAt.plus(ttl).plus(safetyMargin);

        int winnerIndex = -1;
        while (winnerIndex < 0 && Instant.now().isBefore(deadline)) {
            int winnersThisRound = concurrentAttemptCountingWinnerIndex(standbys);
            if (winnersThisRound >= 0) {
                winnerIndex = winnersThisRound;
                break;
            }
            Thread.sleep(pollInterval.toMillis());
        }

        assertThat(winnerIndex).as("standby must take over within TTL(%s) + safety margin(%s)", ttl, safetyMargin)
                .isGreaterThanOrEqualTo(0);

        // One MORE concurrent round including the (now zombie-waking) old leader: the old leader
        // must fail to reacquire, the non-winning standbys must stay non-leader, and ONLY the
        // established winner reports leadership -- proves the handover is stable, not oscillating.
        List<SweepLeaderLease> everyone = new ArrayList<>(standbys);
        everyone.add(leader);
        AtomicReferenceArray<Boolean> finalRoundResults = new AtomicReferenceArray<>(everyone.size());
        CyclicBarrier barrier = new CyclicBarrier(everyone.size());
        ExecutorService pool = Executors.newFixedThreadPool(everyone.size());
        try {
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < everyone.size(); i++) {
                int idx = i;
                futures.add(pool.submit(() -> {
                    awaitBarrier(barrier);
                    finalRoundResults.set(idx, everyone.get(idx).tryAcquireOrRenew());
                }));
            }
            for (java.util.concurrent.Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        int finalRoundWinners = 0;
        for (int i = 0; i < everyone.size(); i++) {
            if (Boolean.TRUE.equals(finalRoundResults.get(i))) {
                finalRoundWinners++;
            }
        }
        assertThat(finalRoundWinners).as("exactly one lease may be leader in ANY concurrent round").isEqualTo(1);
        assertThat(standbys.get(winnerIndex).isLeader()).isTrue();
        // The zombie old leader (index = standbys.size() in `everyone`) must never regain leadership
        // once a standby has taken over -- otherwise two nodes could believe they are the leader.
        assertThat(leader.isLeader()).isFalse();
    }

    /** One barrier-synchronized concurrent round; returns the count of standbys that won (0 or 1 expected). */
    private int concurrentAttempt(List<SweepLeaderLease> standbys) throws Exception {
        AtomicInteger winners = new AtomicInteger();
        CyclicBarrier barrier = new CyclicBarrier(standbys.size());
        ExecutorService pool = Executors.newFixedThreadPool(standbys.size());
        try {
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
            for (SweepLeaderLease standby : standbys) {
                futures.add(pool.submit(() -> {
                    awaitBarrier(barrier);
                    if (standby.tryAcquireOrRenew()) {
                        winners.incrementAndGet();
                    }
                }));
            }
            for (java.util.concurrent.Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        return winners.get();
    }

    /** Same as {@link #concurrentAttempt}, but returns the WINNING standby's index (-1 if none won this round). */
    private int concurrentAttemptCountingWinnerIndex(List<SweepLeaderLease> standbys) throws Exception {
        AtomicReferenceArray<Boolean> results = new AtomicReferenceArray<>(standbys.size());
        CyclicBarrier barrier = new CyclicBarrier(standbys.size());
        ExecutorService pool = Executors.newFixedThreadPool(standbys.size());
        try {
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < standbys.size(); i++) {
                int idx = i;
                futures.add(pool.submit(() -> {
                    awaitBarrier(barrier);
                    results.set(idx, standbys.get(idx).tryAcquireOrRenew());
                }));
            }
            for (java.util.concurrent.Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        int winnerIndex = -1;
        int winnerCount = 0;
        for (int i = 0; i < standbys.size(); i++) {
            if (Boolean.TRUE.equals(results.get(i))) {
                winnerCount++;
                winnerIndex = i;
            }
        }
        // Split-brain would show up here as winnerCount > 1 -- fail loudly rather than silently
        // picking the first.
        assertThat(winnerCount).as("at most one standby may win a single concurrent round").isLessThanOrEqualTo(1);
        return winnerCount == 1 ? winnerIndex : -1;
    }

    private static void awaitBarrier(CyclicBarrier barrier) {
        try {
            barrier.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted awaiting contention barrier", e);
        } catch (BrokenBarrierException | java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("Contention barrier failed", e);
        }
    }
}
