package com.threeai.nats.flowable.externalworker;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.time.Duration;
import java.util.List;

import com.threeai.nats.core.jetstream.SweepLeaderLease;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.JetStream;
import org.flowable.common.engine.api.FlowableException;
import org.flowable.engine.ManagementService;
import org.flowable.job.api.AcquiredExternalWorkerJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Leader-only recovery loop (docs/11 §2 md. 5, D-B'v3): once the engine's own
 * ResetExpiredJobsRunnable releases an expired sentinel lock, the orphan becomes acquirable and
 * this sweep re-acquires it through the PUBLIC {@code acquireAndLock} API with a fresh
 * generation nonce, then re-publishes. Covers publish-loss orphans, late replies whose lock was
 * reset, and jobs created while the adapter was down. Recovery SLO: L + resetInterval + S.
 *
 * <p><b>Variables on the recovery path</b> come from the engine's own acquire resolution
 * ({@link AcquiredExternalWorkerJob#getVariables()}, decision R-red-5 fix) — identical semantics
 * to the fast path. The one field the sweep cannot recover is businessKey (not on the Job row).
 *
 * <p><b>Compensating unacquire:</b> a publish failure immediately releases the fresh lock
 * ({@code unacquireExternalWorkerJob}) so the row does not sit invisible for another L; unlike
 * A2 there is no reset-thread wait — unacquire nulls both lock columns at once. Compensation
 * failures (job gone = {@link FlowableException}, lockOwner-null NPE — 7.1.0 bytecode) are
 * swallowed-with-log and do NOT count toward the fail-fast counter (N-yellow-2): the row
 * self-heals through the next reset cycle anyway.
 *
 * <p>2026-08-20 hardening lessons applied from day one: per-cycle batch cap, consecutive-failure
 * fail-fast, {@code catch (Throwable)} at the cycle root via {@link #runCycleSafely()}, and
 * per-cycle outcome counters.
 */
public class EwOrphanSweep {

    private static final Logger log = LoggerFactory.getLogger(EwOrphanSweep.class);
    private static final int FAIL_FAST_CONSECUTIVE_PUBLISH_FAILURES = 10;

    private final ManagementService managementService;
    private final EwTopicConfig topicConfig;
    private final EwLockConfig lockConfig;
    private final String sentinelWorkerId;
    private final JetStream jetStream;
    private final SweepLeaderLease leaderLease;
    private final NatsChannelMetrics metrics;
    private final int batchSize;

    public EwOrphanSweep(ManagementService managementService, EwTopicConfig topicConfig,
            EwLockConfig lockConfig, String sentinelWorkerId, JetStream jetStream,
            SweepLeaderLease leaderLease, NatsChannelMetrics metrics, int batchSize) {
        this.managementService = managementService;
        this.topicConfig = topicConfig;
        this.lockConfig = lockConfig;
        this.sentinelWorkerId = sentinelWorkerId;
        this.jetStream = jetStream;
        this.leaderLease = leaderLease;
        this.metrics = metrics;
        this.batchSize = batchSize;
    }

    /**
     * Scheduler entry point. Catches {@link Throwable}: an {@link Error} escaping a
     * {@code scheduleWithFixedDelay} task cancels the safety net FOREVER, silently
     * (2026-08-20 incident lesson).
     */
    public void runCycleSafely() {
        try {
            runCycle();
        } catch (Throwable t) {
            log.error("External-worker sweep cycle died — scheduler stays alive", t);
            if (metrics != null) {
                metrics.sweepCycleCount("error").increment();
            }
        }
    }

    void runCycle() {
        if (!leaderLease.tryAcquireOrRenew()) {
            // Not the leader: zero DB reads (A2 parity).
            if (metrics != null) {
                metrics.sweepCycleCount("not_leader").increment();
            }
            return;
        }
        if (lockConfig.isUnsafe()) {
            // BAQ-3: warn every cycle, never silently once.
            log.warn("External-worker sweep running with unsafe umbrella-lock duration (L < floor)");
        }
        long republished = 0;
        long failed = 0;
        for (String topic : topicConfig.topics()) {
            String nonce = EwHeaders.mintNonce();
            String token = sentinelWorkerId + "#" + nonce;
            List<AcquiredExternalWorkerJob> acquired;
            try {
                acquired = managementService.createExternalWorkerJobAcquireBuilder()
                        .topic(topic, Duration.ofMillis(lockConfig.lockDurationMillis()))
                        .onlyBpmn() // CMMN out of scope (docs/11 §3, structural: this seam is BPMN-only)
                        .acquireAndLock(batchSize, token);
            } catch (Exception e) {
                log.error("Sweep acquire failed for topic — skipping this cycle",
                        kv("topic", topic), e);
                if (metrics != null) {
                    metrics.sweepCycleCount("acquire_error").increment();
                }
                continue;
            }
            int consecutiveFailures = 0;
            boolean anySuccess = false;
            for (AcquiredExternalWorkerJob job : acquired) {
                try {
                    // Identity envelope + engine-resolved variables; no businessKey on this path.
                    jetStream.publish(EwJobMessageFactory.build(job, topic, nonce, null, job.getVariables()));
                    republished++;
                    anySuccess = true;
                    consecutiveFailures = 0;
                    if (metrics != null) {
                        metrics.sweepRepublishCount(topic).increment();
                    }
                } catch (Exception publishFailure) {
                    failed++;
                    consecutiveFailures++;
                    log.warn("Sweep re-publish failed — compensating unacquire",
                            kv("job_id", job.getId()), kv("topic", topic), publishFailure);
                    compensatingUnacquire(job.getId(), token);
                    if (consecutiveFailures >= FAIL_FAST_CONSECUTIVE_PUBLISH_FAILURES && !anySuccess) {
                        log.error("Sweep fail-fast: {} consecutive publish failures with zero successes"
                                + " — broker presumed unreachable, aborting cycle",
                                consecutiveFailures, kv("topic", topic));
                        if (metrics != null) {
                            metrics.sweepCycleCount("fail_fast").increment();
                        }
                        return;
                    }
                }
            }
        }
        if (metrics != null) {
            metrics.sweepCycleCount(republished > 0 || failed > 0 ? "recovered" : "clean").increment();
        }
        if (republished > 0 || failed > 0) {
            log.info("External-worker sweep cycle summary",
                    kv("republished", republished), kv("failed", failed));
        }
    }

    private void compensatingUnacquire(String jobId, String token) {
        try {
            managementService.unacquireExternalWorkerJob(jobId, token);
        } catch (FlowableException | NullPointerException benign) {
            // Job gone (FlowableException) or lockOwner already nulled (NPE, 7.1.0 bytecode
            // UnacquireExternalWorkerJobCmd) — the row self-heals via the reset thread; neither
            // outcome feeds the fail-fast counter (N-yellow-2).
            log.warn("Compensating unacquire failed benignly", kv("job_id", jobId), benign);
        }
    }
}
