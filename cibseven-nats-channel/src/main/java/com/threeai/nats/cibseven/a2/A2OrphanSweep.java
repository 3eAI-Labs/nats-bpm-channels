package com.threeai.nats.cibseven.a2;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static net.logstash.logback.argument.StructuredArguments.kv;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.threeai.nats.core.jetstream.SweepLeaderLease;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.JetStream;
import org.cibseven.bpm.engine.ProcessEngine;
import org.cibseven.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.cibseven.bpm.engine.impl.cmd.LockExternalTaskCmd;
import org.cibseven.bpm.engine.impl.externaltask.TopicFetchInstruction;
import org.cibseven.bpm.engine.impl.interceptor.Command;
import org.cibseven.bpm.engine.impl.persistence.entity.ExternalTaskEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cold, leader-only orphan sweep (HLD §2.3, BR-A2-005/013, FR-A5/A6, US-A3, ADR-0002/0003).
 * Recovers external tasks whose fast-path publish never happened (post-commit publish failure,
 * process crash between commit and publish) by finding rows that are already "native-fetchable"
 * (same predicate the classic poller uses) restricted to A2 topics, re-locking them, and
 * re-publishing.
 */
public class A2OrphanSweep {

    /** docs/13 §2.6: non-null only in sharded mode — sweep re-publishes carry the address too. */
    private com.threeai.nats.core.shard.ShardTopology shardTopology;

    public void setShardTopology(com.threeai.nats.core.shard.ShardTopology shardTopology) {
        this.shardTopology = shardTopology;
    }

    private String replySubjectFor(String topic) {
        return shardTopology == null ? null : com.threeai.nats.core.shard.ShardSubjects.scoped(
                shardTopology.getShardId(), "jobs." + topic + ".reply");
    }

    private static final Logger log = LoggerFactory.getLogger(A2OrphanSweep.class);

    /** Default for {@code sweep-batch-size} — see {@code A2Properties.UmbrellaLockDefaults#sweepBatchSize}. */
    static final int DEFAULT_SWEEP_BATCH_SIZE = 5000;
    /**
     * A cycle that has republished NOTHING and failed this many publishes in a row aborts early:
     * the broker is evidently unreachable, each further attempt costs up to a publish timeout,
     * and the whole candidate set is retried next S anyway (rows are compensating-unlocked per
     * attempt). 2026-08-20 plateau shape: without this, every cycle grinds the full candidate
     * list against a dead stream.
     */
    static final int FAIL_FAST_THRESHOLD = 10;

    private final ProcessEngine processEngine;
    private final SweepLeaderLease leaderLease;
    private final JetStream jetStream;
    private final A2TopicConfig topicConfig;
    private final String sentinelWorkerId;
    private final UmbrellaLockResolver lockResolver;
    private final NatsChannelMetrics metrics;
    private final UmbrellaLockValidator lockValidator;
    private final int sweepBatchSize;

    /** Default-batch convenience overload ({@link #DEFAULT_SWEEP_BATCH_SIZE}). */
    public A2OrphanSweep(ProcessEngine processEngine, SweepLeaderLease leaderLease, JetStream jetStream,
            A2TopicConfig topicConfig, String sentinelWorkerId, UmbrellaLockResolver lockResolver,
            NatsChannelMetrics metrics, UmbrellaLockValidator lockValidator) {
        this(processEngine, leaderLease, jetStream, topicConfig, sentinelWorkerId, lockResolver,
                metrics, lockValidator, DEFAULT_SWEEP_BATCH_SIZE);
    }

    /** @param sweepBatchSize per-cycle candidate cap; {@code <= 0} means unbounded */
    public A2OrphanSweep(ProcessEngine processEngine, SweepLeaderLease leaderLease, JetStream jetStream,
            A2TopicConfig topicConfig, String sentinelWorkerId, UmbrellaLockResolver lockResolver,
            NatsChannelMetrics metrics, UmbrellaLockValidator lockValidator, int sweepBatchSize) {
        this.processEngine = processEngine;
        this.leaderLease = leaderLease;
        this.jetStream = jetStream;
        this.topicConfig = topicConfig;
        this.sentinelWorkerId = sentinelWorkerId;
        this.lockResolver = lockResolver;
        this.metrics = metrics;
        this.lockValidator = lockValidator;
        this.sweepBatchSize = sweepBatchSize;
    }

    /** Invoked every S seconds (e.g. {@code @Scheduled(fixedDelayString = "${a2.sweep.period-seconds:120}000")}). */
    public void sweepCycle() {
        if (!leaderLease.tryAcquireOrRenew()) {
            // The lease itself logs every leadership TRANSITION; the counter makes the steady
            // state observable and separates the three states a silent sweep gets confused
            // between: gated (flat not-leader everywhere), dead scheduler (zero increments),
            // and working (leader + cycle summaries). The 2026-08-20 post-mortem initially
            // could not tell these apart and misread a WORKING sweep as a flatlined one.
            if (metrics != null) {
                metrics.sweepCycleCount("not-leader").increment();
            }
            return; // not the leader — zero DB reads (ADR-0002)
        }
        if (metrics != null) {
            metrics.sweepCycleCount("leader").increment();
        }
        if (topicConfig.a2Topics().isEmpty()) {
            return;
        }
        List<ExternalTaskEntity> fetchableCandidates;
        try {
            fetchableCandidates = fetchFetchableParity();
        } catch (Exception e) {
            log.error("Sweep fetchable-parity query failed — cycle skipped, retry next S", e); // SYS_SWEEP_QUERY_FAILED
            return;
        }
        int republished = 0;
        int failed = 0;
        for (ExternalTaskEntity candidate : fetchableCandidates) {
            if (lockValidator.isUnsafe(candidate.getTopicName())) {
                log.warn("Topic running with unsafe umbrella-lock duration (L < floor) — "
                        + "allow-unsafe-lock-duration=true", kv("topic", candidate.getTopicName()));
            }
            if (relockThenPublish(candidate)) {
                republished++;
            } else {
                failed++;
                if (republished == 0 && failed >= FAIL_FAST_THRESHOLD) {
                    log.warn("Sweep cycle aborted early — first attempts all failed to publish,"
                            + " broker unreachable; full candidate set retried next cycle",
                            kv("failed", failed),
                            kv("deferred", fetchableCandidates.size() - failed));
                    break;
                }
            }
        }
        // One summary line per working cycle. Quiet cycles stay at debug — but a cycle that FOUND
        // work says so out loud: the absence of exactly this line let the 2026-08-20 post-mortem
        // misread a working sweep (leader, cycling, republishing) as a flatlined one.
        if (!fetchableCandidates.isEmpty()) {
            log.info("Sweep cycle done", kv("candidates", fetchableCandidates.size()),
                    kv("republished", republished), kv("failed", failed));
        } else {
            log.debug("Sweep cycle done — no orphans");
        }
    }

    /**
     * Native parity: {@code ExternalTaskManager.selectExternalTasksForTopics(...)} — read-only,
     * no {@code FOR UPDATE} (this sweep re-locks as a deliberate, separate second step —
     * NOT the native fetch+lock atomic command, which would reintroduce multi-worker contention).
     *
     * <p>CODER-NOTE: the LLD pseudo-code passed a 4th boolean argument to
     * {@code selectExternalTasksForTopics(...)}; the actual method on
     * {@code ExternalTaskManager} (verified against the compiled 7.24.0 engine) takes only
     * {@code (Collection<TopicFetchInstruction>, int, List<QueryOrderingProperty>)} — the extra
     * argument has been dropped here to compile against the real engine API.
     *
     * <p><b>[BLOCKING] QA review fix (2026-07-15):</b> {@code instructions.values()} is
     * a live {@code Map.values()} view (runtime type {@code java.util.HashMap$Values}) — MyBatis'
     * OGNL evaluator reflects on that ACTUAL runtime class to resolve the {@code
     * ExternalTask.xml} dynamic-SQL guard {@code parameter.topics.size > 0}, and {@code
     * HashMap$Values.size()} is a JDK-internal method JPMS denies reflective access to on
     * JDK16+/21 (no {@code --add-opens java.base/java.util=ALL-UNNAMED} is configured anywhere in
     * this repo) — every {@code sweepCycle()} threw {@code InaccessibleObjectException} here,
     * silently disabling the entire orphan-sweep safety net (ADR-0002/0003). Materializing a
     * plain {@link ArrayList} before crossing into MyBatis/OGNL-reflected code avoids the
     * JPMS-restricted view type entirely. Regression guard: {@code
     * A2OrphanSweepFetchableParityIntegrationTest} (real embedded engine, no mocks).
     */
    private List<ExternalTaskEntity> fetchFetchableParity() {
        return execute(commandContext -> {
            Map<String, TopicFetchInstruction> instructions = topicConfig.a2Topics().stream()
                    .collect(toMap(identity(), topic -> new TopicFetchInstruction(topic, Integer.MAX_VALUE)));
            // Per-cycle cap (sweep-batch-size, 0 = unbounded): the 2026-08-20 incident's first
            // working cycle materialized 93,605 entities in one list — O(backlog) heap and a
            // cycle that outlived the lease TTL. Remaining rows surface again next cycle.
            int maxResults = sweepBatchSize <= 0 ? Integer.MAX_VALUE : sweepBatchSize;
            return commandContext.getExternalTaskManager().selectExternalTasksForTopics(
                    new ArrayList<>(instructions.values()), maxResults, Collections.emptyList());
        });
    }

    /**
     * Re-lock first (BAQ-1 fixed order), then publish; compensating unlock on publish failure
     * (ADR-0003).
     *
     * @return {@code true} when the candidate was republished, {@code false} on any failure path
     */
    private boolean relockThenPublish(ExternalTaskEntity candidate) {
        long lockDurationMillis = lockResolver.resolveMillis(candidate.getTopicName());
        try {
            // 1) RE-LOCK FIRST — always passes with the same sentinelWorkerId:
            //    LockExternalTaskCmd.validateWorkerViolation() sees either the same worker id
            //    (no violation) or an expired lock (no violation either way).
            execute(new LockExternalTaskCmd(candidate.getId(), sentinelWorkerId, lockDurationMillis));
        } catch (Exception relockEx) {
            log.error("Sweep re-lock failed — row skipped, unchanged, retried next cycle",
                    kv("external_task_id", candidate.getId()), relockEx); // SYS_SWEEP_RELOCK_FAILED
            return false; // row state unchanged — harmless, retried next S
        }

        // 2) PUBLISH SECOND
        try {
            jetStream.publish(A2JobMessageFactory.build(candidate, java.util.Map.of(),
                    replySubjectFor(candidate.getTopicName())));
            if (metrics != null) {
                metrics.sweepRepublishCount(candidate.getTopicName()).increment();
            }
            return true;
        } catch (Exception publishEx) {
            // 3) COMPENSATE (ADR-0003): re-lock succeeded, publish failed -> unlock() gives the
            //    lock back. Invisible-orphan window narrows from <=L to <=S.
            try {
                execute(commandContext -> {
                    ExternalTaskEntity task = commandContext.getExternalTaskManager()
                            .findExternalTaskById(candidate.getId());
                    if (task != null) {
                        task.unlock(); // ExternalTaskEntity.unlock() clears workerId+lockExpirationTime
                    }
                    return null;
                });
                log.error("Sweep republish failed — compensating unlock applied, row re-fetchable within S",
                        kv("external_task_id", candidate.getId()), publishEx); // SYS_SWEEP_REPUBLISH_FAILED (compensated)
            } catch (Exception unlockEx) {
                // Compensation also failed (DB+broker down simultaneously) -> falls back to the
                // BAQ-1 default: row surfaces as an old orphan again after L.
                log.error("Sweep republish failed AND compensating unlock failed — row appears "
                        + "freshly-locked but was never delivered; will surface as an old orphan after L",
                        kv("external_task_id", candidate.getId()), unlockEx); // SYS_SWEEP_REPUBLISH_FAILED (worst case)
            }
        }
        return false;
    }

    /**
     * CODER-NOTE: the LLD pseudo-code calls {@code processEngine.getManagementService().executeCommand(...)},
     * but {@code ManagementService} (verified against the compiled 7.24.0 engine) exposes no such
     * public method — only {@code ManagementServiceImpl} (via {@code ServiceImpl.getCommandExecutor()})
     * has one, and it isn't reachable through the public {@code ManagementService} interface this
     * class is handed. This helper reaches the same {@code CommandExecutor} through
     * {@code ProcessEngineConfigurationImpl.getCommandExecutorTxRequired()} instead — a standard,
     * supported extension-point idiom for custom engine plugins.
     */
    private <T> T execute(Command<T> command) {
        ProcessEngineConfigurationImpl configuration =
                (ProcessEngineConfigurationImpl) processEngine.getProcessEngineConfiguration();
        return configuration.getCommandExecutorTxRequired().execute(command);
    }
}
