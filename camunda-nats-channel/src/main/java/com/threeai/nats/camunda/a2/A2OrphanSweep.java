package com.threeai.nats.camunda.a2;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static net.logstash.logback.argument.StructuredArguments.kv;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.threeai.nats.core.jetstream.SweepLeaderLease;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.JetStream;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.impl.cmd.LockExternalTaskCmd;
import org.camunda.bpm.engine.impl.externaltask.TopicFetchInstruction;
import org.camunda.bpm.engine.impl.interceptor.Command;
import org.camunda.bpm.engine.impl.persistence.entity.ExternalTaskEntity;
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

    private static final Logger log = LoggerFactory.getLogger(A2OrphanSweep.class);

    private final ProcessEngine processEngine;
    private final SweepLeaderLease leaderLease;
    private final JetStream jetStream;
    private final A2TopicConfig topicConfig;
    private final String sentinelWorkerId;
    private final UmbrellaLockResolver lockResolver;
    private final NatsChannelMetrics metrics;
    private final UmbrellaLockValidator lockValidator;

    public A2OrphanSweep(ProcessEngine processEngine, SweepLeaderLease leaderLease, JetStream jetStream,
            A2TopicConfig topicConfig, String sentinelWorkerId, UmbrellaLockResolver lockResolver,
            NatsChannelMetrics metrics, UmbrellaLockValidator lockValidator) {
        this.processEngine = processEngine;
        this.leaderLease = leaderLease;
        this.jetStream = jetStream;
        this.topicConfig = topicConfig;
        this.sentinelWorkerId = sentinelWorkerId;
        this.lockResolver = lockResolver;
        this.metrics = metrics;
        this.lockValidator = lockValidator;
    }

    /** Invoked every S seconds (e.g. {@code @Scheduled(fixedDelayString = "${a2.sweep.period-seconds:120}000")}). */
    public void sweepCycle() {
        if (!leaderLease.tryAcquireOrRenew()) {
            return; // not the leader — zero DB reads (ADR-0002)
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
        for (ExternalTaskEntity candidate : fetchableCandidates) {
            if (lockValidator.isUnsafe(candidate.getTopicName())) {
                log.warn("Topic running with unsafe umbrella-lock duration (L < floor) — "
                        + "allow-unsafe-lock-duration=true", kv("topic", candidate.getTopicName()));
            }
            relockThenPublish(candidate);
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
     */
    private List<ExternalTaskEntity> fetchFetchableParity() {
        return execute(commandContext -> {
            Map<String, TopicFetchInstruction> instructions = topicConfig.a2Topics().stream()
                    .collect(toMap(identity(), topic -> new TopicFetchInstruction(topic, Integer.MAX_VALUE)));
            return commandContext.getExternalTaskManager()
                    .selectExternalTasksForTopics(instructions.values(), Integer.MAX_VALUE, Collections.emptyList());
        });
    }

    /** Re-lock first (BAQ-1 fixed order), then publish; compensating unlock on publish failure (ADR-0003). */
    private void relockThenPublish(ExternalTaskEntity candidate) {
        long lockDurationMillis = lockResolver.resolveMillis(candidate.getTopicName());
        try {
            // 1) RE-LOCK FIRST — always passes with the same sentinelWorkerId:
            //    LockExternalTaskCmd.validateWorkerViolation() sees either the same worker id
            //    (no violation) or an expired lock (no violation either way).
            execute(new LockExternalTaskCmd(candidate.getId(), sentinelWorkerId, lockDurationMillis));
        } catch (Exception relockEx) {
            log.error("Sweep re-lock failed — row skipped, unchanged, retried next cycle",
                    kv("external_task_id", candidate.getId()), relockEx); // SYS_SWEEP_RELOCK_FAILED
            return; // row state unchanged — harmless, retried next S
        }

        // 2) PUBLISH SECOND
        try {
            jetStream.publish(A2JobMessageFactory.build(candidate));
            if (metrics != null) {
                metrics.sweepRepublishCount(candidate.getTopicName()).increment();
            }
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
