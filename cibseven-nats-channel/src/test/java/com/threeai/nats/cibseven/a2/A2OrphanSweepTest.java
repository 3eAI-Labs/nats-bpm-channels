package com.threeai.nats.cibseven.a2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import com.threeai.nats.core.jetstream.SweepLeaderLease;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.JetStream;
import io.nats.client.impl.NatsMessage;
import org.cibseven.bpm.engine.ProcessEngine;
import org.cibseven.bpm.engine.ProcessEngineConfiguration;
import org.cibseven.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.cibseven.bpm.engine.impl.cmd.LockExternalTaskCmd;
import org.cibseven.bpm.engine.impl.interceptor.Command;
import org.cibseven.bpm.engine.impl.interceptor.CommandContext;
import org.cibseven.bpm.engine.impl.interceptor.CommandExecutor;
import org.cibseven.bpm.engine.impl.persistence.entity.ExternalTaskEntity;
import org.cibseven.bpm.engine.impl.persistence.entity.ExternalTaskManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * {@code LockExternalTaskCmd} is a real (non-mocked) CibSeven engine command whose
 * {@code execute(CommandContext)} touches internal engine machinery well beyond what a mocked
 * {@code CommandContext} can satisfy — so the shared {@code CommandExecutor} stub below
 * deliberately does NOT invoke it for real; it only fowards {@code A2OrphanSweep}'s OWN lambdas
 * (fetch / compensating-unlock) to the mocked {@code CommandContext}, and treats
 * {@code LockExternalTaskCmd} as a black box whose success/failure each test controls directly.
 */
class A2OrphanSweepTest {

    private ProcessEngine processEngine;
    private CommandExecutor commandExecutor;
    private CommandContext commandContext;
    private ExternalTaskManager externalTaskManager;
    private SweepLeaderLease leaderLease;
    private JetStream jetStream;
    private UmbrellaLockResolver lockResolver;
    private NatsChannelMetrics metrics;
    private UmbrellaLockValidator lockValidator;
    private A2OrphanSweep sweep;
    private boolean relockShouldFail;

    @BeforeEach
    void setUp() {
        processEngine = mock(ProcessEngine.class);
        ProcessEngineConfigurationImpl engineConfiguration = mock(ProcessEngineConfigurationImpl.class);
        commandExecutor = mock(CommandExecutor.class);
        commandContext = mock(CommandContext.class);
        externalTaskManager = mock(ExternalTaskManager.class);
        leaderLease = mock(SweepLeaderLease.class);
        jetStream = mock(JetStream.class);
        lockResolver = mock(UmbrellaLockResolver.class);
        metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        lockValidator = mock(UmbrellaLockValidator.class);
        relockShouldFail = false;

        when(processEngine.getProcessEngineConfiguration()).thenReturn((ProcessEngineConfiguration) engineConfiguration);
        when(engineConfiguration.getCommandExecutorTxRequired()).thenReturn(commandExecutor);
        when(commandContext.getExternalTaskManager()).thenReturn(externalTaskManager);
        when(commandExecutor.execute(org.mockito.ArgumentMatchers.<Command<Object>>any())).thenAnswer(invocation -> {
            Command<?> command = invocation.getArgument(0);
            if (command instanceof LockExternalTaskCmd) {
                if (relockShouldFail) {
                    throw new RuntimeException("lock failed");
                }
                return null; // simulate a successful relock without touching real engine internals
            }
            return command.execute(commandContext); // A2OrphanSweep's own fetch/unlock lambdas — safe to run for real
        });
        when(lockResolver.resolveMillis(org.mockito.ArgumentMatchers.anyString())).thenReturn(320_000L);

        A2Properties properties = new A2Properties();
        properties.setTopics(List.of("order-fulfillment"));
        A2TopicConfig topicConfig = new A2TopicConfig(properties);

        sweep = new A2OrphanSweep(processEngine, leaderLease, jetStream, topicConfig,
                "a2-jetstream-bridge", lockResolver, metrics, lockValidator);
    }

    @Test
    void sweepCycle_notLeader_doesNoDbWork() {
        when(leaderLease.tryAcquireOrRenew()).thenReturn(false);

        sweep.sweepCycle();

        verify(commandExecutor, never()).execute(any());
    }

    @Test
    void sweepCycle_leaderNoCandidates_completesWithoutPublishing() throws Exception {
        when(leaderLease.tryAcquireOrRenew()).thenReturn(true);
        when(externalTaskManager.selectExternalTasksForTopics(anyCollection(), anyInt(), any()))
                .thenReturn(List.of());

        sweep.sweepCycle();

        verify(jetStream, never()).publish(any(NatsMessage.class));
    }

    @Test
    void sweepCycle_fetchQueryThrows_doesNotPropagate() {
        when(leaderLease.tryAcquireOrRenew()).thenReturn(true);
        when(externalTaskManager.selectExternalTasksForTopics(anyCollection(), anyInt(), any()))
                .thenThrow(new RuntimeException("DB down"));

        assertThatCode(() -> sweep.sweepCycle()).doesNotThrowAnyException();
    }

    @Test
    void sweepCycle_candidateFound_relocksThenPublishesAndIncrementsMetric() throws Exception {
        ExternalTaskEntity candidate = mockCandidate("task-1", "order-fulfillment");
        when(leaderLease.tryAcquireOrRenew()).thenReturn(true);
        when(externalTaskManager.selectExternalTasksForTopics(anyCollection(), anyInt(), any()))
                .thenReturn(List.of(candidate));

        sweep.sweepCycle();

        verify(jetStream).publish(any(NatsMessage.class));
        assertThat(metrics.sweepRepublishCount("order-fulfillment").count()).isEqualTo(1.0);
    }

    @Test
    void sweepCycle_relockFails_publishNeverAttempted() throws Exception {
        ExternalTaskEntity candidate = mockCandidate("task-2", "order-fulfillment");
        when(leaderLease.tryAcquireOrRenew()).thenReturn(true);
        when(externalTaskManager.selectExternalTasksForTopics(anyCollection(), anyInt(), any()))
                .thenReturn(List.of(candidate));
        relockShouldFail = true;

        sweep.sweepCycle();

        verify(jetStream, never()).publish(any(NatsMessage.class));
    }

    @Test
    void sweepCycle_publishFails_compensatingUnlockInvoked() throws Exception {
        ExternalTaskEntity candidate = mockCandidate("task-3", "order-fulfillment");
        when(leaderLease.tryAcquireOrRenew()).thenReturn(true);
        when(externalTaskManager.selectExternalTasksForTopics(anyCollection(), anyInt(), any()))
                .thenReturn(List.of(candidate));
        when(jetStream.publish(any(NatsMessage.class))).thenThrow(new IOException("JS down"));
        when(externalTaskManager.findExternalTaskById("task-3")).thenReturn(candidate);

        assertThatCode(() -> sweep.sweepCycle()).doesNotThrowAnyException();

        verify(candidate).unlock();
    }

    @Test
    void sweepCycle_publishFailsAndCompensationFails_doesNotThrow() throws Exception {
        ExternalTaskEntity candidate = mockCandidate("task-4", "order-fulfillment");
        when(leaderLease.tryAcquireOrRenew()).thenReturn(true);
        when(externalTaskManager.selectExternalTasksForTopics(anyCollection(), anyInt(), any()))
                .thenReturn(List.of(candidate));
        when(jetStream.publish(any(NatsMessage.class))).thenThrow(new IOException("JS down"));
        when(externalTaskManager.findExternalTaskById("task-4")).thenThrow(new RuntimeException("db also down"));

        assertThatCode(() -> sweep.sweepCycle()).doesNotThrowAnyException();
    }

    private ExternalTaskEntity mockCandidate(String id, String topic) {
        ExternalTaskEntity task = mock(ExternalTaskEntity.class);
        when(task.getId()).thenReturn(id);
        when(task.getTopicName()).thenReturn(topic);
        return task;
    }
}
