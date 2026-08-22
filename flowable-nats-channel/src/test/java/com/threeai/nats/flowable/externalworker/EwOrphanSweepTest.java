package com.threeai.nats.flowable.externalworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import com.threeai.nats.core.jetstream.SweepLeaderLease;
import io.nats.client.JetStream;
import io.nats.client.impl.NatsMessage;
import org.flowable.common.engine.api.FlowableException;
import org.flowable.engine.ManagementService;
import org.flowable.job.api.AcquiredExternalWorkerJob;
import org.flowable.job.api.ExternalWorkerJobAcquireBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** D-B'v3 sweep: public re-acquire with a fresh generation, compensating unacquire, fail-fast. */
class EwOrphanSweepTest {

    private static final String SENTINEL = "flw-ew-bridge";

    private ManagementService managementService;
    private ExternalWorkerJobAcquireBuilder acquireBuilder;
    private JetStream jetStream;
    private SweepLeaderLease lease;
    private EwOrphanSweep sweep;

    private static EwProperties props() {
        EwProperties p = new EwProperties();
        p.setTopics(List.of("orders"));
        return p;
    }

    private static AcquiredExternalWorkerJob job(String id) {
        AcquiredExternalWorkerJob j = mock(AcquiredExternalWorkerJob.class);
        when(j.getId()).thenReturn(id);
        when(j.getProcessInstanceId()).thenReturn("pi");
        when(j.getVariables()).thenReturn(Map.of("k", "v"));
        return j;
    }

    @BeforeEach
    void setUp() {
        managementService = mock(ManagementService.class);
        acquireBuilder = mock(ExternalWorkerJobAcquireBuilder.class);
        when(acquireBuilder.topic(anyString(), any())).thenReturn(acquireBuilder);
        when(acquireBuilder.onlyBpmn()).thenReturn(acquireBuilder);
        when(managementService.createExternalWorkerJobAcquireBuilder()).thenReturn(acquireBuilder);
        jetStream = mock(JetStream.class);
        lease = mock(SweepLeaderLease.class);
        when(lease.tryAcquireOrRenew()).thenReturn(true);
        EwProperties p = props();
        sweep = new EwOrphanSweep(managementService, new EwTopicConfig(p), new EwLockConfig(p),
                SENTINEL, jetStream, lease, null, 100);
    }

    @Test
    void notLeader_touchesNothing() {
        when(lease.tryAcquireOrRenew()).thenReturn(false);
        sweep.runCycle();
        verifyNoInteractions(managementService);
    }

    @Test
    void acquiredJobs_areRepublished_withFreshSentinelGenerationToken() throws Exception {
        doReturn(List.of(job("j1"), job("j2"))).when(acquireBuilder).acquireAndLock(anyInt(), anyString());

        sweep.runCycle();

        ArgumentCaptor<String> worker = ArgumentCaptor.forClass(String.class);
        verify(acquireBuilder).acquireAndLock(eq(100), worker.capture());
        assertThat(worker.getValue()).startsWith(SENTINEL + "#");
        assertThat(EwHeaders.NONCE_PATTERN.matcher(worker.getValue().split("#", 2)[1]).matches()).isTrue();
        verify(jetStream, times(2)).publish(any(NatsMessage.class));
        verify(managementService, never()).unacquireExternalWorkerJob(anyString(), anyString());
    }

    @Test
    void publishFailure_triggersCompensatingUnacquire_withSameToken() throws Exception {
        doReturn(List.of(job("j1"))).when(acquireBuilder).acquireAndLock(anyInt(), anyString());
        when(jetStream.publish(any(NatsMessage.class))).thenThrow(new RuntimeException("broker down"));

        sweep.runCycle();

        ArgumentCaptor<String> worker = ArgumentCaptor.forClass(String.class);
        verify(acquireBuilder).acquireAndLock(anyInt(), worker.capture());
        verify(managementService).unacquireExternalWorkerJob("j1", worker.getValue());
    }

    @Test
    void compensationFailure_isBenign_notFatal() throws Exception {
        doReturn(List.of(job("j1"))).when(acquireBuilder).acquireAndLock(anyInt(), anyString());
        when(jetStream.publish(any(NatsMessage.class))).thenThrow(new RuntimeException("broker down"));
        doThrow(new FlowableException("job gone")).when(managementService)
                .unacquireExternalWorkerJob(anyString(), anyString());

        assertThatCode(() -> sweep.runCycle()).doesNotThrowAnyException();
    }

    @Test
    void tenConsecutiveFailures_zeroSuccesses_failFastAbortsCycle() throws Exception {
        List<AcquiredExternalWorkerJob> many = IntStream.range(0, 50)
                .mapToObj(i -> job("j" + i)).toList();
        doReturn(many).when(acquireBuilder).acquireAndLock(anyInt(), anyString());
        when(jetStream.publish(any(NatsMessage.class))).thenThrow(new RuntimeException("broker down"));

        sweep.runCycle();

        verify(jetStream, times(10)).publish(any(NatsMessage.class));
    }

    @Test
    void runCycleSafely_swallowsErrors_keepingTheScheduler() {
        when(lease.tryAcquireOrRenew()).thenThrow(new AssertionError("boom"));
        assertThatCode(() -> sweep.runCycleSafely()).doesNotThrowAnyException();
        verify(lease, atLeastOnce()).tryAcquireOrRenew();
    }
}
