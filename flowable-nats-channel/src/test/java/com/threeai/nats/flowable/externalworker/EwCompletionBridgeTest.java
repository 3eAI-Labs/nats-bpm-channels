package com.threeai.nats.flowable.externalworker;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.nio.charset.StandardCharsets;

import com.threeai.nats.core.dlq.DlqPublishOutcome;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.dlq.DlqReason;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import org.flowable.common.engine.api.FlowableIllegalArgumentException;
import org.flowable.common.engine.api.FlowableObjectNotFoundException;
import org.flowable.engine.ManagementService;
import org.flowable.engine.runtime.ExternalWorkerCompletionBuilder;
import org.flowable.job.api.ExternalWorkerJob;
import org.flowable.job.api.ExternalWorkerJobQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The late-reply tree (docs/11 §2 md. 3-4) at unit level — the reviewer's prescribed test #1
 * (nonce-less reply → DLQ, never ack) plus every classification branch.
 */
class EwCompletionBridgeTest {

    private static final String SENTINEL = "flw-ew-bridge";
    private static final String NONCE = "abcdef1234567890";
    private static final String TOKEN = SENTINEL + "#" + NONCE;

    private ManagementService managementService;
    private DlqPublisher dlqPublisher;
    private EwCompletionBridge bridge;
    private ExternalWorkerCompletionBuilder completionBuilder;

    @BeforeEach
    void setUp() {
        managementService = mock(ManagementService.class);
        dlqPublisher = mock(DlqPublisher.class);
        when(dlqPublisher.publish(any(), anyString(), any(), anyString(), anyString()))
                .thenReturn(DlqPublishOutcome.PUBLISHED_JETSTREAM);
        completionBuilder = mock(ExternalWorkerCompletionBuilder.class, withSettings().defaultAnswer(inv ->
                inv.getMethod().getReturnType().isInstance(inv.getMock()) ? inv.getMock() : null));
        when(managementService.createExternalWorkerCompletionBuilder(anyString(), anyString()))
                .thenReturn(completionBuilder);
        bridge = new EwCompletionBridge(null, null, managementService,
                new EwConsumerConfig("orders"), SENTINEL, dlqPublisher, null);
    }

    private Message reply(String json, String nonce) {
        Message m = mock(Message.class);
        when(m.getData()).thenReturn(json.getBytes(StandardCharsets.UTF_8));
        Headers h = new Headers();
        if (nonce != null) {
            h.add(EwHeaders.LOCK_NONCE, nonce);
        }
        when(m.getHeaders()).thenReturn(h);
        when(m.getSubject()).thenReturn("ewjobs.orders.reply");
        return m;
    }

    private ExternalWorkerJobQuery queryReturning(ExternalWorkerJob job) {
        ExternalWorkerJobQuery query = mock(ExternalWorkerJobQuery.class);
        when(query.jobId(anyString())).thenReturn(query);
        when(query.singleResult()).thenReturn(job);
        when(managementService.createExternalWorkerJobQuery()).thenReturn(query);
        return query;
    }

    @Test
    void happyPath_completesWithBridgeBuiltToken_andAcks() {
        Message msg = reply("{\"type\":\"SUCCESS\",\"jobId\":\"j1\"}", NONCE);
        bridge.handleReply(msg);

        verify(managementService).createExternalWorkerCompletionBuilder("j1", TOKEN);
        verify(completionBuilder).complete();
        verify(msg).ack();
    }

    /** Reviewer-prescribed test #1: a reply without the nonce is dead-lettered, never acked as-is. */
    @Test
    void missingNonce_routesToDlq_neverSilentlyAcked() {
        Message msg = reply("{\"type\":\"SUCCESS\",\"jobId\":\"j1\"}", null);
        bridge.handleReply(msg);

        verify(dlqPublisher).publish(any(), eq("dlq.ewjobs.orders"),
                eq(DlqReason.MISSING_LOCK_NONCE), anyString(), anyString());
        verify(managementService, never()).createExternalWorkerCompletionBuilder(anyString(), anyString());
    }

    @Test
    void malformedNonce_routesToDlq() {
        Message msg = reply("{\"type\":\"SUCCESS\",\"jobId\":\"j1\"}", "bad.nonce!");
        bridge.handleReply(msg);
        verify(dlqPublisher).publish(any(), anyString(), eq(DlqReason.MISSING_LOCK_NONCE), anyString(), anyString());
    }

    @Test
    void emptyJobId_routesToDlq() {
        Message msg = reply("{\"type\":\"SUCCESS\"}", NONCE);
        bridge.handleReply(msg);
        verify(dlqPublisher).publish(any(), anyString(), eq(DlqReason.MISSING_LOCK_NONCE), anyString(), anyString());
    }

    @Test
    void jobGone_isIdempotentAck() {
        when(managementService.createExternalWorkerCompletionBuilder(anyString(), anyString()))
                .thenThrow(new FlowableObjectNotFoundException("gone"));
        Message msg = reply("{\"type\":\"SUCCESS\",\"jobId\":\"j1\"}", NONCE);
        bridge.handleReply(msg);
        verify(msg).ack();
        verify(dlqPublisher, never()).publish(any(), anyString(), any(), anyString(), anyString());
    }

    @Test
    void lockReset_owner_null_acksAsRoutine() {
        doThrow(new FlowableIllegalArgumentException("no lock")).when(completionBuilder).complete();
        ExternalWorkerJob job = mock(ExternalWorkerJob.class);
        when(job.getLockOwner()).thenReturn(null);
        queryReturning(job);

        Message msg = reply("{\"type\":\"SUCCESS\",\"jobId\":\"j1\"}", NONCE);
        bridge.handleReply(msg);
        verify(msg).ack();
    }

    @Test
    void staleGeneration_sentinelGrammarDifferentNonce_acksAsSuperseded() {
        doThrow(new FlowableIllegalArgumentException("held")).when(completionBuilder).complete();
        ExternalWorkerJob job = mock(ExternalWorkerJob.class);
        when(job.getLockOwner()).thenReturn(SENTINEL + "#ffffffffffffffff");
        queryReturning(job);

        Message msg = reply("{\"type\":\"SUCCESS\",\"jobId\":\"j1\"}", NONCE);
        bridge.handleReply(msg);
        verify(msg).ack();
    }

    @Test
    void foreignOwner_pages_neitherAckNorNak() {
        doThrow(new FlowableIllegalArgumentException("held")).when(completionBuilder).complete();
        ExternalWorkerJob job = mock(ExternalWorkerJob.class);
        when(job.getLockOwner()).thenReturn("rogue-rest-worker");
        queryReturning(job);

        Message msg = reply("{\"type\":\"SUCCESS\",\"jobId\":\"j1\"}", NONCE);
        bridge.handleReply(msg);
        verify(msg, never()).ack();
        verify(msg, never()).nakWithDelay(any(java.time.Duration.class));
    }

    @Test
    void sameGeneration_stillHeld_pages() {
        doThrow(new FlowableIllegalArgumentException("held")).when(completionBuilder).complete();
        ExternalWorkerJob job = mock(ExternalWorkerJob.class);
        when(job.getLockOwner()).thenReturn(TOKEN);
        queryReturning(job);

        Message msg = reply("{\"type\":\"SUCCESS\",\"jobId\":\"j1\"}", NONCE);
        bridge.handleReply(msg);
        verify(msg, never()).ack();
        verify(msg, never()).nakWithDelay(any(java.time.Duration.class));
    }

    @Test
    void jobVanishedBetweenCallAndQuery_fourthBranch_acks() {
        doThrow(new FlowableIllegalArgumentException("held")).when(completionBuilder).complete();
        queryReturning(null);
        Message msg = reply("{\"type\":\"SUCCESS\",\"jobId\":\"j1\"}", NONCE);
        bridge.handleReply(msg);
        verify(msg).ack();
    }
}
