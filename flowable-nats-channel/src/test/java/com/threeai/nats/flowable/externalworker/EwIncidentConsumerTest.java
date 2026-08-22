package com.threeai.nats.flowable.externalworker;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;

import com.threeai.nats.core.headers.BpmHeaders;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import org.flowable.common.engine.api.FlowableException;
import org.flowable.engine.ManagementService;
import org.flowable.job.api.ExternalWorkerJob;
import org.flowable.job.api.ExternalWorkerJobFailureBuilder;
import org.flowable.job.api.ExternalWorkerJobQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

/** D-D'v2 taxonomy: query-first, fail with the QUERIED owner, defer when not sentinel-held. */
class EwIncidentConsumerTest {

    private static final String SENTINEL = "flw-ew-bridge";
    private static final String OWNER = SENTINEL + "#abcdef1234567890";

    private ManagementService managementService;
    private ExternalWorkerJobFailureBuilder failureBuilder;
    private EwIncidentConsumer consumer;

    @BeforeEach
    void setUp() {
        managementService = mock(ManagementService.class);
        failureBuilder = mock(ExternalWorkerJobFailureBuilder.class, Answers.RETURNS_SELF);
        when(managementService.createExternalWorkerJobFailureBuilder(anyString(), anyString()))
                .thenReturn(failureBuilder);
        consumer = new EwIncidentConsumer(null, null, managementService, SENTINEL, 30, null);
    }

    private void queryReturns(ExternalWorkerJob... jobs) {
        ExternalWorkerJobQuery query = mock(ExternalWorkerJobQuery.class);
        when(query.jobId(anyString())).thenReturn(query);
        var stub = when(query.singleResult());
        for (ExternalWorkerJob j : jobs) {
            stub = stub.thenReturn(j);
        }
        when(managementService.createExternalWorkerJobQuery()).thenReturn(query);
    }

    private static ExternalWorkerJob lockedBy(String owner) {
        ExternalWorkerJob j = mock(ExternalWorkerJob.class);
        when(j.getLockOwner()).thenReturn(owner);
        return j;
    }

    private static Message dlqMsg(String payload, String correlationHeader) {
        Message m = mock(Message.class);
        m = mock(Message.class);
        when(m.getData()).thenReturn(payload == null ? new byte[0] : payload.getBytes(StandardCharsets.UTF_8));
        Headers h = new Headers();
        if (correlationHeader != null) {
            h.add(BpmHeaders.CORRELATION_ID, correlationHeader);
        }
        when(m.getHeaders()).thenReturn(h);
        when(m.getSubject()).thenReturn("dlq.ewjobs.orders");
        return m;
    }

    @Test
    void sentinelLocked_isDeadlettered_withQueriedOwner_andAcked() {
        queryReturns(lockedBy(OWNER));
        Message msg = dlqMsg("{\"jobId\":\"j1\"}", null);
        consumer.handleDlqMessage(msg);

        verify(managementService).createExternalWorkerJobFailureBuilder("j1", OWNER);
        verify(failureBuilder).retries(0);
        verify(failureBuilder).fail();
        verify(msg).ack();
    }

    @Test
    void jobGone_acks() {
        queryReturns((ExternalWorkerJob) null);
        Message msg = dlqMsg("{\"jobId\":\"j1\"}", null);
        consumer.handleDlqMessage(msg);
        verify(msg).ack();
        verify(managementService, never()).createExternalWorkerJobFailureBuilder(anyString(), anyString());
    }

    @Test
    void lockNull_defersToSweep() {
        queryReturns(lockedBy(null));
        Message msg = dlqMsg("{\"jobId\":\"j1\"}", null);
        consumer.handleDlqMessage(msg);
        verify(msg).ack();
        verify(managementService, never()).createExternalWorkerJobFailureBuilder(anyString(), anyString());
    }

    @Test
    void foreignOwner_defers() {
        queryReturns(lockedBy("rogue-worker"));
        Message msg = dlqMsg("{\"jobId\":\"j1\"}", null);
        consumer.handleDlqMessage(msg);
        verify(msg).ack();
        verify(managementService, never()).createExternalWorkerJobFailureBuilder(anyString(), anyString());
    }

    @Test
    void ownerChangedBetweenQueryAndFail_oneRetryOnFreshQuery() {
        queryReturns(lockedBy(OWNER), null); // first query sentinel, fresh query: gone
        doThrow(new FlowableException("owner changed")).when(failureBuilder).fail();
        Message msg = dlqMsg("{\"jobId\":\"j1\"}", null);
        consumer.handleDlqMessage(msg);

        verify(failureBuilder, times(1)).fail();
        verify(msg).ack();
    }

    @Test
    void jobId_fallsBackToCorrelationHeader() {
        queryReturns(lockedBy(OWNER));
        Message msg = dlqMsg("not-json", "j9");
        consumer.handleDlqMessage(msg);
        verify(managementService).createExternalWorkerJobFailureBuilder(eq("j9"), anyString());
    }

    @Test
    void unidentifiable_acksAsDeferred() {
        Message msg = dlqMsg(null, null);
        consumer.handleDlqMessage(msg);
        verify(msg).ack();
        verify(managementService, never()).createExternalWorkerJobQuery();
    }
}
