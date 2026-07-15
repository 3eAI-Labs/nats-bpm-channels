package com.threeai.nats.camunda.a2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsJetStreamMetaData;
import org.camunda.bpm.engine.BadUserRequestException;
import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.exception.NotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class A2CompletionBridgeTest {

    private ExternalTaskService externalTaskService;
    private DlqPublisher dlqPublisher;
    private NatsChannelMetrics metrics;
    private A2ConsumerConfig config;
    private A2CompletionBridge bridge;

    @BeforeEach
    void setUp() {
        externalTaskService = mock(ExternalTaskService.class);
        Connection connection = mock(Connection.class);
        JetStream jetStream = mock(JetStream.class);
        dlqPublisher = new DlqPublisher(jetStream, connection, new NatsChannelMetrics(new SimpleMeterRegistry()));
        metrics = new NatsChannelMetrics(new SimpleMeterRegistry());

        config = new A2ConsumerConfig();
        config.setSubject("jobs.order-fulfillment.reply");
        config.setMessageName("order-fulfillment");
        config.setMaxDeliver(4);
        config.setDlqSubject("dlq.jobs.order-fulfillment");

        bridge = new A2CompletionBridge(connection, jetStream, externalTaskService,
                "a2-jetstream-bridge", config, dlqPublisher, metrics);
    }

    @Test
    void handleReply_success_completesTaskAndAcks() {
        Message msg = successMessage("task-1", "{\"result\":\"ok\"}", 1);

        bridge.handleReply(msg);

        verify(externalTaskService).complete(eq("task-1"), eq("a2-jetstream-bridge"), anyMap());
        verify(msg).ack();
    }

    @Test
    void handleReply_bpmnError_callsHandleBpmnError() {
        Message msg = jsonMessage("task-2", "{\"errorCode\":\"INSUFFICIENT_FUNDS\",\"errorMessage\":\"no funds\"}", 1);

        bridge.handleReply(msg);

        verify(externalTaskService).handleBpmnError(eq("task-2"), eq("a2-jetstream-bridge"),
                eq("INSUFFICIENT_FUNDS"), eq("no funds"), anyMap());
        verify(msg).ack();
    }

    @Test
    void handleReply_transient_callsHandleFailure() {
        Message msg = jsonMessage("task-3", "{\"errorMessage\":\"downstream timeout\",\"errorDetails\":\"stack...\"}", 1);

        bridge.handleReply(msg);

        verify(externalTaskService).handleFailure(eq("task-3"), eq("a2-jetstream-bridge"),
                eq("downstream timeout"), eq("stack..."), eq(0), anyLong());
        verify(msg).ack();
    }

    @Test
    void handleReply_taskNotFound_idempotentAck() {
        Message msg = successMessage("task-4", "{}", 1);
        doThrow(new NotFoundException("no such task", null))
                .when(externalTaskService).complete(any(), any(), anyMap());

        bridge.handleReply(msg);

        verify(msg).ack();
    }

    @Test
    void handleReply_workerConflict_neitherAcksNorNaks_incrementsMetric() {
        Message msg = successMessage("task-5", "{}", 1);
        doThrow(new BadUserRequestException("wrong worker", null))
                .when(externalTaskService).complete(any(), any(), anyMap());

        bridge.handleReply(msg);

        verify(msg, never()).ack();
        verify(msg, never()).nakWithDelay(any(Duration.class));
        assertThat(metrics.sentinelWorkerConflictCount("order-fulfillment").count()).isEqualTo(1.0);
    }

    @Test
    void handleReply_transientDbFailure_naks() {
        Message msg = successMessage("task-6", "{}", 1);
        doThrow(new RuntimeException("db timeout"))
                .when(externalTaskService).complete(any(), any(), anyMap());

        bridge.handleReply(msg);

        verify(msg).nakWithDelay(any(Duration.class));
        verify(msg, never()).ack();
    }

    @Test
    void handleReply_emptyBody_routesToDlqAndAcks() throws Exception {
        Message msg = successMessage("task-7", "", 1);
        JetStream jetStream = mock(JetStream.class);
        DlqPublisher testDlqPublisher = new DlqPublisher(jetStream, mock(Connection.class), metrics);
        A2CompletionBridge freshBridge = new A2CompletionBridge(mock(Connection.class), jetStream,
                externalTaskService, "a2-jetstream-bridge", config, testDlqPublisher, metrics);

        freshBridge.handleReply(msg);

        verify(jetStream).publish(any(io.nats.client.impl.NatsMessage.class));
        verify(msg).ack();
        verify(externalTaskService, never()).complete(any(), any(), anyMap());
    }

    @Test
    void handleReply_deliveryBudgetExceeded_routesToDlqAndAcks() throws Exception {
        Message msg = successMessage("task-8", "{}", 10); // 10 > maxDeliver(4)
        JetStream jetStream = mock(JetStream.class);
        DlqPublisher testDlqPublisher = new DlqPublisher(jetStream, mock(Connection.class), metrics);
        A2CompletionBridge freshBridge = new A2CompletionBridge(mock(Connection.class), jetStream,
                externalTaskService, "a2-jetstream-bridge", config, testDlqPublisher, metrics);

        freshBridge.handleReply(msg);

        verify(jetStream).publish(any(io.nats.client.impl.NatsMessage.class));
        verify(msg).ack();
        verify(externalTaskService, never()).complete(any(), any(), anyMap());
    }

    private Message successMessage(String externalTaskId, String body, long deliveryCount) {
        Headers headers = new Headers();
        headers.add("Nats-Msg-Id", externalTaskId);
        return createMockMessage(body, headers, deliveryCount);
    }

    private Message jsonMessage(String externalTaskId, String body, long deliveryCount) {
        Headers headers = new Headers();
        headers.add("Nats-Msg-Id", externalTaskId);
        headers.add("Content-Type", "application/json");
        return createMockMessage(body, headers, deliveryCount);
    }

    private Message createMockMessage(String body, Headers headers, long deliveryCount) {
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn(body.getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(headers);
        when(msg.getSubject()).thenReturn("jobs.order-fulfillment.reply");
        NatsJetStreamMetaData metaData = mock(NatsJetStreamMetaData.class);
        when(metaData.deliveredCount()).thenReturn(deliveryCount);
        when(msg.metaData()).thenReturn(metaData);
        return msg;
    }
}
