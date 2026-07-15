package com.threeai.nats.cadenzaflow.a2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import com.threeai.nats.core.resilience.DlqBridgeCircuitBreakerFactory;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsJetStreamMetaData;
import org.cadenzaflow.bpm.engine.ExternalTaskService;
import org.cadenzaflow.bpm.engine.exception.NotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class A2IncidentBridgeTest {

    private ExternalTaskService externalTaskService;
    private CircuitBreaker circuitBreaker;
    private A2ConsumerConfig config;
    private A2IncidentBridge bridge;

    @BeforeEach
    void setUp() {
        externalTaskService = mock(ExternalTaskService.class);
        circuitBreaker = DlqBridgeCircuitBreakerFactory.create(
                "cb-incident-bridge-cadenzaflow-test", null, NotFoundException.class);

        config = new A2ConsumerConfig();
        config.setSubject("dlq.jobs.>");
        config.setMessageName("a2-incident-bridge");
        config.setMaxDeliver(4);

        bridge = new A2IncidentBridge(mock(Connection.class), mock(JetStream.class), externalTaskService,
                "a2-jetstream-bridge", config, circuitBreaker, null);
    }

    @Test
    void handleDlqMessage_success_handlesFailureWithRetriesZero_acks() {
        Message msg = dlqMessage("task-1", "BUS_REPLY_DELIVERY_BUDGET_EXCEEDED", 5);

        bridge.handleDlqMessage(msg);

        verify(externalTaskService).handleFailure(
                org.mockito.ArgumentMatchers.eq("task-1"), org.mockito.ArgumentMatchers.eq("a2-jetstream-bridge"),
                anyString(), anyString(), org.mockito.ArgumentMatchers.eq(0), org.mockito.ArgumentMatchers.eq(0L));
        verify(msg).ack();
    }

    @Test
    void handleDlqMessage_taskAlreadyResolved_idempotentAck_doesNotOpenCircuit() {
        Message msg = dlqMessage("task-2", "BUS_REPLY_DELIVERY_BUDGET_EXCEEDED", 5);
        doThrow(new NotFoundException("already resolved", null))
                .when(externalTaskService).handleFailure(anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong());

        // Fire several times — since NotFoundException is ignored by the CB, it must never open.
        for (int i = 0; i < 10; i++) {
            bridge.handleDlqMessage(msg);
        }

        verify(msg, org.mockito.Mockito.times(10)).ack();
        assertThat(circuitBreaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    void handleDlqMessage_downstreamFailure_naks() {
        Message msg = dlqMessage("task-3", "BUS_REPLY_DELIVERY_BUDGET_EXCEEDED", 5);
        doThrow(new RuntimeException("cockpit db down"))
                .when(externalTaskService).handleFailure(anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong());

        bridge.handleDlqMessage(msg);

        verify(msg).nakWithDelay(any(Duration.class));
        verify(msg, never()).ack();
    }

    @Test
    void handleDlqMessage_circuitBreakerOpen_naksFastWithoutCallingService() {
        Message msg = dlqMessage("task-4", "BUS_REPLY_DELIVERY_BUDGET_EXCEEDED", 5);
        doThrow(new RuntimeException("cockpit db down"))
                .when(externalTaskService).handleFailure(anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong());

        // Trip the circuit breaker (5 consecutive failures -> OPEN, ADR-0004).
        for (int i = 0; i < 5; i++) {
            bridge.handleDlqMessage(dlqMessage("task-x" + i, "BUS_REPLY_DELIVERY_BUDGET_EXCEEDED", 5));
        }
        assertThat(circuitBreaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        bridge.handleDlqMessage(msg);

        verify(msg).nakWithDelay(any(Duration.class));
    }

    private Message dlqMessage(String externalTaskId, String reason, long deliveryCount) {
        Headers headers = new Headers();
        headers.add("Nats-Msg-Id", externalTaskId + ".dlq");
        headers.add("X-Cadenzaflow-Dlq-Reason", reason);
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn("{}".getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(headers);
        when(msg.getSubject()).thenReturn("dlq.jobs.order-fulfillment");
        NatsJetStreamMetaData metaData = mock(NatsJetStreamMetaData.class);
        when(metaData.deliveredCount()).thenReturn(deliveryCount);
        when(msg.metaData()).thenReturn(metaData);
        return msg;
    }
}
