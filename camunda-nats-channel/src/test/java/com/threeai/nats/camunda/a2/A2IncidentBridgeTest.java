package com.threeai.nats.camunda.a2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.exception.NotFoundException;
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
                "cb-incident-bridge-camunda-test", null, NotFoundException.class);

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

    @Test
    void subscribe_registersJetStreamPushSubscription_logsSuccess() throws Exception {
        Connection freshConnection = mock(Connection.class);
        JetStream freshJetStream = mock(JetStream.class);
        io.nats.client.Dispatcher dispatcher = mock(io.nats.client.Dispatcher.class);
        when(freshConnection.createDispatcher()).thenReturn(dispatcher);
        A2IncidentBridge freshBridge = new A2IncidentBridge(freshConnection, freshJetStream,
                externalTaskService, "a2-jetstream-bridge", config, circuitBreaker, null);

        freshBridge.subscribe();

        verify(freshJetStream).subscribe(org.mockito.ArgumentMatchers.eq(config.getSubject()), eq(dispatcher),
                org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq(false),
                org.mockito.ArgumentMatchers.any());
    }

    @Test
    void subscribe_jetStreamThrows_wrapsAsIllegalStateException() throws Exception {
        Connection freshConnection = mock(Connection.class);
        JetStream freshJetStream = mock(JetStream.class);
        when(freshConnection.createDispatcher()).thenReturn(mock(io.nats.client.Dispatcher.class));
        when(freshJetStream.subscribe(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyBoolean(),
                org.mockito.ArgumentMatchers.any(io.nats.client.PushSubscribeOptions.class)))
                .thenThrow(new java.io.IOException("subscribe failed"));
        A2IncidentBridge freshBridge = new A2IncidentBridge(freshConnection, freshJetStream,
                externalTaskService, "a2-jetstream-bridge", config, circuitBreaker, null);

        org.assertj.core.api.Assertions.assertThatThrownBy(freshBridge::subscribe)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(config.getSubject());
    }

    @Test
    void unsubscribe_beforeSubscribe_isNoOp_doesNotThrow() {
        A2IncidentBridge freshBridge = new A2IncidentBridge(mock(Connection.class), mock(JetStream.class),
                externalTaskService, "a2-jetstream-bridge", config, circuitBreaker, null);

        org.assertj.core.api.Assertions.assertThatCode(freshBridge::unsubscribe).doesNotThrowAnyException();
    }

    @Test
    void subscribeThenUnsubscribe_drainsDispatcherAndShutsDownExecutor() throws Exception {
        Connection freshConnection = mock(Connection.class);
        JetStream freshJetStream = mock(JetStream.class);
        io.nats.client.Dispatcher dispatcher = mock(io.nats.client.Dispatcher.class);
        when(freshConnection.createDispatcher()).thenReturn(dispatcher);
        A2IncidentBridge freshBridge = new A2IncidentBridge(freshConnection, freshJetStream,
                externalTaskService, "a2-jetstream-bridge", config, circuitBreaker, null);
        freshBridge.subscribe();

        freshBridge.unsubscribe();

        verify(dispatcher).drain(Duration.ofSeconds(10));
    }

    @Test
    void unsubscribe_dispatcherDrainThrows_logsWarningAndStillShutsDownExecutor() throws Exception {
        Connection freshConnection = mock(Connection.class);
        JetStream freshJetStream = mock(JetStream.class);
        io.nats.client.Dispatcher dispatcher = mock(io.nats.client.Dispatcher.class);
        when(freshConnection.createDispatcher()).thenReturn(dispatcher);
        when(dispatcher.drain(any(Duration.class))).thenThrow(new RuntimeException("drain failed"));
        A2IncidentBridge freshBridge = new A2IncidentBridge(freshConnection, freshJetStream,
                externalTaskService, "a2-jetstream-bridge", config, circuitBreaker, null);
        freshBridge.subscribe();

        org.assertj.core.api.Assertions.assertThatCode(freshBridge::unsubscribe).doesNotThrowAnyException();
    }

    /**
     * {@code extractExternalTaskId}/{@code dlqReasonOf} both short-circuit to their "no headers"
     * default (null / "UNKNOWN") on the SAME precondition — a single message with null headers
     * exercises both defensive branches at once, still through the real success path (mocked
     * {@code handleFailure} does not care that the task id is null).
     */
    @Test
    void handleDlqMessage_headersNull_extractsNullTaskIdAndUnknownReason_stillAcks() {
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn("{}".getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(null);
        when(msg.getSubject()).thenReturn("dlq.jobs.order-fulfillment");

        bridge.handleDlqMessage(msg);

        verify(externalTaskService).handleFailure(eq((String) null), eq("a2-jetstream-bridge"),
                anyString(), eq("UNKNOWN"), org.mockito.ArgumentMatchers.eq(0), org.mockito.ArgumentMatchers.eq(0L));
        verify(msg).ack();
    }

    /**
     * {@code Nats-Msg-Id} NOT carrying the {@code .dlq} suffix contract-fix #3 normally guarantees
     * (a malformed/legacy message) — {@code extractExternalTaskId} falls back to the raw header
     * value verbatim rather than stripping a suffix that isn't there.
     */
    @Test
    void handleDlqMessage_msgIdWithoutDlqSuffix_usesRawValueAsExternalTaskId() {
        Headers headers = new Headers();
        headers.add("Nats-Msg-Id", "task-raw-no-suffix"); // no ".dlq" suffix
        headers.add("X-Cadenzaflow-Dlq-Reason", "BUS_REPLY_DELIVERY_BUDGET_EXCEEDED");
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn("{}".getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(headers);
        when(msg.getSubject()).thenReturn("dlq.jobs.order-fulfillment");

        bridge.handleDlqMessage(msg);

        verify(externalTaskService).handleFailure(eq("task-raw-no-suffix"), eq("a2-jetstream-bridge"),
                anyString(), anyString(), org.mockito.ArgumentMatchers.eq(0), org.mockito.ArgumentMatchers.eq(0L));
    }

    @Test
    void handleDlqMessage_metaDataUnavailableOnFailurePath_backoffUsesDefaultDeliveryCountOne() {
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn("{}".getBytes(StandardCharsets.UTF_8));
        Headers headers = new Headers();
        headers.add("Nats-Msg-Id", "task-metaless.dlq");
        headers.add("X-Cadenzaflow-Dlq-Reason", "BUS_REPLY_DELIVERY_BUDGET_EXCEEDED");
        when(msg.getHeaders()).thenReturn(headers);
        when(msg.getSubject()).thenReturn("dlq.jobs.order-fulfillment");
        when(msg.metaData()).thenThrow(new IllegalStateException("not a JetStream message"));
        doThrow(new RuntimeException("cockpit db down"))
                .when(externalTaskService).handleFailure(anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong());

        bridge.handleDlqMessage(msg);

        // deliveryCountOf() caught the metaData() failure and fell back to 1 -> backoff 2^0=1s.
        verify(msg).nakWithDelay(Duration.ofSeconds(1));
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
