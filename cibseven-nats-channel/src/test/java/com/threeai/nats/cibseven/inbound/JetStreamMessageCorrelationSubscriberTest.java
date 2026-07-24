package com.threeai.nats.cibseven.inbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
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
import io.nats.client.impl.NatsMessage;
import org.cibseven.bpm.engine.RuntimeService;
import org.cibseven.bpm.engine.runtime.MessageCorrelationBuilder;
import org.cibseven.bpm.engine.runtime.MessageCorrelationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class JetStreamMessageCorrelationSubscriberTest {

    private Connection connection;
    private JetStream jetStream;
    private RuntimeService runtimeService;
    private MessageCorrelationBuilder correlationBuilder;
    private SubscriptionConfig config;
    private DlqPublisher dlqPublisher;
    private JetStreamMessageCorrelationSubscriber subscriber;

    @BeforeEach
    void setUp() {
        connection = mock(Connection.class);
        jetStream = mock(JetStream.class);
        runtimeService = mock(RuntimeService.class);
        correlationBuilder = mock(MessageCorrelationBuilder.class);
        dlqPublisher = new DlqPublisher(jetStream, connection, new NatsChannelMetrics(new SimpleMeterRegistry()));

        when(runtimeService.createMessageCorrelation(any())).thenReturn(correlationBuilder);
        when(correlationBuilder.processInstanceBusinessKey(any())).thenReturn(correlationBuilder);
        when(correlationBuilder.setVariables(anyMap())).thenReturn(correlationBuilder);
        when(correlationBuilder.correlateWithResult()).thenReturn(mock(MessageCorrelationResult.class));

        config = new SubscriptionConfig();
        config.setSubject("order.new");
        config.setMessageName("OrderReceived");
        config.setMaxDeliver(5);
        config.setDlqSubject("order.dlq");

        subscriber = new JetStreamMessageCorrelationSubscriber(
                connection, jetStream, runtimeService, config, null, dlqPublisher);
    }

    @Test
    void subscribe_registersJetStreamPushSubscription_logsSuccess() throws Exception {
        Connection freshConnection = mock(Connection.class);
        JetStream freshJetStream = mock(JetStream.class);
        io.nats.client.Dispatcher dispatcher = mock(io.nats.client.Dispatcher.class);
        when(freshConnection.createDispatcher()).thenReturn(dispatcher);
        JetStreamMessageCorrelationSubscriber freshSubscriber = new JetStreamMessageCorrelationSubscriber(
                freshConnection, freshJetStream, runtimeService, config, null, dlqPublisher);

        freshSubscriber.subscribe();

        verify(freshJetStream).subscribe(eq(config.getSubject()), eq(dispatcher), any(), eq(false), any());
    }

    @Test
    void subscribe_durableNameConfigured_appliedToConsumerConfiguration() throws Exception {
        config.setDurableName("order-received-durable");
        Connection freshConnection = mock(Connection.class);
        JetStream freshJetStream = mock(JetStream.class);
        when(freshConnection.createDispatcher()).thenReturn(mock(io.nats.client.Dispatcher.class));
        JetStreamMessageCorrelationSubscriber freshSubscriber = new JetStreamMessageCorrelationSubscriber(
                freshConnection, freshJetStream, runtimeService, config, null, dlqPublisher);

        freshSubscriber.subscribe();

        org.mockito.ArgumentCaptor<io.nats.client.PushSubscribeOptions> optsCaptor =
                org.mockito.ArgumentCaptor.forClass(io.nats.client.PushSubscribeOptions.class);
        verify(freshJetStream).subscribe(eq(config.getSubject()), any(), any(), eq(false), optsCaptor.capture());
        assertThat(optsCaptor.getValue().getConsumerConfiguration().getDurable()).isEqualTo("order-received-durable");
    }

    @Test
    void subscribe_jetStreamThrows_wrapsAsIllegalStateException() throws Exception {
        Connection freshConnection = mock(Connection.class);
        JetStream freshJetStream = mock(JetStream.class);
        when(freshConnection.createDispatcher()).thenReturn(mock(io.nats.client.Dispatcher.class));
        when(freshJetStream.subscribe(org.mockito.ArgumentMatchers.anyString(), any(), any(),
                org.mockito.ArgumentMatchers.anyBoolean(), any(io.nats.client.PushSubscribeOptions.class)))
                .thenThrow(new IOException("subscribe failed"));
        JetStreamMessageCorrelationSubscriber freshSubscriber = new JetStreamMessageCorrelationSubscriber(
                freshConnection, freshJetStream, runtimeService, config, null, dlqPublisher);

        org.assertj.core.api.Assertions.assertThatThrownBy(freshSubscriber::subscribe)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(config.getSubject());
    }

    @Test
    void unsubscribe_beforeSubscribe_isNoOp_doesNotThrow() {
        org.assertj.core.api.Assertions.assertThatCode(subscriber::unsubscribe).doesNotThrowAnyException();
    }

    @Test
    void subscribeThenUnsubscribe_drainsDispatcherAndShutsDownExecutor() throws Exception {
        Connection freshConnection = mock(Connection.class);
        JetStream freshJetStream = mock(JetStream.class);
        io.nats.client.Dispatcher dispatcher = mock(io.nats.client.Dispatcher.class);
        when(freshConnection.createDispatcher()).thenReturn(dispatcher);
        JetStreamMessageCorrelationSubscriber freshSubscriber = new JetStreamMessageCorrelationSubscriber(
                freshConnection, freshJetStream, runtimeService, config, null, dlqPublisher);
        freshSubscriber.subscribe();

        freshSubscriber.unsubscribe();

        verify(dispatcher).drain(Duration.ofSeconds(10));
    }

    @Test
    void unsubscribe_dispatcherDrainThrows_logsWarningAndStillShutsDownExecutor() throws Exception {
        Connection freshConnection = mock(Connection.class);
        JetStream freshJetStream = mock(JetStream.class);
        io.nats.client.Dispatcher dispatcher = mock(io.nats.client.Dispatcher.class);
        when(freshConnection.createDispatcher()).thenReturn(dispatcher);
        when(dispatcher.drain(any(Duration.class))).thenThrow(new RuntimeException("drain failed"));
        JetStreamMessageCorrelationSubscriber freshSubscriber = new JetStreamMessageCorrelationSubscriber(
                freshConnection, freshJetStream, runtimeService, config, null, dlqPublisher);
        freshSubscriber.subscribe();

        org.assertj.core.api.Assertions.assertThatCode(freshSubscriber::unsubscribe).doesNotThrowAnyException();
    }

    @Test
    void handleMessage_businessKeyHeaderConfigured_appliedToCorrelation() {
        config.setBusinessKeyHeader("X-Business-Key");
        Headers headers = new Headers();
        headers.add("X-Business-Key", "biz-42");
        Message msg = createMockMessage("{\"orderId\":1}", headers, 1);

        subscriber.handleMessage(msg);

        verify(correlationBuilder).processInstanceBusinessKey("biz-42");
    }

    @Test
    void handleMessage_businessKeyVariableConfigured_extractsFromJsonPayload() {
        config.setBusinessKeyVariable("orderId");
        Message msg = createMockMessage("{\"orderId\":\"biz-json-1\",\"other\":2}", null, 1);

        subscriber.handleMessage(msg);

        verify(correlationBuilder).processInstanceBusinessKey("biz-json-1");
    }

    @Test
    void handleMessage_businessKeyVariableFieldMissing_businessKeyNull_skipsBuilderCall() {
        config.setBusinessKeyVariable("missingField");
        Message msg = createMockMessage("{\"orderId\":1}", null, 1);

        subscriber.handleMessage(msg);

        verify(correlationBuilder, never()).processInstanceBusinessKey(any());
    }

    @Test
    void handleMessage_businessKeyVariableMalformedJson_noColonAfterField_businessKeyNull() {
        config.setBusinessKeyVariable("orderId");
        Message msg = createMockMessage("{\"orderId\"}", null, 1); // field present, no ':' after it

        subscriber.handleMessage(msg);

        verify(correlationBuilder, never()).processInstanceBusinessKey(any());
    }

    @Test
    void handleMessage_businessKeyVariableMalformedJson_noOpeningQuoteAfterColon_businessKeyNull() {
        config.setBusinessKeyVariable("orderId");
        Message msg = createMockMessage("{\"orderId\":42}", null, 1); // numeric value, no quote

        subscriber.handleMessage(msg);

        verify(correlationBuilder, never()).processInstanceBusinessKey(any());
    }

    @Test
    void handleMessage_businessKeyVariableMalformedJson_unterminatedString_businessKeyNull() {
        config.setBusinessKeyVariable("orderId");
        Message msg = createMockMessage("{\"orderId\":\"unterminated", null, 1); // no closing quote

        subscriber.handleMessage(msg);

        verify(correlationBuilder, never()).processInstanceBusinessKey(any());
    }

    @Test
    void handleMessage_withMetrics_naksIncrementsCounterOnCorrelationFailure() {
        NatsChannelMetrics realMetrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        JetStreamMessageCorrelationSubscriber metricsSubscriber = new JetStreamMessageCorrelationSubscriber(
                connection, jetStream, runtimeService, config, realMetrics, dlqPublisher);
        Message msg = createMockMessage("{\"orderId\":1}", null, 1);
        when(correlationBuilder.correlateWithResult()).thenThrow(new RuntimeException("no process"));

        metricsSubscriber.handleMessage(msg);

        assertThat(realMetrics.nakCount("order.new", "OrderReceived").count()).isEqualTo(1.0);
    }

    @Test
    void handleMessage_withMetrics_ackIncrementsCounterOnSuccess() {
        NatsChannelMetrics realMetrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        JetStreamMessageCorrelationSubscriber metricsSubscriber = new JetStreamMessageCorrelationSubscriber(
                connection, jetStream, runtimeService, config, realMetrics, dlqPublisher);
        Message msg = createMockMessage("{\"orderId\":1}", null, 1);

        metricsSubscriber.handleMessage(msg);

        assertThat(realMetrics.ackCount("order.new", "OrderReceived").count()).isEqualTo(1.0);
    }

    @Test
    void handleMessage_withMetrics_maxDeliverExceededAndDlqBothFail_incrementsNakCounter() throws Exception {
        NatsChannelMetrics realMetrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        JetStream failingJetStream = mock(JetStream.class);
        Connection failingConnection = mock(Connection.class);
        when(failingJetStream.publish(any(NatsMessage.class))).thenThrow(new IOException("JS down"));
        doThrow(new RuntimeException("core down"))
                .when(failingConnection).publish(any(String.class), any(Headers.class), any(byte[].class));
        DlqPublisher bothFailDlqPublisher = new DlqPublisher(failingJetStream, failingConnection, realMetrics);
        JetStreamMessageCorrelationSubscriber metricsSubscriber = new JetStreamMessageCorrelationSubscriber(
                connection, failingJetStream, runtimeService, config, realMetrics, bothFailDlqPublisher);
        Message msg = createMockMessage("{\"orderId\":99}", null, 6); // 6 > maxDeliver(5)

        metricsSubscriber.handleMessage(msg);

        assertThat(realMetrics.nakCount("order.new", "OrderReceived").count()).isEqualTo(1.0);
    }

    @Test
    void handleMessage_metaDataUnavailableDuringMaxDeliverCheck_defaultsToDeliveryCountOne_processesNormally() {
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn("{\"orderId\":1}".getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(null);
        when(msg.getSubject()).thenReturn("order.new");
        when(msg.metaData()).thenThrow(new IllegalStateException("not a JetStream message"));

        subscriber.handleMessage(msg);

        // deliveryCount defaults to 1 (<= maxDeliver(5)) -- proceeds to normal correlation, not DLQ.
        verify(runtimeService).createMessageCorrelation("OrderReceived");
        verify(msg).ack();
    }

    @Test
    void handleMessage_correlationFailsAndMetaDataUnavailable_fallsBackToPlainNak() {
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn("{\"orderId\":1}".getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(null);
        when(msg.getSubject()).thenReturn("order.new");
        when(msg.metaData()).thenThrow(new IllegalStateException("not a JetStream message"));
        when(correlationBuilder.correlateWithResult()).thenThrow(new RuntimeException("no process"));

        subscriber.handleMessage(msg);

        // nakWithBackoff()'s OWN metaData() call also fails -> falls back to plain nak(), never
        // reaching nakWithDelay(...) at all.
        verify(msg).nak();
        verify(msg, never()).nakWithDelay(any(Duration.class));
    }

    @Test
    void handleMessage_success_acksMessage() {
        Message msg = createMockMessage("{\"orderId\":1}", null, 1);

        subscriber.handleMessage(msg);

        verify(runtimeService).createMessageCorrelation("OrderReceived");
        verify(correlationBuilder).correlateWithResult();
        verify(msg).ack();
    }

    @Test
    void handleMessage_correlationFails_naksWithDelay() {
        Message msg = createMockMessage("{\"orderId\":1}", null, 1);
        when(correlationBuilder.correlateWithResult())
                .thenThrow(new RuntimeException("No process found"));

        subscriber.handleMessage(msg);

        verify(msg).nakWithDelay(Duration.ofSeconds(1));
    }

    @Test
    void handleMessage_maxDeliverExceeded_publishesToDlqAndAcks() throws Exception {
        Message msg = createMockMessage("{\"orderId\":99}", null, 6);

        subscriber.handleMessage(msg);

        verify(jetStream).publish(any(NatsMessage.class));
        verify(msg).ack();
        verify(runtimeService, never()).createMessageCorrelation(any());
    }

    @Test
    void handleMessage_dlqBothFail_naksInsteadOfAck() throws Exception {
        Message msg = createMockMessage("{\"orderId\":99}", null, 6);
        when(jetStream.publish(any(NatsMessage.class))).thenThrow(new IOException("JS unavailable"));
        doThrow(new RuntimeException("core NATS down"))
                .when(connection).publish(any(String.class), any(Headers.class), any(byte[].class));

        subscriber.handleMessage(msg);

        verify(msg, never()).ack();
        verify(msg).nakWithDelay(any(Duration.class));
    }

    @Test
    void handleMessage_emptyBody_routesToDlqAndAcks() throws Exception {
        Message msg = createMockMessage("", null, 1);

        subscriber.handleMessage(msg);

        verify(jetStream).publish(any(NatsMessage.class));
        verify(msg).ack();
        verify(runtimeService, never()).createMessageCorrelation(any());
    }

    @Test
    void handleMessage_emptyBody_dlqSubjectMissing_naksInsteadOfDiscarding() {
        SubscriptionConfig noDlqConfig = new SubscriptionConfig();
        noDlqConfig.setSubject("order.new");
        noDlqConfig.setMessageName("OrderReceived");
        noDlqConfig.setMaxDeliver(5);
        JetStreamMessageCorrelationSubscriber noDlqSubscriber = new JetStreamMessageCorrelationSubscriber(
                connection, jetStream, runtimeService, noDlqConfig, null, dlqPublisher);

        Message msg = createMockMessage("", null, 1);

        noDlqSubscriber.handleMessage(msg);

        verify(msg, never()).ack();
        verify(msg).nakWithDelay(any(Duration.class));
    }

    @Test
    void handleMessage_propagatesTraceId() {
        Headers headers = new Headers();
        headers.add("X-Trace-Id", "trace-js-456");
        Message msg = createMockMessage("{\"orderId\":1}", headers, 1);

        final String[] capturedTraceId = {null};
        when(correlationBuilder.correlateWithResult()).thenAnswer(invocation -> {
            capturedTraceId[0] = MDC.get("trace_id");
            return mock(MessageCorrelationResult.class);
        });

        subscriber.handleMessage(msg);

        assertThat(capturedTraceId[0]).isEqualTo("trace-js-456");
        assertThat(MDC.get("trace_id")).isNull();
    }

    @Test
    void handleMessage_standardTraceHeaderTakesPrecedenceOverLegacy() {
        Headers headers = new Headers();
        headers.add("X-Cadenzaflow-Trace-Id", "trace-standard");
        headers.add("X-Trace-Id", "trace-legacy");
        Message msg = createMockMessage("{\"orderId\":1}", headers, 1);

        final String[] capturedTraceId = {null};
        when(correlationBuilder.correlateWithResult()).thenAnswer(invocation -> {
            capturedTraceId[0] = MDC.get("trace_id");
            return mock(MessageCorrelationResult.class);
        });

        subscriber.handleMessage(msg);

        assertThat(capturedTraceId[0]).isEqualTo("trace-standard");
    }

    private Message createMockMessage(String body, Headers headers, long deliveryCount) {
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn(body.getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(headers);
        when(msg.getSubject()).thenReturn("order.new");

        NatsJetStreamMetaData metaData = mock(NatsJetStreamMetaData.class);
        when(metaData.deliveredCount()).thenReturn(deliveryCount);
        when(msg.metaData()).thenReturn(metaData);

        return msg;
    }
}
