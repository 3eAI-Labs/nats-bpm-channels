package com.threeai.nats.cadenzaflow.inbound;

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
import org.cadenzaflow.bpm.engine.RuntimeService;
import org.cadenzaflow.bpm.engine.runtime.MessageCorrelationBuilder;
import org.cadenzaflow.bpm.engine.runtime.MessageCorrelationResult;
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
