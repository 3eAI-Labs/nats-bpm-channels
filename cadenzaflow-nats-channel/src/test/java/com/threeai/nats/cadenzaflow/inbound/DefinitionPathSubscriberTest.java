package com.threeai.nats.cadenzaflow.inbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.cadenzaflow.bpm.engine.RuntimeService;
import org.cadenzaflow.bpm.engine.runtime.MessageCorrelationBuilder;
import org.cadenzaflow.bpm.engine.runtime.MessageCorrelationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.threeai.nats.cadenzaflow.eventing.EventDefinition;
import com.threeai.nats.core.dlq.DlqPublishOutcome;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.dlq.DlqReason;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;

/**
 * Slice-5 coverage (docs/12 D-C/D-F v2): definition-aware correlation on both subscribers —
 * typed payload variables, processInstanceVariableEquals per correlation parameter, no
 * natsPayload blob on the definition path, and permanent-failure handling for a payload
 * that lacks a declared correlation field (core: WARN drop; JetStream: DLQ).
 */
class DefinitionPathSubscriberTest {

    private Connection connection;
    private RuntimeService runtimeService;
    private MessageCorrelationBuilder correlationBuilder;
    private SubscriptionConfig config;
    private EventDefinition definition;

    @BeforeEach
    void setUp() {
        connection = mock(Connection.class);
        runtimeService = mock(RuntimeService.class);
        correlationBuilder = mock(MessageCorrelationBuilder.class);

        when(runtimeService.createMessageCorrelation(any())).thenReturn(correlationBuilder);
        when(correlationBuilder.processInstanceBusinessKey(any())).thenReturn(correlationBuilder);
        when(correlationBuilder.processInstanceVariableEquals(any(), any())).thenReturn(correlationBuilder);
        when(correlationBuilder.setVariables(anyMap())).thenReturn(correlationBuilder);
        when(correlationBuilder.correlateWithResult()).thenReturn(mock(MessageCorrelationResult.class));

        config = new SubscriptionConfig();
        config.setSubject("order.new");
        config.setMessageName("OrderReceived");
        config.setMaxDeliver(5);
        config.setDlqSubject("order.dlq");

        definition = new EventDefinition(
                "orderReceived", "Order Received",
                Map.of("orderId", "long"),
                Map.of("orderId", "long", "amount", "double", "note", "string"),
                "OrderReceived");
    }

    private Message coreMessage(String body) {
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn(body.getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(null);
        when(msg.getSubject()).thenReturn("order.new");
        return msg;
    }

    private Message jetStreamMessage(String body) {
        Message msg = coreMessage(body);
        when(msg.metaData()).thenThrow(new IllegalStateException("no metadata in unit test"));
        return msg;
    }

    // --- core subscriber, definition path ---

    @Test
    void core_definitionPath_typedVariables_variableEquals_noNatsPayloadBlob() {
        NatsMessageCorrelationSubscriber subscriber = new NatsMessageCorrelationSubscriber(
                connection, runtimeService, config, null, definition);

        subscriber.handleMessage(coreMessage("{\"orderId\":123,\"amount\":9.5,\"ignored\":\"x\"}"));

        @SuppressWarnings({"unchecked", "rawtypes"})
        ArgumentCaptor<Map<String, Object>> vars = ArgumentCaptor.forClass((Class) Map.class);
        verify(correlationBuilder).setVariables(vars.capture());
        assertThat(vars.getValue())
                .containsEntry("orderId", 123L)
                .containsEntry("amount", 9.5)
                .doesNotContainKeys("natsPayload", "natsSubject", "ignored", "note");
        verify(correlationBuilder).processInstanceVariableEquals("orderId", 123L);
        verify(correlationBuilder).correlateWithResult();
    }

    @Test
    void core_definitionPath_missingCorrelationField_dropsWithoutCorrelating() {
        NatsMessageCorrelationSubscriber subscriber = new NatsMessageCorrelationSubscriber(
                connection, runtimeService, config, null, definition);

        subscriber.handleMessage(coreMessage("{\"amount\":9.5}"));

        verify(correlationBuilder, never()).correlateWithResult();
    }

    @Test
    void core_legacyPath_stillCarriesNatsPayload() {
        NatsMessageCorrelationSubscriber subscriber = new NatsMessageCorrelationSubscriber(
                connection, runtimeService, config, null);

        subscriber.handleMessage(coreMessage("{\"orderId\":123}"));

        @SuppressWarnings({"unchecked", "rawtypes"})
        ArgumentCaptor<Map<String, Object>> vars = ArgumentCaptor.forClass((Class) Map.class);
        verify(correlationBuilder).setVariables(vars.capture());
        assertThat(vars.getValue()).containsKeys("natsPayload", "natsSubject");
        verify(correlationBuilder, never()).processInstanceVariableEquals(any(), any());
    }

    // --- JetStream subscriber, definition path ---

    @Test
    void jetStream_definitionPath_typedVariables_variableEquals_acks() {
        JetStream jetStream = mock(JetStream.class);
        DlqPublisher dlqPublisher = mock(DlqPublisher.class);
        JetStreamMessageCorrelationSubscriber subscriber = new JetStreamMessageCorrelationSubscriber(
                connection, jetStream, runtimeService, config, null, dlqPublisher, definition);

        Message msg = jetStreamMessage("{\"orderId\":123,\"note\":\"hi\"}");
        subscriber.handleMessage(msg);

        @SuppressWarnings({"unchecked", "rawtypes"})
        ArgumentCaptor<Map<String, Object>> vars = ArgumentCaptor.forClass((Class) Map.class);
        verify(correlationBuilder).setVariables(vars.capture());
        assertThat(vars.getValue())
                .containsEntry("orderId", 123L)
                .containsEntry("note", "hi")
                .doesNotContainKeys("natsPayload", "natsSubject");
        verify(correlationBuilder).processInstanceVariableEquals("orderId", 123L);
        verify(msg).ack();
    }

    @Test
    void jetStream_definitionPath_missingCorrelationField_routesToDlqNotNak() {
        JetStream jetStream = mock(JetStream.class);
        DlqPublisher dlqPublisher = mock(DlqPublisher.class);
        when(dlqPublisher.publish(any(), any(), eq(DlqReason.MISSING_CORRELATION_VALUE), any(), any()))
                .thenReturn(DlqPublishOutcome.PUBLISHED_JETSTREAM);
        JetStreamMessageCorrelationSubscriber subscriber = new JetStreamMessageCorrelationSubscriber(
                connection, jetStream, runtimeService, config, null, dlqPublisher, definition);

        Message msg = jetStreamMessage("{\"note\":\"no correlation field\"}");
        subscriber.handleMessage(msg);

        verify(dlqPublisher).publish(eq(msg), eq("order.dlq"),
                eq(DlqReason.MISSING_CORRELATION_VALUE), eq("order.new"), eq("OrderReceived"));
        verify(correlationBuilder, never()).correlateWithResult();
        verify(msg).ack(); // DLQ publish succeeded -> terminal ack, no redelivery
    }

    // --- D-F v2 business-key regression (behavior change, both directions) ---

    @Test
    void businessKey_numericField_nowCorrect_wasNextFieldName() {
        config.setBusinessKeyVariable("orderId");
        NatsMessageCorrelationSubscriber subscriber = new NatsMessageCorrelationSubscriber(
                connection, runtimeService, config, null);

        subscriber.handleMessage(coreMessage("{\"orderId\":123,\"name\":\"x\"}"));

        // Old indexOf scan produced "name" here; Jackson produces the actual value.
        verify(correlationBuilder).processInstanceBusinessKey("123");
    }

    @Test
    void businessKey_nestedField_noLongerFound() {
        config.setBusinessKeyVariable("orderId");
        NatsMessageCorrelationSubscriber subscriber = new NatsMessageCorrelationSubscriber(
                connection, runtimeService, config, null);

        subscriber.handleMessage(coreMessage("{\"outer\":{\"orderId\":\"abc\"}}"));

        // Old scan found nested fields; the documented D-F v2 change stops that.
        verify(correlationBuilder, never()).processInstanceBusinessKey(any());
        verify(correlationBuilder).correlateWithResult();
    }

    @Test
    void eventDefinition_typeDictionary_coversAllSupportedTypes() {
        assertThat(EventDefinition.TYPE_DICTIONARY)
                .isEqualTo(List.of("string", "integer", "long", "double", "boolean", "json"));
    }
}
