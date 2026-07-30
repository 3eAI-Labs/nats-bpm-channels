package com.threeai.nats.camunda.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.model.bpmn.instance.EventDefinition;
import org.camunda.bpm.model.bpmn.instance.FlowElement;
import org.camunda.bpm.model.bpmn.instance.Message;
import org.camunda.bpm.model.bpmn.instance.MessageEventDefinition;
import org.camunda.bpm.model.bpmn.instance.SendTask;
import org.camunda.bpm.model.bpmn.instance.SignalEventDefinition;
import org.camunda.bpm.model.bpmn.instance.ThrowEvent;
import org.camunda.bpm.model.bpmn.instance.UserTask;
import org.junit.jupiter.api.Test;

class OutboundMessageTypeResolverTest {

    @Test
    void resolve_sendTaskWithMessage_returnsMessageName() {
        DelegateExecution execution = mock(DelegateExecution.class);
        SendTask sendTask = mock(SendTask.class);
        Message message = mock(Message.class);
        when(message.getName()).thenReturn("order.created");
        when(sendTask.getMessage()).thenReturn(message);
        when(execution.getBpmnModelElementInstance()).thenReturn(sendTask);

        Optional<String> type = OutboundMessageTypeResolver.resolve(execution);

        assertThat(type).contains("order.created");
    }

    @Test
    void resolve_sendTaskWithoutMessage_returnsEmpty() {
        DelegateExecution execution = mock(DelegateExecution.class);
        SendTask sendTask = mock(SendTask.class);
        when(sendTask.getMessage()).thenReturn(null);
        when(execution.getBpmnModelElementInstance()).thenReturn(sendTask);

        assertThat(OutboundMessageTypeResolver.resolve(execution)).isEmpty();
    }

    @Test
    void resolve_throwEventWithMessageEventDefinition_returnsMessageName() {
        DelegateExecution execution = mock(DelegateExecution.class);
        ThrowEvent throwEvent = mock(ThrowEvent.class);
        MessageEventDefinition messageEventDefinition = mock(MessageEventDefinition.class);
        Message message = mock(Message.class);
        when(message.getName()).thenReturn("payment.requested");
        when(messageEventDefinition.getMessage()).thenReturn(message);
        when(throwEvent.getEventDefinitions()).thenReturn(List.of(messageEventDefinition));
        when(execution.getBpmnModelElementInstance()).thenReturn(throwEvent);

        Optional<String> type = OutboundMessageTypeResolver.resolve(execution);

        assertThat(type).contains("payment.requested");
    }

    @Test
    void resolve_throwEventWithNonMessageEventDefinition_returnsEmpty() {
        DelegateExecution execution = mock(DelegateExecution.class);
        ThrowEvent throwEvent = mock(ThrowEvent.class);
        SignalEventDefinition signalEventDefinition = mock(SignalEventDefinition.class);
        when(throwEvent.getEventDefinitions()).thenReturn(List.<EventDefinition>of(signalEventDefinition));
        when(execution.getBpmnModelElementInstance()).thenReturn(throwEvent);

        // D-B' scope guard: signal/escalation throw events are explicitly out of scope.
        assertThat(OutboundMessageTypeResolver.resolve(execution)).isEmpty();
    }

    @Test
    void resolve_throwEventWithNoEventDefinitions_returnsEmpty() {
        DelegateExecution execution = mock(DelegateExecution.class);
        ThrowEvent throwEvent = mock(ThrowEvent.class);
        when(throwEvent.getEventDefinitions()).thenReturn(List.of());
        when(execution.getBpmnModelElementInstance()).thenReturn(throwEvent);

        assertThat(OutboundMessageTypeResolver.resolve(execution)).isEmpty();
    }

    @Test
    void resolve_blankMessageName_returnsEmpty() {
        DelegateExecution execution = mock(DelegateExecution.class);
        SendTask sendTask = mock(SendTask.class);
        Message message = mock(Message.class);
        when(message.getName()).thenReturn("   ");
        when(sendTask.getMessage()).thenReturn(message);
        when(execution.getBpmnModelElementInstance()).thenReturn(sendTask);

        assertThat(OutboundMessageTypeResolver.resolve(execution)).isEmpty();
    }

    @Test
    void resolve_unsupportedElement_returnsEmpty() {
        DelegateExecution execution = mock(DelegateExecution.class);
        FlowElement userTask = mock(UserTask.class);
        when(execution.getBpmnModelElementInstance()).thenReturn(userTask);

        // D-B' scope: only message-throw + send-task are supported attachment points.
        assertThat(OutboundMessageTypeResolver.resolve(execution)).isEmpty();
    }
}
