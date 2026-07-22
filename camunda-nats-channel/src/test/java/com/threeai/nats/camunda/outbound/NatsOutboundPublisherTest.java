package com.threeai.nats.camunda.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.threeai.nats.core.outbound.OutboundClassificationProperties;
import com.threeai.nats.core.outbound.OutboundMessageOutboxWriter;
import com.threeai.nats.core.outbound.OutboundPostCommitPublisher;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.VariableScope;
import org.camunda.bpm.model.bpmn.instance.Message;
import org.camunda.bpm.model.bpmn.instance.SendTask;
import org.camunda.bpm.model.bpmn.instance.UserTask;
import org.junit.jupiter.api.Test;

/**
 * Mirrors {@code A2ExternalTaskBehaviorTest}'s testability rationale: {@code
 * captureAllowlistedVariables} is package-private + static so it can be exercised against a mocked
 * {@link VariableScope} without needing to mock Camunda's static {@code Context}/{@code
 * CommandContext} bootstrap machinery. The classify-and-dispatch branches of {@code notify()}
 * (which DO touch {@code Context.getCommandContext()}) are exercised via the embedded-engine
 * integration test ({@code NatsOutboundHandoffIntegrationTest}), same pattern as {@code
 * A2ExternalTaskBehavior#execute}.
 */
class NatsOutboundPublisherTest {

    @Test
    void captureAllowlistedVariables_emptyAllowlist_returnsEmptyMap() {
        VariableScope scope = mock(VariableScope.class);

        Map<String, Object> captured = NatsOutboundPublisher.captureAllowlistedVariables(scope, List.of());

        assertThat(captured).isEmpty();
    }

    @Test
    void captureAllowlistedVariables_nullAllowlist_returnsEmptyMap() {
        VariableScope scope = mock(VariableScope.class);

        Map<String, Object> captured = NatsOutboundPublisher.captureAllowlistedVariables(scope, null);

        assertThat(captured).isEmpty();
    }

    @Test
    void captureAllowlistedVariables_populatedAllowlist_capturesOnlyExistingVariables() {
        VariableScope scope = mock(VariableScope.class);
        when(scope.hasVariable("amount")).thenReturn(true);
        when(scope.getVariable("amount")).thenReturn(42);
        when(scope.hasVariable("currency")).thenReturn(true);
        when(scope.getVariable("currency")).thenReturn("EUR");

        Map<String, Object> captured = NatsOutboundPublisher.captureAllowlistedVariables(
                scope, List.of("amount", "currency"));

        assertThat(captured).containsExactly(Map.entry("amount", 42), Map.entry("currency", "EUR"));
    }

    @Test
    void captureAllowlistedVariables_allowlistedVariableMissingFromScope_isOmitted() {
        VariableScope scope = mock(VariableScope.class);
        when(scope.hasVariable("amount")).thenReturn(true);
        when(scope.getVariable("amount")).thenReturn(42);
        when(scope.hasVariable("neverSet")).thenReturn(false);

        Map<String, Object> captured = NatsOutboundPublisher.captureAllowlistedVariables(
                scope, List.of("amount", "neverSet"));

        assertThat(captured).containsOnly(Map.entry("amount", 42));
    }

    @Test
    void notify_unsupportedBpmnElement_skipsPublish_doesNotThrow_neverTouchesWriterOrPublisher() throws Exception {
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.getBpmnModelElementInstance()).thenReturn(mock(UserTask.class));
        when(execution.getCurrentActivityId()).thenReturn("task1");
        when(execution.getProcessInstanceId()).thenReturn("proc-1");

        OutboundClassificationProperties classification = new OutboundClassificationProperties();
        OutboundMessageOutboxWriter outboxWriter = mock(OutboundMessageOutboxWriter.class);
        OutboundPostCommitPublisher postCommitPublisher = mock(OutboundPostCommitPublisher.class);
        NatsOutboundPublisher publisher =
                new NatsOutboundPublisher(classification, outboxWriter, postCommitPublisher, "camunda");

        // The unsupported-element branch returns BEFORE ever touching Context.getCommandContext(),
        // so this is safely unit-testable without mocking Camunda's static command-context machinery.
        assertThatCode(() -> publisher.notify(execution)).doesNotThrowAnyException();
        verifyNoInteractions(outboxWriter, postCommitPublisher);
    }

    @Test
    void notify_criticalTypeWithoutConfiguredOutboxWriter_throwsIllegalStateException() {
        DelegateExecution execution = mock(DelegateExecution.class);
        SendTask sendTask = mock(SendTask.class);
        Message message = mock(Message.class);
        when(message.getName()).thenReturn("payment_requested");
        when(sendTask.getMessage()).thenReturn(message);
        when(execution.getBpmnModelElementInstance()).thenReturn(sendTask);
        when(execution.getProcessInstanceId()).thenReturn("proc-1");

        OutboundClassificationProperties classification = new OutboundClassificationProperties();
        classification.setCriticalTypes(Set.of("payment_requested"));
        OutboundPostCommitPublisher postCommitPublisher = mock(OutboundPostCommitPublisher.class);
        // outboxWriter == null -- DataSource-less deployment (auto-config gating, see class Javadoc).
        NatsOutboundPublisher publisher = new NatsOutboundPublisher(classification, null, postCommitPublisher, "camunda");

        // This branch throws BEFORE ever touching Context.getCommandContext(), so it is safely
        // unit-testable without mocking Camunda's static command-context machinery.
        assertThatThrownBy(() -> publisher.notify(execution))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("payment_requested")
                .hasMessageContaining("CRITICAL");
        verifyNoInteractions(postCommitPublisher);
    }

    // --- Phase-review FINDING-003 (MINOR — messageType subject-token safety) ---

    @Test
    void notify_messageTypeUnsafeForSubjectToken_skipsPublish_doesNotThrow_neverTouchesWriterOrPublisher() throws Exception {
        DelegateExecution execution = mock(DelegateExecution.class);
        SendTask sendTask = mock(SendTask.class);
        Message message = mock(Message.class);
        when(message.getName()).thenReturn("payment.requested"); // dotted -- unsafe subject token
        when(sendTask.getMessage()).thenReturn(message);
        when(execution.getBpmnModelElementInstance()).thenReturn(sendTask);
        when(execution.getProcessInstanceId()).thenReturn("proc-1");

        OutboundClassificationProperties classification = new OutboundClassificationProperties();
        OutboundMessageOutboxWriter outboxWriter = mock(OutboundMessageOutboxWriter.class);
        OutboundPostCommitPublisher postCommitPublisher = mock(OutboundPostCommitPublisher.class);
        NatsOutboundPublisher publisher =
                new NatsOutboundPublisher(classification, outboxWriter, postCommitPublisher, "camunda");

        // This branch also returns BEFORE ever touching Context.getCommandContext().
        assertThatCode(() -> publisher.notify(execution)).doesNotThrowAnyException();
        verifyNoInteractions(outboxWriter, postCommitPublisher);
    }
}
