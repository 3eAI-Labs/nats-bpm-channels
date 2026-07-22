package com.threeai.nats.camunda.outbound;

import java.util.Optional;

import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.model.bpmn.instance.FlowElement;
import org.camunda.bpm.model.bpmn.instance.Message;
import org.camunda.bpm.model.bpmn.instance.MessageEventDefinition;
import org.camunda.bpm.model.bpmn.instance.SendTask;
import org.camunda.bpm.model.bpmn.instance.ThrowEvent;

/**
 * Derives the D-C' classification key ("message type") from the BPMN element {@link
 * NatsOutboundPublisher} is attached to — D-B' scope: Message-throw + Send-task ONLY. Package-
 * private + static so it is directly unit-testable against a mocked {@link DelegateExecution}
 * without needing to bootstrap a real engine (mirrors {@code
 * A2ExternalTaskBehavior#captureAllowlistedVariables}'s testability rationale).
 *
 * <p><b>CODER-NOTE (why type is derived at notify()-time, not field-injected):</b> {@code
 * NatsOutboundPublisher} is registered as a SINGLETON Spring bean (D-A' — tenant BPMN references
 * it via {@code delegateExpression="${natsOutboundPublisher}"} from potentially many different
 * activities). Camunda field injection (`camunda:field`) would mutate INSTANCE state on that
 * shared singleton — a correctness bug if two different message-throw/send-task activities
 * referenced the SAME bean with different intended message types (a race/clobber, not merely a
 * style choice). Deriving the type from the model element at invocation time keeps the listener
 * stateless and thread-safe. See phase-5 return report CODER-NOTE.
 */
final class OutboundMessageTypeResolver {

    private OutboundMessageTypeResolver() {
    }

    static Optional<String> resolve(DelegateExecution execution) {
        FlowElement element = execution.getBpmnModelElementInstance();
        if (element instanceof SendTask sendTask) {
            return messageNameOf(sendTask.getMessage());
        }
        if (element instanceof ThrowEvent throwEvent) {
            return throwEvent.getEventDefinitions().stream()
                    .filter(MessageEventDefinition.class::isInstance)
                    .map(MessageEventDefinition.class::cast)
                    .findFirst()
                    .flatMap(med -> messageNameOf(med.getMessage()));
        }
        // D-B' scope guard: any other element (user task, script task, signal/escalation throw,
        // etc.) is NOT a supported outbound-handoff attachment point — fail-safe skip, not an error
        // (the tenant may have attached the listener to the wrong element by mistake; we do not
        // want a misconfigured listener to break process execution).
        return Optional.empty();
    }

    private static Optional<String> messageNameOf(Message message) {
        if (message == null) {
            return Optional.empty();
        }
        String name = message.getName();
        return (name == null || name.isBlank()) ? Optional.empty() : Optional.of(name);
    }
}
