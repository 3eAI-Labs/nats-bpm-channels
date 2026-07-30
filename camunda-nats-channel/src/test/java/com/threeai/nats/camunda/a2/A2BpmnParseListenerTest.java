package com.threeai.nats.camunda.a2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.camunda.bpm.engine.impl.bpmn.behavior.ExternalTaskActivityBehavior;
import org.camunda.bpm.engine.impl.bpmn.parser.BpmnParse;
import org.camunda.bpm.engine.impl.core.variable.mapping.value.ParameterValueProvider;
import org.camunda.bpm.engine.impl.pvm.delegate.ActivityBehavior;
import org.camunda.bpm.engine.impl.pvm.process.ActivityImpl;
import org.camunda.bpm.engine.impl.pvm.process.ScopeImpl;
import org.camunda.bpm.engine.impl.util.xml.Element;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class A2BpmnParseListenerTest {

    private A2TopicConfig topicConfig;
    private A2PostCommitPublisher publisher;
    private UmbrellaLockResolver lockResolver;
    private A2BpmnParseListener listener;

    @BeforeEach
    void setUp() {
        A2Properties properties = new A2Properties();
        properties.setTopics(List.of("order-fulfillment"));
        topicConfig = new A2TopicConfig(properties);
        publisher = mock(A2PostCommitPublisher.class);
        lockResolver = mock(UmbrellaLockResolver.class);
        when(lockResolver.resolveMillis("order-fulfillment")).thenReturn(320_000L);

        listener = new A2BpmnParseListener(topicConfig, "a2-jetstream-bridge", lockResolver, publisher);
    }

    @Test
    void parseServiceTask_a2Topic_swapsToA2Behavior() {
        Element element = mock(Element.class);
        when(element.attributeNS(BpmnParse.CAMUNDA_BPMN_EXTENSIONS_NS, "topic")).thenReturn("order-fulfillment");
        ActivityImpl activity = mock(ActivityImpl.class);
        ExternalTaskActivityBehavior nativeBehavior = mock(ExternalTaskActivityBehavior.class);
        ParameterValueProvider priorityProvider = mock(ParameterValueProvider.class);
        when(nativeBehavior.getPriorityValueProvider()).thenReturn(priorityProvider);
        when(activity.getActivityBehavior()).thenReturn(nativeBehavior);

        listener.parseServiceTask(element, mock(ScopeImpl.class), activity);

        verify(activity).setActivityBehavior(org.mockito.ArgumentMatchers.any(A2ExternalTaskBehavior.class));
    }

    @Test
    void parseServiceTask_nonA2Topic_leavesBehaviorUnchanged() {
        Element element = mock(Element.class);
        when(element.attributeNS(BpmnParse.CAMUNDA_BPMN_EXTENSIONS_NS, "topic")).thenReturn("some-other-topic");
        ActivityImpl activity = mock(ActivityImpl.class);
        ExternalTaskActivityBehavior nativeBehavior = mock(ExternalTaskActivityBehavior.class);
        when(activity.getActivityBehavior()).thenReturn(nativeBehavior);

        listener.parseServiceTask(element, mock(ScopeImpl.class), activity);

        verify(activity, never()).setActivityBehavior(any());
    }

    @Test
    void parseServiceTask_expressionTopic_leavesBehaviorUnchanged() {
        Element element = mock(Element.class);
        // Literal-attribute lookup returns null for expression-based topics (parse-time can't resolve them).
        when(element.attributeNS(BpmnParse.CAMUNDA_BPMN_EXTENSIONS_NS, "topic")).thenReturn(null);
        ActivityImpl activity = mock(ActivityImpl.class);
        ExternalTaskActivityBehavior nativeBehavior = mock(ExternalTaskActivityBehavior.class);
        when(activity.getActivityBehavior()).thenReturn(nativeBehavior);

        listener.parseServiceTask(element, mock(ScopeImpl.class), activity);

        verify(activity, never()).setActivityBehavior(any());
    }

    @Test
    void parseServiceTask_notExternalTaskBehavior_leavesUnchanged() {
        Element element = mock(Element.class);
        ActivityImpl activity = mock(ActivityImpl.class);
        ActivityBehavior otherBehavior = mock(ActivityBehavior.class);
        when(activity.getActivityBehavior()).thenReturn(otherBehavior);

        listener.parseServiceTask(element, mock(ScopeImpl.class), activity);

        verify(activity, never()).setActivityBehavior(any());
        verify(element, never()).attributeNS(any(), any());
    }

    @Test
    void parseServiceTask_alreadySwapped_isIdempotent() {
        Element element = mock(Element.class);
        ActivityImpl activity = mock(ActivityImpl.class);
        A2ExternalTaskBehavior alreadySwapped = mock(A2ExternalTaskBehavior.class);
        when(activity.getActivityBehavior()).thenReturn(alreadySwapped);

        listener.parseServiceTask(element, mock(ScopeImpl.class), activity);

        verify(activity, never()).setActivityBehavior(any());
    }
}
