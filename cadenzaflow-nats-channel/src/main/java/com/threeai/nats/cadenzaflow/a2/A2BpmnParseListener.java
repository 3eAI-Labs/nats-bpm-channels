package com.threeai.nats.cadenzaflow.a2;

import org.cadenzaflow.bpm.engine.impl.bpmn.behavior.ExternalTaskActivityBehavior;
import org.cadenzaflow.bpm.engine.impl.bpmn.parser.AbstractBpmnParseListener;
import org.cadenzaflow.bpm.engine.impl.bpmn.parser.BpmnParse;
import org.cadenzaflow.bpm.engine.impl.core.variable.mapping.value.ConstantValueProvider;
import org.cadenzaflow.bpm.engine.impl.pvm.process.ActivityImpl;
import org.cadenzaflow.bpm.engine.impl.pvm.process.ScopeImpl;
import org.cadenzaflow.bpm.engine.impl.util.xml.Element;

/**
 * Parse-time swap: for every {@code camunda:type="external"} service task whose
 * {@code camunda:topic} is a configured A2 topic, replaces the native
 * {@link ExternalTaskActivityBehavior} with {@link A2ExternalTaskBehavior} (HLD §2.1,
 * BR-A2-002/003/012, FR-A2/A3/A13, US-A2/A8, ADR-0005).
 *
 * <p><b>Scope note (literal topic only):</b> {@code BpmnParse.parseTopic(...)} resolves the
 * topic attribute through {@code createParameterValueProvider(...)}, which also supports EL
 * expressions (e.g. {@code camunda:topic="${myTopicExpr}"}) resolvable only at runtime. A2-topic
 * membership must be decided at parse time (deployment), since the behavior-swap itself is a
 * parse-time decision. Consequence: the swap only applies to activities with a LITERAL
 * {@code camunda:topic}; expression-based dynamic topics stay on the classic external-task
 * poller. This is consistent with BR-A2-012/US-A8 ("non-A2 tasks are unaffected") — an
 * expression-topic activity falls naturally into the "not A2" bucket. PO-accepted 2026-07-15
 * (phase-review MINOR-2).
 */
public class A2BpmnParseListener extends AbstractBpmnParseListener {

    private final A2TopicConfig topicConfig;
    private final String sentinelWorkerId;
    private final UmbrellaLockResolver lockResolver;
    private final A2PostCommitPublisher publisher;

    public A2BpmnParseListener(A2TopicConfig topicConfig, String sentinelWorkerId,
            UmbrellaLockResolver lockResolver, A2PostCommitPublisher publisher) {
        this.topicConfig = topicConfig;
        this.sentinelWorkerId = sentinelWorkerId;
        this.lockResolver = lockResolver;
        this.publisher = publisher;
    }

    @Override
    public void parseServiceTask(Element serviceTaskElement, ScopeImpl scope, ActivityImpl activity) {
        if (!(activity.getActivityBehavior() instanceof ExternalTaskActivityBehavior nativeBehavior)
                || activity.getActivityBehavior() instanceof A2ExternalTaskBehavior) {
            return; // neither external-task nor already-swapped — leave untouched (idempotent re-entry guard)
        }
        // Same extension attribute BpmnParse.parseTopic(...) uses (BpmnParse.java:2558) —
        // only a LITERAL (constant) topic string is matched here (see class-level scope note).
        String topic = serviceTaskElement.attributeNS(BpmnParse.CAMUNDA_BPMN_EXTENSIONS_NS, "topic");
        if (topic == null || !topicConfig.isA2Topic(topic)) {
            return; // classic external task — behavior UNCHANGED (BR-A2-012 migration guard, US-A8)
        }
        long lockDurationMillis = lockResolver.resolveMillis(topic);
        activity.setActivityBehavior(new A2ExternalTaskBehavior(
                new ConstantValueProvider(topic),
                nativeBehavior.getPriorityValueProvider(),
                sentinelWorkerId, lockDurationMillis, publisher,
                topicConfig.variableAllowlistFor(topic)));
    }
}
