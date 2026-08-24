package com.threeai.nats.camunda.shard;

import com.threeai.nats.core.shard.ShardTopology;

import org.camunda.bpm.engine.delegate.ExecutionListener;
import org.camunda.bpm.engine.impl.bpmn.parser.AbstractBpmnParseListener;
import org.camunda.bpm.engine.impl.persistence.entity.ProcessDefinitionEntity;
import org.camunda.bpm.engine.impl.util.xml.Element;

/**
 * Installs the {@link ShardBirthGuard} on every parsed process definition's scope as an
 * {@code EVENTNAME_START} execution listener — the seam the design review verified as the
 * ONLY supported one that both sees the businessKey ({@code setBusinessKey} runs before
 * {@code start()}) and can veto with a rollback (listener exceptions propagate). The
 * rejected alternative (a custom CommandInterceptor) would need eight reflection-based
 * extractors across the instance-creating command family, two of which carry no key at all.
 */
public class ShardBirthGuardParseListener extends AbstractBpmnParseListener {

    private final ShardBirthGuard guard;

    public ShardBirthGuardParseListener(ShardTopology topology) {
        this.guard = new ShardBirthGuard(topology);
    }

    @Override
    public void parseProcess(Element processElement, ProcessDefinitionEntity processDefinition) {
        processDefinition.addListener(ExecutionListener.EVENTNAME_START, guard);
    }
}
