package com.threeai.nats.cadenzaflow.shard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import com.threeai.nats.core.shard.ShardTopology;

import org.cadenzaflow.bpm.engine.ProcessEngine;
import org.cadenzaflow.bpm.engine.ProcessEngineConfiguration;
import org.cadenzaflow.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * docs/13 §2.5 slice 4 against a REAL engine — G1 (root guard) and G1b (exemptions,
 * including the F-5 false-green killer: none-start + signal-start in the SAME definition).
 * Frozen hash vectors used throughout: "ORD-123" -> shard 1 of 2; "a" -> shard 0 of 2.
 */
class ShardBirthGuardIntegrationTest {

    private static final String SIMPLE = """
            <?xml version="1.0" encoding="UTF-8"?>
            <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                         xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                         targetNamespace="shardtest">
              <process id="simpleProc" isExecutable="true" camunda:historyTimeToLive="P1D">
                <startEvent id="s"/>
                <sequenceFlow id="f1" sourceRef="s" targetRef="wait"/>
                <userTask id="wait"/>
                <sequenceFlow id="f2" sourceRef="wait" targetRef="e"/>
                <endEvent id="e"/>
              </process>
            </definitions>
            """;

    private static final String PARENT_WITH_CALL = """
            <?xml version="1.0" encoding="UTF-8"?>
            <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                         xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                         targetNamespace="shardtest">
              <process id="parentProc" isExecutable="true" camunda:historyTimeToLive="P1D">
                <startEvent id="s"/>
                <sequenceFlow id="f1" sourceRef="s" targetRef="call"/>
                <callActivity id="call" calledElement="childProc"/>
                <sequenceFlow id="f2" sourceRef="call" targetRef="e"/>
                <endEvent id="e"/>
              </process>
              <process id="childProc" isExecutable="true" camunda:historyTimeToLive="P1D">
                <startEvent id="cs"/>
                <sequenceFlow id="cf1" sourceRef="cs" targetRef="cwait"/>
                <userTask id="cwait"/>
                <sequenceFlow id="cf2" sourceRef="cwait" targetRef="ce"/>
                <endEvent id="ce"/>
              </process>
            </definitions>
            """;

    /** F-5 false-green killer: none start AND signal start in the SAME definition. */
    private static final String NONE_PLUS_SIGNAL = """
            <?xml version="1.0" encoding="UTF-8"?>
            <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                         xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                         targetNamespace="shardtest">
              <signal id="sig1" name="growthSignal"/>
              <process id="mixedProc" isExecutable="true" camunda:historyTimeToLive="P1D">
                <startEvent id="noneStart"/>
                <sequenceFlow id="f1" sourceRef="noneStart" targetRef="wait"/>
                <startEvent id="sigStart">
                  <signalEventDefinition signalRef="sig1"/>
                </startEvent>
                <sequenceFlow id="f2" sourceRef="sigStart" targetRef="wait"/>
                <userTask id="wait"/>
                <sequenceFlow id="f3" sourceRef="wait" targetRef="e"/>
                <endEvent id="e"/>
              </process>
            </definitions>
            """;

    private ProcessEngine engine;

    private ProcessEngine engine(int shardCount, int shardId) {
        ProcessEngineConfigurationImpl configuration = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneInMemProcessEngineConfiguration()
                        .setJdbcUrl("jdbc:h2:mem:shard-guard-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1")
                        .setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE)
                        .setJobExecutorActivate(false);
        configuration.setCustomPreBPMNParseListeners(List.of(
                new ShardBirthGuardParseListener(new ShardTopology(shardCount, shardId))));
        engine = configuration.buildProcessEngine();
        return engine;
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.close();
        }
    }

    @Test
    void rootStart_withoutBusinessKey_rejected_andRolledBack() {
        ProcessEngine e = engine(2, 0);
        e.getRepositoryService().createDeployment().addString("p.bpmn20.xml", SIMPLE).deploy();

        assertThatThrownBy(() -> e.getRuntimeService().startProcessInstanceByKey("simpleProc"))
                .hasMessageContaining("VAL_SHARD_BUSINESS_KEY_REQUIRED");
        assertThat(e.getRuntimeService().createProcessInstanceQuery().count()).isZero();
    }

    @Test
    void rootStart_wrongShard_rejected_withOwnerInMessage() {
        ProcessEngine e = engine(2, 0); // ORD-123 -> shard 1; this engine is shard 0
        e.getRepositoryService().createDeployment().addString("p.bpmn20.xml", SIMPLE).deploy();

        assertThatThrownBy(() -> e.getRuntimeService()
                .startProcessInstanceByKey("simpleProc", "ORD-123"))
                .hasMessageContaining("VAL_SHARD_WRONG_SHARD")
                .hasMessageContaining("shard 1");
        assertThat(e.getRuntimeService().createProcessInstanceQuery().count()).isZero();
    }

    @Test
    void rootStart_owningShard_passes() {
        ProcessEngine e = engine(2, 1); // ORD-123 -> shard 1 = own
        e.getRepositoryService().createDeployment().addString("p.bpmn20.xml", SIMPLE).deploy();

        assertThatCode(() -> e.getRuntimeService()
                .startProcessInstanceByKey("simpleProc", "ORD-123"))
                .doesNotThrowAnyException();
        assertThat(e.getRuntimeService().createProcessInstanceQuery().count()).isEqualTo(1);
    }

    @Test
    void callActivityChild_exemptDespiteNullBusinessKey() {
        ProcessEngine e = engine(2, 1);
        e.getRepositoryService().createDeployment().addString("p.bpmn20.xml", PARENT_WITH_CALL).deploy();

        // parent born correctly on its shard; child has NO camunda:businessKey expression
        e.getRuntimeService().startProcessInstanceByKey("parentProc", "ORD-123");

        assertThat(e.getRuntimeService().createProcessInstanceQuery()
                .processDefinitionKey("childProc").count()).isEqualTo(1); // born, exempt
        assertThat(e.getTaskService().createTaskQuery().taskDefinitionKey("cwait").count())
                .isEqualTo(1);
    }

    @Test
    void signalStart_exempt_shardLocalClass() {
        ProcessEngine e = engine(2, 0);
        e.getRepositoryService().createDeployment().addString("p.bpmn20.xml", NONE_PLUS_SIGNAL).deploy();

        e.getRuntimeService().signalEventReceived("growthSignal");

        assertThat(e.getRuntimeService().createProcessInstanceQuery().count()).isEqualTo(1);
        assertThat(e.getRuntimeService().createProcessInstanceQuery().singleResult()
                .getBusinessKey()).isNull(); // engine design: no key on signal path
    }

    @Test
    void mixedDefinition_noneStartStillGuarded_signalStartStillExempt() {
        // The F-5 false-green killer: SAME definition, both paths, opposite outcomes.
        ProcessEngine e = engine(2, 0);
        e.getRepositoryService().createDeployment().addString("p.bpmn20.xml", NONE_PLUS_SIGNAL).deploy();

        assertThatThrownBy(() -> e.getRuntimeService().startProcessInstanceByKey("mixedProc"))
                .hasMessageContaining("VAL_SHARD_BUSINESS_KEY_REQUIRED"); // none-start guarded

        e.getRuntimeService().signalEventReceived("growthSignal");        // signal exempt
        assertThat(e.getRuntimeService().createProcessInstanceQuery().count()).isEqualTo(1);
    }

    @Test
    void messageStartViaCorrelation_guarded() {
        ProcessEngine e = engine(2, 0); // "a" -> shard 0: correct; ORD-123 -> wrong
        String bpmn = SIMPLE.replace("<startEvent id=\"s\"/>",
                "<startEvent id=\"s\"><messageEventDefinition messageRef=\"m1\"/></startEvent>")
                .replace("<process ", "<message id=\"m1\" name=\"StartMsg\"/><process ");
        e.getRepositoryService().createDeployment().addString("p.bpmn20.xml", bpmn).deploy();

        assertThatCode(() -> e.getRuntimeService().createMessageCorrelation("StartMsg")
                .processInstanceBusinessKey("a").correlateStartMessage())
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> e.getRuntimeService().createMessageCorrelation("StartMsg")
                .processInstanceBusinessKey("ORD-123").correlateStartMessage())
                .hasMessageContaining("VAL_SHARD_WRONG_SHARD");
        assertThat(e.getRuntimeService().createProcessInstanceQuery().count()).isEqualTo(1);
    }
}
