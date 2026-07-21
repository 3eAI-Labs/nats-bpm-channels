package com.threeai.nats.camunda.history;

import static org.assertj.core.api.Assertions.assertThat;

import com.threeai.nats.core.history.HistoryClassNames;
import org.camunda.bpm.engine.impl.batch.history.HistoricBatchEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricActivityInstanceEventEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricCaseInstanceEventEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricDecisionEvaluationEvent;
import org.camunda.bpm.engine.impl.history.event.HistoricExternalTaskLogEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricIdentityLinkLogEventEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricIncidentEventEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricProcessInstanceEventEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricTaskInstanceEventEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricVariableUpdateEventEntity;
import org.camunda.bpm.engine.impl.history.event.UserOperationLogEntryEventEntity;
import org.camunda.bpm.engine.impl.persistence.entity.HistoricJobLogEventEntity;
import org.junit.jupiter.api.Test;

class HistoryEventClassResolverTest {

    @Test
    void resolve_entityLifecycleClasses() {
        assertThat(HistoryEventClassResolver.resolve(new HistoricProcessInstanceEventEntity()))
                .contains(HistoryClassNames.PROCINST);
        assertThat(HistoryEventClassResolver.resolve(new HistoricActivityInstanceEventEntity()))
                .contains(HistoryClassNames.ACTINST);
        assertThat(HistoryEventClassResolver.resolve(new HistoricTaskInstanceEventEntity()))
                .contains(HistoryClassNames.TASKINST);
        assertThat(HistoryEventClassResolver.resolve(new HistoricIncidentEventEntity()))
                .contains(HistoryClassNames.INCIDENT);
        assertThat(HistoryEventClassResolver.resolve(new HistoricCaseInstanceEventEntity()))
                .contains(HistoryClassNames.CASEINST);
    }

    @Test
    void resolve_appendOnlyLogClasses() {
        assertThat(HistoryEventClassResolver.resolve(new HistoricVariableUpdateEventEntity()))
                .contains(HistoryClassNames.DETAIL);
        assertThat(HistoryEventClassResolver.resolve(new HistoricIdentityLinkLogEventEntity()))
                .contains(HistoryClassNames.IDENTITYLINK);
        assertThat(HistoryEventClassResolver.resolve(new UserOperationLogEntryEventEntity()))
                .contains(HistoryClassNames.OP_LOG);
        assertThat(HistoryEventClassResolver.resolve(new HistoricExternalTaskLogEntity()))
                .contains(HistoryClassNames.EXT_TASK_LOG);
        assertThat(HistoryEventClassResolver.resolve(new HistoricJobLogEventEntity()))
                .contains(HistoryClassNames.JOB_LOG);
        assertThat(HistoryEventClassResolver.resolve(new HistoricDecisionEvaluationEvent()))
                .contains(HistoryClassNames.DECINST);
        assertThat(HistoryEventClassResolver.resolve(new HistoricBatchEntity()))
                .contains(HistoryClassNames.BATCH);
    }

    @Test
    void resolve_unknownEventType_returnsEmpty_failSafeUnclassified() {
        // HistoricFormPropertyEventEntity is a real fork event type this basamak does not
        // classify -- VAL_HISTORY_CLASS_UNCLASSIFIED path.
        assertThat(HistoryEventClassResolver.resolve(
                new org.camunda.bpm.engine.impl.history.event.HistoricFormPropertyEventEntity())).isEmpty();
    }
}
