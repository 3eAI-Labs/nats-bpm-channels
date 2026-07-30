package com.threeai.nats.camunda.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

import com.threeai.nats.core.history.HistoryClassNames;
import org.camunda.bpm.engine.impl.batch.history.HistoricBatchEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricCaseInstanceEventEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricDecisionEvaluationEvent;
import org.camunda.bpm.engine.impl.history.event.HistoricDecisionInstanceEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricDetailEventEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricExternalTaskLogEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricIdentityLinkLogEventEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricIncidentEventEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricProcessInstanceEventEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricVariableUpdateEventEntity;
import org.camunda.bpm.engine.impl.history.event.UserOperationLogEntryEventEntity;
import org.camunda.bpm.engine.impl.persistence.entity.HistoricJobLogEventEntity;
import org.junit.jupiter.api.Test;

class HistoryEventFieldExtractorTest {

    @Test
    void extractFields_processInstance_capturesKeyFields() {
        HistoricProcessInstanceEventEntity event = new HistoricProcessInstanceEventEntity();
        event.setBusinessKey("biz-1");
        event.setProcessDefinitionKey("myProcess");

        Map<String, Object> fields = HistoryEventFieldExtractor.extractFields(event);

        assertThat(fields).containsEntry("businessKey", "biz-1");
        assertThat(fields).containsEntry("processDefinitionKey", "myProcess");
    }

    @Test
    void extractFields_userOperationLog_capturesBoundedFieldsOnly() {
        UserOperationLogEntryEventEntity event = new UserOperationLogEntryEventEntity();
        event.setUserId("user-42");
        event.setOperationType("Complete");
        event.setEntityType("Task");
        event.setProperty("assignee");
        event.setOrgValue("old");
        event.setNewValue("new");

        Map<String, Object> fields = HistoryEventFieldExtractor.extractFields(event);

        assertThat(fields).containsEntry("userId", "user-42");
        assertThat(fields).containsEntry("operationType", "Complete");
        assertThat(fields).containsEntry("entityType", "Task");
        assertThat(fields).containsEntry("property", "assignee");
        assertThat(fields).containsEntry("orgValue", "old");
        assertThat(fields).containsEntry("newValue", "new");
    }

    @Test
    void extractFields_variableUpdate_capturesVariableFields() {
        HistoricVariableUpdateEventEntity event = new HistoricVariableUpdateEventEntity();
        event.setVariableInstanceId("var-1");
        event.setVariableName("orderStatus");
        event.setRevision(3);
        event.setTextValue("APPROVED");

        Map<String, Object> fields = HistoryEventFieldExtractor.extractFields(event);

        assertThat(fields).containsEntry("variableInstanceId", "var-1");
        assertThat(fields).containsEntry("variableName", "orderStatus");
        assertThat(fields).containsEntry("revision", 3);
        assertThat(fields).containsEntry("textValue", "APPROVED");
    }

    @Test
    void extractFields_caseInstance_capturesCaseFields() {
        HistoricCaseInstanceEventEntity event = new HistoricCaseInstanceEventEntity();
        event.setBusinessKey("case-biz-1");
        event.setCreateUserId("user-7");

        Map<String, Object> fields = HistoryEventFieldExtractor.extractFields(event);

        assertThat(fields).containsEntry("businessKey", "case-biz-1");
        assertThat(fields).containsEntry("createUserId", "user-7");
    }

    @Test
    void extractFields_identityLinkLog_capturesLinkFields() {
        HistoricIdentityLinkLogEventEntity event = new HistoricIdentityLinkLogEventEntity();
        event.setType("candidate");
        event.setUserId("user-9");
        event.setGroupId("group-1");
        event.setTaskId("task-5");
        event.setOperationType("Add");

        Map<String, Object> fields = HistoryEventFieldExtractor.extractFields(event);

        assertThat(fields).containsEntry("type", "candidate");
        assertThat(fields).containsEntry("userId", "user-9");
        assertThat(fields).containsEntry("groupId", "group-1");
        assertThat(fields).containsEntry("taskId", "task-5");
        assertThat(fields).containsEntry("operationType", "Add");
    }

    @Test
    void extractFields_jobLog_capturesJobFields() {
        HistoricJobLogEventEntity event = new HistoricJobLogEventEntity();
        event.setJobId("job-1");
        event.setJobDefinitionType("timer");
        event.setJobExceptionMessage("boom");
        event.setJobRetries(2);

        Map<String, Object> fields = HistoryEventFieldExtractor.extractFields(event);

        assertThat(fields).containsEntry("jobId", "job-1");
        assertThat(fields).containsEntry("jobDefinitionType", "timer");
        assertThat(fields).containsEntry("jobExceptionMessage", "boom");
        assertThat(fields).containsEntry("retries", 2);
    }

    @Test
    void extractFields_decisionEvaluation_withRootDecisionInstance_capturesDecisionFields() {
        HistoricDecisionInstanceEntity rootInstance = new HistoricDecisionInstanceEntity();
        rootInstance.setDecisionDefinitionId("decision-1");
        rootInstance.setDecisionDefinitionKey("myDecision");
        HistoricDecisionEvaluationEvent event = new HistoricDecisionEvaluationEvent();
        event.setRootHistoricDecisionInstance(rootInstance);

        Map<String, Object> fields = HistoryEventFieldExtractor.extractFields(event);

        assertThat(fields).containsEntry("decisionDefinitionId", "decision-1");
        assertThat(fields).containsEntry("decisionDefinitionKey", "myDecision");
    }

    @Test
    void extractFields_decisionEvaluation_withoutRootDecisionInstance_emptyFields() {
        HistoricDecisionEvaluationEvent event = new HistoricDecisionEvaluationEvent();

        Map<String, Object> fields = HistoryEventFieldExtractor.extractFields(event);

        assertThat(fields).isEmpty();
    }

    @Test
    void extractFields_batch_capturesBatchFields() {
        HistoricBatchEntity event = new HistoricBatchEntity();
        event.setId("batch-1");
        event.setType("instance-migration");
        event.setTotalJobs(42);

        Map<String, Object> fields = HistoryEventFieldExtractor.extractFields(event);

        assertThat(fields).containsEntry("batchId", "batch-1");
        assertThat(fields).containsEntry("type", "instance-migration");
        assertThat(fields).containsEntry("totalJobs", 42);
    }

    @Test
    void businessKeyOf_caseInstance_returnsBusinessKey() {
        HistoricCaseInstanceEventEntity event = new HistoricCaseInstanceEventEntity();
        event.setBusinessKey("case-biz-2");

        assertThat(HistoryEventFieldExtractor.businessKeyOf(event)).isEqualTo("case-biz-2");
    }

    @Test
    void eventTimeOf_detail_usesTimestamp() {
        HistoricDetailEventEntity event = new HistoricDetailEventEntity();
        Date timestamp = Date.from(Instant.parse("2026-04-01T00:00:00Z"));
        event.setTimestamp(timestamp);

        assertThat(HistoryEventFieldExtractor.eventTimeOf(event)).isEqualTo(timestamp.toInstant());
    }

    @Test
    void eventTimeOf_identityLinkLog_usesTime() {
        HistoricIdentityLinkLogEventEntity event = new HistoricIdentityLinkLogEventEntity();
        Date time = Date.from(Instant.parse("2026-04-02T00:00:00Z"));
        event.setTime(time);

        assertThat(HistoryEventFieldExtractor.eventTimeOf(event)).isEqualTo(time.toInstant());
    }

    @Test
    void eventTimeOf_externalTaskLog_usesTimestamp() {
        HistoricExternalTaskLogEntity event = new HistoricExternalTaskLogEntity();
        Date timestamp = Date.from(Instant.parse("2026-04-03T00:00:00Z"));
        event.setTimestamp(timestamp);

        assertThat(HistoryEventFieldExtractor.eventTimeOf(event)).isEqualTo(timestamp.toInstant());
    }

    @Test
    void eventTimeOf_jobLog_usesTimestamp() {
        HistoricJobLogEventEntity event = new HistoricJobLogEventEntity();
        Date timestamp = Date.from(Instant.parse("2026-04-04T00:00:00Z"));
        event.setTimestamp(timestamp);

        assertThat(HistoryEventFieldExtractor.eventTimeOf(event)).isEqualTo(timestamp.toInstant());
    }

    @Test
    void eventTimeOf_decisionEvaluation_withRootInstance_usesEvaluationTime() {
        HistoricDecisionInstanceEntity rootInstance = new HistoricDecisionInstanceEntity();
        Date evaluationTime = Date.from(Instant.parse("2026-04-05T00:00:00Z"));
        rootInstance.setEvaluationTime(evaluationTime);
        HistoricDecisionEvaluationEvent event = new HistoricDecisionEvaluationEvent();
        event.setRootHistoricDecisionInstance(rootInstance);

        assertThat(HistoryEventFieldExtractor.eventTimeOf(event)).isEqualTo(evaluationTime.toInstant());
    }

    @Test
    void eventTimeOf_batch_prefersEndTimeOverStartTime() {
        HistoricBatchEntity event = new HistoricBatchEntity();
        Date startTime = Date.from(Instant.parse("2026-04-06T00:00:00Z"));
        Date endTime = Date.from(Instant.parse("2026-04-07T00:00:00Z"));
        event.setStartTime(startTime);
        event.setEndTime(endTime);

        assertThat(HistoryEventFieldExtractor.eventTimeOf(event)).isEqualTo(endTime.toInstant());
    }

    @Test
    void businessKeyOf_processInstance_returnsBusinessKey() {
        HistoricProcessInstanceEventEntity event = new HistoricProcessInstanceEventEntity();
        event.setBusinessKey("biz-2");

        assertThat(HistoryEventFieldExtractor.businessKeyOf(event)).isEqualTo("biz-2");
    }

    @Test
    void businessKeyOf_classWithoutBusinessKey_returnsNull() {
        UserOperationLogEntryEventEntity event = new UserOperationLogEntryEventEntity();

        assertThat(HistoryEventFieldExtractor.businessKeyOf(event)).isNull();
    }

    @Test
    void eventTimeOf_incident_prefersEndTimeOverCreateTime() {
        HistoricIncidentEventEntity event = new HistoricIncidentEventEntity();
        Date createTime = Date.from(Instant.parse("2026-01-01T00:00:00Z"));
        Date endTime = Date.from(Instant.parse("2026-01-02T00:00:00Z"));
        event.setCreateTime(createTime);
        event.setEndTime(endTime);

        assertThat(HistoryEventFieldExtractor.eventTimeOf(event)).isEqualTo(endTime.toInstant());
    }

    @Test
    void eventTimeOf_incidentWithoutEndTime_fallsBackToCreateTime() {
        HistoricIncidentEventEntity event = new HistoricIncidentEventEntity();
        Date createTime = Date.from(Instant.parse("2026-01-01T00:00:00Z"));
        event.setCreateTime(createTime);

        assertThat(HistoryEventFieldExtractor.eventTimeOf(event)).isEqualTo(createTime.toInstant());
    }

    @Test
    void eventTimeOf_unknownType_fallsBackToNow() {
        UserOperationLogEntryEventEntity event = new UserOperationLogEntryEventEntity();
        event.setTimestamp(Date.from(Instant.parse("2026-03-03T00:00:00Z")));

        assertThat(HistoryEventFieldExtractor.eventTimeOf(event))
                .isEqualTo(Instant.parse("2026-03-03T00:00:00Z"));
    }

    @Test
    void extractLargePayload_extTaskLogWithErrorDetails_returnsBytes() {
        HistoricExternalTaskLogEntity event = new HistoricExternalTaskLogEntity() {
            @Override
            public String getErrorDetails() {
                return "stack trace details";
            }
        };

        assertThat(HistoryEventFieldExtractor.extractLargePayload(event, HistoryClassNames.EXT_TASK_LOG))
                .contains("stack trace details".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void extractLargePayload_extTaskLogWithoutErrorDetails_empty() {
        HistoricExternalTaskLogEntity event = new HistoricExternalTaskLogEntity();

        assertThat(HistoryEventFieldExtractor.extractLargePayload(event, HistoryClassNames.EXT_TASK_LOG)).isEmpty();
    }

    @Test
    void extractLargePayload_detailWithByteValue_returnsBytes() {
        HistoricVariableUpdateEventEntity event = new HistoricVariableUpdateEventEntity();
        event.setByteValue("byte-content".getBytes(StandardCharsets.UTF_8));

        assertThat(HistoryEventFieldExtractor.extractLargePayload(event, HistoryClassNames.DETAIL))
                .contains("byte-content".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void extractLargePayload_nonMatchingClass_empty() {
        HistoricProcessInstanceEventEntity event = new HistoricProcessInstanceEventEntity();

        assertThat(HistoryEventFieldExtractor.extractLargePayload(event, HistoryClassNames.PROCINST)).isEmpty();
    }
}
