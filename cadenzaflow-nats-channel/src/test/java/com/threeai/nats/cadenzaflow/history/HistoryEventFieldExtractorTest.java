package com.threeai.nats.cadenzaflow.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

import com.threeai.nats.core.history.HistoryClassNames;
import org.cadenzaflow.bpm.engine.impl.history.event.HistoricExternalTaskLogEntity;
import org.cadenzaflow.bpm.engine.impl.history.event.HistoricIncidentEventEntity;
import org.cadenzaflow.bpm.engine.impl.history.event.HistoricProcessInstanceEventEntity;
import org.cadenzaflow.bpm.engine.impl.history.event.HistoricVariableUpdateEventEntity;
import org.cadenzaflow.bpm.engine.impl.history.event.UserOperationLogEntryEventEntity;
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
