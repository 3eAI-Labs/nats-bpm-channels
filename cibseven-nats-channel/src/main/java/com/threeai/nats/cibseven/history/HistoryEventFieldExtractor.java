package com.threeai.nats.cibseven.history;

import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import com.threeai.nats.core.history.HistoryClassNames;
import org.cibseven.bpm.engine.impl.batch.history.HistoricBatchEntity;
import org.cibseven.bpm.engine.impl.history.event.HistoricActivityInstanceEventEntity;
import org.cibseven.bpm.engine.impl.history.event.HistoricCaseInstanceEventEntity;
import org.cibseven.bpm.engine.impl.history.event.HistoricDecisionEvaluationEvent;
import org.cibseven.bpm.engine.impl.history.event.HistoricDetailEventEntity;
import org.cibseven.bpm.engine.impl.history.event.HistoricExternalTaskLogEntity;
import org.cibseven.bpm.engine.impl.history.event.HistoricIdentityLinkLogEventEntity;
import org.cibseven.bpm.engine.impl.history.event.HistoricIncidentEventEntity;
import org.cibseven.bpm.engine.impl.history.event.HistoricProcessInstanceEventEntity;
import org.cibseven.bpm.engine.impl.history.event.HistoricScopeInstanceEvent;
import org.cibseven.bpm.engine.impl.history.event.HistoricTaskInstanceEventEntity;
import org.cibseven.bpm.engine.impl.history.event.HistoricVariableUpdateEventEntity;
import org.cibseven.bpm.engine.impl.history.event.HistoryEvent;
import org.cibseven.bpm.engine.impl.history.event.UserOperationLogEntryEventEntity;
import org.cibseven.bpm.engine.impl.persistence.entity.HistoricJobLogEventEntity;

/**
 * Extracts a class-specific scalar field map (`Map&lt;String,Object&gt;`) out of a concrete
 * {@link HistoryEvent} instance — shared by {@link CompactHistoryOutboxWriter} (audit-critical
 * {@code payload_scalar} JSONB, `03_classes/1_handler_outbox.md` §2) and
 * {@link HistoryPostCommitPublisher} (bulk wire payload, §3). Also resolves the two fields
 * {@code HistoryEvent} does not expose uniformly across its subtypes: event timestamp and
 * business key.
 *
 * <p><b>CODER-NOTE (single shared field set, not two separate "bounded vs full" extractors):</b>
 * {@code DB_SCHEMA.md §1}'s comment lists 11 named bounded fields for {@code payload_scalar}
 * ("userId, operationType, entityType, property, orgValue, newValue, incidentMessage,
 * configuration, activityId, workerId, errorMessage"). This extractor returns that same field set
 * (plus a small number of equally bounded scalars for the OTHER 12 classes, reused by the bulk
 * wire path) rather than maintaining two parallel per-class field lists — every field here is a
 * short scalar (never a byte-array/blob; those are handled separately, see
 * {@link CompactHistoryOutboxWriter}'s large-payload branch), so NFR-P2's guarantee (row COUNT,
 * not field count) is unaffected by the small superset.
 */
final class HistoryEventFieldExtractor {

    private HistoryEventFieldExtractor() {
    }

    static Map<String, Object> extractFields(HistoryEvent event) {
        Map<String, Object> fields = new LinkedHashMap<>();
        if (event instanceof HistoricProcessInstanceEventEntity e) {
            fields.put("processDefinitionKey", e.getProcessDefinitionKey());
            fields.put("processDefinitionId", e.getProcessDefinitionId());
            fields.put("businessKey", e.getBusinessKey());
            fields.put("startUserId", e.getStartUserId());
            fields.put("superProcessInstanceId", e.getSuperProcessInstanceId());
            fields.put("deleteReason", e.getDeleteReason());
            fields.put("state", e.getState());
        } else if (event instanceof HistoricActivityInstanceEventEntity e) {
            fields.put("activityId", e.getActivityId());
            fields.put("activityName", e.getActivityName());
            fields.put("activityType", e.getActivityType());
            fields.put("taskAssignee", e.getTaskAssignee());
        } else if (event instanceof HistoricTaskInstanceEventEntity e) {
            fields.put("taskId", e.getTaskId());
            fields.put("name", e.getName());
            fields.put("assignee", e.getAssignee());
            fields.put("owner", e.getOwner());
            fields.put("deleteReason", e.getDeleteReason());
            fields.put("priority", e.getPriority());
        } else if (event instanceof HistoricVariableUpdateEventEntity e) {
            fields.put("variableInstanceId", e.getVariableInstanceId());
            fields.put("variableName", e.getVariableName());
            fields.put("variableType", e.getSerializerName());
            fields.put("revision", e.getRevision());
            fields.put("textValue", e.getTextValue());
        } else if (event instanceof HistoricIncidentEventEntity e) {
            fields.put("incidentType", e.getIncidentType());
            fields.put("incidentMessage", e.getIncidentMessage());
            fields.put("configuration", e.getConfiguration());
            fields.put("activityId", e.getActivityId());
        } else if (event instanceof HistoricCaseInstanceEventEntity e) {
            fields.put("businessKey", e.getBusinessKey());
            fields.put("createUserId", e.getCreateUserId());
            fields.put("state", e.getState());
        } else if (event instanceof HistoricIdentityLinkLogEventEntity e) {
            fields.put("type", e.getType());
            fields.put("userId", e.getUserId());
            fields.put("groupId", e.getGroupId());
            fields.put("taskId", e.getTaskId());
            fields.put("operationType", e.getOperationType());
        } else if (event instanceof UserOperationLogEntryEventEntity e) {
            fields.put("operationId", e.getOperationId());
            fields.put("userId", e.getUserId());
            fields.put("operationType", e.getOperationType());
            fields.put("entityType", e.getEntityType());
            fields.put("property", e.getProperty());
            fields.put("orgValue", e.getOrgValue());
            fields.put("newValue", e.getNewValue());
        } else if (event instanceof HistoricExternalTaskLogEntity e) {
            fields.put("externalTaskId", e.getExternalTaskId());
            fields.put("workerId", e.getWorkerId());
            fields.put("topicName", e.getTopicName());
            fields.put("activityId", e.getActivityId());
            fields.put("errorMessage", e.getErrorMessage());
        } else if (event instanceof HistoricJobLogEventEntity e) {
            fields.put("jobId", e.getJobId());
            fields.put("jobDefinitionType", e.getJobDefinitionType());
            fields.put("jobExceptionMessage", e.getJobExceptionMessage());
            fields.put("retries", e.getJobRetries());
        } else if (event instanceof HistoricDecisionEvaluationEvent e) {
            if (e.getRootHistoricDecisionInstance() != null) {
                fields.put("decisionDefinitionId", e.getRootHistoricDecisionInstance().getDecisionDefinitionId());
                fields.put("decisionDefinitionKey", e.getRootHistoricDecisionInstance().getDecisionDefinitionKey());
            }
        } else if (event instanceof HistoricBatchEntity e) {
            fields.put("batchId", e.getId());
            fields.put("type", e.getType());
            fields.put("totalJobs", e.getTotalJobs());
        }
        return fields;
    }

    /**
     * The only two fork event/class combinations with a known byte-array-backed field
     * (`03_classes/1_handler_outbox.md` §2 "ARCH-Q1 nadir büyük-payload"; DETAIL byte-array
     * variable values are the general case the same reference pattern applies to, beyond the
     * LLD's EXT_TASK_LOG example). Used by BOTH the audit-critical outbox writer (companion row)
     * and the bulk post-commit publisher (inline base64, no outbox row exists for bulk).
     */
    static Optional<byte[]> extractLargePayload(HistoryEvent event, String historyClass) {
        if (HistoryClassNames.EXT_TASK_LOG.equals(historyClass) && event instanceof HistoricExternalTaskLogEntity e) {
            String errorDetails = e.getErrorDetails();
            if (errorDetails != null && !errorDetails.isEmpty()) {
                return Optional.of(errorDetails.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
        }
        if (HistoryClassNames.DETAIL.equals(historyClass) && event instanceof HistoricVariableUpdateEventEntity e) {
            byte[] byteValue = e.getByteValue();
            if (byteValue != null && byteValue.length > 0) {
                return Optional.of(byteValue);
            }
        }
        return Optional.empty();
    }

    static String businessKeyOf(HistoryEvent event) {
        if (event instanceof HistoricProcessInstanceEventEntity e) {
            return e.getBusinessKey();
        }
        if (event instanceof HistoricCaseInstanceEventEntity e) {
            return e.getBusinessKey();
        }
        return null;
    }

    static Instant eventTimeOf(HistoryEvent event) {
        Date resolved = null;
        if (event instanceof HistoricScopeInstanceEvent e) {
            resolved = e.getEndTime() != null ? e.getEndTime() : e.getStartTime();
        } else if (event instanceof HistoricDetailEventEntity e) {
            resolved = e.getTimestamp();
        } else if (event instanceof HistoricIncidentEventEntity e) {
            resolved = e.getEndTime() != null ? e.getEndTime() : e.getCreateTime();
        } else if (event instanceof HistoricIdentityLinkLogEventEntity e) {
            resolved = e.getTime();
        } else if (event instanceof UserOperationLogEntryEventEntity e) {
            resolved = e.getTimestamp();
        } else if (event instanceof HistoricExternalTaskLogEntity e) {
            resolved = e.getTimestamp();
        } else if (event instanceof HistoricJobLogEventEntity e) {
            resolved = e.getTimestamp();
        } else if (event instanceof HistoricDecisionEvaluationEvent e && e.getRootHistoricDecisionInstance() != null) {
            resolved = e.getRootHistoricDecisionInstance().getEvaluationTime();
        } else if (event instanceof HistoricBatchEntity e) {
            resolved = e.getEndTime() != null ? e.getEndTime() : e.getStartTime();
        }
        return resolved != null ? resolved.toInstant() : Instant.now();
    }
}
