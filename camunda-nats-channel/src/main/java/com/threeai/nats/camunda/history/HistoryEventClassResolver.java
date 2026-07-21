package com.threeai.nats.camunda.history;

import java.util.Optional;

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
import org.camunda.bpm.engine.impl.history.event.HistoryEvent;
import org.camunda.bpm.engine.impl.history.event.UserOperationLogEntryEventEntity;
import org.camunda.bpm.engine.impl.persistence.entity.HistoricJobLogEventEntity;

/**
 * Maps a fork {@link HistoryEvent} instance to one of the 15 {@code ACT_HI_*} class names this
 * basamak offloads (`DB_SCHEMA.md` §2.1). Package-private decomposition out of
 * {@code NatsHistoryEventHandler} for direct unit-testability without engine bootstrap
 * (`03_classes/1_handler_outbox.md` §1 step 2: "haritada YOKSA → VAL_HISTORY_CLASS_UNCLASSIFIED").
 *
 * <p><b>CODER-NOTE (VARINST vs DETAIL, fork-verified):</b> the fork's own
 * {@code DbHistoryEventHandler.insertHistoricVariableUpdateEntity(...)} receives exactly ONE
 * {@link HistoricVariableUpdateEventEntity} per variable change and derives TWO writes from it:
 * the append-only {@code ACT_HI_DETAIL} row (the event itself, written when the history level
 * enables detail capture) AND an in-place update of a SEPARATE {@code HistoricVariableInstanceEntity}
 * (current-value {@code ACT_HI_VARINST} row) that is synthesized internally and never itself
 * flows through the {@code HistoryEventHandler} SPI a second time. This resolver therefore
 * classifies every {@link HistoricVariableUpdateEventEntity} as {@code DETAIL} (the 1:1 match with
 * the actual wire event) — {@code VARINST} current-value projection from the DETAIL stream is a
 * documented, bounded follow-up (the {@code variable_instance_history} table and
 * {@code ProjectionStore.upsertEntity(VARINST, ...)} entry point already exist and accept it; only
 * the engine-side emission is deferred). See CODER-QUESTIONS in the phase-5 return report.
 *
 * <p><b>CODER-NOTE (COMMENT/ATTACHMENT unreachable, fork-verified):</b> {@code CommentEntity}/
 * {@code AttachmentEntity} implement {@code Event}/{@code DbEntity} — NEITHER extends/implements
 * {@link HistoryEvent} — so they are written directly by {@code CommentManager}/
 * {@code AttachmentManager} and NEVER reach {@code HistoryEventHandler.handleEvent(...)}. This
 * resolver has no branch for them (by design — there is nothing to match); the
 * {@code comment_history}/{@code attachment_history} projection tables remain schema-ready but
 * unpopulated by this basamak. Not an oversight — a genuine fork-architecture gap outside this
 * LLD's routing mechanism (composite {@code HistoryEventHandler}), documented for a future
 * increment (e.g. a {@code CommandInterceptor} around the two managers).
 */
final class HistoryEventClassResolver {

    private HistoryEventClassResolver() {
    }

    static Optional<String> resolve(HistoryEvent historyEvent) {
        if (historyEvent instanceof HistoricProcessInstanceEventEntity) {
            return Optional.of(HistoryClassNames.PROCINST);
        }
        if (historyEvent instanceof HistoricActivityInstanceEventEntity) {
            return Optional.of(HistoryClassNames.ACTINST);
        }
        if (historyEvent instanceof HistoricTaskInstanceEventEntity) {
            return Optional.of(HistoryClassNames.TASKINST);
        }
        if (historyEvent instanceof HistoricVariableUpdateEventEntity) {
            return Optional.of(HistoryClassNames.DETAIL); // see class Javadoc CODER-NOTE
        }
        if (historyEvent instanceof HistoricIncidentEventEntity) {
            return Optional.of(HistoryClassNames.INCIDENT);
        }
        if (historyEvent instanceof HistoricCaseInstanceEventEntity) {
            return Optional.of(HistoryClassNames.CASEINST);
        }
        if (historyEvent instanceof HistoricIdentityLinkLogEventEntity) {
            return Optional.of(HistoryClassNames.IDENTITYLINK);
        }
        if (historyEvent instanceof UserOperationLogEntryEventEntity) {
            return Optional.of(HistoryClassNames.OP_LOG);
        }
        if (historyEvent instanceof HistoricExternalTaskLogEntity) {
            return Optional.of(HistoryClassNames.EXT_TASK_LOG);
        }
        if (historyEvent instanceof HistoricJobLogEventEntity) {
            return Optional.of(HistoryClassNames.JOB_LOG);
        }
        if (historyEvent instanceof HistoricDecisionEvaluationEvent) {
            return Optional.of(HistoryClassNames.DECINST);
        }
        if (historyEvent instanceof HistoricBatchEntity) {
            return Optional.of(HistoryClassNames.BATCH);
        }
        // e.g. HistoricCaseActivityInstanceEventEntity, HistoricFormPropertyEventEntity — real
        // engine events this basamak does not (yet) offload; VAL_HISTORY_CLASS_UNCLASSIFIED.
        return Optional.empty();
    }
}
