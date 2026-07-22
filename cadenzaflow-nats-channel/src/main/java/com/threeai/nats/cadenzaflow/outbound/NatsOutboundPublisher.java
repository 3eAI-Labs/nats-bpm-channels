package com.threeai.nats.cadenzaflow.outbound;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.threeai.nats.core.outbound.OutboundClassification;
import com.threeai.nats.core.outbound.OutboundClassificationProperties;
import com.threeai.nats.core.outbound.OutboundMessageDraft;
import com.threeai.nats.core.outbound.OutboundMessageOutboxWriter;
import com.threeai.nats.core.outbound.OutboundPostCommitPublisher;
import com.threeai.nats.core.outbound.OutboundSubjectBuilder;
import com.threeai.nats.core.outbound.OutboundWireMessageFactory;
import org.cadenzaflow.bpm.engine.delegate.DelegateExecution;
import org.cadenzaflow.bpm.engine.delegate.ExecutionListener;
import org.cadenzaflow.bpm.engine.delegate.VariableScope;
import org.cadenzaflow.bpm.engine.impl.cfg.TransactionState;
import org.cadenzaflow.bpm.engine.impl.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * D-A' outbound-handoff dikişi: the transactional-safe successor of the deleted {@code
 * NatsPublishDelegate}. Tenant BPMN attaches this bean via {@code camunda:executionListener
 * event="end" delegateExpression="${natsOutboundPublisher}"} on a message-throw event or send-task
 * (D-B' scope). {@code notify()} itself NEVER publishes — it classifies (D-C') and either writes a
 * tx-in {@code outbound_message_outbox} row (critical, at-least-once) or registers a post-commit
 * {@code TransactionListener} (best-effort, at-most-once).
 *
 * <p><b>{@code outboxWriter} nullability:</b> the auto-config gates {@code OutboundMessageOutboxWriter}
 * behind the SAME engine-{@link javax.sql.DataSource} bean presence as {@code OutboundMessageRelay}
 * (kept in lock-step deliberately — a writer without a relay would accumulate rows forever, an
 * operational hazard). A DataSource-less deployment can still use this listener for BEST_EFFORT
 * types; dispatching a CRITICAL-classified type without a configured writer fails loudly (see
 * {@link #notify}) rather than silently downgrading the at-least-once contract.
 *
 * <p><b>ADIM-0 verified (phase-5 return report):</b> {@code ExecutionListener.notify(...)} runs
 * synchronously inside the SAME {@code CommandContext} the enclosing engine command already
 * opened — {@code CommandContextInterceptor.execute()} sets {@code Context.setCommandContext(...)}
 * BEFORE {@code next.execute(command)}, and every atomic operation (including listener invocation)
 * runs within that same call frame, never leaving it until the command completes. {@code
 * Context.getCommandContext()} is therefore guaranteed non-null here, exactly like the sibling
 * {@code A2ExternalTaskBehavior}/{@code NatsHistoryEventHandler} call sites.
 */
public class NatsOutboundPublisher implements ExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(NatsOutboundPublisher.class);

    private final OutboundClassificationProperties classification;
    private final OutboundMessageOutboxWriter outboxWriter;
    private final OutboundPostCommitPublisher postCommitPublisher;
    private final String engineId;

    public NatsOutboundPublisher(OutboundClassificationProperties classification,
            OutboundMessageOutboxWriter outboxWriter, OutboundPostCommitPublisher postCommitPublisher,
            String engineId) {
        this.classification = classification;
        this.outboxWriter = outboxWriter;
        this.postCommitPublisher = postCommitPublisher;
        this.engineId = engineId;
    }

    @Override
    public void notify(DelegateExecution execution) throws Exception {
        Optional<String> messageType = OutboundMessageTypeResolver.resolve(execution);
        if (messageType.isEmpty()) {
            // VAL_OUTBOUND_UNSUPPORTED_ELEMENT-equivalent -- fail-safe skip, not an error (D-B'
            // scope guard; a misattached listener should not break process execution).
            log.warn("Outbound-handoff listener attached to an unsupported BPMN element — publish skipped",
                    kv("activity_id", execution.getCurrentActivityId()),
                    kv("process_instance_id", execution.getProcessInstanceId()));
            return;
        }
        String type = messageType.get();
        String processInstanceId = execution.getProcessInstanceId();
        String businessKey = execution.getProcessBusinessKey();
        String traceId = UUID.randomUUID().toString();
        String subject = OutboundSubjectBuilder.build(engineId, type, processInstanceId);

        Map<String, Object> variables = captureAllowlistedVariables(execution, classification.variableAllowlistFor(type));
        byte[] payload = OutboundWireMessageFactory.buildPayload(engineId, type, processInstanceId, businessKey, variables);
        OutboundMessageDraft draft = new OutboundMessageDraft(engineId, type, processInstanceId, businessKey,
                traceId, subject, payload);

        OutboundClassification outcome = classification.classify(type);
        if (outcome == OutboundClassification.CRITICAL) {
            if (outboxWriter == null) {
                // SYS_OUTBOUND_OUTBOX_NOT_CONFIGURED-equivalent -- a message type was marked
                // critical (spring.nats.outbound.critical-types) but this deployment has no engine
                // DataSource bean wired (outboxWriter/relay are gated together, see auto-config
                // CODER-NOTE), so the critical/at-least-once contract cannot be honored. Fail loud
                // and roll back the transaction rather than silently downgrading to best-effort.
                throw new IllegalStateException("Outbound message type '" + type
                        + "' is classified CRITICAL but no OutboundMessageOutboxWriter is configured "
                        + "(missing engine DataSource bean) — cannot honor the at-least-once contract");
            }
            // Tx-in write -- SQL failure propagates and rolls back the runtime transaction together
            // with the engine's own write (D-A'/D-C' critical-path atomicity guarantee).
            outboxWriter.write(currentTransactionConnection(), draft);
        } else {
            // Post-commit publish hook (TransactionContext.java:49, TransactionState.java:25 =
            // COMMITTED) -- basamak-1/2 pattern's 3rd use.
            Context.getCommandContext().getTransactionContext()
                    .addTransactionListener(TransactionState.COMMITTED,
                            commandContext -> postCommitPublisher.publish(draft));
        }
    }

    /**
     * Package-private + static so it is directly unit-testable against a mocked {@link
     * VariableScope} — same rationale as {@code A2ExternalTaskBehavior#captureAllowlistedVariables}.
     * An allowlisted variable that does not exist on this scope is silently omitted.
     */
    static Map<String, Object> captureAllowlistedVariables(VariableScope scope, List<String> variableAllowlist) {
        if (variableAllowlist == null || variableAllowlist.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> captured = new LinkedHashMap<>();
        for (String variableName : variableAllowlist) {
            if (scope.hasVariable(variableName)) {
                captured.put(variableName, scope.getVariable(variableName));
            }
        }
        return captured;
    }

    /**
     * MyBatis' {@code SqlSession} wraps the engine transaction's live JDBC {@link Connection} —
     * same pattern {@code NatsHistoryEventHandler#currentTransactionConnection} already
     * established (verified against the compiled 7.24.0 engine source).
     */
    private Connection currentTransactionConnection() {
        try {
            return Context.getCommandContext().getDbSqlSession().getSqlSession().getConnection();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to obtain the current engine transaction's JDBC connection for outbound-outbox write", e);
        }
    }
}
