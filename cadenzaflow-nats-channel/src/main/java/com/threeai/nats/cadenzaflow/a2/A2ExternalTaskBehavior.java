package com.threeai.nats.cadenzaflow.a2;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.cadenzaflow.bpm.engine.delegate.VariableScope;
import org.cadenzaflow.bpm.engine.impl.PriorityProvider;
import org.cadenzaflow.bpm.engine.impl.bpmn.behavior.ExternalTaskActivityBehavior;
import org.cadenzaflow.bpm.engine.impl.cfg.TransactionState;
import org.cadenzaflow.bpm.engine.impl.context.Context;
import org.cadenzaflow.bpm.engine.impl.core.variable.mapping.value.ParameterValueProvider;
import org.cadenzaflow.bpm.engine.impl.persistence.entity.ExecutionEntity;
import org.cadenzaflow.bpm.engine.impl.persistence.entity.ExternalTaskEntity;
import org.cadenzaflow.bpm.engine.impl.pvm.delegate.ActivityExecution;

/**
 * A2 swap-in for {@link ExternalTaskActivityBehavior} (HLD §2.1, BR-A2-002/003/012, FR-A2/A3/A13,
 * US-A2/A8, ADR-0005). Creates the external task already SENTINEL-locked, in the same
 * transaction, with zero additional DB round-trips (BR-A2-002): {@code createAndInsert(...)}
 * queues an INSERT without flushing; {@code lock(...)} only sets two in-memory fields — a
 * single flush later produces one INSERT, never a follow-up UPDATE (guard-tested,
 * TEST_SPECIFICATIONS.md (a)). The post-commit publish hook runs strictly after the
 * transaction commits (COMMITTED listener, {@code TransactionContext.java:49}) — outside the
 * transaction, so JetStream publish latency can never hold the engine DB transaction open.
 *
 * <p><b>In-tx variable capture (Sentinel Phase 5.5 QA fix, item 5, Levent karari 2026-07-15):</b>
 * a topic-configured {@code variableAllowlist} (default EMPTY — PII opt-in, preserves the
 * existing identity-only envelope) is captured HERE, during {@link #execute}, because this is
 * the only point where a DB/variable read is still legal — the post-commit publish path itself
 * must stay DB-query-free (BR-A2-004). The captured snapshot is handed to the post-commit
 * listener as a plain, already-resolved {@link Map} (no further engine access at publish time).
 */
public class A2ExternalTaskBehavior extends ExternalTaskActivityBehavior {

    private final String sentinelWorkerId;
    private final long lockDurationMillis;
    private final A2PostCommitPublisher publisher;
    private final List<String> variableAllowlist;

    public A2ExternalTaskBehavior(ParameterValueProvider topicNameProvider,
            ParameterValueProvider priorityProvider, String sentinelWorkerId,
            long lockDurationMillis, A2PostCommitPublisher publisher, List<String> variableAllowlist) {
        super(topicNameProvider, priorityProvider);
        this.sentinelWorkerId = sentinelWorkerId;
        this.lockDurationMillis = lockDurationMillis;
        this.publisher = publisher;
        this.variableAllowlist = variableAllowlist != null ? variableAllowlist : List.of();
    }

    @Override
    public void execute(ActivityExecution execution) throws Exception {
        ExecutionEntity executionEntity = (ExecutionEntity) execution;
        PriorityProvider<ExternalTaskActivityBehavior> priorityProvider =
                Context.getProcessEngineConfiguration().getExternalTaskPriorityProvider();
        long priority = priorityProvider.determinePriority(executionEntity, this, null);
        String topic = (String) topicNameValueProvider.getValue(executionEntity);

        // 1) Born locked — createAndInsert() queues an INSERT without flushing (ExternalTaskEntity.java:568-588).
        ExternalTaskEntity task = ExternalTaskEntity.createAndInsert(executionEntity, topic, priority);

        // 2) Same-tx, pre-flush lock — two in-memory field sets only (ExternalTaskEntity.java:471-474).
        //    Guard test (single-INSERT proof): TEST_SPECIFICATIONS.md (a).
        task.lock(sentinelWorkerId, lockDurationMillis);

        // 2b) Capture allowlisted process variables IN-TX (DB read allowed here — still inside
        // the transaction; see class Javadoc). Resolved to a plain Map now so the post-commit
        // listener below needs no further engine/DB access (BR-A2-004 unaffected).
        Map<String, Object> capturedVariables = captureAllowlistedVariables(execution, variableAllowlist);

        // 3) Post-commit publish hook (TransactionContext.java:49, TransactionState.java:25 = COMMITTED).
        Context.getCommandContext().getTransactionContext()
                .addTransactionListener(TransactionState.COMMITTED,
                        commandContext -> publisher.publish(task, capturedVariables));
    }

    /**
     * Package-private + static so it is directly unit-testable against a mocked {@link
     * VariableScope} — no CadenzaFlow static-context ({@code Context}/{@code CommandContext})
     * mocking needed. An allowlist entry naming a variable that does not exist on this scope is
     * silently omitted (not an error — different process instances of the same topic may set
     * different subsets of variables).
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

    // signal(...), migrateScope(...), onParseMigratingInstance(...) are inherited unmodified from
    // ExternalTaskActivityBehavior (no override) — migration behavior is byte-identical to native.
}
