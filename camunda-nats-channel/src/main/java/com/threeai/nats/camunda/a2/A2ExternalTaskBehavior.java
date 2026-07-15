package com.threeai.nats.camunda.a2;

import org.camunda.bpm.engine.impl.PriorityProvider;
import org.camunda.bpm.engine.impl.bpmn.behavior.ExternalTaskActivityBehavior;
import org.camunda.bpm.engine.impl.cfg.TransactionState;
import org.camunda.bpm.engine.impl.context.Context;
import org.camunda.bpm.engine.impl.core.variable.mapping.value.ParameterValueProvider;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExternalTaskEntity;
import org.camunda.bpm.engine.impl.pvm.delegate.ActivityExecution;

/**
 * A2 swap-in for {@link ExternalTaskActivityBehavior} (HLD §2.1, BR-A2-002/003/012, FR-A2/A3/A13,
 * US-A2/A8, ADR-0005). Creates the external task already SENTINEL-locked, in the same
 * transaction, with zero additional DB round-trips (BR-A2-002): {@code createAndInsert(...)}
 * queues an INSERT without flushing; {@code lock(...)} only sets two in-memory fields — a
 * single flush later produces one INSERT, never a follow-up UPDATE (guard-tested,
 * TEST_SPECIFICATIONS.md (a)). The post-commit publish hook runs strictly after the
 * transaction commits (COMMITTED listener, {@code TransactionContext.java:49}) — outside the
 * transaction, so JetStream publish latency can never hold the engine DB transaction open.
 */
public class A2ExternalTaskBehavior extends ExternalTaskActivityBehavior {

    private final String sentinelWorkerId;
    private final long lockDurationMillis;
    private final A2PostCommitPublisher publisher;

    public A2ExternalTaskBehavior(ParameterValueProvider topicNameProvider,
            ParameterValueProvider priorityProvider, String sentinelWorkerId,
            long lockDurationMillis, A2PostCommitPublisher publisher) {
        super(topicNameProvider, priorityProvider);
        this.sentinelWorkerId = sentinelWorkerId;
        this.lockDurationMillis = lockDurationMillis;
        this.publisher = publisher;
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

        // 3) Post-commit publish hook (TransactionContext.java:49, TransactionState.java:25 = COMMITTED).
        Context.getCommandContext().getTransactionContext()
                .addTransactionListener(TransactionState.COMMITTED, commandContext -> publisher.publish(task));
    }

    // signal(...), migrateScope(...), onParseMigratingInstance(...) are inherited unmodified from
    // ExternalTaskActivityBehavior (no override) — migration behavior is byte-identical to native.
}
