package com.threeai.nats.flowable.externalworker;

import java.util.Date;
import java.util.Map;

import org.flowable.common.engine.impl.cfg.TransactionState;
import org.flowable.common.engine.impl.context.Context;
import org.flowable.engine.impl.util.CommandContextUtil;
import org.flowable.engine.interceptor.CreateExternalWorkerJobAfterContext;
import org.flowable.engine.interceptor.CreateExternalWorkerJobBeforeContext;
import org.flowable.engine.interceptor.CreateExternalWorkerJobInterceptor;
import org.flowable.job.service.InternalJobManager;
import org.flowable.job.service.impl.persistence.entity.ExternalWorkerJobEntity;

/**
 * The dispatch seam (docs/11 D-A'v3): rides Flowable's official
 * {@link CreateExternalWorkerJobInterceptor} hook — no behavior subclass, no parse-time swap,
 * topic Expressions already resolved by the stock behavior before this hook fires.
 *
 * <p><b>Born-locked:</b> in {@link #afterCreateExternalWorkerJob} (same command context, before
 * flush — single INSERT, characterization-probe-proven) the job gets
 * {@code lockOwner = <sentinelWorkerId>#<nonce>} and {@code lockExpirationTime = now + L}, so
 * no classic poller can acquire it during the delivery window L.
 *
 * <p><b>In-tx capture, post-commit publish:</b> businessKey (from the execution — the one field
 * the sweep path cannot recover) and the variables (via the ENGINE's own
 * {@link InternalJobManager#resolveVariablesForExternalWorkerJob} three-branch semantics —
 * inParameters / doNotIncludeVariables / all, decision D-J'v2+G5, no copied logic) are read
 * inside the transaction; the publish happens on {@link TransactionState#COMMITTED}, so a
 * rollback publishes nothing (gate G2).
 *
 * <p><b>Delegate-wrap (N-yellow-10b):</b> a host-application interceptor installed before this
 * one is never silently overwritten — it is invoked first on both hooks.
 */
public class EwCreateJobInterceptor implements CreateExternalWorkerJobInterceptor {

    private final EwTopicConfig topicConfig;
    private final EwLockConfig lockConfig;
    private final String sentinelWorkerId;
    private final EwPostCommitPublisher publisher;
    private final CreateExternalWorkerJobInterceptor delegate;

    public EwCreateJobInterceptor(EwTopicConfig topicConfig, EwLockConfig lockConfig,
            String sentinelWorkerId, EwPostCommitPublisher publisher,
            CreateExternalWorkerJobInterceptor delegate) {
        this.topicConfig = topicConfig;
        this.lockConfig = lockConfig;
        this.sentinelWorkerId = sentinelWorkerId;
        this.publisher = publisher;
        this.delegate = delegate;
    }

    @Override
    public void beforeCreateExternalWorkerJob(CreateExternalWorkerJobBeforeContext beforeContext) {
        if (delegate != null) {
            delegate.beforeCreateExternalWorkerJob(beforeContext);
        }
    }

    @Override
    public void afterCreateExternalWorkerJob(CreateExternalWorkerJobAfterContext context) {
        if (delegate != null) {
            delegate.afterCreateExternalWorkerJob(context);
        }
        ExternalWorkerJobEntity job = context.getExternalWorkerJobEntity();
        // Stock behavior stores the RESOLVED topic in jobHandlerConfiguration (7.1.0 bytecode).
        String topic = job.getJobHandlerConfiguration();
        if (topic == null || !topicConfig.isDispatchTopic(topic)) {
            return;
        }

        String nonce = EwHeaders.mintNonce();
        Date now = CommandContextUtil.getProcessEngineConfiguration().getClock().getCurrentTime();
        job.setLockOwner(sentinelWorkerId + "#" + nonce);
        job.setLockExpirationTime(new Date(now.getTime() + lockConfig.lockDurationMillis()));

        String businessKey = context.getExecution().getProcessInstanceBusinessKey();
        InternalJobManager internalJobManager = CommandContextUtil.getProcessEngineConfiguration()
                .getJobServiceConfiguration().getInternalJobManager();
        Map<String, Object> variables = internalJobManager != null
                ? internalJobManager.resolveVariablesForExternalWorkerJob(job)
                : Map.of();

        Context.getTransactionContext().addTransactionListener(TransactionState.COMMITTED,
                commandContext -> publisher.publish(job, topic, nonce, businessKey, variables));
    }
}
