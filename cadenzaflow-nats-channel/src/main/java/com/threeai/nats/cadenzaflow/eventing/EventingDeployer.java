package com.threeai.nats.cadenzaflow.eventing;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Consumer;

import org.cadenzaflow.bpm.engine.impl.cfg.TransactionState;
import org.cadenzaflow.bpm.engine.impl.context.Context;
import org.cadenzaflow.bpm.engine.impl.persistence.deploy.Deployer;
import org.cadenzaflow.bpm.engine.impl.persistence.entity.DeploymentEntity;
import org.cadenzaflow.bpm.engine.impl.persistence.entity.ResourceEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The deploy-moment TRIGGER of the definitions layer (docs/12 D-B v2) — deliberately NOT the
 * registration authority (that is {@link EventingReconciler}).
 *
 * <p>Runs inside the deployment transaction under the engine's global exclusive deployment
 * lock, therefore does exactly two things and NOTHING broker-shaped:
 * <ol>
 *   <li><b>Parse + validate</b> every {@code .event}/{@code .channel} resource in the
 *       deployment — an invalid definition throws {@link EventingDefinitionException} and fails
 *       the deployment BEFORE any broker contact (gate G1).</li>
 *   <li><b>Nudge post-commit</b> — registers a {@code COMMITTED} transaction listener that
 *       pokes the reconciler (the nudge coalesces there, F-2). Nothing on rollback.</li>
 * </ol>
 *
 * <p>Re-invocation rules (the engine calls deployers again on cache-miss re-resolves with a
 * RESTRICTED resource set): absence of eventing resources is never an error and never an
 * unregister — "my resource is absent from this call" never means "unregister" (review R-1 rule); this deployer never
 * unregisters anything at all.
 */
public class EventingDeployer implements Deployer {

    private static final Logger log = LoggerFactory.getLogger(EventingDeployer.class);
    private static final String EVENT_SUFFIX = ".event";
    private static final String CHANNEL_SUFFIX = ".channel";

    private final Runnable reconcilerNudge;
    private final Consumer<Runnable> postCommitHook;

    public EventingDeployer(Runnable reconcilerNudge) {
        this(reconcilerNudge, EventingDeployer::registerCommittedListener);
    }

    /** Test seam: the hook receives the action to run on COMMITTED. */
    EventingDeployer(Runnable reconcilerNudge, Consumer<Runnable> postCommitHook) {
        this.reconcilerNudge = reconcilerNudge;
        this.postCommitHook = postCommitHook;
    }

    @Override
    public void deploy(DeploymentEntity deployment) {
        Map<String, ResourceEntity> resources = deployment.getResources();
        if (resources == null || resources.isEmpty()) {
            return;
        }
        boolean sawEventingResource = false;
        for (Map.Entry<String, ResourceEntity> entry : resources.entrySet()) {
            String name = entry.getKey();
            if (name.endsWith(EVENT_SUFFIX)) {
                sawEventingResource = true;
                EventingDefinitionParser.parseEvent(name, text(entry.getValue()));
            } else if (name.endsWith(CHANNEL_SUFFIX)) {
                sawEventingResource = true;
                EventingDefinitionParser.parseChannel(name, text(entry.getValue()));
            }
        }
        if (sawEventingResource) {
            log.info("Eventing definitions validated in deployment — reconciler will register"
                    + " post-commit", kv("deployment_id", deployment.getId()));
            postCommitHook.accept(reconcilerNudge);
        }
    }

    private static void registerCommittedListener(Runnable nudge) {
        Context.getCommandContext().getTransactionContext()
                .addTransactionListener(TransactionState.COMMITTED, commandContext -> nudge.run());
    }

    private static String text(ResourceEntity resource) {
        return new String(resource.getBytes(), StandardCharsets.UTF_8);
    }
}
