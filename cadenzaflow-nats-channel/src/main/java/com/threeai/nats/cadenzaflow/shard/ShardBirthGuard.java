package com.threeai.nats.cadenzaflow.shard;

import static net.logstash.logback.argument.StructuredArguments.kv;

import com.threeai.nats.core.shard.ShardTopology;

import org.cadenzaflow.bpm.engine.ProcessEngineException;
import org.cadenzaflow.bpm.engine.delegate.DelegateExecution;
import org.cadenzaflow.bpm.engine.delegate.ExecutionListener;
import org.cadenzaflow.bpm.engine.impl.pvm.delegate.ActivityExecution;
import org.cadenzaflow.bpm.engine.impl.pvm.process.ActivityImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The birth guard (docs/13 D-B v2/§2.5): fires as a process-scope START execution listener
 * on EVERY instance birth. Scope rules, each bytecode-verified in the design reviews:
 *
 * <ul>
 *   <li><b>Root only</b> — a call-activity child ({@code getSuperExecution() != null}) is
 *       exempt: its co-location comes from being born inside the owning shard's transaction,
 *       and its businessKey is structurally null unless {@code camunda:businessKey} is set.</li>
 *   <li><b>Signal-start = shard-local class</b> (locked 2026-08-24) — the engine's signal
 *       API has no businessKey field at all; such instances live on the shard that processed
 *       the signal, work jobs/replies normally, and cannot be targeted by key-routed
 *       correlation (USER_GUIDE limitation). Discriminator (F-5): the EXECUTION'S OWN start
 *       activity's {@code type} property — never the definition's {@code initial} (that
 *       points at a none/timer start and misclassifies mixed-start definitions).</li>
 *   <li>Root API/message/conditional start: null businessKey → {@code
 *       VAL_SHARD_BUSINESS_KEY_REQUIRED}; key hashing to a foreign shard → {@code
 *       VAL_SHARD_WRONG_SHARD} (the LB rule in the runbook prevents this in practice).
 *       The thrown exception propagates → the start transaction rolls back.</li>
 * </ul>
 */
public class ShardBirthGuard implements ExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(ShardBirthGuard.class);
    private static final String SIGNAL_START_TYPE = "signalStartEvent";

    private final ShardTopology topology;

    public ShardBirthGuard(ShardTopology topology) {
        this.topology = topology;
    }

    @Override
    public void notify(DelegateExecution execution) {
        if (execution.getSuperExecution() != null) {
            return; // call-activity child: co-located by transaction, exempt
        }
        if (isSignalStart(execution)) {
            log.debug("Signal-start instance born shard-local (null businessKey by engine"
                    + " design) — exempt from the birth guard",
                    kv("process_instance_id", execution.getProcessInstanceId()),
                    kv("shard", topology.getShardId()));
            return;
        }
        String businessKey = execution.getProcessBusinessKey();
        if (businessKey == null || businessKey.isBlank()) {
            throw new ProcessEngineException("[VAL_SHARD_BUSINESS_KEY_REQUIRED] sharding is"
                    + " enabled on this engine: a root process instance must be started with a"
                    + " businessKey (the shard key). Start rejected, transaction rolled back.");
        }
        int owner = topology.shardOf(businessKey);
        if (owner != topology.getShardId()) {
            throw new ProcessEngineException("[VAL_SHARD_WRONG_SHARD] businessKey hashes to"
                    + " shard " + owner + " but this engine is shard " + topology.getShardId()
                    + " — route the start to the owning shard (see the runbook's LB rule)."
                    + " Start rejected, transaction rolled back.");
        }
    }

    private boolean isSignalStart(DelegateExecution execution) {
        if (execution instanceof ActivityExecution activityExecution) {
            var activity = activityExecution.getActivity();
            if (activity instanceof ActivityImpl activityImpl) {
                return SIGNAL_START_TYPE.equals(activityImpl.getProperty("type"));
            }
        }
        return false;
    }
}
