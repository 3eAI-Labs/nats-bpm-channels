package com.threeai.nats.cadenzaflow.variable;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.threeai.nats.core.largepayload.ContentAddressedLargePayloadStore;
import com.threeai.nats.core.largepayload.ExternalizationMarker;
import com.threeai.nats.core.largepayload.LargePayloadReference;
import com.threeai.nats.core.largepayload.LargeVariableExternalizationProperties;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import org.cadenzaflow.bpm.engine.OptimisticLockingException;
import org.cadenzaflow.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.cadenzaflow.bpm.engine.impl.cfg.TransactionState;
import org.cadenzaflow.bpm.engine.impl.context.Context;
import org.cadenzaflow.bpm.engine.impl.persistence.entity.VariableInstanceEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deferred/post-commit externalization worker (`docs/08-large-variable-externalization.md`
 * D-A'/D-B'/D-D'/D-F'). Two entry points share the SAME idempotent, race-safe core:
 * {@link #scheduleExternalization} (the fast path — a {@code TransactionState.COMMITTED}
 * listener registered by {@link LargeVariableSerializer#writeValue} while still inside the
 * ORIGINAL transaction) and {@link #externalizeNow} (also called directly by {@code
 * LargeVariableExternalizationSweep} for the slow/catch-all path).
 *
 * <p><b>CODER-NOTE (background executor, not a same-thread post-commit call — beyond basamak-2's
 * {@code HistoryPostCommitPublisher} precedent):</b> {@code HistoryPostCommitPublisher} runs its
 * (single NATS publish) work SYNCHRONOUSLY on the calling thread from within the COMMITTED
 * listener. This class instead only REGISTERS the listener synchronously (cheap, in-memory) and
 * dispatches the actual work — a store round-trip PLUS a full second engine command/commit — to a
 * background executor from within that listener, so the ORIGINAL caller's thread is never blocked
 * by any of it. `docs/08` §5 explicitly grants basamak-3 more flexibility than basamak-2 here
 * ("basamak-2'den daha esnek bir 'yumuşak-geçiş' imkânı verir") — externalization failure only
 * means the value stays in {@code ACT_GE_BYTEARRAY} exactly as it does today (no audit-completeness
 * concern the way a lost bulk HISTORY event has), so there is no correctness reason to accept the
 * EXTRA latency (a store write + a second commit, vs. one NATS publish) on the caller's thread.
 *
 * <p><b>Race safety:</b> {@link #externalizeNow} re-reads the variable's CURRENT byte content at
 * execution time (never trusts a value captured earlier) and mutates it through the engine's own
 * {@code DbEntityManager}-backed OCC (loads {@link VariableInstanceEntity} via {@code selectById}
 * inside a command, calls its plain {@code ValueFields} setters — the SAME command-context/flush
 * machinery every other command uses) rather than raw SQL against the fork's tables. A concurrent
 * write to the SAME variable (a real user overwrite, another externalization attempt, or the
 * sweep racing the fast path) is resolved by the engine's own {@link OptimisticLockingException} —
 * caught here as a benign, expected "someone already changed it" outcome, never retried
 * synchronously (the sweep will reconsider it, if still eligible, next cycle).
 *
 * <p><b>CODER-NOTE (late-bound {@code ProcessEngineConfigurationImpl}, not a constructor-injected
 * {@code ProcessEngine} bean):</b> this class must be handed to {@link LargeVariableSerializer}
 * instances that {@code CadenzaFlowNatsAutoConfiguration}'s {@code ProcessEnginePlugin.preInit(...)}
 * registers into {@code customPreVariableSerializers} — which runs BEFORE {@code
 * ProcessEngineConfigurationImpl.buildProcessEngine()} returns, i.e. before any {@code
 * ProcessEngine} Spring bean can possibly exist yet (a genuine bootstrap-ordering constraint, not a
 * design preference). {@link #bindConfiguration} is called from that SAME {@code preInit(...)} —
 * which DOES receive a live {@code ProcessEngineConfigurationImpl} reference — so by the time this
 * class's own methods ever actually run (post-commit of a real variable write, or the leader-elected
 * sweep — both necessarily AFTER the engine has fully booted), {@code
 * configuration.getCommandExecutorTxRequired()} on that SAME captured object correctly reflects the
 * fully-initialized engine (object identity is preserved across {@code init()}; only its fields
 * mutate over bootstrap time).
 */
public class LargeVariablePostCommitExternalizer {

    private static final Logger log = LoggerFactory.getLogger(LargeVariablePostCommitExternalizer.class);

    private final ContentAddressedLargePayloadStore largePayloadStore;
    private final LargeVariableExternalizationProperties properties;
    private final NatsChannelMetrics metrics;
    private final String engineId;
    private final ExecutorService executor;
    private volatile ProcessEngineConfigurationImpl configuration;

    public LargeVariablePostCommitExternalizer(ContentAddressedLargePayloadStore largePayloadStore,
            LargeVariableExternalizationProperties properties, NatsChannelMetrics metrics, String engineId) {
        this.largePayloadStore = largePayloadStore;
        this.properties = properties;
        this.metrics = metrics;
        this.engineId = engineId;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    /** Called exactly once, from {@code ProcessEnginePlugin.preInit(...)} — see class Javadoc. */
    public void bindConfiguration(ProcessEngineConfigurationImpl configuration) {
        this.configuration = configuration;
    }

    /**
     * Called from {@link LargeVariableSerializer#writeValue} — MUST run inside the still-open
     * (uncommitted) command that just staged the over-threshold value, so the listener fires only
     * if that write actually commits (a rolled-back write must never trigger externalization of
     * content that was never durably staged).
     *
     * <p><b>CODER-NOTE (deferred {@code getId()} read, production bug found via this class's own
     * E2E test):</b> for a BRAND NEW variable, {@code VariableInstanceEntity.getId()} is still
     * {@code null} at {@code writeValue()} time — the fork's ID generator assigns it only during
     * the command's {@code DbEntityManager} flush/insert, which has not happened yet while the
     * serializer is still populating the entity's {@code ValueFields}. Accepting the LIVE entity
     * reference (not a pre-extracted id string) and reading {@code getId()} lazily INSIDE the
     * {@code COMMITTED} listener — which by definition only fires after that same flush has
     * durably inserted the row with its real, non-null primary key — is what makes this correct
     * for both brand-new variables and in-place updates of an existing one.
     */
    public void scheduleExternalization(VariableInstanceEntity variableInstance) {
        Context.getCommandContext().getTransactionContext()
                .addTransactionListener(TransactionState.COMMITTED, commandContext -> dispatchAsync(variableInstance.getId()));
    }

    private void dispatchAsync(String variableId) {
        executor.submit(() -> externalizeNow(variableId));
    }

    /**
     * Idempotent, safe to call redundantly (fast path AND sweep may both target the same variable):
     * re-checks eligibility against the CURRENT stored bytes before doing anything.
     */
    public void externalizeNow(String variableId) {
        ProcessEngineConfigurationImpl boundConfiguration = this.configuration;
        if (boundConfiguration == null) {
            // Should not happen in practice (bindConfiguration runs at preInit, before the engine
            // can process any real command) -- defensive, not a silent no-op.
            log.warn("Large-variable externalization attempted before engine configuration was bound — skipped",
                    kv("variable_id", variableId), kv("engine_id", engineId));
            return;
        }
        try {
            boundConfiguration.getCommandExecutorTxRequired().execute(commandContext -> {
                VariableInstanceEntity entity = commandContext.getDbEntityManager()
                        .selectById(VariableInstanceEntity.class, variableId);
                if (entity == null) {
                    // BUS_LARGE_VARIABLE_EXTERNALIZATION_SKIPPED -- benign: the variable (or its
                    // owning execution) was deleted before this ran.
                    log.debug("Large-variable externalization skipped — variable no longer exists",
                            kv("variable_id", variableId), kv("engine_id", engineId));
                    return null;
                }
                byte[] current = entity.getByteArrayValue();
                if (current == null || ExternalizationMarker.decode(current).isPresent()
                        || !properties.exceedsThreshold(current.length)) {
                    // Already externalized (race with another attempt), or the value changed to
                    // something no longer eligible since this was scheduled -- re-check, not a bug.
                    log.debug("Large-variable externalization skipped — no longer eligible",
                            kv("variable_id", variableId), kv("engine_id", engineId));
                    return null;
                }
                LargePayloadReference ref = largePayloadStore.storeAndAcquireReference(current, "runtime." + engineId);
                entity.setByteArrayValue(ExternalizationMarker.encode(ref.contentHash()));
                if (metrics != null) {
                    metrics.largeVariableExternalizedCount(engineId).increment();
                }
                // BUS_LARGE_VARIABLE_EXTERNALIZED -- DP-1: never logs the variable's own value.
                log.info("Large variable externalized", kv("variable_id", variableId), kv("engine_id", engineId),
                        kv("content_hash", ref.contentHash()), kv("byte_length", current.length),
                        kv("dedup_hit", !ref.newlyStored()));
                return null;
            });
        } catch (OptimisticLockingException raced) {
            // BUS_LARGE_VARIABLE_EXTERNALIZATION_RACED -- expected under concurrent modification;
            // not retried synchronously (the sweep re-evaluates eligibility on its own cadence).
            log.debug("Large-variable externalization lost a race to a concurrent modification — skipped",
                    kv("variable_id", variableId), kv("engine_id", engineId));
        } catch (Exception e) {
            // SYS_LARGE_VARIABLE_EXTERNALIZATION_FAILED -- soft-fail by design (D-A'/docs/08 §5):
            // the value simply stays in ACT_GE_BYTEARRAY, exactly as it does today. No audit/data
            // loss -- only a missed offload opportunity, picked up again by the next sweep cycle.
            log.warn("Large-variable externalization failed — value remains inline, will be retried by the sweep",
                    kv("variable_id", variableId), kv("engine_id", engineId), e);
        }
    }
}
