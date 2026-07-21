package com.threeai.nats.history.cutover;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.history.cutover.ClassCutoverState.CutoverState;
import io.nats.client.Connection;
import io.nats.client.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operator/automation-triggered cutover request (BR-CUT-002 + BR-HDL-005, ADR-0015+0009,
 * ARCH-Q5, `03_classes/4_cutover_reconciliation.md` §2). Rejects if the gate is not open (state
 * != N_GUN_TEMIZ). On success: writes the {@code history-cutover-state} KV key (LLD-Q3 mechanism)
 * and sets {@code class_cutover_state.state=CUTOVER_TALEP}. The rolling-restart trigger itself
 * and its completion confirmation are deployment-specific (`99_deployment.md` §3 — "bu repo
 * tetikleyiciyi SAĞLAMAZ, yalnız KV sinyalini üretir").
 *
 * <p><b>CODER-NOTE (constructor + confirmCutoverApplied method, beyond the LLD sketch):</b> (1) a
 * {@code Connection} parameter was added — writing to a KV bucket requires a live NATS
 * connection, which {@code JetStreamKvManager} (bucket PROVISIONING only) does not itself carry;
 * (2) {@link #confirmCutoverApplied} was added because {@code 06_state_machines.md}'s {@code
 * CUTOVER_TALEP -> CUTOVERLANMIS} transition is explicitly described as observed via a
 * "health-check/deployment-status sinyali" SEPARATE from {@code requestCutover(...)} itself — the
 * LLD's single-method sketch has no other entry point for this transition to ever complete.
 */
public class CutoverControlPlane {

    private static final Logger log = LoggerFactory.getLogger(CutoverControlPlane.class);
    private static final String BUCKET = "history-cutover-state";

    private final ClassCutoverStateStore stateStore;
    private final JetStreamKvManager kvManager;
    private final HistoryCutoverProperties properties;
    private final Connection connection;

    public CutoverControlPlane(ClassCutoverStateStore stateStore, JetStreamKvManager kvManager,
            HistoryCutoverProperties properties, Connection connection) {
        this.stateStore = stateStore;
        this.kvManager = kvManager;
        this.properties = properties;
        this.connection = connection;
    }

    public CutoverOutcome requestCutover(String engineId, String historyClass) {
        ClassCutoverState state = stateStore.find(engineId, historyClass).orElse(null);
        if (state == null || state.state() != CutoverState.N_GUN_TEMIZ) {
            log.warn("Cutover requested but gate not met — rejected", kv("engine_id", engineId),
                    kv("history_class", historyClass),
                    kv("current_state", state != null ? state.state() : "NONE")); // BUS_CUTOVER_GATE_NOT_MET
            return CutoverOutcome.GATE_NOT_MET;
        }

        try {
            KeyValue kv = connection.keyValue(BUCKET);
            kv.put("cutover." + engineId + "." + historyClass, "true".getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("Cutover KV write failed — dual-run continues (fail-safe)", kv("engine_id", engineId),
                    kv("history_class", historyClass), e); // SYS_CUTOVER_CONFIG_APPLY_FAILED
            return CutoverOutcome.APPLY_FAILED;
        }

        try {
            stateStore.markCutoverRequested(engineId, historyClass);
        } catch (Exception e) {
            log.error("Cutover state-transition write failed after KV succeeded — dual-run continues",
                    kv("engine_id", engineId), kv("history_class", historyClass), e); // SYS_CUTOVER_CONFIG_APPLY_FAILED
            return CutoverOutcome.APPLY_FAILED;
        }

        log.info("Cutover requested — awaiting rolling-restart confirmation (deployment-specific)",
                kv("engine_id", engineId), kv("history_class", historyClass));
        return CutoverOutcome.REQUESTED;
    }

    /**
     * Deployment tooling calls this once the rolling-restart health-check confirms every engine
     * node replica has re-read {@code ClassCutoverStateRegistry.loadAtBootstrap()} with the new
     * KV value (`99_deployment.md` §3 step 4). {@code CUTOVER_TALEP -> CUTOVERLANMIS}.
     */
    public void confirmCutoverApplied(String engineId, String historyClass) {
        stateStore.markCutoverApplied(engineId, historyClass, Instant.now());
        log.info("Cutover applied (rolling-restart confirmed)", kv("engine_id", engineId), kv("history_class", historyClass));
    }

    /** Volume-priority order for operators/automation deciding cutover sequencing (BR-HDL-005). */
    public java.util.List<String> volumePriorityOrder() {
        return properties.getVolumePriorityOrder();
    }
}
