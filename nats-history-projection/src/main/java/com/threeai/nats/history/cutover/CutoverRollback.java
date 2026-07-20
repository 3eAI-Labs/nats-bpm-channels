package com.threeai.nats.history.cutover;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import com.threeai.nats.core.jetstream.JetStreamKvManager;
import io.nats.client.Connection;
import io.nats.client.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operator-triggered rollback (BR-CUT-003, ADR-0015, `03_classes/4_cutover_reconciliation.md`
 * §3). Writes {@code cutover.<engineId>.<class>=false}, sets
 * {@code class_cutover_state.state=DUAL_RUN}, {@code clean_streak_days=0},
 * {@code rollback_count++}, {@code last_rollback_at=now()} — audit-logged
 * ({@code BUS_CUTOVER_ROLLBACK_TRIGGERED}, INFO, operator-planned). Requires the SAME
 * rolling-restart trigger as forward cutover (ARCH-Q5 — symmetric mechanism, deployment-specific,
 * not this class's concern).
 *
 * <p><b>Kalıcı dual-run reddi (NFR-R5):</b> this class only ever moves a class BACK to
 * {@code DUAL_RUN} — there is no "remove from cutover queue permanently" option
 * ({@code ClassCutoverStateStore} exposes no such transition either); a rolled-back class simply
 * re-enters {@code RECONCILING} on its next clean cycle and can eventually be cut over again
 * (BR-HDL-005).
 *
 * <p><b>CODER-NOTE (constructor, beyond the LLD sketch):</b> a {@code Connection} parameter was
 * added — see {@link CutoverControlPlane} class Javadoc CODER-NOTE for the identical rationale.
 */
public class CutoverRollback {

    private static final Logger log = LoggerFactory.getLogger(CutoverRollback.class);
    private static final String BUCKET = "history-cutover-state";

    private final ClassCutoverStateStore stateStore;
    private final JetStreamKvManager kvManager;
    private final Connection connection;

    public CutoverRollback(ClassCutoverStateStore stateStore, JetStreamKvManager kvManager, Connection connection) {
        this.stateStore = stateStore;
        this.kvManager = kvManager;
        this.connection = connection;
    }

    public void rollback(String engineId, String historyClass, String operatorId, String reason) {
        try {
            KeyValue kv = connection.keyValue(BUCKET);
            kv.put("cutover." + engineId + "." + historyClass, "false".getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to write rollback KV value for " + engineId + "/" + historyClass, e);
        }

        stateStore.rollback(engineId, historyClass, Instant.now());

        log.info("Cutover rollback triggered (operator-planned)", kv("engine_id", engineId),
                kv("history_class", historyClass), kv("operator_id", operatorId), kv("reason", reason));
    }
}
