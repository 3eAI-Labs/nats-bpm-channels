package com.threeai.nats.core.outbound;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

import com.threeai.nats.core.metrics.NatsChannelMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Critical-path tx-in writer (docs/09-outbound-handoff.md D-A'/D-C'/D-F'). Called from the
 * per-engine {@code NatsOutboundPublisher} ExecutionListener in the SAME transaction as the
 * runtime engine write it is attached to (NFR-parity with basamak-2 NFR-P2 &lt;=1 {@code
 * outbound_message_outbox} row/tx). Engine-neutral by design — accepts the CALLER's live
 * transactional {@link Connection} (never opens its own), the same discipline
 * {@code CompactHistoryOutboxWriter} established for basamak-2.
 *
 * <p><b>CODER-NOTE (placement — nats-core, not per-engine):</b> unlike {@code
 * CompactHistoryOutboxWriter} (camunda-nats-channel, byte-mirrored into cadenzaflow-nats-channel),
 * this class carries NO camunda/cadenzaflow-fork types in its signature — {@code write(...)} only
 * needs a plain {@link Connection} and an {@link OutboundMessageDraft} — so it can live ONCE here
 * and be reused directly by both engine modules' {@code NatsOutboundPublisher}, with zero
 * mechanical mirroring required for this specific class (only the {@code ExecutionListener}
 * itself, which DOES touch fork types, needs mirroring).
 */
public class OutboundMessageOutboxWriter {

    private static final Logger log = LoggerFactory.getLogger(OutboundMessageOutboxWriter.class);

    private static final String INSERT_SQL =
            "INSERT INTO outbound_message_outbox "
          + "(id, engine_id, message_type, process_instance_id, business_key, trace_id, subject, payload) "
          + "VALUES (?,?,?,?,?,?,?,?)";

    private final NatsChannelMetrics metrics;

    public OutboundMessageOutboxWriter(NatsChannelMetrics metrics) {
        this.metrics = metrics;
    }

    /**
     * @param engineTxConnection the CALLING engine command's live transactional JDBC connection —
     *                           NOT a fresh connection from a pool (would not be tx-joined).
     * @return the outbox row's own id — used verbatim as the {@code Nats-Msg-Id} dedup key when
     *         the relay eventually publishes this row (stable across relay retries).
     */
    public UUID write(Connection engineTxConnection, OutboundMessageDraft draft) {
        UUID id = UUID.randomUUID();
        try (PreparedStatement insert = engineTxConnection.prepareStatement(INSERT_SQL)) {
            insert.setObject(1, id);
            insert.setString(2, draft.engineId());
            insert.setString(3, draft.messageType());
            insert.setString(4, draft.processInstanceId());
            insert.setString(5, draft.businessKey());
            insert.setString(6, draft.traceId());
            insert.setString(7, draft.subject());
            insert.setBytes(8, draft.payload());
            insert.executeUpdate();
        } catch (SQLException e) {
            // Engine-native tx-fail (basamak-2 07_errors.md §3.2 row 4 precedent): NOT a publish
            // failure — a DB constraint/connectivity failure on the WRITE side, propagates and
            // rolls back the runtime transaction together with the engine's own write, by design
            // (D-A'/D-C' critical-path atomicity guarantee).
            throw new IllegalStateException("Failed to write outbound_message_outbox row for message type '"
                    + draft.messageType() + "', process instance " + draft.processInstanceId(), e);
        }

        if (metrics != null) {
            metrics.outboundOutboxWrittenCount(draft.messageType(), draft.engineId()).increment();
        }
        log.debug("Wrote outbound_message_outbox row", kv("message_type", draft.messageType()),
                kv("engine_id", draft.engineId()), kv("process_instance_id", draft.processInstanceId()));
        return id;
    }
}
