package com.threeai.nats.cadenzaflow.history;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.core.history.PseudonymTokenGenerator;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import org.cadenzaflow.bpm.engine.impl.history.event.HistoryEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Audit-critical tx-in writer (BR-HDL-003, ADR-0010 + ARCH-Q1, `03_classes/1_handler_outbox.md`
 * §2). Called from {@link NatsHistoryEventHandler#handleEvent} in the SAME transaction as the
 * runtime engine write (NFR-P2 &lt;=1 {@code compact_history_outbox} row/tx). Pseudonymizes
 * {@code OP_LOG.userId} in-tx (pure, no I/O, BA-Q5) when tenant opt-in is enabled BEFORE building
 * {@code payload_scalar} — the raw value is never persisted in the same row as the token.
 *
 * <p><b>CODER-NOTE (constructor/method signature, beyond the LLD sketch):</b> the LLD constructor
 * is {@code (DataSource, PseudonymTokenGenerator)} and {@code write(HistoryEvent, String
 * historyClass, Connection engineTxConnection)}. Two params were added: (1) {@code
 * HistoryClassificationProperties} — the pseudonymization opt-in/tenant-key fields it carries are
 * required to implement BA-Q5 and were not otherwise reachable; (2) an {@code engineId} parameter
 * on {@code write(...)} — {@code compact_history_outbox.engine_id} is {@code NOT NULL} and,
 * exactly like the sibling {@code HistoryPostCommitPublisher.publish(HistoryEvent, String, String
 * engineId)}, this class has no per-event source for it otherwise (the fork's fixed
 * {@code HistoryEventHandler.handleEvent(HistoryEvent)} SPI carries no engine identity of its
 * own). {@code (DataSource engineDataSource)} from the LLD sketch is intentionally UNUSED by
 * {@code write(...)} — the LLD's own class Javadoc ("SAME transaction as the runtime write")
 * requires the CALLER's live transactional {@code Connection}, not a fresh one obtained from the
 * pool (which would NOT be tx-joined); the field is kept for signature parity / potential future
 * non-tx callers (e.g. compensating writes) but is not read on the hot path.
 */
public class CompactHistoryOutboxWriter {

    private static final Logger log = LoggerFactory.getLogger(CompactHistoryOutboxWriter.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String INSERT_OUTBOX_SQL =
            "INSERT INTO compact_history_outbox "
          + "(id, history_event_id, event_type, history_class, engine_id, process_instance_id, "
          + "business_key, payload_scalar, payload_large_ref, event_time) "
          + "VALUES (?,?,?,?,?,?,?,CAST(? AS JSONB),?,?)";

    private static final String INSERT_PAYLOAD_SQL =
            "INSERT INTO compact_history_outbox_payload (id, outbox_row_id, payload_bytes) VALUES (?,?,?)";

    private final DataSource engineDataSource;
    private final PseudonymTokenGenerator pseudonymGenerator;
    private final HistoryClassificationProperties classification;
    private final NatsChannelMetrics metrics;

    public CompactHistoryOutboxWriter(DataSource engineDataSource, PseudonymTokenGenerator pseudonymGenerator,
            HistoryClassificationProperties classification, NatsChannelMetrics metrics) {
        this.engineDataSource = engineDataSource;
        this.pseudonymGenerator = pseudonymGenerator;
        this.classification = classification;
        this.metrics = metrics;
    }

    public void write(HistoryEvent historyEvent, String historyClass, String engineId,
            java.sql.Connection engineTxConnection) {
        Map<String, Object> fields = HistoryEventFieldExtractor.extractFields(historyEvent);
        applyPseudonymizationIfApplicable(historyClass, fields);

        String businessKey = HistoryEventFieldExtractor.businessKeyOf(historyEvent);
        Instant eventTime = HistoryEventFieldExtractor.eventTimeOf(historyEvent);
        byte[] largePayload = HistoryEventFieldExtractor.extractLargePayload(historyEvent, historyClass).orElse(null);

        UUID outboxId = UUID.randomUUID();
        UUID payloadId = largePayload != null ? UUID.randomUUID() : null;

        try (PreparedStatement insertOutbox = engineTxConnection.prepareStatement(INSERT_OUTBOX_SQL)) {
            insertOutbox.setObject(1, outboxId);
            insertOutbox.setString(2, historyEvent.getId());
            insertOutbox.setString(3, historyEvent.getEventType());
            insertOutbox.setString(4, historyClass);
            insertOutbox.setString(5, engineId);
            insertOutbox.setString(6, historyEvent.getProcessInstanceId());
            insertOutbox.setString(7, businessKey);
            insertOutbox.setString(8, toJson(fields));
            if (payloadId != null) {
                insertOutbox.setObject(9, payloadId);
            } else {
                insertOutbox.setNull(9, Types.OTHER);
            }
            insertOutbox.setTimestamp(10, Timestamp.from(eventTime));
            insertOutbox.executeUpdate();
        } catch (SQLException e) {
            // Engine-native tx-fail (07_errors.md §3.2 row 4 note: NOT SYS_OUTBOX_RELAY_PUBLISH_FAILED --
            // this is a DB constraint/connectivity failure on the WRITE side, propagates and rolls
            // back the runtime transaction together with the ACT_HI write, by design (BR-HDL-003).
            throw new IllegalStateException("Failed to write compact_history_outbox row for event "
                    + historyEvent.getId(), e);
        }

        if (largePayload != null) {
            try (PreparedStatement insertPayload = engineTxConnection.prepareStatement(INSERT_PAYLOAD_SQL)) {
                insertPayload.setObject(1, payloadId);
                insertPayload.setObject(2, outboxId);
                insertPayload.setBytes(3, largePayload);
                insertPayload.executeUpdate();
            } catch (SQLException e) {
                throw new IllegalStateException("Failed to write compact_history_outbox_payload row for event "
                        + historyEvent.getId(), e);
            }
        }

        if (metrics != null) {
            metrics.historyOutboxWrittenCount(historyClass, engineId).increment();
        }
        log.debug("Wrote compact_history_outbox row", kv("history_class", historyClass),
                kv("history_event_id", historyEvent.getId()), kv("has_large_payload", largePayload != null));
    }

    /** BA-Q5: only {@code OP_LOG.userId}, only when tenant opt-in is enabled. */
    private void applyPseudonymizationIfApplicable(String historyClass, Map<String, Object> fields) {
        if (!HistoryClassNames.OP_LOG.equals(historyClass) || !classification.isPseudonymizationOptIn()) {
            return;
        }
        Object userIdValue = fields.get("userId");
        if (!(userIdValue instanceof String userId) || userId.isBlank()) {
            return;
        }
        String token = pseudonymGenerator.generate(userId, classification.getTenantKeyId(), classification.getTenantKeyVersion());
        fields.remove("userId"); // raw value never persisted alongside the token in the same row
        fields.put("userIdPseudonymized", true);
        fields.put("pseudonymToken", token);
    }

    private static String toJson(Map<String, Object> fields) {
        try {
            return JSON.writeValueAsString(fields);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize compact_history_outbox payload_scalar", e);
        }
    }

    /** Kept for LLD signature parity — see class Javadoc CODER-NOTE. */
    DataSource engineDataSource() {
        return engineDataSource;
    }
}
