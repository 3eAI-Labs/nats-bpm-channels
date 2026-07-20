package com.threeai.nats.cadenzaflow.history;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;

import com.threeai.nats.core.jetstream.SweepLeaderLease;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.JetStream;
import io.nats.client.impl.NatsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Leader-elected, custody-transfer relay (BR-REL-001, ADR-0010+0002, `03_classes/2_relay_projection.md`
 * §1). Reads {@code compact_history_outbox} oldest-first, dereferences the large-payload companion
 * when present, publishes, and DELETEs the row only AFTER a successful PubAck (at-least-once,
 * never publish-then-forget, never delete-before-PubAck).
 *
 * <p><b>CODER-NOTE (constructor, beyond the LLD sketch):</b> a trailing {@code String engineId}
 * parameter was added for the same reason as {@link NatsHistoryEventHandler} and
 * {@link CompactHistoryOutboxWriter} — the relay's own leader-elected read filters by
 * {@code engine_id} in multi-engine deployments (`03_classes/2_relay_projection.md` §1 Javadoc)
 * and builds the {@code history.<engineId>.<class>.<processInstanceId>} subject; nothing else in
 * this class's constructor sketch carries that identity.
 */
public class HistoryOutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(HistoryOutboxRelay.class);
    private static final int BATCH_SIZE = 500;

    private static final String SELECT_OLDEST_FIRST_SQL =
            "SELECT id, history_event_id, event_type, history_class, engine_id, process_instance_id, "
          + "business_key, payload_scalar, payload_large_ref, event_time, created_at "
          + "FROM compact_history_outbox WHERE engine_id = ? ORDER BY created_at ASC LIMIT " + BATCH_SIZE;

    private static final String SELECT_PAYLOAD_BYTES_SQL =
            "SELECT payload_bytes FROM compact_history_outbox_payload WHERE id = ?";

    private static final String DELETE_ROW_SQL = "DELETE FROM compact_history_outbox WHERE id = ?";

    private static final String SELECT_OLDEST_AGE_SQL =
            "SELECT EXTRACT(EPOCH FROM (now() - MIN(created_at))) FROM compact_history_outbox WHERE engine_id = ?";

    private final DataSource engineDataSource;
    private final JetStream jetStream;
    private final SweepLeaderLease leaderLease;
    private final HistoryOutboxProperties properties;
    private final NatsChannelMetrics metrics;
    private final String engineId;

    public HistoryOutboxRelay(DataSource engineDataSource, JetStream jetStream, SweepLeaderLease leaderLease,
            HistoryOutboxProperties properties, NatsChannelMetrics metrics, String engineId) {
        this.engineDataSource = engineDataSource;
        this.jetStream = jetStream;
        this.leaderLease = leaderLease;
        this.properties = properties;
        this.metrics = metrics;
        this.engineId = engineId;
        if (metrics != null) {
            metrics.registerHistoryOutboxOldestRowAgeGauge(engineId, this::oldestRowAgeSecondsSafe);
        }
    }

    @Scheduled(fixedDelayString = "${spring.nats.cadenzaflow.history.outbox.relay-cycle-period-seconds:30}000")
    public void relayCycle() {
        if (!leaderLease.tryAcquireOrRenew()) {
            return; // not the leader -- zero DB reads (basamak-1 parity, ADR-0002)
        }
        List<OutboxRow> rows;
        try {
            rows = fetchOldestFirst();
        } catch (SQLException e) {
            log.error("Failed to read compact_history_outbox oldest-first — cycle skipped, retried next cycle",
                    kv("engine_id", engineId), e);
            return;
        }
        for (OutboxRow row : rows) {
            relayRow(row);
        }
        checkStuckRows();
    }

    protected void relayRow(OutboxRow row) {
        try {
            byte[] largePayload = row.payloadLargeRef() != null ? dereferenceLargePayload(row.payloadLargeRef()) : null;
            String payload = HistoryWireMessageFactory.encodePayloadFromRawFieldsJson(row.payloadScalar(), largePayload);
            NatsMessage msg = buildMessage(row, payload);

            jetStream.publish(msg); // synchronous publish -- throws unless PubAck is received
            deleteOutboxRow(row.id());

            if (metrics != null) {
                metrics.historyOutboxRelayedCount(row.historyClass(), "published").increment();
            }
        } catch (Exception e) {
            // SYS_OUTBOX_RELAY_PUBLISH_FAILED -- row NOT deleted, retried next relayCycle().
            log.warn("Outbox row relay publish failed — row retained, will retry next cycle",
                    kv("history_class", row.historyClass()), kv("history_event_id", row.historyEventId()), e);
            if (metrics != null) {
                metrics.historyOutboxRelayedCount(row.historyClass(), "failed").increment();
            }
        }
    }

    /**
     * {@code SYS_OUTBOX_ROW_STUCK} — age vs {@code stuckThresholdMultiplier * relayCyclePeriod}
     * (default 5x30s=150s, LLD-Q5). Row is NOT lost — this is only an exposure-window ops signal
     * (DP-12).
     */
    protected void checkStuckRows() {
        double ageSeconds = oldestRowAgeSecondsSafe().doubleValue();
        if (ageSeconds > properties.stuckThresholdSeconds()) {
            log.warn("Outbox row age exceeds stuck threshold — relay/leader stuck suspicion",
                    kv("engine_id", engineId), kv("age_seconds", ageSeconds),
                    kv("threshold_seconds", properties.stuckThresholdSeconds()));
        }
    }

    private Number oldestRowAgeSecondsSafe() {
        try (Connection connection = engineDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(SELECT_OLDEST_AGE_SQL)) {
            stmt.setString(1, engineId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    double value = rs.getDouble(1);
                    return rs.wasNull() ? 0.0 : value;
                }
                return 0.0;
            }
        } catch (SQLException e) {
            log.warn("Failed to compute oldest compact_history_outbox row age", kv("engine_id", engineId), e);
            return 0.0;
        }
    }

    private List<OutboxRow> fetchOldestFirst() throws SQLException {
        List<OutboxRow> rows = new ArrayList<>();
        try (Connection connection = engineDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(SELECT_OLDEST_FIRST_SQL)) {
            stmt.setString(1, engineId);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    rows.add(new OutboxRow(
                            (UUID) rs.getObject("id"),
                            rs.getString("history_event_id"),
                            rs.getString("event_type"),
                            rs.getString("history_class"),
                            rs.getString("engine_id"),
                            rs.getString("process_instance_id"),
                            rs.getString("business_key"),
                            rs.getString("payload_scalar"),
                            (UUID) rs.getObject("payload_large_ref"),
                            toInstant(rs.getTimestamp("event_time")),
                            toInstant(rs.getTimestamp("created_at"))));
                }
            }
        }
        return rows;
    }

    private byte[] dereferenceLargePayload(UUID payloadId) throws SQLException {
        try (Connection connection = engineDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(SELECT_PAYLOAD_BYTES_SQL)) {
            stmt.setObject(1, payloadId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getBytes("payload_bytes");
                }
                log.warn("compact_history_outbox_payload row missing for referenced payload_large_ref",
                        kv("payload_id", payloadId));
                return null;
            }
        }
    }

    private void deleteOutboxRow(UUID id) throws SQLException {
        try (Connection connection = engineDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(DELETE_ROW_SQL)) {
            stmt.setObject(1, id);
            stmt.executeUpdate(); // CASCADE removes the companion payload row, if any
        }
    }

    private NatsMessage buildMessage(OutboxRow row, String payload) {
        io.nats.client.impl.Headers headers = new io.nats.client.impl.Headers();
        headers.add(io.nats.client.support.NatsJetStreamConstants.MSG_ID_HDR,
                row.historyEventId() + ":" + row.eventType());
        headers.add(com.threeai.nats.core.history.HistoryHeaders.ENGINE_ID, row.engineId());
        headers.add(com.threeai.nats.core.history.HistoryHeaders.CLASS, row.historyClass());
        headers.add(com.threeai.nats.core.history.HistoryHeaders.EVENT_TYPE, row.eventType());
        headers.add(com.threeai.nats.core.history.HistoryHeaders.EVENT_ID, row.historyEventId());
        headers.add(com.threeai.nats.core.history.HistoryHeaders.PROCESS_INSTANCE_ID, row.processInstanceId());
        // FINDING-001 (faz-5 review, Levent kararı 2026-07-20): the outbox row's OWN event_time
        // column is already the engine's real event timestamp (CompactHistoryOutboxWriter ->
        // HistoryEventFieldExtractor.eventTimeOf, set at tx-write time) -- relayed verbatim, not
        // recomputed at relay/publish time.
        headers.add(com.threeai.nats.core.history.HistoryHeaders.EVENT_TIME, String.valueOf(row.eventTime().toEpochMilli()));
        if (row.businessKey() != null && !row.businessKey().isBlank()) {
            headers.add(com.threeai.nats.core.headers.BpmHeaders.BUSINESS_KEY, row.businessKey());
        }
        String subject = "history." + row.engineId() + "." + row.historyClass() + "." + row.processInstanceId();
        return NatsMessage.builder()
                .subject(subject)
                .headers(headers)
                .data(payload.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .build();
    }

    private static Instant toInstant(Timestamp timestamp) {
        return timestamp != null ? timestamp.toInstant() : null;
    }

    /** One {@code compact_history_outbox} row (package-private DAO carrier, not an LLD DTO). */
    record OutboxRow(UUID id, String historyEventId, String eventType, String historyClass, String engineId,
            String processInstanceId, String businessKey, String payloadScalar, UUID payloadLargeRef,
            Instant eventTime, Instant createdAt) {
    }
}
