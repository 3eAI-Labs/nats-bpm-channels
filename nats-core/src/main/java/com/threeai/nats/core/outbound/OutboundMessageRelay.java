package com.threeai.nats.core.outbound;

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

import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.JetStream;
import io.nats.client.impl.NatsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Leader-elected, custody-transfer relay for {@code outbound_message_outbox}
 * (docs/09-outbound-handoff.md D-F' — basamak-2 {@code HistoryOutboxRelay}/{@code
 * CompactHistoryOutboxWriter} skeleton transplant, {@code SweepLeaderLease} reuse). Reads rows
 * oldest-first, publishes, and DELETEs only AFTER a successful PubAck (at-least-once, never
 * publish-then-forget, never delete-before-PubAck).
 *
 * <p>Engine-neutral (no camunda/cadenzaflow-fork types) — a SINGLE instance, parameterized by
 * {@code engineId}, serves whichever engine module wires it (mirrors {@code HistoryOutboxRelay}'s
 * per-engine-id filtering, but the CLASS itself lives once in {@code nats-core} per the basamak-4
 * module placement).
 */
public class OutboundMessageRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboundMessageRelay.class);
    private static final int BATCH_SIZE = 500;

    private static final String SELECT_OLDEST_FIRST_SQL =
            "SELECT id, engine_id, message_type, process_instance_id, business_key, trace_id, subject, payload, created_at "
          + "FROM outbound_message_outbox WHERE engine_id = ? ORDER BY created_at ASC LIMIT " + BATCH_SIZE;

    private static final String DELETE_ROW_SQL = "DELETE FROM outbound_message_outbox WHERE id = ?";

    private static final String SELECT_OLDEST_AGE_SQL =
            "SELECT EXTRACT(EPOCH FROM (now() - MIN(created_at))) FROM outbound_message_outbox WHERE engine_id = ?";

    private final DataSource engineDataSource;
    private final JetStream jetStream;
    private final com.threeai.nats.core.jetstream.SweepLeaderLease leaderLease;
    private final OutboundMessageOutboxProperties properties;
    private final NatsChannelMetrics metrics;
    private final String engineId;

    public OutboundMessageRelay(DataSource engineDataSource, JetStream jetStream,
            com.threeai.nats.core.jetstream.SweepLeaderLease leaderLease, OutboundMessageOutboxProperties properties,
            NatsChannelMetrics metrics, String engineId) {
        this.engineDataSource = engineDataSource;
        this.jetStream = jetStream;
        this.leaderLease = leaderLease;
        this.properties = properties;
        this.metrics = metrics;
        this.engineId = engineId;
        if (metrics != null) {
            metrics.registerOutboundOutboxOldestRowAgeGauge(engineId, this::oldestRowAgeSecondsSafe);
        }
    }

    @Scheduled(fixedDelayString = "${spring.nats.outbound.outbox.relay-cycle-period-seconds:30}000")
    public void relayCycle() {
        boolean wasLeader = leaderLease.isLeader();
        if (!leaderLease.tryAcquireOrRenew()) {
            if (wasLeader) {
                log.warn("Outbound-outbox relay leadership lost — another node will take over relaying",
                        kv("engine_id", engineId));
            }
            return; // not the leader -- zero DB reads
        }
        List<OutboxRow> rows;
        try {
            rows = fetchOldestFirst();
        } catch (SQLException e) {
            log.error("Failed to read outbound_message_outbox oldest-first — cycle skipped, retried next cycle",
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
            NatsMessage msg = buildMessage(row);
            io.nats.client.api.PublishAck ack = jetStream.publish(msg); // synchronous -- throws unless PubAck is received
            if (ack.isDuplicate()) {
                log.info("Outbound-outbox row relay publish was a JetStream-deduplicated redelivery",
                        kv("message_type", row.messageType()), kv("outbox_row_id", row.id()));
            }
            deleteOutboxRow(row.id());

            if (metrics != null) {
                metrics.outboundOutboxRelayedCount(row.messageType(), "published").increment();
            }
        } catch (Exception e) {
            // Row NOT deleted, retried next relayCycle() — matches HistoryOutboxRelay precedent
            // (indefinite retry, no DLQ escape on this critical/at-least-once path).
            log.warn("Outbound-outbox row relay publish failed — row retained, will retry next cycle",
                    kv("message_type", row.messageType()), kv("outbox_row_id", row.id()), e);
            if (metrics != null) {
                metrics.outboundOutboxRelayedCount(row.messageType(), "failed").increment();
            }
        }
    }

    /** Age vs {@code stuckThresholdMultiplier * relayCyclePeriod} — row is NOT lost, only an ops exposure signal. */
    protected void checkStuckRows() {
        double ageSeconds = oldestRowAgeSecondsSafe().doubleValue();
        if (ageSeconds > properties.stuckThresholdSeconds()) {
            log.warn("Outbound-outbox row age exceeds stuck threshold — relay/leader stuck suspicion",
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
            log.warn("Failed to compute oldest outbound_message_outbox row age", kv("engine_id", engineId), e);
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
                            rs.getString("engine_id"),
                            rs.getString("message_type"),
                            rs.getString("process_instance_id"),
                            rs.getString("business_key"),
                            rs.getString("trace_id"),
                            rs.getString("subject"),
                            rs.getBytes("payload"),
                            toInstant(rs.getTimestamp("created_at"))));
                }
            }
        }
        return rows;
    }

    private void deleteOutboxRow(UUID id) throws SQLException {
        try (Connection connection = engineDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(DELETE_ROW_SQL)) {
            stmt.setObject(1, id);
            stmt.executeUpdate();
        }
    }

    private NatsMessage buildMessage(OutboxRow row) {
        OutboundMessageDraft draft = new OutboundMessageDraft(row.engineId(), row.messageType(),
                row.processInstanceId(), row.businessKey(), row.traceId(), row.subject(), row.payload());
        // The outbox row's OWN id is the dedup key — stable across relay retries (custody-transfer).
        return OutboundWireMessageFactory.buildMessage(draft, row.id().toString());
    }

    private static Instant toInstant(Timestamp timestamp) {
        return timestamp != null ? timestamp.toInstant() : null;
    }

    /** One {@code outbound_message_outbox} row (package-private DAO carrier). */
    record OutboxRow(UUID id, String engineId, String messageType, String processInstanceId, String businessKey,
            String traceId, String subject, byte[] payload, Instant createdAt) {
    }
}
