package com.threeai.nats.history.projection;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.threeai.nats.core.dlq.DlqPublishOutcome;
import com.threeai.nats.core.dlq.DlqReason;
import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.core.history.HistoryHeaders;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.JetStream;
import io.nats.client.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instance-partition + merge-upsert consumer (BR-REL-002/006, ADR-0011/0012,
 * `03_classes/2_relay_projection.md` §2). One instance per partition (ARCH-Q3, N=8 default,
 * LLD-Q2). Deserializes the {@code HistoryEventMessage} wire contract, routes to
 * {@link ProjectionStore#upsertEntity} or {@link ProjectionStore#insertLogEvent} depending on
 * {@code historyClass}, and applies at-least-once + idempotent custody-transfer semantics
 * (ack/nak/DLQ).
 *
 * <p><b>CODER-NOTE (constructor, beyond the LLD sketch):</b> a trailing {@code int maxDeliver}
 * parameter was added — the asyncapi {@code historyEvent} channel's {@code x-jetstream.maxDeliver}
 * is 4; this consumer needs that budget to implement the delivery-count-exceeded → DLQ escalation
 * the asyncapi {@code consumeHistoryEvent} operation describes ("deliveryCount &gt; maxDeliver →
 * dlq.history.&lt;...&gt;"), which the LLD's method-only sketch does not otherwise carry.
 *
 * <p><b>CQ-1 (Levent, önerilen — pseudonym-vault write ownership):</b> this consumer previously
 * carried a {@code PseudonymizationVaultClient} dependency for an enqueue-only stub that could
 * never actually call {@code persistMapping(...)} with a real value — it only ever sees the
 * pseudonym TOKEN on the wire (BA-Q5/DP-1: the raw value is removed from {@code fields} before the
 * outbox row is even built, engine-side). That dependency and the stub have been REMOVED; the
 * vault WRITE now happens engine-side, at the point the token is actually computed (see {@code
 * CompactHistoryOutboxWriter.applyPseudonymizationIfApplicable}, `camunda-nats-channel`/{@code
 * cadenzaflow-nats-channel} mirror) — this class stays entirely vault-unaware, consistent with
 * ARCH-Q2's DB isolation (it never needs a vault {@code DataSource} at all).
 */
public class HistoryProjectionConsumer {

    private static final Logger log = LoggerFactory.getLogger(HistoryProjectionConsumer.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String LARGE_PAYLOAD_KEY = "_largePayloadBase64";

    private final int partitionIndex;
    private final JetStream jetStream;
    private final ProjectionStore projectionStore;
    private final HistoryDlqConsumer dlqConsumer;
    private final NatsChannelMetrics metrics;
    private final int maxDeliver;

    public HistoryProjectionConsumer(int partitionIndex, JetStream jetStream, ProjectionStore projectionStore,
            HistoryDlqConsumer dlqConsumer, NatsChannelMetrics metrics, int maxDeliver) {
        this.partitionIndex = partitionIndex;
        this.jetStream = jetStream;
        this.projectionStore = projectionStore;
        this.dlqConsumer = dlqConsumer;
        this.metrics = metrics;
        this.maxDeliver = maxDeliver;
    }

    public void onMessage(Message msg) {
        long deliveryCount = deliveryCountOf(msg);
        ParsedEnvelope parsed;
        try {
            parsed = parseEnvelope(msg);
        } catch (Exception schemaDrift) {
            handleSchemaDrift(msg, schemaDrift);
            return;
        }

        try {
            UpsertOutcome outcome = route(parsed);
            recordConsumedMetric(parsed.historyClass);
            if (outcome == UpsertOutcome.STALE_DISCARDED) {
                recordStaleDiscardedMetric(parsed.historyClass);
            }
            msg.ack();
        } catch (Exception transientFailure) {
            // SYS_PROJECTION_WRITE_FAILED
            if (deliveryCount > maxDeliver) {
                routeToDlqThenAck(msg, DlqReason.HISTORY_DELIVERY_BUDGET_EXCEEDED);
                return;
            }
            log.error("Projection write failed — nak for redelivery", kv("history_class", parsed.historyClass),
                    kv("partition", partitionIndex), kv("delivery_count", deliveryCount), transientFailure);
            msg.nakWithDelay(backoffFor(deliveryCount));
        }
    }

    /**
     * Entity-lifecycle {@code entityId = envelope.eventId}: fork-verified
     * ({@code DefaultHistoryEventProducer.initProcessInstanceEvent/initActivityInstanceEvent/
     * initTaskInstanceEvent}, {@code createHistoricIncidentEvt}) — for every entity-lifecycle
     * class, {@code HistoryEvent.setId(...)} is ALWAYS set to that entity's own natural id
     * ({@code processInstanceId}/{@code activityInstanceId}/{@code taskId}/{@code incidentId}),
     * never an independent surrogate — so the wire envelope's {@code historyEventId} IS the
     * correct merge-upsert key with no separate "entity id" field needed on the wire.
     */
    private UpsertOutcome route(ParsedEnvelope parsed) {
        if (HistoryClassNames.ENTITY_LIFECYCLE_CLASSES.contains(parsed.historyClass)) {
            EntityHistoryRecord record = new EntityHistoryRecord(parsed.engineId, parsed.eventId,
                    parsed.processInstanceId, parsed.streamSequence, parsed.eventTime, parsed.fields);
            return projectionStore.upsertEntity(parsed.historyClass, record);
        }
        LogHistoryRecord record = new LogHistoryRecord(parsed.engineId, parsed.processInstanceId, parsed.eventId,
                parsed.eventType, parsed.streamSequence, parsed.eventTime, parsed.fields);
        return projectionStore.insertLogEvent(parsed.historyClass, record);
    }

    private void handleSchemaDrift(Message msg, Exception cause) {
        log.error("History event envelope schema drift — routing to DLQ", kv("subject", msg.getSubject()), cause);
        routeToDlqThenAck(msg, DlqReason.HISTORY_SCHEMA_DRIFT);
    }

    /**
     * FINDING-004 (faz-5 review): custody-transfer is complete (ack) ONLY once the DLQ publish
     * genuinely succeeded ({@code PUBLISHED_JETSTREAM}/{@code PUBLISHED_CORE_FALLBACK}, IR-4). A
     * DLQ publish failure ({@code FAILED_NO_DLQ_SUBJECT}/{@code FAILED_BOTH_PUBLISH}) previously
     * still acked unconditionally — silently losing the message. Now naks instead, so the message
     * is redelivered and DLQ-routing retried.
     */
    private void routeToDlqThenAck(Message msg, DlqReason reason) {
        DlqPublishOutcome outcome = dlqConsumer.routeToDlq(msg, reason);
        if (outcome == DlqPublishOutcome.PUBLISHED_JETSTREAM || outcome == DlqPublishOutcome.PUBLISHED_CORE_FALLBACK) {
            msg.ack(); // custody-transfer complete once handed to DLQ (IR-4)
            return;
        }
        // SYS_HISTORY_DLQ_PUBLISH_FAILED -- nak+alert (both JetStream and core-NATS DLQ publish
        // failed, or no DLQ subject configured): the message is NOT discarded, redelivery will
        // retry DLQ-routing on the next attempt.
        log.error("History DLQ publish failed — original message NAKed for redelivery, not discarded",
                kv("subject", msg.getSubject()), kv("outcome", outcome));
        msg.nak();
    }

    private ParsedEnvelope parseEnvelope(Message msg) {
        if (msg.getHeaders() == null) {
            throw new IllegalStateException("History event message missing headers");
        }
        String engineId = requireHeader(msg, HistoryHeaders.ENGINE_ID);
        String historyClass = requireHeader(msg, HistoryHeaders.CLASS);
        String eventType = requireHeader(msg, HistoryHeaders.EVENT_TYPE);
        String eventId = requireHeader(msg, HistoryHeaders.EVENT_ID);
        String processInstanceId = requireHeader(msg, HistoryHeaders.PROCESS_INSTANCE_ID);
        java.time.Instant eventTime = requireEventTimeHeader(msg);

        @SuppressWarnings("unchecked")
        Map<String, Object> fields = new LinkedHashMap<>((Map<String, Object>) parseBody(msg));
        Object largePayloadB64 = fields.remove(LARGE_PAYLOAD_KEY);
        if (largePayloadB64 instanceof String b64) {
            applyLargePayload(historyClass, fields, Base64.getDecoder().decode(b64));
        }

        long streamSequence = streamSequenceOf(msg);

        return new ParsedEnvelope(engineId, historyClass, eventType, eventId, processInstanceId,
                streamSequence, eventTime, fields);
    }

    /**
     * FINDING-001 (faz-5 review, Levent kararı 2026-07-20): {@code event_time} is a range-partition
     * ANCHOR and an append-log dedup unique-key component — it MUST be the engine's real event
     * timestamp, never this consumer's own clock. A missing/malformed {@link
     * HistoryHeaders#EVENT_TIME} header is therefore a WIRE-CONTRACT VIOLATION (the header is
     * {@code required} in {@code asyncapi.yaml} as of this fix), routed to DLQ exactly like any
     * other missing required header — NEVER silently defaulted to {@code Instant.now()}.
     */
    private java.time.Instant requireEventTimeHeader(Message msg) {
        String raw = requireHeader(msg, HistoryHeaders.EVENT_TIME);
        try {
            return java.time.Instant.ofEpochMilli(Long.parseLong(raw));
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    "Malformed " + HistoryHeaders.EVENT_TIME + " header (expected epoch-millis): " + raw, e);
        }
    }

    private void applyLargePayload(String historyClass, Map<String, Object> fields, byte[] payload) {
        if (HistoryClassNames.EXT_TASK_LOG.equals(historyClass)) {
            java.util.UUID ref = projectionStore.storeLargePayload(payload, "ext_task_log_history");
            fields.put("errorDetailsRef", ref);
        } else {
            // e.g. DETAIL (byte-array variable values) -- variable_detail_history has no _ref
            // column in this basamak's schema (only VARINST's variable_instance_history does,
            // and VARINST projection is a documented, bounded gap — see engine-side
            // HistoryEventClassResolver CODER-NOTE). Dropped, not silently lost-without-a-trace.
            log.warn("Large payload present but no projection column reference exists for this class — dropped",
                    kv("history_class", historyClass), kv("payload_bytes", payload.length));
        }
    }

    private Object parseBody(Message msg) {
        try {
            return JSON.readValue(msg.getData(), Map.class);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse history event payload JSON", e);
        }
    }

    private String requireHeader(Message msg, String name) {
        String value = msg.getHeaders().getLast(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required history header: " + name);
        }
        return value;
    }

    private long streamSequenceOf(Message msg) {
        try {
            return msg.metaData().streamSequence();
        } catch (Exception e) {
            return 0;
        }
    }

    private long deliveryCountOf(Message msg) {
        try {
            return msg.metaData().deliveredCount();
        } catch (Exception e) {
            return 1;
        }
    }

    private Duration backoffFor(long deliveryCount) {
        long seconds = (long) Math.pow(2, Math.max(0, deliveryCount - 1));
        return Duration.ofSeconds(Math.min(seconds, 30));
    }

    private void recordConsumedMetric(String historyClass) {
        if (metrics != null) {
            metrics.historyProjectionConsumedCount(historyClass, String.valueOf(partitionIndex)).increment();
        }
    }

    private void recordStaleDiscardedMetric(String historyClass) {
        if (metrics != null) {
            metrics.historyProjectionStaleDiscardedCount(historyClass).increment();
        }
    }

    private record ParsedEnvelope(String engineId, String historyClass, String eventType, String eventId,
            String processInstanceId, long streamSequence, java.time.Instant eventTime, Map<String, Object> fields) {
    }
}
