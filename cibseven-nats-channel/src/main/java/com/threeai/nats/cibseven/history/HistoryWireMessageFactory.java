package com.threeai.nats.cibseven.history;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.threeai.nats.core.headers.BpmHeaders;
import com.threeai.nats.core.history.HistoryHeaders;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.NatsJetStreamConstants;

/**
 * Builds the single wire-contract {@code HistoryEventMessage} shape (`api/asyncapi.yaml`
 * {@code historyEvent} channel) shared by BOTH publish paths — {@link HistoryOutboxRelay}
 * (audit-critical) and {@link HistoryPostCommitPublisher} (bulk) — per NFR-M3 "İki yol da AYNI
 * subject/dedup şemasını kullanır". Package-private: not part of the LLD's public class list, a
 * decomposition to avoid duplicating subject/header-building logic between the two publishers.
 *
 * <p>Payload encoding: a flat JSON object of the extracted scalar fields, plus an optional
 * {@code _largePayloadBase64} key when a byte-array-backed field is present (ARCH-Q1 reference
 * pattern's wire-level counterpart — kept as ONE flat object rather than a nested envelope so the
 * shape stored in {@code compact_history_outbox.payload_scalar} and the shape published on the
 * wire differ only by that one optional key).
 */
final class HistoryWireMessageFactory {

    private static final ObjectMapper JSON = new ObjectMapper();

    private HistoryWireMessageFactory() {
    }

    /**
     * @param eventTime the ENGINE's real history-event timestamp (FINDING-001, faz-5 review,
     *                  Levent kararı 2026-07-20) — carried on the wire as {@link
     *                  HistoryHeaders#EVENT_TIME} (epoch-millis), NEVER the publish/consume-time
     *                  clock. Required (non-null) — every concrete {@code HistoryEvent} this
     *                  basamak classifies resolves one via {@code HistoryEventFieldExtractor
     *                  .eventTimeOf}.
     */
    static NatsMessage build(String engineId, String historyClass, String historyEventId, String eventType,
            String processInstanceId, String businessKey, Map<String, Object> fields, byte[] largePayloadOrNull,
            Instant eventTime) {
        String subject = "history." + engineId + "." + historyClass + "." + processInstanceId;
        String dedupId = historyEventId + ":" + eventType;

        Headers headers = new Headers();
        headers.add(NatsJetStreamConstants.MSG_ID_HDR, dedupId);
        headers.add(HistoryHeaders.ENGINE_ID, engineId);
        headers.add(HistoryHeaders.CLASS, historyClass);
        headers.add(HistoryHeaders.EVENT_TYPE, eventType);
        headers.add(HistoryHeaders.EVENT_ID, historyEventId);
        headers.add(HistoryHeaders.PROCESS_INSTANCE_ID, processInstanceId);
        headers.add(HistoryHeaders.EVENT_TIME, String.valueOf(eventTime.toEpochMilli()));
        if (businessKey != null && !businessKey.isBlank()) {
            headers.add(BpmHeaders.BUSINESS_KEY, businessKey);
        }

        return NatsMessage.builder()
                .subject(subject)
                .headers(headers)
                .data(encodePayload(fields, largePayloadOrNull).getBytes(StandardCharsets.UTF_8))
                .build();
    }

    static String encodePayload(Map<String, Object> fields, byte[] largePayloadOrNull) {
        Map<String, Object> payload = new LinkedHashMap<>(fields);
        if (largePayloadOrNull != null) {
            payload.put("_largePayloadBase64", Base64.getEncoder().encodeToString(largePayloadOrNull));
        }
        try {
            return JSON.writeValueAsString(payload);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize history wire payload", e);
        }
    }

    /** Re-encodes an already-JSON {@code payload_scalar} string, adding the large-payload key. */
    static String encodePayloadFromRawFieldsJson(String rawFieldsJson, byte[] largePayloadOrNull) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> fields = JSON.readValue(rawFieldsJson, Map.class);
            return encodePayload(fields, largePayloadOrNull);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Failed to re-encode compact_history_outbox payload_scalar for relay", e);
        }
    }
}
