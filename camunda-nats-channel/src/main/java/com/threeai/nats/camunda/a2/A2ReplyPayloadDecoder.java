package com.threeai.nats.camunda.a2;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import io.nats.client.Message;

/**
 * Decodes {@code jobs.<topic>.reply} messages per the asyncapi {@code JobSuccessReply} /
 * {@code JobBpmnErrorReply} / {@code JobTransientFailureReply} schemas.
 *
 * <p><b>Reply discriminator (Sentinel Phase 5.5 QA fix, Levent karari 2026-07-15):</b> the
 * original errorCode-presence heuristic (classify by {@code Content-Type} first, then by whether
 * an {@code errorCode} field was present) was an implementation choice, not an explicitly
 * mandated wire rule — and it silently misclassified any {@code TransientFailurePayload} that
 * happened to include an {@code errorCode}-shaped field, or any reply missing a body field it
 * expected. The wire contract now mandates a {@code type: SUCCESS|BPMN_ERROR|TRANSIENT}
 * discriminator field on EVERY reply payload (asyncapi {@code JobSuccessPayload} /
 * {@code BpmnErrorPayload} / {@code TransientFailurePayload}, all JSON — {@code JobSuccessReply}'s
 * {@code contentType} changed from {@code application/octet-stream} to {@code application/json}
 * to carry it). {@link #classify(Message)} reads ONLY this field; a missing or unrecognized value
 * returns {@link Optional#empty()}, which {@link A2CompletionBridge} routes to the DLQ as
 * {@code VAL_INVALID_REPLY_TYPE} ({@code DlqReason#INVALID_REPLY_TYPE}) instead of guessing.
 */
final class A2ReplyPayloadDecoder {

    private static final String TYPE_FIELD = "type";

    private A2ReplyPayloadDecoder() {
    }

    /** @return the reply's declared type, or {@link Optional#empty()} if missing/unrecognized (routes to DLQ). */
    static Optional<ReplyType> classify(Message msg) {
        String typeValue = extractJsonField(bodyAsString(msg), TYPE_FIELD);
        if (typeValue == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(ReplyType.valueOf(typeValue.trim()));
        } catch (IllegalArgumentException unknownType) {
            return Optional.empty();
        }
    }

    /**
     * CODER-QUESTION: full structured variable-map deserialization is tenant-defined
     * (asyncapi {@code OpaqueBusinessPayload}) and out of this repo's contract — the raw
     * payload is passed through under a single {@code natsPayload} variable, consistent with
     * the existing {@code JetStreamMessageCorrelationSubscriber} convention.
     */
    static Map<String, Object> variablesOf(Message msg) {
        Map<String, Object> variables = new HashMap<>();
        variables.put("natsPayload", bodyAsString(msg));
        return variables;
    }

    static String errorCodeOf(Message msg) {
        return extractJsonField(bodyAsString(msg), "errorCode");
    }

    static String errorMessageOf(Message msg) {
        return extractJsonField(bodyAsString(msg), "errorMessage");
    }

    static String errorDetailsOf(Message msg) {
        return extractJsonField(bodyAsString(msg), "errorDetails");
    }

    static int retriesOf(Message msg) {
        String retries = extractJsonField(bodyAsString(msg), "retries");
        try {
            return retries != null ? Integer.parseInt(retries) : 0;
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static String bodyAsString(Message msg) {
        byte[] data = msg.getData();
        return data != null ? new String(data, StandardCharsets.UTF_8) : "";
    }

    private static String extractJsonField(String json, String fieldName) {
        if (json == null) {
            return null;
        }
        String pattern = "\"" + fieldName + "\"";
        int idx = json.indexOf(pattern);
        if (idx < 0) {
            return null;
        }
        int colonIdx = json.indexOf(':', idx + pattern.length());
        if (colonIdx < 0) {
            return null;
        }
        int valueStart = colonIdx + 1;
        while (valueStart < json.length() && Character.isWhitespace(json.charAt(valueStart))) {
            valueStart++;
        }
        if (valueStart < json.length() && json.charAt(valueStart) == '"') {
            int quoteStart = valueStart + 1;
            int quoteEnd = json.indexOf('"', quoteStart);
            return quoteEnd < 0 ? null : json.substring(quoteStart, quoteEnd);
        }
        int valueEnd = valueStart;
        while (valueEnd < json.length() && ",}\n\r".indexOf(json.charAt(valueEnd)) < 0) {
            valueEnd++;
        }
        String raw = json.substring(valueStart, valueEnd).trim();
        return raw.isEmpty() ? null : raw;
    }
}
