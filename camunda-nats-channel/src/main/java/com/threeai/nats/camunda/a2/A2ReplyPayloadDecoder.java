package com.threeai.nats.camunda.a2;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
 *
 * <p><b>Top-level-only JSON parsing (Sentinel Phase 6 follow-up fix F-1, Levent karari
 * 2026-07-15):</b> field extraction used to be a depth-unaware string search
 * ({@code json.indexOf("\"" + fieldName + "\"")}) that could match a same-named key nested
 * inside an object-valued field (asyncapi permits nested objects, e.g. {@code BpmnErrorPayload
 * .variables}, {@code additionalProperties: true}) — a payload shaped like
 * {@code {"data":{"type":"BPMN_ERROR"},"type":"SUCCESS"}} could misread the nested {@code type}
 * as the wire-critical top-level discriminator. Field extraction now parses the body into a
 * Jackson {@link JsonNode} tree once per call and reads ONLY direct children of the root object
 * ({@link JsonNode#get(String)} never descends into nested objects/arrays), so a same-named key
 * nested inside any field can no longer shadow the top-level value. Malformed or non-object JSON
 * still degrades to {@code null}/defaults rather than throwing — this class never throws;
 * callers route missing/invalid data to the DLQ instead of receiving an exception.
 */
final class A2ReplyPayloadDecoder {

    private static final String TYPE_FIELD = "type";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private A2ReplyPayloadDecoder() {
    }

    /** @return the reply's declared type, or {@link Optional#empty()} if missing/unrecognized (routes to DLQ). */
    static Optional<ReplyType> classify(Message msg) {
        String typeValue = topLevelField(bodyAsString(msg), TYPE_FIELD);
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
        return topLevelField(bodyAsString(msg), "errorCode");
    }

    static String errorMessageOf(Message msg) {
        return topLevelField(bodyAsString(msg), "errorMessage");
    }

    static String errorDetailsOf(Message msg) {
        return topLevelField(bodyAsString(msg), "errorDetails");
    }

    static int retriesOf(Message msg) {
        String retries = topLevelField(bodyAsString(msg), "retries");
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

    /**
     * Reads {@code fieldName} from the JSON root object ONLY — {@link JsonNode#get(String)}
     * looks up a direct child of the root, never descending into nested objects/arrays, so a
     * same-named key inside a nested value (e.g. {@code variables.type}) cannot shadow the
     * top-level field. Returns {@code null} for a missing field, an explicit JSON {@code null},
     * a non-object body, or malformed JSON — this method never throws.
     */
    private static String topLevelField(String json, String fieldName) {
        if (json == null || json.isBlank()) {
            return null;
        }
        JsonNode root;
        try {
            root = OBJECT_MAPPER.readTree(json);
        } catch (JsonProcessingException malformed) {
            return null;
        }
        if (root == null || !root.isObject()) {
            return null;
        }
        JsonNode value = root.get(fieldName);
        if (value == null || value.isNull()) {
            return null;
        }
        return value.isContainerNode() ? value.toString() : value.asText();
    }
}
