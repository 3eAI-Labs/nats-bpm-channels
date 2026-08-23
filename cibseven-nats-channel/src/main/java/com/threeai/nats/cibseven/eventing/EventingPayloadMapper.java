package com.threeai.nats.cibseven.eventing;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Payload semantics of the definition path (docs/12 D-C v2): Jackson TOP-LEVEL-ONLY reads
 * mapped through the {@code EventPayloadTypes} dictionary. Also hosts the replacement for the
 * legacy {@code indexOf} field scan (D-F v2) — a documented BEHAVIOR CHANGE: nested fields are
 * no longer found, and non-string scalars now return their correct textual value (the old scan
 * returned the NEXT field's name for numeric values).
 */
public final class EventingPayloadMapper {

    /** Permanent, non-retryable mapping failure — JetStream path routes it to the DLQ. */
    public static class MissingCorrelationValueException extends RuntimeException {
        public MissingCorrelationValueException(String field) {
            super("[VAL_EVENTING_MISSING_CORRELATION_VALUE] payload lacks correlation field '"
                    + field + "'");
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private EventingPayloadMapper() {
    }

    /** @return the top-level object node, or null when the payload is not a JSON object. */
    public static JsonNode parse(String payload) {
        try {
            JsonNode root = MAPPER.readTree(payload);
            return root != null && root.isObject() ? root : null;
        } catch (Exception e) {
            return null;
        }
    }

    /** Declared payload fields -> typed process variables; absent fields are skipped. */
    public static Map<String, Object> typedVariables(JsonNode root, Map<String, String> payloadTypes) {
        Map<String, Object> variables = new LinkedHashMap<>();
        if (root == null) {
            return variables;
        }
        for (Map.Entry<String, String> entry : payloadTypes.entrySet()) {
            JsonNode node = root.get(entry.getKey());
            if (node == null || node.isNull()) {
                continue;
            }
            variables.put(entry.getKey(), convert(node, entry.getValue()));
        }
        return variables;
    }

    /**
     * Declared correlation parameters -> typed match values. A missing field is a PERMANENT
     * failure (the correlation could silently mismatch otherwise) and throws.
     */
    public static Map<String, Object> correlationValues(JsonNode root, Map<String, String> correlationTypes) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : correlationTypes.entrySet()) {
            JsonNode node = root == null ? null : root.get(entry.getKey());
            if (node == null || node.isNull()) {
                throw new MissingCorrelationValueException(entry.getKey());
            }
            values.put(entry.getKey(), convert(node, entry.getValue()));
        }
        return values;
    }

    /**
     * Legacy business-key replacement: TOP-LEVEL scalar as text. Nested fields: null (was:
     * found by the depth-unaware scan). Numeric/boolean scalars: correct textual value (was:
     * the next field's NAME). Containers: null.
     */
    public static String topLevelScalarAsText(String payload, String field) {
        JsonNode root = parse(payload);
        if (root == null) {
            return null;
        }
        JsonNode node = root.get(field);
        if (node == null || node.isNull() || node.isContainerNode()) {
            return null;
        }
        return node.asText();
    }

    private static Object convert(JsonNode node, String type) {
        return switch (type) {
            case "string" -> node.asText();
            case "integer" -> node.asInt();
            case "long" -> node.asLong();
            case "double" -> node.asDouble();
            case "boolean" -> node.asBoolean();
            case "json" -> node.toString(); // v1: raw String (docs/12 D-A v2)
            default -> node.asText();       // unreachable — parser enforces the dictionary
        };
    }
}
