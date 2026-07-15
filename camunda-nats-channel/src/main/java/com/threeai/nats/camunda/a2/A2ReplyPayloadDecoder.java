package com.threeai.nats.camunda.a2;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import io.nats.client.Message;

/**
 * Decodes {@code jobs.<topic>.reply} messages per the asyncapi {@code JobSuccessReply} /
 * {@code JobBpmnErrorReply} / {@code JobTransientFailureReply} schemas.
 *
 * <p><b>CODER-QUESTION (see CODER-QUESTIONS list):</b> the asyncapi contract fixes
 * {@code contentType: application/octet-stream} for {@code JobSuccessReply} and
 * {@code application/json} for BOTH {@code JobBpmnErrorReply} and
 * {@code JobTransientFailureReply} — it does not name an explicit discriminator header between
 * the latter two. This decoder classifies by {@code Content-Type} first (octet-stream/absent =
 * SUCCESS), then, for JSON bodies, by the presence of the {@code errorCode} field (required by
 * {@code BpmnErrorPayload}, absent from {@code TransientFailurePayload}). This is an
 * implementation choice, not an explicitly mandated wire rule — flagged for architect review.
 */
final class A2ReplyPayloadDecoder {

    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String JSON_CONTENT_TYPE = "json";

    private A2ReplyPayloadDecoder() {
    }

    static ReplyType classify(Message msg) {
        String contentType = headerValue(msg, CONTENT_TYPE_HEADER);
        if (contentType == null || !contentType.toLowerCase(java.util.Locale.ROOT).contains(JSON_CONTENT_TYPE)) {
            return ReplyType.SUCCESS;
        }
        String body = bodyAsString(msg);
        return extractJsonField(body, "errorCode") != null ? ReplyType.BPMN_ERROR : ReplyType.TRANSIENT;
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

    /**
     * CODER-QUESTION: {@code TransientFailurePayload} (asyncapi) does not define a
     * retry-timeout/retry-duration field — this repo has no wire signal for it. Defaults to a
     * fixed 5s residual delay (same order of magnitude as the adapters' own backoff floor)
     * pending an explicit contract decision.
     */
    static long retryTimeoutOf(Message msg) {
        return 5000L;
    }

    private static String headerValue(Message msg, String name) {
        return msg.getHeaders() != null ? msg.getHeaders().getLast(name) : null;
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
