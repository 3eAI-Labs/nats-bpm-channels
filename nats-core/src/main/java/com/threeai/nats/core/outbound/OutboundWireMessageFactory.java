package com.threeai.nats.core.outbound;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import com.threeai.nats.core.headers.BpmHeaders;
import com.threeai.nats.core.headers.OutboundHeaders;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.NatsJetStreamConstants;

/**
 * Builds the single outbound-handoff wire-contract shape (docs/09-outbound-handoff.md §3 "ortak
 * wire-contract") shared by both publish paths — the critical outbox+relay path and the
 * best-effort post-commit path. Identity-only envelope by default (processInstanceId/businessKey/
 * messageType/engineId), plus an OPTIONAL {@code variables} object when the tenant's
 * {@link OutboundClassificationProperties#variableAllowlistFor} opts a type in (PII minimization
 * by default, DP-1).
 *
 * <p><b>CODER-NOTE (no Jackson dependency):</b> {@code nats-core} carries no JSON library
 * dependency ({@code pom.xml} verified) — this factory hand-rolls minimal JSON encoding, the SAME
 * pattern {@code A2JobMessageFactory} (camunda-nats-channel) already established for exactly this
 * reason, rather than silently adding a new dependency to a shared, engine-neutral module.
 */
public final class OutboundWireMessageFactory {

    private OutboundWireMessageFactory() {
    }

    /**
     * @param dedupId the {@code Nats-Msg-Id} value: the outbox row's own UUID for the critical
     *                path (stable across relay retries — proper idempotent redelivery), or a
     *                freshly minted UUID for the best-effort path (no retry, so no stability
     *                requirement — A2/history post-commit precedent).
     */
    public static NatsMessage buildMessage(OutboundMessageDraft draft, String dedupId) {
        Headers headers = BpmHeaders.build(draft.traceId(), draft.businessKey(), null);
        headers.add(NatsJetStreamConstants.MSG_ID_HDR, dedupId);
        headers.add(OutboundHeaders.ENGINE_ID, draft.engineId());
        headers.add(OutboundHeaders.MESSAGE_TYPE, draft.messageType());
        headers.add(OutboundHeaders.PROCESS_INSTANCE_ID, draft.processInstanceId());
        return NatsMessage.builder()
                .subject(draft.subject())
                .headers(headers)
                .data(draft.payload())
                .build();
    }

    /**
     * Convenience overload — mints a fresh dedup id (best-effort/at-most-once path; no relay
     * retries this message, so a stable id across attempts is unnecessary).
     */
    public static NatsMessage buildMessage(OutboundMessageDraft draft) {
        return buildMessage(draft, UUID.randomUUID().toString());
    }

    /**
     * Identity-only envelope, plus an optional {@code variables} object. Dependency-free JSON
     * encoding (see class Javadoc CODER-NOTE) — mirrors {@code A2JobMessageFactory#serialize}.
     */
    public static byte[] buildPayload(String engineId, String messageType, String processInstanceId,
            String businessKey, Map<String, Object> variables) {
        StringBuilder json = new StringBuilder();
        json.append("{\"engineId\":\"").append(escapeJsonString(engineId)).append("\",")
                .append("\"messageType\":\"").append(escapeJsonString(messageType)).append("\",")
                .append("\"processInstanceId\":\"").append(escapeJsonString(processInstanceId)).append('"');
        if (businessKey != null && !businessKey.isBlank()) {
            json.append(",\"businessKey\":\"").append(escapeJsonString(businessKey)).append('"');
        }
        if (variables != null && !variables.isEmpty()) {
            json.append(",\"variables\":{");
            boolean first = true;
            for (Map.Entry<String, Object> entry : variables.entrySet()) {
                if (!first) {
                    json.append(',');
                }
                json.append('"').append(escapeJsonString(entry.getKey())).append("\":")
                        .append(jsonValue(entry.getValue()));
                first = false;
            }
            json.append('}');
        }
        json.append('}');
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static String jsonValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        return "\"" + escapeJsonString(value.toString()) + "\"";
    }

    private static String escapeJsonString(String raw) {
        if (raw == null) {
            return "";
        }
        StringBuilder escaped = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (c < 0x20) {
                        escaped.append(String.format("\\u%04x", (int) c));
                    } else {
                        escaped.append(c);
                    }
                }
            }
        }
        return escaped.toString();
    }
}
