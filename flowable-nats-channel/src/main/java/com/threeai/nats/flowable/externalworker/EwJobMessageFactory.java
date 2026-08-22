package com.threeai.nats.flowable.externalworker;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import com.threeai.nats.core.headers.BpmHeaders;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.NatsJetStreamConstants;
import org.flowable.job.api.ExternalWorkerJob;

/**
 * Builds the {@code ewjobs.<topic>} dispatch message (docs/11 D-I'v2). Shared by the fast path
 * (interceptor post-commit publish) and the sweep re-publish so the wire format cannot drift.
 *
 * <p><b>Identity:</b> the envelope {@code jobId} field is the identity source — NOT
 * {@code Nats-Msg-Id}, which is the composite generation-scoped dedup key
 * {@code <jobId>#<nonce>} (decision N-red-2: the sweep's re-publish carries a new nonce and is
 * therefore never swallowed by the stream's duplicate window, while a same-generation duplicate
 * still deduplicates).
 *
 * <p><b>businessKey:</b> present only on the fast path (read in-tx from the execution by the
 * interceptor); the sweep path has no execution context — the one remaining documented gap
 * (A2 precedent, docs/11 D-I'v2).
 *
 * <p>JSON is hand-rolled dependency-free, consistent with {@code A2JobMessageFactory}.
 */
final class EwJobMessageFactory {

    private EwJobMessageFactory() {
    }

    static NatsMessage build(ExternalWorkerJob job, String topic, String nonce,
            String businessKey, Map<String, Object> variables) {
        return NatsMessage.builder()
                .subject("ewjobs." + topic)
                .data(serialize(job, topic, businessKey, variables))
                .headers(buildHeaders(job, nonce, businessKey))
                .build();
    }

    private static Headers buildHeaders(ExternalWorkerJob job, String nonce, String businessKey) {
        Headers h = BpmHeaders.build(UUID.randomUUID().toString(), businessKey, job.getId());
        h.add(BpmHeaders.CORRELATION_ID, job.getId());
        h.add(EwHeaders.LOCK_NONCE, nonce);
        h.add(NatsJetStreamConstants.MSG_ID_HDR, job.getId() + "#" + nonce);
        return h;
    }

    private static byte[] serialize(ExternalWorkerJob job, String topic,
            String businessKey, Map<String, Object> variables) {
        StringBuilder json = new StringBuilder();
        json.append("{\"jobId\":\"").append(escapeJsonString(job.getId())).append("\",")
                .append("\"topic\":\"").append(escapeJsonString(topic)).append("\",")
                .append("\"processInstanceId\":\"")
                .append(escapeJsonString(nullToEmpty(job.getProcessInstanceId()))).append('"');
        if (job.getTenantId() != null && !job.getTenantId().isEmpty()) {
            json.append(",\"tenantId\":\"").append(escapeJsonString(job.getTenantId())).append('"');
        }
        if (businessKey != null && !businessKey.isEmpty()) {
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

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    /** Minimal dependency-free JSON value encoder (A2JobMessageFactory parity). */
    private static String jsonValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        return '"' + escapeJsonString(value.toString()) + '"';
    }

    private static String escapeJsonString(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }
}
