package com.threeai.nats.camunda.a2;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import com.threeai.nats.core.headers.BpmHeaders;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.NatsJetStreamConstants;
import org.camunda.bpm.engine.impl.persistence.entity.ExternalTaskEntity;

/**
 * Shared {@code jobs.<topic>} message construction — used by both {@link A2PostCommitPublisher}
 * (fast-path dispatch) and {@link A2OrphanSweep} (cold re-publish) so the wire format never
 * drifts between the two producers.
 *
 * <p><b>CODER-NOTE:</b> process-variable/payload serialization is explicitly tenant-defined and
 * out of this repo's contract (asyncapi {@code OpaqueBusinessPayload}) — see CODER-QUESTIONS.
 * traceId/idempotencyKey are not first-class {@code ExternalTaskEntity} fields; reading them
 * from process variables here would require a DB round-trip BR-A2-004 forbids for the
 * post-commit path, so a fresh trace id is minted per dispatch/re-dispatch and
 * {@code externalTaskId} (already the natural, stable identity) is reused as the idempotency key.
 *
 * <p><b>Sentinel Phase 5.5 QA fix (item 5, Levent karari 2026-07-15):</b> {@link
 * #build(ExternalTaskEntity, Map)} appends an OPTIONAL {@code variables} object to this
 * identity-only envelope — only when the caller passes a non-empty captured-variables map (see
 * {@code A2ExternalTaskBehavior#captureAllowlistedVariables}). Default behavior (empty map,
 * including the no-arg {@link #build(ExternalTaskEntity)} overload {@link A2OrphanSweep} still
 * uses) is UNCHANGED — the sweep's cold re-publish path does not carry captured variables (it
 * only has the bare {@code ExternalTaskEntity} row, no execution/variable-scope context); this is
 * an accepted, documented gap, not an oversight.
 */
final class A2JobMessageFactory {

    private A2JobMessageFactory() {
    }

    /** Convenience overload — identity-only envelope (no captured variables). */
    static NatsMessage build(ExternalTaskEntity task) {
        return build(task, Map.of());
    }

    static NatsMessage build(ExternalTaskEntity task, Map<String, Object> capturedVariables) {
        String subject = "jobs." + task.getTopicName();
        return NatsMessage.builder()
                .subject(subject)
                .data(serialize(task, capturedVariables))
                .headers(buildHeaders(task))
                .build();
    }

    private static Headers buildHeaders(ExternalTaskEntity task) {
        Headers h = BpmHeaders.build(UUID.randomUUID().toString(), task.getBusinessKey(), task.getId());
        h.add(NatsJetStreamConstants.MSG_ID_HDR, task.getId()); // A2 dedup key = externalTaskId (IR-3)
        return h;
    }

    private static byte[] serialize(ExternalTaskEntity task, Map<String, Object> capturedVariables) {
        String businessKey = task.getBusinessKey() != null ? task.getBusinessKey() : "";
        StringBuilder json = new StringBuilder();
        json.append("{\"externalTaskId\":\"").append(task.getId()).append("\",")
                .append("\"topic\":\"").append(task.getTopicName()).append("\",")
                .append("\"businessKey\":\"").append(businessKey).append('"');
        if (capturedVariables != null && !capturedVariables.isEmpty()) {
            json.append(",\"variables\":{");
            boolean first = true;
            for (Map.Entry<String, Object> entry : capturedVariables.entrySet()) {
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

    /**
     * Minimal, dependency-free JSON value encoder for the {@code variables} object (this module
     * has no Jackson dependency, consistent with the rest of the a2 package's lightweight JSON
     * handling).
     */
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
