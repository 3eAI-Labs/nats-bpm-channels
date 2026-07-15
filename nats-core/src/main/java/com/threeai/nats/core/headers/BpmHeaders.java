package com.threeai.nats.core.headers;

import com.threeai.nats.core.NatsHeaderUtils;
import io.nats.client.Message;
import io.nats.client.impl.Headers;

/**
 * BPM-engine-neutral NATS header builder for the CadenzaFlow connector wire contract
 * (§4.1 mandatory headers). Engine-specific extraction (variable lookup, business key,
 * activity-instance composition) lives in each engine module's binder.
 */
public final class BpmHeaders {

    public static final String TRACE_ID = "X-Cadenzaflow-Trace-Id";
    public static final String BUSINESS_KEY = "X-Cadenzaflow-Business-Key";
    public static final String IDEMPOTENCY_KEY = "X-Cadenzaflow-Idempotency-Key";

    /** LLD 03_classes/1_nats_core_common.md §1.1 (FR-C7/IR-2) — A2 completion/DLQ correlation headers. */
    public static final String CORRELATION_ID = "X-Cadenzaflow-Correlation-Id";
    public static final String REPLY_SUBJECT = "X-Cadenzaflow-Reply-Subject";

    /**
     * Legacy write-side header name. READ-ONLY fallback (BR-SUB-006) — never written by this
     * codebase, only read for backward-compatibility with older producers.
     */
    public static final String LEGACY_TRACE_ID = "X-Trace-Id";

    private BpmHeaders() {
    }

    /**
     * Reads {@link #TRACE_ID}; falls back to {@link #LEGACY_TRACE_ID} only if {@link #TRACE_ID}
     * is absent. Never writes {@link #LEGACY_TRACE_ID} (contract-fix #4, BR-SUB-006/FR-C7).
     */
    public static String extractTraceIdWithFallback(Message msg) {
        String traceId = NatsHeaderUtils.extractHeader(msg, TRACE_ID);
        return traceId != null ? traceId : NatsHeaderUtils.extractHeader(msg, LEGACY_TRACE_ID);
    }

    /**
     * Builds NATS headers for the three mandatory CadenzaFlow connector headers.
     * Null or blank values are skipped (header is not added).
     */
    public static Headers build(String traceId, String businessKey, String idempotencyKey) {
        Headers headers = new Headers();
        addIfPresent(headers, TRACE_ID, traceId);
        addIfPresent(headers, BUSINESS_KEY, businessKey);
        addIfPresent(headers, IDEMPOTENCY_KEY, idempotencyKey);
        return headers;
    }

    private static void addIfPresent(Headers headers, String name, String value) {
        if (value != null && !value.isBlank()) {
            headers.add(name, value);
        }
    }
}
