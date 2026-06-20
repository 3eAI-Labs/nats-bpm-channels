package com.threeai.nats.core.headers;

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

    private BpmHeaders() {
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
