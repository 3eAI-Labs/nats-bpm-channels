package com.threeai.nats.core.headers;

/**
 * Meta-header names appended by {@link com.threeai.nats.core.dlq.DlqPublisher} on every
 * DLQ-routed message (contract-fix #1, BR-SUB-001/FR-C1/US-C1).
 */
public final class DlqHeaders {

    public static final String ORIGINAL_SUBJECT = "X-Cadenzaflow-Dlq-Original-Subject";
    public static final String DELIVERY_COUNT = "X-Cadenzaflow-Dlq-Delivery-Count";
    /** DP-6: carries only the {@link com.threeai.nats.core.dlq.DlqReason} code — never payload/PII. */
    public static final String REASON = "X-Cadenzaflow-Dlq-Reason";
    public static final String TIMESTAMP = "X-Cadenzaflow-Dlq-Timestamp";

    private DlqHeaders() {
    }
}
