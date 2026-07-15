package com.threeai.nats.core.dlq;

/**
 * DLQ routing reasons produced by this repository (LLD 03_classes/1_nats_core_common.md §2.1).
 *
 * <p>Worker-side delivery-budget-exceeded routing on {@code jobs.<topic>} (Matrix 1.A row 5,
 * {@code BUS_JOB_DELIVERY_BUDGET_EXCEEDED}) is out of scope — the worker implementation lives
 * outside this repository (see {@code DECISION_MATRIX.md} 1.A header note). This enum only
 * covers the two DLQ paths this repository itself produces: engine-inbound consumer delivery
 * budget exceeded, and empty message body.
 */
public enum DlqReason {

    /** Matrix 1.B row 3 (engine-inbound consumer, deliveryCount &gt; maxDeliver). BR-A2-009/BR-FLW-003. */
    DELIVERY_BUDGET_EXCEEDED("BUS_REPLY_DELIVERY_BUDGET_EXCEEDED"),
    /** BAQ-5 decision — contract-fix #5. BR-SUB-007. */
    EMPTY_MESSAGE_BODY("VAL_EMPTY_MESSAGE_BODY");

    private final String exceptionCode;

    DlqReason(String exceptionCode) {
        this.exceptionCode = exceptionCode;
    }

    /** DP-6: only this code string is written to the DLQ header — never the payload/business-key value. */
    public String headerValue() {
        return exceptionCode;
    }
}
