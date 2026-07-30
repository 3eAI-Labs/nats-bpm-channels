package com.threeai.nats.core.dlq;

/**
 * DLQ routing reasons produced by this repository.
 *
 * <p>Worker-side delivery-budget-exceeded routing on {@code jobs.<topic>} (Matrix 1.A row 5,
 * {@code BUS_JOB_DELIVERY_BUDGET_EXCEEDED}) is out of scope — the worker implementation lives
 * outside this repository. This enum only
 * covers the DLQ paths this repository itself produces: engine-inbound consumer delivery
 * budget exceeded, empty message body, and (A2 reply path only) an invalid reply discriminator.
 */
public enum DlqReason {

    /** Matrix 1.B row 3 (engine-inbound consumer, deliveryCount &gt; maxDeliver). BR-A2-009/BR-FLW-003. */
    DELIVERY_BUDGET_EXCEEDED("BUS_REPLY_DELIVERY_BUDGET_EXCEEDED"),
    /** BAQ-5 decision — contract-fix #5. BR-SUB-007. */
    EMPTY_MESSAGE_BODY("VAL_EMPTY_MESSAGE_BODY"),
    /**
     * QA review fix (reply discriminator, decision 2026-07-15) — {@code
     * jobs.<topic>.reply} payload is missing the mandatory {@code type} field, or its value is
     * not one of {@code SUCCESS|BPMN_ERROR|TRANSIENT}. {@code A2ReplyPayloadDecoder} reads this
     * field instead of the old errorCode-presence heuristic.
     */
    INVALID_REPLY_TYPE("VAL_INVALID_REPLY_TYPE"),
    /** Increment 2 — {@code HistoryProjectionConsumer} deliveryCount &gt; maxDeliver. */
    HISTORY_DELIVERY_BUDGET_EXCEEDED("BUS_HISTORY_DELIVERY_BUDGET_EXCEEDED"),
    /** Increment 2 — envelope does not match the asyncapi contract. */
    HISTORY_SCHEMA_DRIFT("SYS_PROJECTION_SCHEMA_DRIFT"),
    /**
     * Increment 4 (decision D-G') — Flowable {@code NatsOutboundEventChannelAdapter}/
     * {@code JetStreamOutboundEventChannelAdapter} publish attempt failed (broker unreachable, publish
     * exception). Routed to DLQ as a last-resort custody-transfer instead of a silent message loss.
     */
    OUTBOUND_PUBLISH_FAILED("SYS_OUTBOUND_PUBLISH_FAILED");

    private final String exceptionCode;

    DlqReason(String exceptionCode) {
        this.exceptionCode = exceptionCode;
    }

    /** DP-6: only this code string is written to the DLQ header — never the payload/business-key value. */
    public String headerValue() {
        return exceptionCode;
    }
}
