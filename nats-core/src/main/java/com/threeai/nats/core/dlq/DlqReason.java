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
    /**
     * Reply-variables increment (decision 2026-08-19) — the reply carries a structured
     * {@code variables}/{@code localVariables} object that cannot be converted to engine
     * variables: not a JSON object, an entry that is not an object, an unsupported {@code type},
     * an undecodable value, or {@code localVariables} on a BPMN_ERROR reply (the engine's
     * {@code handleBpmnError} API has no local-variables parameter). Completing WITHOUT the
     * variables the worker asked for would let the process continue on wrong data — a gateway
     * reading a variable that was never written takes the wrong path silently — so the reply is
     * routed to the DLQ instead.
     */
    INVALID_REPLY_VARIABLES("VAL_INVALID_REPLY_VARIABLES"),
    /** Increment 2 — {@code HistoryProjectionConsumer} deliveryCount &gt; maxDeliver. */
    HISTORY_DELIVERY_BUDGET_EXCEEDED("BUS_HISTORY_DELIVERY_BUDGET_EXCEEDED"),
    /** Increment 2 — envelope does not match the asyncapi contract. */
    HISTORY_SCHEMA_DRIFT("SYS_PROJECTION_SCHEMA_DRIFT"),
    /**
     * Increment 4 (decision D-G') — Flowable {@code NatsOutboundEventChannelAdapter}/
     * {@code JetStreamOutboundEventChannelAdapter} publish attempt failed (broker unreachable, publish
     * exception). Routed to DLQ as a last-resort custody-transfer instead of a silent message loss.
     */
    OUTBOUND_PUBLISH_FAILED("SYS_OUTBOUND_PUBLISH_FAILED"),
    /**
     * Flowable external-worker dispatch (docs/11 §2.3, decision N-red-1, 2026-08-22) — the reply
     * is missing the {@code X-Cadenzaflow-Lock-Nonce} header, or its value fails the
     * {@code ^[A-Za-z0-9_-]{8,}$} token rule, or the envelope {@code jobId} is empty. The lock
     * token is constructed BRIDGE-side from the configured sentinel worker id plus this nonce —
     * the sentinel half never travels on the wire — so a reply without a valid nonce cannot be
     * classified against any lock generation and MUST be dead-lettered, never silently acked:
     * acking it would leave the job locked until reset and re-dispatched forever with zero errors.
     */
    MISSING_LOCK_NONCE("VAL_MISSING_LOCK_NONCE"),
    /**
     * Flowable external-worker dispatch (docs/11 D-C'v2, 2026-08-22) — the reply uses a shape the
     * Flowable 7.1.0 completion API cannot express: {@code localVariables} on any reply (no
     * engine parameter exists). Mirrors {@code INVALID_REPLY_VARIABLES}'s rationale: degrading
     * silently would let the process continue on wrong data.
     */
    UNSUPPORTED_REPLY_SHAPE("VAL_UNSUPPORTED_REPLY_SHAPE");

    private final String exceptionCode;

    DlqReason(String exceptionCode) {
        this.exceptionCode = exceptionCode;
    }

    /** DP-6: only this code string is written to the DLQ header — never the payload/business-key value. */
    public String headerValue() {
        return exceptionCode;
    }
}
