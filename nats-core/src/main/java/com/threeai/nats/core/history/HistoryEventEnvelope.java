package com.threeai.nats.core.history;

import java.time.Instant;

/**
 * Java-side counterpart of the AsyncAPI {@code components/schemas/HistoryEventPayload} schema. The
 * {@code payload} field is an OPAQUE string (ARCH-Q1) — relay/consumer carry it WITHOUT
 * deserializing it; class-specific field extraction happens only inside the
 * {@code ProjectionStore} call (NOT this class's responsibility).
 *
 * @param engineId          engine instance id (INTERNAL)
 * @param historyClass      ACT_HI event class (PUBLIC)
 * @param eventType         event type (INTERNAL; part of the dedup key)
 * @param historyEventId    engine history event surrogate id (INTERNAL; part of the dedup key)
 * @param processInstanceId process-instance id (PSEUDONYMOUS; partition + ordering key)
 * @param businessKey       business identifier (CONFIDENTIAL/conditional PII; nullable; NEVER
 *                          embedded in the subject, DP-2)
 * @param streamSequence    JetStream-assigned stream sequence — ABSENT on the publisher side (0),
 *                          filled from {@code msg.metaData()} on the consumer side (merge-upsert
 *                          tie-break/version field, ADR-0012)
 * @param eventTime         source {@code HistoryEvent} timestamp (display-only, ADR-0012)
 * @param payload           opaque variable/event body (RESTRICTED/PII, tenant-defined; wire form
 *                          per ARCH-Q1 — inline scalar JSON or a reference)
 */
public record HistoryEventEnvelope(
        String engineId,
        String historyClass,
        String eventType,
        String historyEventId,
        String processInstanceId,
        String businessKey,
        long streamSequence,
        Instant eventTime,
        String payload) {

    /** Wire-contract dedup value: {@code <historyEventId>:<eventType>} (IR-3 / BR-HDL-006). */
    public String dedupId() {
        return historyEventId + ":" + eventType;
    }

    /** Subject the envelope maps to: {@code history.<engineId>.<class>.<processInstanceId>}. */
    public String subject() {
        return "history." + engineId + "." + historyClass + "." + processInstanceId;
    }

    /**
     * Consumer-side copy carrying the JetStream-assigned stream sequence (ADR-0012 tie-break
     * authority) — publisher-side envelopes always carry {@code streamSequence == 0}.
     */
    public HistoryEventEnvelope withStreamSequence(long resolvedStreamSequence) {
        return new HistoryEventEnvelope(engineId, historyClass, eventType, historyEventId,
                processInstanceId, businessKey, resolvedStreamSequence, eventTime, payload);
    }
}
