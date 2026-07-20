package com.threeai.nats.core.history;

import java.time.Instant;

/**
 * Java-tarafı karşılığı: {@code api/asyncapi.yaml} {@code components/schemas/HistoryEventPayload}
 * (`docs/sentinel/step2/phase4/lld/history-offload/04_interfaces/2_projection_dtos.md` §1). {@code
 * payload} alanı OPAK string'tir (ARCH-Q1) — relay/consumer onu deserialize ETMEDEN taşır; yalnız
 * {@code ProjectionStore} çağrısı sırasında {@code historyClass}'a göre spesifik alan çıkarımı
 * yapılır (bu sınıfın sorumluluğu DEĞİL).
 *
 * @param engineId          motor örneği kimliği (INTERNAL)
 * @param historyClass      ACT_HI event sınıfı (PUBLIC)
 * @param eventType         event tipi (INTERNAL; dedup anahtarının parçası)
 * @param historyEventId    motor history event surrogate id (INTERNAL; dedup anahtarının parçası)
 * @param processInstanceId process-instance id (PSEUDONYMOUS; partition + sıra anahtarı)
 * @param businessKey       iş kimliği (CONFIDENTIAL/koşullu PII; nullable; subject'e GÖMÜLMEZ, DP-2)
 * @param streamSequence    JetStream tarafından atanan stream-sequence — publisher tarafında
 *                          YOKTUR (0), consumer tarafında {@code msg.metaData()}'dan doldurulur
 *                          (merge-upsert tie-break/versiyon alanı, ADR-0012)
 * @param eventTime         kaynak {@code HistoryEvent} zaman damgası (display-only, ADR-0012)
 * @param payload           opak variable/event gövdesi (RESTRICTED/PII, kiracı-tanımlı; taşıma
 *                          biçimi ARCH-Q1 — inline scalar JSON veya referans)
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
