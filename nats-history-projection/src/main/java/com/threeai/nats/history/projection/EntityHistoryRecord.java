package com.threeai.nats.history.projection;

import java.time.Instant;
import java.util.Map;

/**
 * Entity-lifecycle tabloları (`DB_SCHEMA.md` §2.2) için ortak zarf — alan haritalama
 * {@code historyClass}'a göre {@link HistoryProjectionConsumer}'da yapılır (bu record yalnız
 * TAŞIYICI, iş kuralı taşımaz). `04_interfaces/2_projection_dtos.md` §2.
 *
 * @param engineId          motor örneği kimliği
 * @param entityId          {@code processInstanceId|activityInstanceId|...} — sınıfa göre
 * @param processInstanceId çekirdek-4 desen 4 sorgu anahtarı
 * @param streamSequence    ADR-0012 merge-upsert tie-break otoritesi
 * @param eventTime         display-only (ADR-0012)
 * @param fields            sınıf-özgü kolonlar (`DB_SCHEMA.md` §2.2 sütun listesi)
 */
public record EntityHistoryRecord(
        String engineId,
        String entityId,
        String processInstanceId,
        long streamSequence,
        Instant eventTime,
        Map<String, Object> fields) {
}
