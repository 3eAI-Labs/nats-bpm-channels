package com.threeai.nats.history.projection;

import java.time.Instant;
import java.util.Map;

/**
 * Append-only log tabloları (`DB_SCHEMA.md` §2.4) için ortak zarf. `04_interfaces/2_projection_dtos.md` §2.
 *
 * @param engineId          motor örneği kimliği
 * @param processInstanceId çekirdek-4 desen 4 sorgu anahtarı
 * @param historyEventId    dedup anahtarının parçası (`unq_*_dedup` benzersizlik kısıtı)
 * @param eventType         dedup anahtarının parçası
 * @param streamSequence    sıralama/görüntüleme amaçlı (append-only için tie-break kavramı YOK)
 * @param eventTime         partition anahtarı (append-only tablolarda doğrudan)
 * @param fields            sınıf-özgü kolonlar
 */
public record LogHistoryRecord(
        String engineId,
        String processInstanceId,
        String historyEventId,
        String eventType,
        long streamSequence,
        Instant eventTime,
        Map<String, Object> fields) {
}
