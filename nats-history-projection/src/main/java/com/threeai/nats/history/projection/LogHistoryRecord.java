package com.threeai.nats.history.projection;

import java.time.Instant;
import java.util.Map;

/**
 * Shared envelope for the append-only log tables.
 *
 * @param engineId          engine instance id
 * @param processInstanceId core-4 pattern 4 query key
 * @param historyEventId    part of the dedup key (`unq_*_dedup` uniqueness constraint)
 * @param eventType         part of the dedup key
 * @param streamSequence    ordering/display only (append-only has NO tie-break concept)
 * @param eventTime         partition key (used directly in append-only tables)
 * @param fields            class-specific columns
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
