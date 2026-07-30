package com.threeai.nats.history.projection;

import java.time.Instant;
import java.util.Map;

/**
 * Shared envelope for the entity-lifecycle tables — field mapping is done per
 * {@code historyClass} in {@link HistoryProjectionConsumer} (this record is a CARRIER only, it
 * holds no business rules).
 *
 * @param engineId          engine instance id
 * @param entityId          {@code processInstanceId|activityInstanceId|...} — per class
 * @param processInstanceId core-4 pattern 4 query key
 * @param streamSequence    ADR-0012 merge-upsert tie-break authority
 * @param eventTime         display-only (ADR-0012)
 * @param fields            class-specific columns
 */
public record EntityHistoryRecord(
        String engineId,
        String entityId,
        String processInstanceId,
        long streamSequence,
        Instant eventTime,
        Map<String, Object> fields) {
}
