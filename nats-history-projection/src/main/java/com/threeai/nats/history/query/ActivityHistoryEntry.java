package com.threeai.nats.history.query;

import java.time.Instant;

/** {@code openapi.yaml} {@code ActivityInstanceHistory} schema. {@code assignee} is RESTRICTED/PII. */
public record ActivityHistoryEntry(String activityId, String activityType, String assignee,
        Instant startTime, Instant endTime) {

    ActivityHistoryEntry masked() {
        return new ActivityHistoryEntry(activityId, activityType, PiiMaskingService.MASK_PLACEHOLDER, startTime, endTime);
    }
}
