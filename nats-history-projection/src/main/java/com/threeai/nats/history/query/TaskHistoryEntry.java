package com.threeai.nats.history.query;

import java.time.Instant;

/** {@code openapi.yaml} {@code TaskInstanceHistory} schema. {@code assignee}/{@code owner} RESTRICTED/PII. */
public record TaskHistoryEntry(String taskId, String name, String assignee, String owner,
        Instant startTime, Instant endTime) {

    TaskHistoryEntry masked() {
        return new TaskHistoryEntry(taskId, name, PiiMaskingService.MASK_PLACEHOLDER,
                PiiMaskingService.MASK_PLACEHOLDER, startTime, endTime);
    }
}
