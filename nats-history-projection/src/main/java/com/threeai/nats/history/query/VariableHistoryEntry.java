package com.threeai.nats.history.query;

import java.time.Instant;

/**
 * {@code openapi.yaml} {@code VariableHistory} schema. {@code value} is RESTRICTED/PII (DP-9/DP-10)
 * — masked when the caller lacks PII-view permission; {@code masked} is an informational marker
 * mirroring {@code BUS_QUERY_PII_MASKED} in the response body.
 */
public record VariableHistoryEntry(String variableName, String variableType, String value,
        boolean masked, Instant timestamp) {

    VariableHistoryEntry maskedCopy() {
        return new VariableHistoryEntry(variableName, variableType, PiiMaskingService.MASK_PLACEHOLDER, true, timestamp);
    }
}
