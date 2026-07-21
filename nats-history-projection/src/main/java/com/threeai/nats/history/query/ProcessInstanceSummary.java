package com.threeai.nats.history.query;

import java.time.Instant;

/**
 * {@code openapi.yaml} {@code ProcessInstanceHistory} schema. {@code startUserId} is
 * RESTRICTED/PII (DP-11) — masked when {@code !ctx.piiViewPermitted()} (DP-15).
 */
public record ProcessInstanceSummary(
        String processInstanceId,
        String processDefinitionKey,
        String businessKey,
        String startUserId,
        Instant startTime,
        Instant endTime,
        String state,
        String cutoverState) {

    ProcessInstanceSummary masked() {
        return new ProcessInstanceSummary(processInstanceId, processDefinitionKey, businessKey,
                PiiMaskingService.MASK_PLACEHOLDER, startTime, endTime, state, cutoverState);
    }
}
