package com.threeai.nats.history.query;

import java.time.Instant;

/**
 * core-4 pattern 2 (businessKey) / pattern 3 (time-range+definition) filter — {@code
 * openapi.yaml} {@code /history/process-instances} query parameters. At least one filter is
 * required; a filterless full-scan request is {@code VAL_QUERY_UNSUPPORTED_PATTERN}.
 */
public record ProcessInstanceListQuery(
        String businessKey,
        String processDefinitionKey,
        Instant startedAfter,
        Instant startedBefore,
        PageRequest page) {

    public boolean hasAnyFilter() {
        return (businessKey != null && !businessKey.isBlank())
                || (processDefinitionKey != null && !processDefinitionKey.isBlank())
                || startedAfter != null || startedBefore != null;
    }
}
