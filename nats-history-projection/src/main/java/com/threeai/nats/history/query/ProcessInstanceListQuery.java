package com.threeai.nats.history.query;

import java.time.Instant;

/**
 * çekirdek-4 desen 2 (businessKey) / desen 3 (zaman-aralığı+definition) filter — {@code
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
