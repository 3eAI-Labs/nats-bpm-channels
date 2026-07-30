package com.threeai.nats.cadenzaflow.outbound;

import java.util.UUID;

import com.threeai.nats.core.headers.BpmHeaders;
import io.nats.client.impl.Headers;
import org.cadenzaflow.bpm.engine.delegate.DelegateExecution;

/**
 * Extracts the three mandatory CadenzaFlow connector headers from a {@link DelegateExecution}
 * and delegates construction to the engine-neutral {@link BpmHeaders} builder.
 */
public final class CadenzaflowHeaderBinder {

    static final String TRACE_ID_VARIABLE = "traceId";

    private CadenzaflowHeaderBinder() {
    }

    /**
     * Build mandatory headers for an outbound NATS message.
     *
     * @param execution current delegate execution
     * @param explicitIdempotencyKey BPMN field value (may be null/blank — default is derived)
     */
    public static Headers from(DelegateExecution execution, String explicitIdempotencyKey) {
        return BpmHeaders.build(
                resolveTraceId(execution),
                execution.getProcessBusinessKey(),
                resolveIdempotencyKey(execution, explicitIdempotencyKey));
    }

    private static String resolveTraceId(DelegateExecution execution) {
        Object value = execution.getVariable(TRACE_ID_VARIABLE);
        if (value instanceof String str && !str.isBlank()) {
            return str;
        }
        return UUID.randomUUID().toString();
    }

    private static String resolveIdempotencyKey(DelegateExecution execution, String explicit) {
        if (explicit != null && !explicit.isBlank()) {
            return explicit;
        }
        return execution.getProcessInstanceId() + ":" + execution.getActivityInstanceId();
    }
}
