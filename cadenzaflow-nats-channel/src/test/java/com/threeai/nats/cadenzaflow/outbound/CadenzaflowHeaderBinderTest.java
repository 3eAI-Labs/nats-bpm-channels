package com.threeai.nats.cadenzaflow.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;

import com.threeai.nats.core.headers.BpmHeaders;
import io.nats.client.impl.Headers;
import org.cadenzaflow.bpm.engine.delegate.DelegateExecution;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CadenzaflowHeaderBinderTest {

    private DelegateExecution execution;

    @BeforeEach
    void setUp() {
        execution = mock(DelegateExecution.class);
        when(execution.getProcessInstanceId()).thenReturn("pi-100");
        when(execution.getActivityInstanceId()).thenReturn("ai-200");
        when(execution.getProcessBusinessKey()).thenReturn("order-42");
    }

    @Test
    void from_traceIdVariablePresent_usesIt() {
        when(execution.getVariable("traceId")).thenReturn("trace-from-var");

        Headers headers = CadenzaflowHeaderBinder.from(execution, null);

        assertThat(headers.getLast(BpmHeaders.TRACE_ID)).isEqualTo("trace-from-var");
    }

    @Test
    void from_traceIdVariableMissing_generatesUuid() {
        when(execution.getVariable("traceId")).thenReturn(null);

        Headers headers = CadenzaflowHeaderBinder.from(execution, null);

        String trace = headers.getLast(BpmHeaders.TRACE_ID);
        assertThat(trace).isNotBlank();
        UUID.fromString(trace); // throws if not a valid UUID
    }

    @Test
    void from_traceIdVariableBlank_generatesUuid() {
        when(execution.getVariable("traceId")).thenReturn("   ");

        Headers headers = CadenzaflowHeaderBinder.from(execution, null);

        UUID.fromString(headers.getLast(BpmHeaders.TRACE_ID));
    }

    @Test
    void from_businessKeyNull_skipsHeader() {
        when(execution.getProcessBusinessKey()).thenReturn(null);

        Headers headers = CadenzaflowHeaderBinder.from(execution, null);

        assertThat(headers.containsKey(BpmHeaders.BUSINESS_KEY)).isFalse();
    }

    @Test
    void from_businessKeyPresent_addsHeader() {
        Headers headers = CadenzaflowHeaderBinder.from(execution, null);

        assertThat(headers.getLast(BpmHeaders.BUSINESS_KEY)).isEqualTo("order-42");
    }

    @Test
    void from_idempotencyKeyExplicit_usesIt() {
        Headers headers = CadenzaflowHeaderBinder.from(execution, "explicit-idem");

        assertThat(headers.getLast(BpmHeaders.IDEMPOTENCY_KEY)).isEqualTo("explicit-idem");
    }

    @Test
    void from_idempotencyKeyMissing_usesDeterministicDefault() {
        Headers headers = CadenzaflowHeaderBinder.from(execution, null);

        assertThat(headers.getLast(BpmHeaders.IDEMPOTENCY_KEY)).isEqualTo("pi-100:ai-200");
    }

    @Test
    void from_idempotencyKeyBlank_fallsBackToDefault() {
        Headers headers = CadenzaflowHeaderBinder.from(execution, "  ");

        assertThat(headers.getLast(BpmHeaders.IDEMPOTENCY_KEY)).isEqualTo("pi-100:ai-200");
    }
}
