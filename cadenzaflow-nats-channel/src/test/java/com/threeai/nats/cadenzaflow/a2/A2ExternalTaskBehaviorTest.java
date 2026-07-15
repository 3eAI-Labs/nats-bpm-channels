package com.threeai.nats.cadenzaflow.a2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.cadenzaflow.bpm.engine.delegate.VariableScope;
import org.junit.jupiter.api.Test;

/**
 * Sentinel Phase 5.5 QA fix (item 5, Levent karari 2026-07-15) — in-tx variable capture. Only
 * {@link A2ExternalTaskBehavior#captureAllowlistedVariables} is unit-tested directly: it is
 * package-private + static specifically so it can be exercised against a mocked {@link
 * VariableScope} without needing to mock CadenzaFlow's static {@code Context}/{@code
 * CommandContext} bootstrap machinery (not done anywhere else in this codebase either —
 * {@code execute()} itself is exercised indirectly via the embedded-engine integration tests,
 * e.g. {@code CadenzaFlowA2GuardTest}/{@code A2OrphanSweepFetchableParityIntegrationTest}).
 */
class A2ExternalTaskBehaviorTest {

    @Test
    void captureAllowlistedVariables_emptyAllowlist_returnsEmptyMap() {
        VariableScope scope = mock(VariableScope.class);

        Map<String, Object> captured = A2ExternalTaskBehavior.captureAllowlistedVariables(scope, List.of());

        assertThat(captured).isEmpty();
    }

    @Test
    void captureAllowlistedVariables_nullAllowlist_returnsEmptyMap() {
        VariableScope scope = mock(VariableScope.class);

        Map<String, Object> captured = A2ExternalTaskBehavior.captureAllowlistedVariables(scope, null);

        assertThat(captured).isEmpty();
    }

    @Test
    void captureAllowlistedVariables_populatedAllowlist_capturesOnlyExistingVariables() {
        VariableScope scope = mock(VariableScope.class);
        when(scope.hasVariable("amount")).thenReturn(true);
        when(scope.getVariable("amount")).thenReturn(42);
        when(scope.hasVariable("currency")).thenReturn(true);
        when(scope.getVariable("currency")).thenReturn("EUR");

        Map<String, Object> captured = A2ExternalTaskBehavior.captureAllowlistedVariables(
                scope, List.of("amount", "currency"));

        assertThat(captured).containsExactly(Map.entry("amount", 42), Map.entry("currency", "EUR"));
    }

    /** Allowlist naming a variable that does not exist on this scope — silently omitted, not an error. */
    @Test
    void captureAllowlistedVariables_allowlistedVariableMissingFromScope_isOmitted() {
        VariableScope scope = mock(VariableScope.class);
        when(scope.hasVariable("amount")).thenReturn(true);
        when(scope.getVariable("amount")).thenReturn(42);
        when(scope.hasVariable("neverSet")).thenReturn(false);

        Map<String, Object> captured = A2ExternalTaskBehavior.captureAllowlistedVariables(
                scope, List.of("amount", "neverSet"));

        assertThat(captured).containsOnly(Map.entry("amount", 42));
    }
}
