package com.threeai.nats.core.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Bootstrap fail-fast exception (BAQ-3, VAL_UMBRELLA_LOCK_TOO_SHORT) — thrown from {@code
 * UmbrellaLockValidator.afterPropertiesSet()} (camunda/cadenzaflow modules) when the configured
 * umbrella-lock duration is below the ADR-0001 floor. The formatted message and error-code CONTENT
 * is itself the operator-facing contract (surfaced verbatim in Spring context-refresh failure
 * logs), so it is worth pinning exactly, not just constructing-and-ignoring.
 */
class UmbrellaLockConfigurationExceptionTest {

    @Test
    void constructor_populatesAllAccessors() {
        UmbrellaLockConfigurationException ex =
                new UmbrellaLockConfigurationException("jobs.order-fulfillment", 100L, 320L);

        assertThat(ex.getCode()).isEqualTo("VAL_UMBRELLA_LOCK_TOO_SHORT");
        assertThat(ex.getTopic()).isEqualTo("jobs.order-fulfillment");
        assertThat(ex.getConfiguredLSeconds()).isEqualTo(100L);
        assertThat(ex.getFloorSeconds()).isEqualTo(320L);
    }

    @Test
    void getMessage_containsTopicConfiguredAndFloorValuesAndCode() {
        UmbrellaLockConfigurationException ex =
                new UmbrellaLockConfigurationException("jobs.order-fulfillment", 100L, 320L);

        assertThat(ex.getMessage())
                .contains("jobs.order-fulfillment")
                .contains("L=100s")
                .contains("floor=320s")
                .contains("VAL_UMBRELLA_LOCK_TOO_SHORT");
    }

    @Test
    void isRuntimeException_notCheckedByCallers() {
        UmbrellaLockConfigurationException ex =
                new UmbrellaLockConfigurationException("jobs.foo", 1L, 2L);

        assertThat(ex).isInstanceOf(RuntimeException.class);
    }
}
