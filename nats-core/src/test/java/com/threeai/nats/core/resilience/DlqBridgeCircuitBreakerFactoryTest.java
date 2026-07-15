package com.threeai.nats.core.resilience;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

class DlqBridgeCircuitBreakerFactoryTest {

    @Test
    void create_startsClosedAndBindsMetrics() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        CircuitBreaker cb = DlqBridgeCircuitBreakerFactory.create("cb-test-1", registry);

        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(registry.find("resilience4j.circuitbreaker.state").meters()).isNotEmpty();
    }

    @Test
    void create_fiveConsecutiveFailures_opensCircuit() {
        CircuitBreaker cb = DlqBridgeCircuitBreakerFactory.create("cb-test-2", null);

        for (int i = 0; i < 5; i++) {
            cb.onError(0, java.util.concurrent.TimeUnit.MILLISECONDS, new RuntimeException("boom"));
        }

        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);
    }

    @Test
    void create_openCircuit_rejectsCallsFast() {
        CircuitBreaker cb = DlqBridgeCircuitBreakerFactory.create("cb-test-3", null);
        for (int i = 0; i < 5; i++) {
            cb.onError(0, java.util.concurrent.TimeUnit.MILLISECONDS, new RuntimeException("boom"));
        }

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> cb.executeCallable(() -> "won't run"))
                .isInstanceOf(CallNotPermittedException.class);
    }

    @Test
    void create_ignoredException_doesNotCountTowardFailureAccounting() {
        CircuitBreaker cb = DlqBridgeCircuitBreakerFactory.create("cb-test-4", null, IllegalStateException.class);

        for (int i = 0; i < 10; i++) {
            cb.onError(0, java.util.concurrent.TimeUnit.MILLISECONDS, new IllegalStateException("benign"));
        }

        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }
}
