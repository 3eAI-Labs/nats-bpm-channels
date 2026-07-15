package com.threeai.nats.core.resilience;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.time.Duration;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds one isolated {@link CircuitBreaker} per DLQ-bridge downstream (ADR-0004), with the
 * fixed thresholds ADR-0004 specifies as non-configurable (LLD 03_classes/1_nats_core_common.md §4,
 * 08_config.md §4): 5 consecutive failures -&gt; OPEN, 30s -&gt; HALF_OPEN, 3 permitted trial calls.
 */
public final class DlqBridgeCircuitBreakerFactory {

    private static final Logger log = LoggerFactory.getLogger(DlqBridgeCircuitBreakerFactory.class);

    private DlqBridgeCircuitBreakerFactory() {
    }

    /**
     * @param benignExceptions exception types that must never count toward the circuit
     *                         breaker's success/failure accounting (review MAJOR-1a) — each
     *                         caller passes the exception type(s) that represent its own
     *                         "already resolved via another path" idempotent-swallow path
     *                         (e.g. {@code NotFoundException.class} for {@code A2IncidentBridge}),
     *                         which are not a downstream-health signal.
     */
    @SafeVarargs
    public static CircuitBreaker create(String name, MeterRegistry registry,
            Class<? extends Throwable>... benignExceptions) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(5)
                .minimumNumberOfCalls(5)
                .failureRateThreshold(100.0f)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(3)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .ignoreExceptions(benignExceptions)
                .build();
        CircuitBreakerRegistry cbRegistry = CircuitBreakerRegistry.of(config);
        CircuitBreaker cb = cbRegistry.circuitBreaker(name);
        if (registry != null) {
            TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(cbRegistry).bindTo(registry);
        }
        cb.getEventPublisher().onStateTransition(event ->
                log.warn("DLQ-bridge circuit breaker state transition",
                        kv("cb_name", name),
                        kv("from", event.getStateTransition().getFromState()),
                        kv("to", event.getStateTransition().getToState())));
        return cb;
    }
}
