package com.threeai.nats.core.resilience;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Isolates the {@code resilience4j-micrometer} type reference from {@link
 * DlqBridgeCircuitBreakerFactory} (QA-FINDING-1, resilience4j-micrometer optional-dependency
 * leak). {@code resilience4j-micrometer} is an {@code optional=true} dependency of {@code
 * nats-core} that consumer modules (flowable/camunda/cadenzaflow-nats-channel,
 * nats-history-projection) do not re-declare; if {@link TaggedCircuitBreakerMetrics} were
 * referenced directly inside {@code DlqBridgeCircuitBreakerFactory.create(...)}, the JVM would
 * throw {@code NoClassDefFoundError} the moment that method executes on a consumer's classpath
 * where a {@code MeterRegistry} bean exists but the jar does not (e.g. Spring Boot
 * Actuator/Prometheus without resilience4j-micrometer).
 *
 * <p>Keeping the reference in this small, separate class means the JVM only ever attempts to
 * resolve {@link TaggedCircuitBreakerMetrics} when THIS class is loaded — which {@link
 * DlqBridgeCircuitBreakerFactory} only triggers after confirming the class is present on the
 * classpath via {@code ClassUtils.isPresent(...)}. When it is not present, this class is simply
 * never loaded, so no {@code NoClassDefFoundError} can occur.
 */
final class Resilience4jMicrometerMetricsBinder {

    private Resilience4jMicrometerMetricsBinder() {
    }

    static void bind(CircuitBreakerRegistry circuitBreakerRegistry, MeterRegistry meterRegistry) {
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(circuitBreakerRegistry).bindTo(meterRegistry);
    }
}
