package com.threeai.nats.core.resilience;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.time.Duration;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;

/**
 * Builds one isolated {@link CircuitBreaker} per DLQ-bridge downstream (ADR-0004), with the
 * fixed thresholds ADR-0004 specifies as non-configurable (LLD 03_classes/1_nats_core_common.md §4,
 * 08_config.md §4): 5 consecutive failures -&gt; OPEN, 30s -&gt; HALF_OPEN, 3 permitted trial calls.
 */
public final class DlqBridgeCircuitBreakerFactory {

    private static final Logger log = LoggerFactory.getLogger(DlqBridgeCircuitBreakerFactory.class);

    private static final String TAGGED_METRICS_CLASS_NAME =
            "io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics";

    /**
     * QA-FINDING-1: {@code resilience4j-micrometer} is an {@code optional=true} nats-core
     * dependency that consumer modules do not always re-declare. Computed once (not per {@link
     * #create}) so the classpath probe cost is paid at most once per classloader; the classpath
     * shape cannot change at runtime.
     */
    private static final boolean MICROMETER_METRICS_AVAILABLE =
            ClassUtils.isPresent(TAGGED_METRICS_CLASS_NAME, DlqBridgeCircuitBreakerFactory.class.getClassLoader());

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
            bindMicrometerMetricsIfAvailable(name, cbRegistry, registry);
        }
        cb.getEventPublisher().onStateTransition(event ->
                log.warn("DLQ-bridge circuit breaker state transition",
                        kv("cb_name", name),
                        kv("from", event.getStateTransition().getFromState()),
                        kv("to", event.getStateTransition().getToState())));
        return cb;
    }

    /**
     * QA-FINDING-1 fix: only ever touches {@link Resilience4jMicrometerMetricsBinder} (and
     * transitively {@code TaggedCircuitBreakerMetrics}) once the classpath probe above confirms
     * resilience4j-micrometer is present. Degrades gracefully otherwise — the circuit breaker
     * itself keeps working, only its Micrometer metrics are skipped.
     */
    private static void bindMicrometerMetricsIfAvailable(String name, CircuitBreakerRegistry cbRegistry,
            MeterRegistry registry) {
        if (MICROMETER_METRICS_AVAILABLE) {
            Resilience4jMicrometerMetricsBinder.bind(cbRegistry, registry);
        } else {
            log.warn("DLQ-bridge circuit breaker Micrometer metrics not bound: resilience4j-micrometer is not "
                            + "on the classpath (add it as a runtime dependency to enable metrics for this "
                            + "circuit breaker)",
                    kv("cb_name", name),
                    kv("missing_class", TAGGED_METRICS_CLASS_NAME));
        }
    }
}
