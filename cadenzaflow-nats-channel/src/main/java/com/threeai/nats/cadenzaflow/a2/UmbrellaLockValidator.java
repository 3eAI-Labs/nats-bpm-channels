package com.threeai.nats.cadenzaflow.a2;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import com.threeai.nats.core.config.UmbrellaLockCalculator;
import com.threeai.nats.core.exception.UmbrellaLockConfigurationException;
import org.springframework.beans.factory.InitializingBean;

/**
 * Bootstrap-time umbrella-lock floor check (BAQ-3, VAL_UMBRELLA_LOCK_TOO_SHORT,
 * LLD 08_config.md §1.4). Reject-startup by default when {@code L < floor(W,M,S,eps)};
 * {@code allow-unsafe-lock-duration=true} downgrades this to a permanent per-cycle WARN
 * instead (§1.4.1 — "warn once and forget" is explicitly NOT allowed).
 */
public class UmbrellaLockValidator implements InitializingBean {

    private final A2Properties properties;
    private final UmbrellaLockResolver resolver;
    private final Set<String> unsafeTopics = Collections.synchronizedSet(new LinkedHashSet<>());

    public UmbrellaLockValidator(A2Properties properties, UmbrellaLockResolver resolver) {
        this.properties = properties;
        this.resolver = resolver;
    }

    @Override
    public void afterPropertiesSet() {
        for (String topic : properties.getTopics()) {
            A2Properties.TopicLockOverride override = properties.getTopicOverrides().get(topic);
            long w = override != null && override.getAckWaitSeconds() != null
                    ? override.getAckWaitSeconds() : properties.getDefaults().getAckWaitSeconds();
            int m = override != null && override.getMaxDeliver() != null
                    ? override.getMaxDeliver() : properties.getDefaults().getMaxDeliver();
            long floor = UmbrellaLockCalculator.floorSeconds(w, m,
                    properties.getDefaults().getSweepPeriodSeconds(), properties.getDefaults().getEpsilonSeconds());

            long l = resolver.deriveAndCache(topic);

            if (l < floor) {
                if (!properties.isAllowUnsafeLockDuration()) {
                    throw new UmbrellaLockConfigurationException(topic, l, floor);
                }
                unsafeTopics.add(topic);
            }
        }
    }

    /** @return topics whose configured L is below the ADR-0001 floor but were allowed to activate anyway. */
    public boolean isUnsafe(String topic) {
        return unsafeTopics.contains(topic);
    }
}
