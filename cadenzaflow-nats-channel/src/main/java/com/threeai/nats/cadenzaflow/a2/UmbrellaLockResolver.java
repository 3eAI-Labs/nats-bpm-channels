package com.threeai.nats.cadenzaflow.a2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.threeai.nats.core.config.UmbrellaLockCalculator;

/**
 * Per-topic umbrella-lock resolver (ADR-0001, LLD 08_config.md §1.3). Values are computed once
 * at bootstrap ({@link UmbrellaLockValidator#afterPropertiesSet()}) and cached — resolution
 * itself never re-derives on the hot path.
 */
public class UmbrellaLockResolver {

    private final A2Properties properties;
    private final Map<String, Long> resolvedLSecondsCache = new ConcurrentHashMap<>();

    public UmbrellaLockResolver(A2Properties properties) {
        this.properties = properties;
    }

    /** @return L for the given topic, in milliseconds. Falls back to the default derivation if not yet resolved. */
    public long resolveMillis(String topic) {
        return resolveSeconds(topic) * 1000;
    }

    long resolveSeconds(String topic) {
        Long cached = resolvedLSecondsCache.get(topic);
        if (cached != null) {
            return cached;
        }
        return deriveAndCache(topic);
    }

    /** Called by {@link UmbrellaLockValidator} while validating each configured topic. */
    long deriveAndCache(String topic) {
        A2Properties.TopicLockOverride override = properties.getTopicOverrides().get(topic);
        long w = overrideOrDefault(override != null ? override.getAckWaitSeconds() : null,
                properties.getDefaults().getAckWaitSeconds());
        int m = override != null && override.getMaxDeliver() != null
                ? override.getMaxDeliver() : properties.getDefaults().getMaxDeliver();
        long s = properties.getDefaults().getSweepPeriodSeconds();
        long eps = properties.getDefaults().getEpsilonSeconds();

        long l = override != null && override.getLockDurationSeconds() != null
                ? override.getLockDurationSeconds()
                : UmbrellaLockCalculator.deriveDefaultLSeconds(w, m, s, eps);

        resolvedLSecondsCache.put(topic, l);
        return l;
    }

    private long overrideOrDefault(Long override, long defaultValue) {
        return override != null ? override : defaultValue;
    }
}
