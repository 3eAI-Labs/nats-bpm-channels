package com.threeai.nats.flowable.externalworker;

import com.threeai.nats.core.config.UmbrellaLockCalculator;

/**
 * Resolves the lock duration L against the ADR-0001 umbrella floor (docs/11 D-B'v3):
 * {@code L >= M*W + sum(backoff) + S + epsilon}. The floor is a HARD constraint — construction
 * throws unless {@code allow-unsafe-lock-duration=true}, which downgrades to a per-cycle WARN
 * surfaced through {@link #isUnsafe()} ("warn once and forget" explicitly rejected, BAQ-3).
 *
 * <p>The recovery bound {@code L + resetInterval + S} is a DERIVED SLO, not a constraint —
 * dedup safety is provided by the generation-scoped {@code Nats-Msg-Id = <jobId>#<nonce>}
 * key, never by an inequality (decision N-red-2).
 */
public class EwLockConfig {

    private final long lockDurationMillis;
    private final boolean unsafe;

    public EwLockConfig(EwProperties properties) {
        long floorSeconds = UmbrellaLockCalculator.floorSeconds(
                properties.getAckWaitSeconds(), properties.getMaxDeliver(),
                properties.getSweepPeriodSeconds(), properties.getEpsilonSeconds());
        long lSeconds = properties.getLockDurationSeconds() != null
                ? properties.getLockDurationSeconds()
                : UmbrellaLockCalculator.deriveDefaultLSeconds(
                        properties.getAckWaitSeconds(), properties.getMaxDeliver(),
                        properties.getSweepPeriodSeconds(), properties.getEpsilonSeconds());
        boolean belowFloor = lSeconds < floorSeconds;
        if (belowFloor && !properties.isAllowUnsafeLockDuration()) {
            throw new IllegalStateException("[VAL_UMBRELLA_LOCK_TOO_SHORT] external-worker lock duration L="
                    + lSeconds + "s is below the ADR-0001 floor " + floorSeconds
                    + "s (M*W + backoff + S + epsilon); a worker still inside its redelivery budget "
                    + "would lose the lock to the engine reset thread and the sweep would re-dispatch "
                    + "the job concurrently. Set spring.nats.flowable.external-worker."
                    + "allow-unsafe-lock-duration=true only if you accept per-cycle WARNs.");
        }
        this.unsafe = belowFloor;
        this.lockDurationMillis = lSeconds * 1000L;
    }

    public long lockDurationMillis() {
        return lockDurationMillis;
    }

    public boolean isUnsafe() {
        return unsafe;
    }
}
