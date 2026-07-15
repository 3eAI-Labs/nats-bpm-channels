package com.threeai.nats.core.config;

/**
 * Stateless umbrella-lock arithmetic (ADR-0001), LLD 08_config.md §1.2.
 *
 * <p>The 13s margin is an absolute number of seconds, not a percentage of the floor (review
 * NIT-4) — with large {@code W} overrides the floor grows proportionally to {@code M*W} while
 * the margin stays fixed at 13s; the safety guarantee is unaffected because
 * {@code UmbrellaLockValidator} always enforces {@code L >= floor} as an absolute check,
 * independent of the margin's relative size.
 */
public final class UmbrellaLockCalculator {

    private static final long MARGIN_SECONDS = 13;
    private static final long MAX_BACKOFF_SECONDS = 30;

    private UmbrellaLockCalculator() {
    }

    /** Sum(backoff) = Sum 2^(n-1), n=1..maxDeliver-1, capped at 30s per term (matches the adapters' calculateBackoff). */
    public static long backoffSumSeconds(int maxDeliver) {
        long sum = 0;
        for (int n = 1; n < maxDeliver; n++) {
            sum += Math.min(MAX_BACKOFF_SECONDS, (long) Math.pow(2, n - 1));
        }
        return sum;
    }

    /** Umbrella-lock floor: {@code L >= M*W + Sum(backoff) + S + eps} (ADR-0001). */
    public static long floorSeconds(long ackWaitSeconds, int maxDeliver, long sweepPeriodSeconds, long epsilonSeconds) {
        return maxDeliver * ackWaitSeconds + backoffSumSeconds(maxDeliver) + sweepPeriodSeconds + epsilonSeconds;
    }

    /** Default L when not explicitly configured: {@code floor + fixed 13s margin} (ADR-0001, 307+13=320 example). */
    public static long deriveDefaultLSeconds(long ackWaitSeconds, int maxDeliver, long sweepPeriodSeconds, long epsilonSeconds) {
        return floorSeconds(ackWaitSeconds, maxDeliver, sweepPeriodSeconds, epsilonSeconds) + MARGIN_SECONDS;
    }
}
