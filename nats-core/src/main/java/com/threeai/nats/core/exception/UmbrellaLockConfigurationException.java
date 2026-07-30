package com.threeai.nats.core.exception;

/**
 * Bootstrap-time fail-fast for an umbrella-lock duration below the ADR-0001 floor
 * ({@code L >= M*W + Sum(backoff) + S + eps}) when {@code allow-unsafe-lock-duration} is not
 * set. Thrown from an {@link org.springframework.beans.factory.InitializingBean#afterPropertiesSet()}
 * so Spring context refresh fails (VAL_UMBRELLA_LOCK_TOO_SHORT, BAQ-3).
 */
public class UmbrellaLockConfigurationException extends RuntimeException {

    private static final String CODE = "VAL_UMBRELLA_LOCK_TOO_SHORT";

    private final String topic;
    private final long configuredLSeconds;
    private final long floorSeconds;

    public UmbrellaLockConfigurationException(String topic, long configuredLSeconds, long floorSeconds) {
        super("Umbrella-lock duration for topic '" + topic + "' is below the safety floor: "
                + "configured L=" + configuredLSeconds + "s, floor=" + floorSeconds + "s ("
                + CODE + ")");
        this.topic = topic;
        this.configuredLSeconds = configuredLSeconds;
        this.floorSeconds = floorSeconds;
    }

    public String getCode() {
        return CODE;
    }

    public String getTopic() {
        return topic;
    }

    public long getConfiguredLSeconds() {
        return configuredLSeconds;
    }

    public long getFloorSeconds() {
        return floorSeconds;
    }
}
