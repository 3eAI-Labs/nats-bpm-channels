package com.threeai.nats.flowable.externalworker;

/**
 * Thrown at config load when an external-worker topic fails the D-H' token rule
 * ({@code ^[A-Za-z0-9_-]+$}). Error code {@code VAL_EW_TOPIC_INVALID}.
 */
public class InvalidEwTopicException extends RuntimeException {

    public InvalidEwTopicException(String topic) {
        super("[VAL_EW_TOPIC_INVALID] external-worker topic "
                + (topic == null ? "null" : "'" + topic + "'")
                + " is not a safe subject token (^[A-Za-z0-9_-]+$): it would be embedded in "
                + "ewjobs.<topic>, dlq.ewjobs.<topic> and the flw-ew-completion-<topic> durable");
    }
}
