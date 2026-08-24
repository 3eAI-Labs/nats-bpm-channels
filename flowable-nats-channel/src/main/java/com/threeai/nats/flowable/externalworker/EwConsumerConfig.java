package com.threeai.nats.flowable.externalworker;

/**
 * Reply-consumer configuration for one external-worker topic (docs/11 D-E'v3 naming scheme).
 * Deliberately its own type (A2ConsumerConfig precedent): the maxDeliver here IS the umbrella M
 * and must never be confused with plain subscription budgets.
 */
public class EwConsumerConfig {

    private final String topic;

    public EwConsumerConfig(String topic) {
        this.topic = topic;
    }

    private long ackWaitSeconds = 30;
    private int maxDeliver = 4;
    private long retryTimeoutMillis = 5000;

    public String getTopic() {
        return topic;
    }

    /**
     * docs/13 Y-4 (0.12.0): subject/durable production is overridable — hardcoded values
     * blocked any topology (e.g. a future sharded Flowable) from scoping them. Defaults
     * preserve the exact 0.10.0 wire values; behavior is unchanged unless a caller sets
     * the overrides explicitly.
     */
    private String subjectOverride;
    private String durableOverride;

    public void setSubjectOverride(String subjectOverride) {
        this.subjectOverride = subjectOverride;
    }

    public void setDurableOverride(String durableOverride) {
        this.durableOverride = durableOverride;
    }

    public String getSubject() {
        return subjectOverride != null ? subjectOverride : "ewjobs." + topic + ".reply";
    }

    /** Durable and deliver group are ALWAYS the same value and always both set (SUB-90012 lesson). */
    public String getDurableName() {
        return durableOverride != null ? durableOverride : "flw-ew-completion-" + topic;
    }

    public String getDeliverGroup() {
        return getDurableName();
    }

    public String getDlqSubject() {
        return "dlq.ewjobs." + topic;
    }

    public long getAckWaitSeconds() {
        return ackWaitSeconds;
    }

    public void setAckWaitSeconds(long ackWaitSeconds) {
        this.ackWaitSeconds = ackWaitSeconds;
    }

    public int getMaxDeliver() {
        return maxDeliver;
    }

    public void setMaxDeliver(int maxDeliver) {
        this.maxDeliver = maxDeliver;
    }

    public long getRetryTimeoutMillis() {
        return retryTimeoutMillis;
    }

    public void setRetryTimeoutMillis(long retryTimeoutMillis) {
        this.retryTimeoutMillis = retryTimeoutMillis;
    }
}
