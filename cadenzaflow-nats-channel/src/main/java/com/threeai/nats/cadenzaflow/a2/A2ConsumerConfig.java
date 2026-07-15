package com.threeai.nats.cadenzaflow.a2;

/**
 * A2-specific reply/DLQ consumer configuration — deliberately NOT
 * {@code com.threeai.nats.cadenzaflow.inbound.SubscriptionConfig} (review MINOR-1,
 * LLD 03_classes/2_camunda_a2.md §4.0 / 08_config.md §1.5, mirrored per ADR-0007). {@code SubscriptionConfig}'s
 * {@code maxDeliver} default (5) is for plain message-correlation subjects (US-E2) and is
 * structurally unrelated to the umbrella-lock M (ADR-0001 default 4) — using two distinct
 * types makes it a compile-time guarantee that the two configs can never be confused.
 */
public class A2ConsumerConfig {

    private String subject;
    private String messageName;
    private String durableName;
    /** W — must stay aligned with {@link UmbrellaLockResolver}'s topic-override W. */
    private long ackWaitSeconds = 30;
    /** M — asyncapi {@code a2JobReply.x-jetstream.maxDeliver=4}, ADR-0001 default. */
    private int maxDeliver = 4;
    /** Only used by {@link A2CompletionBridge} — {@link A2IncidentBridge} is the DLQ's target, not source. */
    private String dlqSubject;

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getMessageName() {
        return messageName;
    }

    public void setMessageName(String messageName) {
        this.messageName = messageName;
    }

    public String getDurableName() {
        return durableName;
    }

    public void setDurableName(String durableName) {
        this.durableName = durableName;
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

    public String getDlqSubject() {
        return dlqSubject;
    }

    public void setDlqSubject(String dlqSubject) {
        this.dlqSubject = dlqSubject;
    }
}
