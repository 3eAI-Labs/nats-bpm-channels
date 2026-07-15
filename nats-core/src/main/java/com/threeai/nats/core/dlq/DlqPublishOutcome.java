package com.threeai.nats.core.dlq;

/**
 * Custody-transfer decision returned by {@link DlqPublisher#publish}. The publisher never
 * ack/naks the original message itself — the caller applies this outcome in its own
 * consumer/subscriber context (LLD 03_classes/1_nats_core_common.md §2.2).
 */
public enum DlqPublishOutcome {

    /** BR-SUB-002 row 3: caller acks. */
    PUBLISHED_JETSTREAM,
    /**
     * BR-SUB-002 row 5: caller acks (existing behavior preserved — HLD §11 finding #4a: a
     * core-NATS publish gives no PubAck, but if it doesn't throw it is treated as "successful",
     * an honest boundary).
     */
    PUBLISHED_CORE_FALLBACK,
    /** BR-SUB-002 row 4 (fix): missing dlqSubject configuration -&gt; caller naks, no discard. */
    FAILED_NO_DLQ_SUBJECT,
    /** BR-SUB-002 row 5 (fix): both JetStream and core-NATS publish failed -&gt; caller naks+alerts. */
    FAILED_BOTH_PUBLISH
}
