package com.threeai.nats.core.config;

import com.threeai.nats.core.exception.TopicNamespaceCollisionException;

/**
 * Reserves the {@code jobs.*} subject namespace for A2 (BAQ-4, BR-SUB-004,
 * VAL_TOPIC_NAMESPACE_COLLISION). Called during Flowable channel registration bootstrap so a
 * misconfigured channel subject cannot silently collide with A2 job dispatch subjects.
 *
 * <p><b>Increment 4 (decision D-E'):</b> {@link #assertNotReservedForOutbound}
 * applies the SAME guard pattern to {@code events.*} (the outbound-handoff subject scheme,
 * {@code events.<engineId>.<type>.<processInstanceId>}) and its {@code dlq.events.*} DLQ
 * counterpart — a tenant-defined Flowable channel subject must not collide with either.
 */
public final class NamespaceValidator {

    private static final String A2_RESERVED_PREFIX = "jobs.";
    private static final String EW_RESERVED_PREFIX = "ewjobs.";
    private static final String EW_DLQ_RESERVED_PREFIX = "dlq.ewjobs.";
    private static final String OUTBOUND_DLQ_RESERVED_PREFIX = "dlq.events.";
    private static final String OUTBOUND_RESERVED_PREFIX = "events.";
    private static final String SHARD_DLQ_RESERVED_PREFIX = "dlq.shard.";
    private static final String SHARD_RESERVED_PREFIX = "shard.";

    private NamespaceValidator() {
    }

    /**
     * Flowable external-worker dispatch (docs/11 D-E'v3, 2026-08-22) — {@code ewjobs.*} and
     * {@code dlq.ewjobs.*} are reserved for external-worker job dispatch, mirroring the
     * {@code jobs.*}/A2 and {@code events.*}/outbound reservations (BAQ-4 precedent, docs/06 §7
     * dual-reservation note). Specific (longer) DLQ prefix checked first, same rationale as
     * {@link #assertNotReservedForOutbound}.
     */
    public static void assertNotReservedForExternalWorker(String subject, String context) {
        if (subject == null) {
            return;
        }
        if (subject.startsWith(EW_DLQ_RESERVED_PREFIX)) {
            throw new TopicNamespaceCollisionException(subject, context,
                    "'dlq.ewjobs.*' namespace reserved for external-worker dispatch DLQ (docs/11)");
        }
        if (subject.startsWith(EW_RESERVED_PREFIX)) {
            throw new TopicNamespaceCollisionException(subject, context,
                    "'ewjobs.*' namespace reserved for external-worker dispatch (docs/11)");
        }
    }

    public static void assertNotReservedForA2(String subject, String context) {
        if (subject != null && subject.startsWith(A2_RESERVED_PREFIX)) {
            throw new TopicNamespaceCollisionException(subject, context);
        }
    }

    /**
     * Increment 4 (D-E') — {@code events.*}/{@code dlq.events.*} are reserved for the outbound
     * message-handoff mechanism (message-throw/send-task publish + its DLQ), mirroring the
     * {@code jobs.*}/A2 reservation (BAQ-4 precedent). The DLQ prefix is checked FIRST since it is
     * the more specific (longer) prefix — {@code dlq.events.foo} would also match the shorter
     * {@code events.} check only if tested out of order against a naive substring scan; checking
     * the specific prefix first keeps the reported message accurate.
     */
    /**
     * docs/13 §2.4 (0.12.0) — {@code shard.*}/{@code dlq.shard.*} are reserved for the
     * instance-sharding router (routed twins of inbound subjects + the router DLQ that must
     * escape every incident bridge). Same dual-reservation pattern as the three above; the
     * check runs on the ORIGINAL subject BEFORE any shard rewrite (Y-9 ordering rule). NOTE:
     * v1 grammar has no fleet token — this literal prefix is exactly what the router emits.
     */
    public static void assertNotReservedForSharding(String subject, String context) {
        if (subject == null) {
            return;
        }
        if (subject.startsWith(SHARD_DLQ_RESERVED_PREFIX)) {
            throw new TopicNamespaceCollisionException(subject, context,
                    "'dlq.shard.*' namespace reserved for the sharding router DLQ (docs/13)");
        }
        if (subject.startsWith(SHARD_RESERVED_PREFIX)) {
            throw new TopicNamespaceCollisionException(subject, context,
                    "'shard.*' namespace reserved for instance-sharding routing (docs/13)");
        }
    }

    public static void assertNotReservedForOutbound(String subject, String context) {
        if (subject == null) {
            return;
        }
        if (subject.startsWith(OUTBOUND_DLQ_RESERVED_PREFIX)) {
            throw new TopicNamespaceCollisionException(subject, context,
                    "'dlq.events.*' namespace reserved for outbound-handoff DLQ (increment 4)");
        }
        if (subject.startsWith(OUTBOUND_RESERVED_PREFIX)) {
            throw new TopicNamespaceCollisionException(subject, context,
                    "'events.*' namespace reserved for outbound-handoff (increment 4)");
        }
    }
}
