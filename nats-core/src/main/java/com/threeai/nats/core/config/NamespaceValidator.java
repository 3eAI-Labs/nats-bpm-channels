package com.threeai.nats.core.config;

import com.threeai.nats.core.exception.TopicNamespaceCollisionException;

/**
 * Reserves the {@code jobs.*} subject namespace for A2 (BAQ-4, BR-SUB-004,
 * VAL_TOPIC_NAMESPACE_COLLISION). Called during Flowable channel registration bootstrap so a
 * misconfigured channel subject cannot silently collide with A2 job dispatch subjects.
 *
 * <p><b>Basamak-4 (docs/09-outbound-handoff.md D-E'):</b> {@link #assertNotReservedForOutbound}
 * applies the SAME guard pattern to {@code events.*} (the outbound-handoff subject scheme,
 * {@code events.<engineId>.<type>.<processInstanceId>}) and its {@code dlq.events.*} DLQ
 * counterpart — a tenant-defined Flowable channel subject must not collide with either.
 */
public final class NamespaceValidator {

    private static final String A2_RESERVED_PREFIX = "jobs.";
    private static final String OUTBOUND_DLQ_RESERVED_PREFIX = "dlq.events.";
    private static final String OUTBOUND_RESERVED_PREFIX = "events.";

    private NamespaceValidator() {
    }

    public static void assertNotReservedForA2(String subject, String context) {
        if (subject != null && subject.startsWith(A2_RESERVED_PREFIX)) {
            throw new TopicNamespaceCollisionException(subject, context);
        }
    }

    /**
     * Basamak-4 (D-E') — {@code events.*}/{@code dlq.events.*} are reserved for the outbound
     * message-handoff mechanism (message-throw/send-task publish + its DLQ), mirroring the
     * {@code jobs.*}/A2 reservation (BAQ-4 emsali). The DLQ prefix is checked FIRST since it is
     * the more specific (longer) prefix — {@code dlq.events.foo} would also match the shorter
     * {@code events.} check only if tested out of order against a naive substring scan; checking
     * the specific prefix first keeps the reported message accurate.
     */
    public static void assertNotReservedForOutbound(String subject, String context) {
        if (subject == null) {
            return;
        }
        if (subject.startsWith(OUTBOUND_DLQ_RESERVED_PREFIX)) {
            throw new TopicNamespaceCollisionException(subject, context,
                    "'dlq.events.*' namespace reserved for outbound-handoff DLQ (basamak-4)");
        }
        if (subject.startsWith(OUTBOUND_RESERVED_PREFIX)) {
            throw new TopicNamespaceCollisionException(subject, context,
                    "'events.*' namespace reserved for outbound-handoff (basamak-4)");
        }
    }
}
