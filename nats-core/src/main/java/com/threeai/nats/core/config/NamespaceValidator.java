package com.threeai.nats.core.config;

import com.threeai.nats.core.exception.TopicNamespaceCollisionException;

/**
 * Reserves the {@code jobs.*} subject namespace for A2 (BAQ-4, BR-SUB-004,
 * VAL_TOPIC_NAMESPACE_COLLISION). Called during Flowable channel registration bootstrap so a
 * misconfigured channel subject cannot silently collide with A2 job dispatch subjects.
 */
public final class NamespaceValidator {

    private static final String A2_RESERVED_PREFIX = "jobs.";

    private NamespaceValidator() {
    }

    public static void assertNotReservedForA2(String subject, String context) {
        if (subject != null && subject.startsWith(A2_RESERVED_PREFIX)) {
            throw new TopicNamespaceCollisionException(subject, context);
        }
    }
}
