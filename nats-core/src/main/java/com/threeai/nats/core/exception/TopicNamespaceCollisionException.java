package com.threeai.nats.core.exception;

/**
 * Thrown by {@link com.threeai.nats.core.config.NamespaceValidator} when a channel/subject
 * collides with the {@code jobs.*} namespace reserved for A2 (BAQ-4, VAL_TOPIC_NAMESPACE_COLLISION).
 *
 * <p><b>CODER-NOTE (LLD deviation):</b> {@code ERROR_REGISTRY.md} §1 specifies this class as
 * {@code extends org.flowable.common.engine.api.FlowableException}. {@code nats-core} has zero
 * dependency on any engine module by architectural invariant (`02_package_structure.md` §3,
 * confirmed by the current {@code nats-core/pom.xml}), so this class extends
 * {@link RuntimeException} instead. The Flowable-side caller
 * ({@code NatsChannelDefinitionProcessor.validateSubject}) wraps it in a
 * {@code FlowableException} before it leaves the Flowable module, preserving the exception
 * surface the engine expects while keeping {@code nats-core} engine-neutral.
 */
public class TopicNamespaceCollisionException extends RuntimeException {

    private static final String CODE = "VAL_TOPIC_NAMESPACE_COLLISION";

    private final String subject;
    private final String channelKey;

    public TopicNamespaceCollisionException(String subject, String channelKey) {
        super("NATS channel '" + channelKey + "': subject '" + subject
                + "' collides with the 'jobs.*' namespace reserved for A2 (" + CODE + ")");
        this.subject = subject;
        this.channelKey = channelKey;
    }

    public String getCode() {
        return CODE;
    }

    public String getSubject() {
        return subject;
    }

    public String getChannelKey() {
        return channelKey;
    }
}
