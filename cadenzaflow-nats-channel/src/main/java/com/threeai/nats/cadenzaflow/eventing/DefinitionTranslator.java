package com.threeai.nats.cadenzaflow.eventing;

import com.threeai.nats.cadenzaflow.inbound.SubscriptionConfig;

/**
 * (event, channel) definition pair -> the lineage's existing {@link SubscriptionConfig} POJO
 * (docs/12 §2.2: both sources converge on the same translation, which is what keeps the
 * four-engine parity claim honest). {@code queueGroup} maps to {@code deliverGroup}; the
 * definition's message name follows the {@code extension.messageName} / key convention.
 */
public final class DefinitionTranslator {

    private DefinitionTranslator() {
    }

    public static SubscriptionConfig translate(EventDefinition event, ChannelDefinition channel) {
        SubscriptionConfig config = new SubscriptionConfig();
        config.setSubject(channel.subject());
        config.setMessageName(event.resolvedMessageName());
        config.setDeliverGroup(channel.queueGroup());
        config.setJetstream(channel.jetstream());
        config.setDurableName(channel.durableName());
        config.setMaxDeliver(channel.maxDeliver());
        config.setDlqSubject(channel.dlqSubject());
        config.setAutoCreateStream(channel.autoCreateStream());
        config.setStreamName(channel.streamName());
        return config;
    }

    /** Stable fingerprint for idempotent reconciliation (config-equality without equals()). */
    public static String fingerprint(EventDefinition event, ChannelDefinition channel) {
        return String.join("|", channel.subject(), event.resolvedMessageName(),
                channel.queueGroup(), String.valueOf(channel.jetstream()),
                String.valueOf(channel.durableName()), String.valueOf(channel.maxDeliver()),
                String.valueOf(channel.dlqSubject()), String.valueOf(channel.autoCreateStream()),
                String.valueOf(channel.streamName()),
                String.valueOf(event.correlationParameterTypes()),
                String.valueOf(event.payloadTypes()));
    }
}
