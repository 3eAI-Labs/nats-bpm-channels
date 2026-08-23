package com.threeai.nats.camunda.eventing;

/**
 * Parsed inbound {@code .channel} artifact — the portable subset (docs/12 D-A v2).
 * {@code queueGroup} is MANDATORY in portable files (F-6): Flowable has no default (a null
 * queue group means per-node duplicate delivery there) and an implicit lineage default would
 * fail the parsed-model-equality gate G4 by construction.
 */
public record ChannelDefinition(
        String key,
        String subject,
        String eventKey,          // channelEventKeyDetection.fixedValue
        String queueGroup,
        boolean jetstream,
        String durableName,
        int maxDeliver,
        String dlqSubject,
        boolean autoCreateStream,
        String streamName) implements ChannelResource {
}
