package com.threeai.nats.camunda.eventing;

/**
 * A parsed {@code .channel} resource — inbound ({@link ChannelDefinition}, subscription
 * registration) or outbound ({@link OutboundOverlayDefinition}, classification/allowlist
 * overlay per docs/12 D-D v2). Sealed so reconciler/deployer switches stay exhaustive.
 */
public sealed interface ChannelResource permits ChannelDefinition, OutboundOverlayDefinition {

    String key();
}
