package com.threeai.nats.cibseven.eventing;

import com.threeai.nats.cibseven.inbound.SubscriptionConfig;

/**
 * Creates a live subscriber for a translated definition. Production wiring builds the existing
 * Nats/JetStream correlation subscribers (slice 5 swaps in the definition-aware variant);
 * tests substitute a recording fake. Implementations MUST subscribe before returning.
 */
public interface SubscriberFactory {
    SubscriberHandle subscribe(SubscriptionConfig config, EventDefinition event);
}
