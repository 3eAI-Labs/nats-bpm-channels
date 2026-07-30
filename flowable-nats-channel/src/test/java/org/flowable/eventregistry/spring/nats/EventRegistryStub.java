package org.flowable.eventregistry.spring.nats;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.flowable.eventregistry.api.EventRegistry;
import org.flowable.eventregistry.api.EventRegistryEvent;
import org.flowable.eventregistry.api.EventRegistryEventConsumer;
import org.flowable.eventregistry.api.InboundEvent;
import org.flowable.eventregistry.api.InboundEventProcessor;
import org.flowable.eventregistry.api.OutboundEventProcessor;
import org.flowable.eventregistry.api.runtime.EventInstance;
import org.flowable.eventregistry.model.ChannelModel;
import org.flowable.eventregistry.model.InboundChannelModel;

/**
 * Test stub for {@link EventRegistry} that captures received inbound events.
 */
class EventRegistryStub implements EventRegistry {

    private final CopyOnWriteArrayList<InboundEvent> receivedEvents = new CopyOnWriteArrayList<>();

    List<InboundEvent> getReceivedEvents() {
        return receivedEvents;
    }

    @Override
    public void eventReceived(InboundChannelModel channelModel, InboundEvent event) {
        receivedEvents.add(event);
    }

    @Override
    public void eventReceived(InboundChannelModel channelModel, String rawEvent) {
        // no-op for integration tests
    }

    @Override
    public void setInboundEventProcessor(InboundEventProcessor inboundEventProcessor) {
        // no-op
    }

    @Override
    public void setOutboundEventProcessor(OutboundEventProcessor outboundEventProcessor) {
        // no-op
    }

    @Override
    public OutboundEventProcessor getSystemOutboundEventProcessor() {
        return null;
    }

    @Override
    public void setSystemOutboundEventProcessor(OutboundEventProcessor outboundEventProcessor) {
        // no-op
    }

    @Override
    public void registerEventRegistryEventConsumer(EventRegistryEventConsumer eventConsumer) {
        // no-op
    }

    @Override
    public void removeFlowableEventRegistryEventConsumer(EventRegistryEventConsumer eventConsumer) {
        // no-op
    }

    @Override
    public String generateKey(Map<String, Object> data) {
        return null;
    }

    @Override
    public void sendEventToConsumers(EventRegistryEvent event) {
        // no-op
    }

    @Override
    public void sendSystemEventOutbound(EventInstance eventInstance) {
        // no-op
    }

    @Override
    public void sendEventOutbound(EventInstance eventInstance, Collection<ChannelModel> channels) {
        // no-op
    }
}
