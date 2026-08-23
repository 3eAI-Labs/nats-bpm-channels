package com.threeai.nats.camunda.eventing;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;

/**
 * Parses {@code spring.nats.camunda.eventing.definitions.locations} once at boot into the
 * reconciler's secondary source (docs/12 F-4a). Locations are operator configuration — a
 * broken file fails startup (rejection over silent discard), unlike deployment-sourced
 * definitions whose failures are isolated per deployment (F-3).
 */
public final class LocationDefinitionLoader {

    private LocationDefinitionLoader() {
    }

    public static EventingReconciler.StaticDefinitions load(ResourcePatternResolver resolver,
            List<String> locations) {
        Map<String, EventDefinition> events = new LinkedHashMap<>();
        Map<String, ChannelDefinition> channels = new LinkedHashMap<>();
        Map<String, OutboundOverlayDefinition> outbound = new LinkedHashMap<>();
        for (String location : locations) {
            try {
                for (Resource resource : resolver.getResources(location)) {
                    String name = String.valueOf(resource.getFilename());
                    if (name.endsWith(".event")) {
                        EventDefinition event = EventingDefinitionParser.parseEvent(name, read(resource));
                        events.put(event.key(), event);
                    } else if (name.endsWith(".channel")) {
                        ChannelResource channel = EventingDefinitionParser.parseChannel(name, read(resource));
                        if (channel instanceof ChannelDefinition inbound) {
                            channels.put(inbound.key(), inbound);
                        } else if (channel instanceof OutboundOverlayDefinition overlay) {
                            outbound.put(overlay.key(), overlay);
                        }
                    }
                }
            } catch (EventingDefinitionException e) {
                throw new IllegalStateException(
                        "Eventing definition location failed to parse: " + location, e);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Eventing definition location failed to load: " + location, e);
            }
        }
        return new EventingReconciler.StaticDefinitions(
                Map.copyOf(events), Map.copyOf(channels), Map.copyOf(outbound));
    }

    private static String read(Resource resource) throws Exception {
        try (InputStream in = resource.getInputStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
