package com.threeai.nats.camunda.eventing;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.camunda.bpm.engine.RepositoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.repository.Deployment;
import org.camunda.bpm.engine.runtime.EventSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The registration authority's convergence loop (docs/12 D-B v2, F-1..F-5). Runs on EVERY
 * node: boot + fixed-delay period + post-commit nudges (coalesced by the scheduler wrapper).
 *
 * <p><b>Scan strategy (F-1):</b> one metadata query per pass
 * ({@code createDeploymentQuery().list()}), diffed against the node-local cache. Deployment
 * resources are write-once (no update path exists in the engine's Resource mapping), so a
 * deployment is inspected exactly once: {@code getDeploymentResourceNames} filtered by the
 * {@code .event}/{@code .channel} suffixes, then targeted {@code getResourceAsStream} reads.
 * {@code getDeploymentResources()} (a {@code select *} materializing every BLOB) is never
 * called. Steady-state pass cost: the metadata query.
 *
 * <p><b>Winner rule:</b> per definition key, the lexicographically greatest
 * {@code (deploymentTime, deploymentId)} wins — deployChangedOnly may split sibling resources
 * across deployments, so resolution is key-based, never deployment-based.
 *
 * <p><b>Start+correlationParameters (F-5):</b> unenforceable at load time; evaluated here each
 * pass through the engine's own event-subscription state — a message-start subscription
 * ({@code executionId == null}) for the definition's message name while the event carries
 * correlation parameters blocks registration ({@code VAL_EVENTING_START_WITH_CORRELATION}).
 *
 * <p><b>Failure isolation (F-3):</b> per-definition; a failing definition is skipped with
 * pass-count exponential backoff and one ERROR per state change; the pass continues.
 *
 * <p><b>Stale-unregister guard (F-2):</b> a pass computes registers AND unregisters from the
 * same in-pass snapshot; passes are serialized by the synchronized entry point.
 */
public class EventingReconciler {

    public static final String CODE_START_WITH_CORRELATION = "VAL_EVENTING_START_WITH_CORRELATION";

    private static final Logger log = LoggerFactory.getLogger(EventingReconciler.class);
    private static final String EVENT_SUFFIX = ".event";
    private static final String CHANNEL_SUFFIX = ".channel";
    private static final int MAX_BACKOFF_SKIPPED_PASSES = 8;

    record ScannedDeployment(String tenantId, java.util.Date deployTime,
            Map<String, EventDefinition> events, Map<String, ChannelDefinition> channels,
            Map<String, OutboundOverlayDefinition> outbound) {
    }

    /**
     * The secondary source (docs/12 D-B v2 / F-4a): Spring {@code definitions.locations},
     * parsed once at boot. Locations have no tenant — they always live under {@code null#key};
     * an engine deployment with the EXACT same key wins with one WARN per key, so in
     * multi-tenant setups locations act as the global fallback.
     */
    public record StaticDefinitions(Map<String, EventDefinition> events,
            Map<String, ChannelDefinition> channels,
            Map<String, OutboundOverlayDefinition> outbound) {
        public static StaticDefinitions empty() {
            return new StaticDefinitions(Map.of(), Map.of(), Map.of());
        }
    }

    private static class FailureState {
        int attempts;
        int skipRemaining;
    }

    private final RepositoryService repositoryService;
    private final RuntimeService runtimeService;
    private final EventingRegistry registry;
    private final StaticDefinitions locations;
    private final java.util.function.Consumer<Map<String, OutboundOverlayDefinition>> outboundOverlaySink;

    private final Map<String, ScannedDeployment> scanned = new HashMap<>();
    private final Map<String, FailureState> failures = new HashMap<>();
    private final Set<String> locationOverriddenWarned = new HashSet<>();
    private Map<String, OutboundOverlayDefinition> lastOverlay = Map.of();

    public EventingReconciler(RepositoryService repositoryService, RuntimeService runtimeService,
            EventingRegistry registry) {
        this(repositoryService, runtimeService, registry, StaticDefinitions.empty(), null);
    }

    public EventingReconciler(RepositoryService repositoryService, RuntimeService runtimeService,
            EventingRegistry registry, StaticDefinitions locations,
            java.util.function.Consumer<Map<String, OutboundOverlayDefinition>> outboundOverlaySink) {
        this.repositoryService = repositoryService;
        this.runtimeService = runtimeService;
        this.registry = registry;
        this.locations = locations == null ? StaticDefinitions.empty() : locations;
        this.outboundOverlaySink = outboundOverlaySink;
    }

    public synchronized void runPass() {
        List<Deployment> deployments;
        try {
            deployments = repositoryService.createDeploymentQuery().list();
        } catch (Exception e) {
            log.warn("Eventing reconcile pass skipped — deployment metadata query failed", e);
            return;
        }

        Set<String> liveIds = new HashSet<>();
        for (Deployment deployment : deployments) {
            liveIds.add(deployment.getId());
            scanned.computeIfAbsent(deployment.getId(), id -> scan(deployment));
        }
        scanned.keySet().retainAll(liveIds);

        Map<String, Winner<EventDefinition>> events = new LinkedHashMap<>();
        Map<String, Winner<ChannelDefinition>> channels = new LinkedHashMap<>();
        Map<String, OutboundOverlayDefinition> outboundByType = new LinkedHashMap<>();

        // F-4a: locations seed the maps under null#key; any deployment-sourced definition with
        // the exact same key overwrites below (deployment wins) and WARNs once per key.
        for (EventDefinition event : locations.events().values()) {
            events.put(tenantKey(null, event.key()), new Winner<>(event, null));
        }
        for (ChannelDefinition channel : locations.channels().values()) {
            channels.put(tenantKey(null, channel.key()), new Winner<>(channel, null));
        }
        for (OutboundOverlayDefinition outbound : locations.outbound().values()) {
            outboundByType.put(outbound.messageType(), outbound);
        }

        List<Map.Entry<String, ScannedDeployment>> ordered = new ArrayList<>(scanned.entrySet());
        ordered.sort(Comparator.comparing((Map.Entry<String, ScannedDeployment> e) ->
                e.getValue().deployTime()).thenComparing(Map.Entry::getKey));
        for (Map.Entry<String, ScannedDeployment> entry : ordered) {
            ScannedDeployment sd = entry.getValue();
            for (EventDefinition event : sd.events().values()) {
                String key = tenantKey(sd.tenantId(), event.key());
                warnLocationOverride(key, locations.events().containsKey(event.key())
                        && sd.tenantId() == null);
                events.put(key, new Winner<>(event, sd.tenantId()));
            }
            for (ChannelDefinition channel : sd.channels().values()) {
                String key = tenantKey(sd.tenantId(), channel.key());
                warnLocationOverride(key, locations.channels().containsKey(channel.key())
                        && sd.tenantId() == null);
                channels.put(key, new Winner<>(channel, sd.tenantId()));
            }
            for (OutboundOverlayDefinition outbound : sd.outbound().values()) {
                boolean overridesLocation = locations.outbound().values().stream()
                        .anyMatch(o -> o.messageType().equals(outbound.messageType()));
                warnLocationOverride("outbound:" + outbound.messageType(), overridesLocation);
                outboundByType.put(outbound.messageType(), outbound);
            }
        }

        Map<String, DesiredRegistration> desired = new LinkedHashMap<>();
        for (Map.Entry<String, Winner<ChannelDefinition>> entry : channels.entrySet()) {
            ChannelDefinition channel = entry.getValue().value();
            String tenantId = entry.getValue().tenantId();
            Winner<EventDefinition> event = events.get(tenantKey(tenantId, channel.eventKey()));
            if (event == null) {
                event = events.get(tenantKey(null, channel.eventKey())); // tenantless fallback
            }
            if (event == null) {
                warnOncePerPass("Channel definition has no matching event definition",
                        entry.getKey(), channel.eventKey());
                continue;
            }
            desired.put(entry.getKey(), new DesiredRegistration(event.value(), channel));
        }

        // F-5: start-subscription x correlationParameters — evaluated against engine state.
        desired.entrySet().removeIf(entry -> {
            EventDefinition event = entry.getValue().event();
            if (event.correlationParameterTypes().isEmpty()) {
                return false;
            }
            if (hasMessageStartSubscription(event.resolvedMessageName())) {
                log.warn("[{}] definition blocked: message start event exists while the"
                        + " definition carries correlationParameters — the engine's start"
                        + " fallback ignores correlation keys",
                        CODE_START_WITH_CORRELATION, kv("key", entry.getKey()),
                        kv("message_name", event.resolvedMessageName()));
            return true;
            }
            return false;
        });

        for (Map.Entry<String, DesiredRegistration> entry : desired.entrySet()) {
            String key = entry.getKey();
            FailureState failure = failures.get(key);
            if (failure != null && failure.skipRemaining > 0) {
                failure.skipRemaining--;
                continue;
            }
            try {
                registry.register(key, entry.getValue().event(), entry.getValue().channel());
                if (failure != null) {
                    failures.remove(key);
                    log.info("Eventing definition recovered", kv("key", key));
                }
            } catch (Exception e) {
                FailureState state = failures.computeIfAbsent(key, k -> new FailureState());
                state.attempts++;
                state.skipRemaining = Math.min(MAX_BACKOFF_SKIPPED_PASSES,
                        (1 << Math.min(6, state.attempts - 1)) - 1);
                if (state.attempts == 1) {
                    log.error("Eventing definition failed to register — will retry with backoff",
                            kv("key", key), e);
                }
            }
        }

        for (String activeKey : registry.activeKeys()) {
            if (!desired.containsKey(activeKey)) {
                registry.unregister(activeKey);
                log.warn("Eventing definition no longer present in any deployment — unregistered"
                        + " (operator rule: redeploy eventing resources with the current"
                        + " deployment before deleting the old one)", kv("key", activeKey));
            }
        }

        // D-D v2: the outbound overlay is swapped from the same in-pass snapshot the
        // subscription decisions used; removal reverts covered keys to the YAML base.
        if (outboundOverlaySink != null && !outboundByType.equals(lastOverlay)) {
            outboundOverlaySink.accept(Map.copyOf(outboundByType));
            log.info("Outbound classification overlay swapped",
                    kv("message_types", outboundByType.keySet()));
            lastOverlay = Map.copyOf(outboundByType);
        }
    }

    private void warnLocationOverride(String key, boolean overridesLocation) {
        if (overridesLocation && locationOverriddenWarned.add(key)) {
            log.warn("Definition exists in both a location file and an engine deployment —"
                    + " the deployment wins (docs/12 F-4a)", kv("key", key));
        }
    }

    private ScannedDeployment scan(Deployment deployment) {
        Map<String, EventDefinition> events = new LinkedHashMap<>();
        Map<String, ChannelDefinition> channels = new LinkedHashMap<>();
        Map<String, OutboundOverlayDefinition> outbound = new LinkedHashMap<>();
        try {
            for (String name : repositoryService.getDeploymentResourceNames(deployment.getId())) {
                if (name.endsWith(EVENT_SUFFIX)) {
                    EventDefinition event = EventingDefinitionParser.parseEvent(name,
                            read(deployment.getId(), name));
                    events.put(event.key(), event);
                } else if (name.endsWith(CHANNEL_SUFFIX)) {
                    ChannelResource channel = EventingDefinitionParser.parseChannel(name,
                            read(deployment.getId(), name));
                    if (channel instanceof ChannelDefinition inbound) {
                        channels.put(inbound.key(), inbound);
                    } else if (channel instanceof OutboundOverlayDefinition overlay) {
                        outbound.put(overlay.key(), overlay);
                    }
                }
            }
        } catch (Exception e) {
            // F-3: a broken resource poisons only this deployment's scan; cached as empty so the
            // pass never rescans a permanently broken deployment every period.
            log.error("Eventing scan failed for deployment — its definitions are ignored",
                    kv("deployment_id", deployment.getId()), e);
            return new ScannedDeployment(deployment.getTenantId(),
                    deployment.getDeploymentTime(), Map.of(), Map.of(), Map.of());
        }
        return new ScannedDeployment(deployment.getTenantId(), deployment.getDeploymentTime(),
                Map.copyOf(events), Map.copyOf(channels), Map.copyOf(outbound));
    }

    private boolean hasMessageStartSubscription(String messageName) {
        try {
            List<EventSubscription> subs = runtimeService.createEventSubscriptionQuery()
                    .eventType("message").eventName(messageName).list();
            for (EventSubscription sub : subs) {
                if (sub.getExecutionId() == null) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            log.warn("Start-subscription check failed — definition allowed this pass",
                    kv("message_name", messageName), e);
            return false;
        }
    }

    private String read(String deploymentId, String resourceName) throws Exception {
        try (InputStream in = repositoryService.getResourceAsStream(deploymentId, resourceName)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String tenantKey(String tenantId, String definitionKey) {
        return (tenantId == null ? "" : tenantId) + "#" + definitionKey;
    }

    private void warnOncePerPass(String message, String key, String eventKey) {
        log.warn(message, kv("key", key), kv("event_key", eventKey));
    }

    private record Winner<T>(T value, String tenantId) {
    }

    private record DesiredRegistration(EventDefinition event, ChannelDefinition channel) {
    }
}
