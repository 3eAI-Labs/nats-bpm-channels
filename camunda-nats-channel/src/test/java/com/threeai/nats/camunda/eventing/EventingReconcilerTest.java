package com.threeai.nats.camunda.eventing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.threeai.nats.camunda.inbound.SubscriptionConfig;
import org.camunda.bpm.engine.RepositoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.repository.Deployment;
import org.camunda.bpm.engine.repository.DeploymentQuery;
import org.camunda.bpm.engine.runtime.EventSubscription;
import org.camunda.bpm.engine.runtime.EventSubscriptionQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** docs/12 D-B v2 / F-1..F-5 at unit level with a mocked engine. */
class EventingReconcilerTest {

    private static final String EVENT_JSON = """
            { "key": "orderEvent", "payload": [ { "name": "orderId", "type": "string" } ] }
            """;
    private static final String EVENT_JSON_WITH_CORRELATION = """
            { "key": "orderEvent",
              "correlationParameters": [ { "name": "orderId", "type": "string" } ] }
            """;
    private static final String CHANNEL_JSON = """
            { "key": "orderChannel", "channelType": "inbound", "type": "nats",
              "channelEventKeyDetection": { "fixedValue": "orderEvent" },
              "queueGroup": "g1", "subject": "order.new" }
            """;

    private RepositoryService repositoryService;
    private RuntimeService runtimeService;
    private EventingRegistryTest.RecordingFactory factory;
    private EventingRegistry registry;
    private EventingReconciler reconciler;
    private final List<Deployment> deployments = new ArrayList<>();

    @BeforeEach
    void setUp() {
        repositoryService = mock(RepositoryService.class);
        runtimeService = mock(RuntimeService.class);
        DeploymentQuery query = mock(DeploymentQuery.class);
        when(repositoryService.createDeploymentQuery()).thenReturn(query);
        when(query.list()).thenAnswer(inv -> new ArrayList<>(deployments));

        EventSubscriptionQuery esq = mock(EventSubscriptionQuery.class);
        when(runtimeService.createEventSubscriptionQuery()).thenReturn(esq);
        when(esq.eventType(anyString())).thenReturn(esq);
        when(esq.eventName(anyString())).thenReturn(esq);
        when(esq.list()).thenReturn(List.of());

        factory = new EventingRegistryTest.RecordingFactory();
        registry = new EventingRegistry(factory);
        reconciler = new EventingReconciler(repositoryService, runtimeService, registry);
    }

    private Deployment deployment(String id, String tenant, long time, String... resources) {
        Deployment d = mock(Deployment.class);
        when(d.getId()).thenReturn(id);
        when(d.getTenantId()).thenReturn(tenant);
        when(d.getDeploymentTime()).thenReturn(new Date(time));
        when(repositoryService.getDeploymentResourceNames(id)).thenReturn(List.of(resources));
        return d;
    }

    private void resource(String deploymentId, String name, String content) {
        when(repositoryService.getResourceAsStream(eq(deploymentId), eq(name)))
                .thenAnswer(inv -> new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void newDeployment_registersDefinition_underTenantKey() {
        deployments.add(deployment("d1", "acme", 1000, "order.event", "order.channel"));
        resource("d1", "order.event", EVENT_JSON);
        resource("d1", "order.channel", CHANNEL_JSON);

        reconciler.runPass();

        assertThat(registry.activeKeys()).containsExactly("acme#orderChannel");
        assertThat(factory.subscribed).containsExactly("order.new/g1");
    }

    @Test
    void writeOnceCache_scansEachDeploymentExactlyOnce() {
        deployments.add(deployment("d1", null, 1000, "order.event", "order.channel"));
        resource("d1", "order.event", EVENT_JSON);
        resource("d1", "order.channel", CHANNEL_JSON);

        reconciler.runPass();
        reconciler.runPass();
        reconciler.runPass();

        verify(repositoryService, times(1)).getDeploymentResourceNames("d1");
        assertThat(factory.subscribed).hasSize(1); // idempotent no-op on later passes
    }

    @Test
    void newestDeploymentWins_perKey() {
        deployments.add(deployment("d1", null, 1000, "order.channel", "order.event"));
        resource("d1", "order.event", EVENT_JSON);
        resource("d1", "order.channel", CHANNEL_JSON);
        deployments.add(deployment("d2", null, 2000, "order.channel"));
        resource("d2", "order.channel", CHANNEL_JSON.replace("\"g1\"", "\"g2\""));

        reconciler.runPass();

        assertThat(factory.subscribed).containsExactly("order.new/g2");
        assertThat(registry.activeKeys()).containsExactly("#orderChannel");
    }

    @Test
    void deploymentRemoved_unregistersItsDefinitions() {
        Deployment d1 = deployment("d1", null, 1000, "order.event", "order.channel");
        deployments.add(d1);
        resource("d1", "order.event", EVENT_JSON);
        resource("d1", "order.channel", CHANNEL_JSON);
        reconciler.runPass();
        assertThat(registry.activeKeys()).hasSize(1);

        deployments.clear();
        reconciler.runPass();

        assertThat(registry.activeKeys()).isEmpty();
        assertThat(factory.closed).containsExactly("order.new/g1");
    }

    @Test
    void startSubscription_withCorrelationParameters_blocksRegistration() {
        deployments.add(deployment("d1", null, 1000, "order.event", "order.channel"));
        resource("d1", "order.event", EVENT_JSON_WITH_CORRELATION);
        resource("d1", "order.channel", CHANNEL_JSON);
        EventSubscription startSub = mock(EventSubscription.class);
        when(startSub.getExecutionId()).thenReturn(null); // message START event subscription
        EventSubscriptionQuery esq = runtimeService.createEventSubscriptionQuery();
        when(esq.list()).thenReturn(List.of(startSub));

        reconciler.runPass();

        assertThat(registry.activeKeys()).isEmpty();
    }

    @Test
    void brokenResource_poisonsOnlyItsDeployment_passContinues() {
        deployments.add(deployment("d1", null, 1000, "broken.channel"));
        resource("d1", "broken.channel", "{ not json");
        deployments.add(deployment("d2", null, 2000, "order.event", "order.channel"));
        resource("d2", "order.event", EVENT_JSON);
        resource("d2", "order.channel", CHANNEL_JSON);

        reconciler.runPass();

        assertThat(registry.activeKeys()).containsExactly("#orderChannel");
    }

    @Test
    void factoryFailure_isIsolated_andRetriedWithBackoff() {
        deployments.add(deployment("d1", null, 1000, "order.event", "order.channel"));
        resource("d1", "order.event", EVENT_JSON);
        resource("d1", "order.channel", CHANNEL_JSON);
        deployments.add(deployment("d2", null, 1000, "pay.event", "pay.channel"));
        resource("d2", "pay.event", EVENT_JSON.replace("orderEvent", "payEvent"));
        resource("d2", "pay.channel", CHANNEL_JSON.replace("orderChannel", "payChannel")
                .replace("orderEvent", "payEvent").replace("order.new", "pay.new")
                .replace("\"g1\"", "\"g-pay\""));

        EventingRegistry failing = new EventingRegistry((config, event) -> {
            if (config.getSubject().equals("pay.new")) {
                throw new RuntimeException("broker down for pay");
            }
            factory.subscribed.add(config.getSubject() + "/" + config.getDeliverGroup());
            return () -> factory.closed.add(config.getSubject());
        });
        reconciler = new EventingReconciler(repositoryService, runtimeService, failing);

        reconciler.runPass(); // pay fails (attempt 1), order registers
        assertThat(failing.activeKeys()).containsExactly("#orderChannel");

        reconciler.runPass(); // pay in backoff (skip 0 -> attempt 2? skip=0 after first)
        reconciler.runPass();
        // order stayed registered throughout; no churn
        assertThat(factory.subscribed).containsExactly("order.new/g1");
    }

    // --- slice 6: outbound overlay + locations secondary source ---

    private static final String OUTBOUND_CHANNEL_JSON = """
            { "key": "orderOut", "channelType": "outbound", "type": "nats",
              "subject": "ignored.on.lineage",
              "extension": { "messageType": "order-created", "critical": true,
                             "variableAllowlist": ["orderId"] } }
            """;

    @Test
    void outboundChannel_feedsOverlaySink_andRevertsOnRemoval() {
        java.util.concurrent.atomic.AtomicReference<java.util.Map<String, OutboundOverlayDefinition>> sunk =
                new java.util.concurrent.atomic.AtomicReference<>();
        reconciler = new EventingReconciler(repositoryService, runtimeService, registry,
                EventingReconciler.StaticDefinitions.empty(), sunk::set);

        Deployment d1 = deployment("d1", null, 1000, "order.channel-out.channel");
        deployments.add(d1);
        resource("d1", "order.channel-out.channel", OUTBOUND_CHANNEL_JSON);
        reconciler.runPass();

        assertThat(sunk.get()).containsOnlyKeys("order-created");
        assertThat(sunk.get().get("order-created").critical()).isTrue();
        assertThat(sunk.get().get("order-created").variableAllowlist()).containsExactly("orderId");

        deployments.clear(); // deployment deleted -> overlay reverts (D-D v2)
        reconciler.runPass();
        assertThat(sunk.get()).isEmpty();
    }

    @Test
    void locations_registerUnderNullTenantKey() {
        EventDefinition event = EventingDefinitionParser.parseEvent("order.event", EVENT_JSON);
        ChannelDefinition channel = (ChannelDefinition)
                EventingDefinitionParser.parseChannel("order.channel", CHANNEL_JSON);
        reconciler = new EventingReconciler(repositoryService, runtimeService, registry,
                new EventingReconciler.StaticDefinitions(
                        java.util.Map.of(event.key(), event),
                        java.util.Map.of(channel.key(), channel), java.util.Map.of()), null);

        reconciler.runPass();

        assertThat(registry.activeKeys()).containsExactly("#orderChannel");
        assertThat(factory.subscribed).containsExactly("order.new/g1");
    }

    @Test
    void locations_exactKeyCollision_deploymentWins() {
        EventDefinition event = EventingDefinitionParser.parseEvent("order.event", EVENT_JSON);
        ChannelDefinition channel = (ChannelDefinition)
                EventingDefinitionParser.parseChannel("order.channel", CHANNEL_JSON);
        reconciler = new EventingReconciler(repositoryService, runtimeService, registry,
                new EventingReconciler.StaticDefinitions(
                        java.util.Map.of(event.key(), event),
                        java.util.Map.of(channel.key(), channel), java.util.Map.of()), null);

        // tenantless deployment carries the SAME channel key on a different subject
        deployments.add(deployment("d1", null, 1000, "order.event", "order.channel"));
        resource("d1", "order.event", EVENT_JSON);
        resource("d1", "order.channel", CHANNEL_JSON.replace("order.new", "order.v2"));

        reconciler.runPass();

        assertThat(registry.activeKeys()).containsExactly("#orderChannel");
        assertThat(factory.subscribed).containsExactly("order.v2/g1"); // deployment won (F-4a)
    }
}
