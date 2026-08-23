package com.threeai.nats.camunda.eventing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.threeai.nats.camunda.inbound.NatsMessageCorrelationSubscriber;
import com.threeai.nats.camunda.inbound.SubscriptionConfig;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import com.threeai.nats.core.metrics.NatsChannelMetrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.Nats;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.ProcessEngineConfiguration;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * docs/12 §5 acceptance against a REAL engine and a REAL broker: tx-safe deployer (G1),
 * reconciler as registration authority (G2 restart / DELETE), single durable across redeploy
 * (G3), definition-path correlation with correlationParameters + typed payload (D-C v2,
 * G5-lite: wrong key does not advance), reserved-subject THROW (D-F v2) and legacy YAML
 * coexistence. Tenant note (G6 fail-path taken): tenant ids key the registry but the v1
 * correlation does NOT add a tenant filter — carried-but-not-filtering, documented in the
 * USER_GUIDE.
 */
@Testcontainers
class EventingParityAcceptanceTest {

    private static final String BPMN = """
            <?xml version="1.0" encoding="UTF-8"?>
            <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                         xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                         targetNamespace="ertest">
              <message id="m1" name="OrderMessage"/>
              <process id="orderProc" isExecutable="true" camunda:historyTimeToLive="P1D">
                <startEvent id="s"/>
                <sequenceFlow id="f1" sourceRef="s" targetRef="catch"/>
                <intermediateCatchEvent id="catch">
                  <messageEventDefinition messageRef="m1"/>
                </intermediateCatchEvent>
                <sequenceFlow id="f2" sourceRef="catch" targetRef="wait"/>
                <userTask id="wait"/>
                <sequenceFlow id="f3" sourceRef="wait" targetRef="e"/>
                <endEvent id="e"/>
              </process>
            </definitions>
            """;

    private static final String EVENT_JSON = """
            { "key": "orderEvent", "name": "Order Event",
              "correlationParameters": [ { "name": "orderId", "type": "string" } ],
              "payload": [ { "name": "amount", "type": "double" } ],
              "extension": { "messageName": "OrderMessage" } }
            """;

    private static final String CHANNEL_JSON = """
            { "key": "orderChannel", "channelType": "inbound", "type": "nats",
              "channelEventKeyDetection": { "fixedValue": "orderEvent" },
              "queueGroup": "eventing-orders", "subject": "evt.order.accept" }
            """;

    @Container
    static GenericContainer<?> natsContainer = new GenericContainer<>("nats:2.10-alpine")
            .withExposedPorts(4222).withCommand("-js");

    private Connection connection;
    private ProcessEngine engine;
    private EventingRegistry registry;
    private EventingReconciler reconciler;
    private final AtomicInteger nudges = new AtomicInteger();
    private NatsMessageCorrelationSubscriber legacySubscriber;

    @BeforeEach
    void setUp() throws Exception {
        String url = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);
        connection = Nats.connect(url);

        ProcessEngineConfigurationImpl configuration = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneInMemProcessEngineConfiguration()
                        .setJdbcUrl("jdbc:h2:mem:er-acc-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1")
                        .setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE)
                        .setJobExecutorActivate(false);
        configuration.setCustomPostDeployers(new java.util.ArrayList<>(
                List.of(new EventingDeployer(nudges::incrementAndGet))));
        engine = configuration.buildProcessEngine();

        registry = new EventingRegistry(newFactory());
        reconciler = new EventingReconciler(engine.getRepositoryService(),
                engine.getRuntimeService(), registry);
    }

    private DefinitionSubscriberFactory newFactory() throws Exception {
        return new DefinitionSubscriberFactory(connection, connection.jetStream(),
                new JetStreamStreamManager(1), engine.getRuntimeService(),
                new NatsChannelMetrics(new SimpleMeterRegistry()),
                new DlqPublisher(connection.jetStream(), connection,
                        new NatsChannelMetrics(new SimpleMeterRegistry())));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (legacySubscriber != null) {
            legacySubscriber.unsubscribe();
        }
        if (registry != null) {
            registry.close();
        }
        if (engine != null) {
            engine.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    private void deployAll() {
        engine.getRepositoryService().createDeployment()
                .addString("order.bpmn20.xml", BPMN)
                .addString("order.event", EVENT_JSON)
                .addString("order.channel", CHANNEL_JSON)
                .deploy();
    }

    @Test
    void definitionDeploy_correlatesByParameter_typedPayload_wrongKeyDoesNotAdvance() throws Exception {
        deployAll();
        assertThat(nudges.get()).isEqualTo(1); // COMMITTED listener fired exactly once

        reconciler.runPass();
        assertThat(registry.activeKeys()).containsExactly("#orderChannel");

        var rt = engine.getRuntimeService();
        String i1 = rt.startProcessInstanceByKey("orderProc", Map.of("orderId", "A-1")).getId();
        String i2 = rt.startProcessInstanceByKey("orderProc", Map.of("orderId", "A-2")).getId();

        connection.publish("evt.order.accept",
                "{\"orderId\":\"A-2\",\"amount\":9.5}".getBytes(StandardCharsets.UTF_8));
        connection.flush(Duration.ofSeconds(5));

        awaitTrue(() -> engine.getTaskService().createTaskQuery()
                .processInstanceId(i2).count() == 1, 10000, "instance A-2 to advance");
        // wrong correlation key: A-1 must still be waiting at the catch event (G5-lite)
        assertThat(engine.getTaskService().createTaskQuery().processInstanceId(i1).count()).isZero();

        // typed payload landed; the definition path writes NO natsPayload blob (D-C v2)
        assertThat(rt.getVariable(i2, "amount")).isEqualTo(9.5);
        assertThat(rt.getVariable(i2, "natsPayload")).isNull();
    }

    @Test
    void brokenDefinition_failsDeployment_noNudge_noSubscription_bpmnRolledBack() {
        String broken = CHANNEL_JSON.replace("\"queueGroup\": \"eventing-orders\", ", "");
        assertThatThrownBy(() -> engine.getRepositoryService().createDeployment()
                .addString("order.bpmn20.xml", BPMN)
                .addString("order.event", EVENT_JSON)
                .addString("order.channel", broken)
                .deploy()).hasMessageContaining("VAL_EVENTING_QUEUE_GROUP_REQUIRED");

        assertThat(nudges.get()).isZero(); // nothing committed, nothing nudged (G1)
        assertThat(engine.getRepositoryService().createDeploymentQuery().count()).isZero();
        reconciler.runPass();
        assertThat(registry.activeKeys()).isEmpty();
    }

    @Test
    void reservedSubject_throwsAtDeployment() {
        String reserved = CHANNEL_JSON.replace("evt.order.accept", "ewjobs.orders");
        assertThatThrownBy(() -> engine.getRepositoryService().createDeployment()
                .addString("order.channel", reserved)
                .addString("order.event", EVENT_JSON)
                .deploy()).hasMessageContaining("ewjobs");
        assertThat(engine.getRepositoryService().createDeploymentQuery().count()).isZero();
    }

    @Test
    void restart_reconcilerRebuildsSubscriptionsFromEngineState() throws Exception {
        deployAll();
        reconciler.runPass();
        assertThat(registry.activeKeys()).containsExactly("#orderChannel");

        // node restart: registry and reconciler die with the process; engine state survives
        registry.close();
        registry = new EventingRegistry(newFactory());
        reconciler = new EventingReconciler(engine.getRepositoryService(),
                engine.getRuntimeService(), registry);

        reconciler.runPass();
        assertThat(registry.activeKeys()).containsExactly("#orderChannel"); // G2
    }

    @Test
    void deploymentDelete_unregisters() {
        deployAll();
        reconciler.runPass();
        assertThat(registry.activeKeys()).containsExactly("#orderChannel");

        String deploymentId = engine.getRepositoryService().createDeploymentQuery()
                .singleResult().getId();
        engine.getRepositoryService().deleteDeployment(deploymentId, true);

        reconciler.runPass();
        assertThat(registry.activeKeys()).isEmpty(); // G2 DELETE half
    }

    @Test
    void redeploy_sameKey_keepsSingleDurable() throws Exception {
        String jsChannel = """
                { "key": "orderChannel", "channelType": "inbound", "type": "nats",
                  "channelEventKeyDetection": { "fixedValue": "orderEvent" },
                  "queueGroup": "eventing-orders", "subject": "evt.order.js",
                  "jetstream": true, "durableName": "er-orders",
                  "autoCreateStream": true, "streamName": "ER_ORDERS", "maxDeliver": 5 }
                """;
        engine.getRepositoryService().createDeployment()
                .addString("order.event", EVENT_JSON).addString("order.channel", jsChannel).deploy();
        reconciler.runPass();
        assertThat(connection.jetStreamManagement().getConsumerNames("ER_ORDERS"))
                .containsExactly("er-orders");

        // redeploy the same key with a changed knob -> atomic replace, still ONE durable (G3)
        engine.getRepositoryService().createDeployment()
                .addString("order.event", EVENT_JSON)
                .addString("order.channel", jsChannel.replace("\"maxDeliver\": 5", "\"maxDeliver\": 7"))
                .deploy();
        reconciler.runPass();
        assertThat(registry.activeKeys()).containsExactly("#orderChannel");
        assertThat(connection.jetStreamManagement().getConsumerNames("ER_ORDERS"))
                .containsExactly("er-orders");
    }

    @Test
    void legacyYamlSubscription_coexists_andKeepsNatsPayload() throws Exception {
        deployAll();
        reconciler.runPass();

        SubscriptionConfig legacy = new SubscriptionConfig();
        legacy.setSubject("legacy.order");
        legacy.setMessageName("OrderMessage");
        legacySubscriber = new NatsMessageCorrelationSubscriber(connection,
                engine.getRuntimeService(), legacy, new NatsChannelMetrics(new SimpleMeterRegistry()));
        legacySubscriber.subscribe();

        var rt = engine.getRuntimeService();
        String i1 = rt.startProcessInstanceByKey("orderProc", Map.of("orderId", "L-1")).getId();
        connection.publish("legacy.order", "{\"any\":\"thing\"}".getBytes(StandardCharsets.UTF_8));
        connection.flush(Duration.ofSeconds(5));

        awaitTrue(() -> engine.getTaskService().createTaskQuery()
                .processInstanceId(i1).count() == 1, 10000, "legacy correlation to advance");
        assertThat((String) rt.getVariable(i1, "natsPayload")).contains("thing");
    }

    private void awaitTrue(java.util.function.BooleanSupplier condition, long timeoutMs, String what) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError("Timed out waiting for: " + what);
    }
}
