package com.threeai.nats.cibseven.inbound;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.Nats;
import org.cibseven.bpm.engine.ProcessEngine;
import org.cibseven.bpm.engine.ProcessEngineConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Start-by-event on the Camunda lineage, with a REAL engine (USER_GUIDE "Starting processes
 * from events"): the correlation subscriber's {@code correlateWithResult()} matches a BPMN
 * MESSAGE START EVENT subscription just as it matches a waiting execution — so an inbound NATS
 * message starts a NEW process instance with no additional code. The subscriber's standard
 * variables ({@code natsPayload}, {@code natsSubject}) land on the started instance.
 */
@Testcontainers
class MessageStartEventIntegrationTest {

    private static final String SUBJECT = "order.start.cibseven";

    private static final String BPMN = """
            <?xml version="1.0" encoding="UTF-8"?>
            <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                         xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                         targetNamespace="starttest">
              <message id="m1" name="OrderMessage"/>
              <process id="orderStart" isExecutable="true" camunda:historyTimeToLive="P1D">
                <startEvent id="s">
                  <messageEventDefinition messageRef="m1"/>
                </startEvent>
                <sequenceFlow id="f1" sourceRef="s" targetRef="wait"/>
                <userTask id="wait"/>
                <sequenceFlow id="f2" sourceRef="wait" targetRef="e"/>
                <endEvent id="e"/>
              </process>
            </definitions>
            """;

    @Container
    static GenericContainer<?> natsContainer = new GenericContainer<>("nats:2.10-alpine")
            .withExposedPorts(4222);

    private Connection connection;
    private ProcessEngine engine;
    private NatsMessageCorrelationSubscriber subscriber;

    @BeforeEach
    void setUp() throws Exception {
        String url = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);
        connection = Nats.connect(url);
        engine = ProcessEngineConfiguration.createStandaloneInMemProcessEngineConfiguration()
                .setJdbcUrl("jdbc:h2:mem:msg-start-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1")
                .setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE)
                .setJobExecutorActivate(false)
                .buildProcessEngine();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (subscriber != null) {
            subscriber.unsubscribe();
        }
        if (engine != null) {
            engine.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void inboundMessage_matchesMessageStartEvent_andStartsNewInstance() throws Exception {
        engine.getRepositoryService().createDeployment()
                .addString("order-start.bpmn20.xml", BPMN).deploy();

        SubscriptionConfig config = new SubscriptionConfig();
        config.setSubject(SUBJECT);
        config.setMessageName("OrderMessage");
        subscriber = new NatsMessageCorrelationSubscriber(connection, engine.getRuntimeService(),
                config, new NatsChannelMetrics(new SimpleMeterRegistry()));
        subscriber.subscribe();

        connection.publish(SUBJECT, "{\"orderId\":\"A-77\"}".getBytes(StandardCharsets.UTF_8));
        connection.flush(Duration.ofSeconds(5));

        awaitTrue(() -> engine.getRuntimeService().createProcessInstanceQuery()
                .processDefinitionKey("orderStart").count() == 1,
                10000, "message start event to create an instance");
        String payload = (String) engine.getRuntimeService().createVariableInstanceQuery()
                .variableName("natsPayload").singleResult().getValue();
        assertThat(payload).contains("A-77");
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
