package org.flowable.eventregistry.spring.nats;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Nats;
import org.flowable.common.engine.impl.AbstractEngineConfiguration;
import org.flowable.common.engine.impl.interceptor.EngineConfigurationConstants;
import org.flowable.engine.ProcessEngine;
import org.flowable.engine.ProcessEngineConfiguration;
import org.flowable.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.flowable.eventregistry.api.EventRepositoryService;
import org.flowable.eventregistry.impl.EventRegistryEngineConfiguration;
import org.flowable.eventregistry.model.ChannelModel;
import org.flowable.eventregistry.spring.nats.channel.NatsInboundChannelModel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Start-by-event, end to end on the REAL pipeline (USER_GUIDE "Starting processes from
 * events"): a NATS message on a deployed inbound channel starts a NEW process instance through
 * the engine's own event-registry start-event mechanics — our adapter only delivers the event;
 * the start decision belongs to {@code BpmnEventRegistryEventConsumer}. Also asserts the
 * declared payload mapping ({@code eventOutParameter}) lands as a process variable.
 */
@Testcontainers
class EventStartedProcessIntegrationTest {

    private static final String SUBJECT = "order.start.test";

    private static final String EVENT_JSON = """
            { "key": "orderEvent", "name": "Order Event",
              "correlationParameters": [],
              "payload": [ { "name": "orderId", "type": "string" } ] }
            """;

    private static final String CHANNEL_JSON = """
            { "key": "orderStartChannel", "channelType": "inbound", "type": "nats",
              "deserializerType": "json",
              "channelEventKeyDetection": { "fixedValue": "orderEvent" },
              "subject": "%s" }
            """.formatted(SUBJECT);

    private static final String BPMN = """
            <?xml version="1.0" encoding="UTF-8"?>
            <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                         xmlns:flowable="http://flowable.org/bpmn"
                         targetNamespace="ewtest">
              <process id="orderProcess" isExecutable="true">
                <startEvent id="start">
                  <extensionElements>
                    <flowable:eventType>orderEvent</flowable:eventType>
                    <flowable:eventOutParameter source="orderId" sourceType="string" target="orderId"/>
                  </extensionElements>
                </startEvent>
                <sequenceFlow id="f1" sourceRef="start" targetRef="wait"/>
                <userTask id="wait"/>
                <sequenceFlow id="f2" sourceRef="wait" targetRef="end"/>
                <endEvent id="end"/>
              </process>
            </definitions>
            """;

    @Container
    static GenericContainer<?> natsContainer = new GenericContainer<>("nats:2.10-alpine")
            .withExposedPorts(4222);

    private Connection adapterConnection;
    private Connection publisherConnection;
    private ProcessEngine processEngine;
    private NatsChannelDefinitionProcessor processor;
    private ChannelModel channelModel;

    @BeforeEach
    void setUp() throws Exception {
        String url = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);
        adapterConnection = Nats.connect(url);
        publisherConnection = Nats.connect(url);

        ProcessEngineConfigurationImpl configuration = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneInMemProcessEngineConfiguration();
        configuration.setJdbcUrl("jdbc:h2:mem:event-start-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        configuration.setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE);
        configuration.setAsyncExecutorActivate(false);
        processEngine = configuration.buildProcessEngine();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (processEngine != null) {
            processEngine.close();
        }
        if (adapterConnection != null) {
            adapterConnection.close();
        }
        if (publisherConnection != null) {
            publisherConnection.close();
        }
    }

    private EventRegistryEngineConfiguration eventRegistryConfiguration() {
        ProcessEngineConfigurationImpl configuration =
                (ProcessEngineConfigurationImpl) processEngine.getProcessEngineConfiguration();
        Map<String, AbstractEngineConfiguration> engineConfigurations =
                configuration.getEngineConfigurations();
        return (EventRegistryEngineConfiguration) engineConfigurations
                .get(EngineConfigurationConstants.KEY_EVENT_REGISTRY_CONFIG);
    }

    @Test
    void inboundEvent_onDeployedChannel_startsNewProcessInstance_withMappedPayload() throws Exception {
        EventRegistryEngineConfiguration erConfiguration = eventRegistryConfiguration();
        assertThat(erConfiguration).as("event registry attached to the engine").isNotNull();

        // Deploy the EVENT definition through the engine's own repository.
        erConfiguration.getEventRepositoryService().createDeployment()
                .addString("order.event", EVENT_JSON).deploy();

        // Deploy the BPMN whose START EVENT references the event type.
        processEngine.getRepositoryService().createDeployment()
                .addString("order.bpmn20.xml", BPMN).deploy();

        // Register OUR processor and the "nats" model class on the ENGINE, then deploy the
        // channel THROUGH the engine — so the default inbound pipeline (deserializer, key
        // detection, payload extraction) is built alongside our live NATS subscription. This is
        // the auto-configuration's production wiring, reproduced manually.
        erConfiguration.getChannelJsonConverter()
                .addInboundChannelModelClass("nats", NatsInboundChannelModel.class);
        JetStream jetStream = adapterConnection.jetStream();
        NatsChannelMetrics metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        processor = new NatsChannelDefinitionProcessor(adapterConnection, jetStream,
                new JetStreamStreamManager(), metrics,
                new DlqPublisher(jetStream, adapterConnection, metrics));
        erConfiguration.addChannelModelProcessor(processor);
        erConfiguration.getEventRepositoryService().createDeployment()
                .addString("order.channel", CHANNEL_JSON).deploy();
        adapterConnection.flush(Duration.ofSeconds(5));

        // A message on the subject: the event starts a NEW instance.
        publisherConnection.publish(SUBJECT, "{\"orderId\":\"A-1001\"}".getBytes(StandardCharsets.UTF_8));
        publisherConnection.flush(Duration.ofSeconds(5));

        awaitTrue(() -> processEngine.getRuntimeService().createProcessInstanceQuery()
                .processDefinitionKey("orderProcess").count() == 1,
                10000, "event-registry start event to create an instance");
        Object orderId = processEngine.getRuntimeService().createVariableInstanceQuery()
                .variableName("orderId").singleResult().getValue();
        assertThat(orderId).isEqualTo("A-1001");
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
