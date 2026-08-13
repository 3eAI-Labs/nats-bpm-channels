package org.flowable.eventregistry.spring.nats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Nats;
import org.flowable.eventregistry.api.EventRepositoryService;
import org.flowable.eventregistry.api.InboundEvent;
import org.flowable.eventregistry.impl.EventRegistryEngineConfiguration;
import org.flowable.eventregistry.json.converter.ChannelJsonConverter;
import org.flowable.eventregistry.model.ChannelModel;
import org.flowable.eventregistry.spring.nats.channel.NatsInboundChannelModel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The path a user actually has: write a channel definition, deploy it, receive an event.
 *
 * <p>Every other test in this module starts from a {@link NatsInboundChannelModel} built with
 * {@code new}, and most build the adapter directly too. Two layers were therefore never executed —
 * Flowable turning channel JSON into our model, and {@link NatsChannelDefinitionProcessor} turning
 * that model into a live subscription. Both were broken, and both looked fine: deploying the
 * definition documented in QUICK_START.md failed first with
 * {@code Not supported inbound channel model type was found nats} (the type was never registered
 * with {@code ChannelJsonConverter}) and then, once that was fixed, with
 * {@code Unrecognized field "channelFields"} (the documented JSON used a shape typed channel models
 * do not accept).
 *
 * <p>This test starts from the JSON and ends at a received event, so neither layer can rot again
 * without something going red.
 */
@Testcontainers
class ChannelDefinitionDeploymentIntegrationTest {

    @Container
    static GenericContainer<?> natsContainer = new GenericContainer<>("nats:2.10-alpine")
            .withExposedPorts(4222);

    private Connection adapterConnection;
    private Connection publisherConnection;
    private NatsChannelDefinitionProcessor processor;
    private ChannelModel deployedModel;

    @BeforeEach
    void setUp() throws Exception {
        String natsUrl = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);
        adapterConnection = Nats.connect(natsUrl);
        publisherConnection = Nats.connect(natsUrl);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (processor != null && deployedModel != null) {
            processor.unregisterChannelModel(deployedModel, "", mock(EventRepositoryService.class));
        }
        for (Connection connection : new Connection[] { adapterConnection, publisherConnection }) {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    void channelDefinitionFromJson_receivesAnEvent() throws Exception {
        // 1. The converter as the engine configures it — this is what the auto-configuration's
        //    registrar does, and without it the next line throws.
        EventRegistryEngineConfiguration engineConfiguration = new EventRegistryEngineConfiguration();
        ChannelJsonConverter converter = engineConfiguration.getChannelJsonConverter();
        converter.addInboundChannelModelClass("nats", NatsInboundChannelModel.class);

        // 2. The definition exactly as QUICK_START.md documents it.
        deployedModel = converter.convertToChannelModel("""
                { "key": "orderInboundChannel", "channelType": "inbound", "type": "nats",
                  "deserializerType": "json",
                  "channelEventKeyDetection": { "fixedValue": "orderEvent" },
                  "subject": "order.new" }
                """);
        assertThat(deployedModel).isInstanceOf(NatsInboundChannelModel.class);

        // 3. Deployment: the processor turns the model into a live subscription.
        EventRegistryStub eventRegistry = new EventRegistryStub();
        JetStream jetStream = adapterConnection.jetStream();
        NatsChannelMetrics metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        processor = new NatsChannelDefinitionProcessor(adapterConnection, jetStream,
                new JetStreamStreamManager(), metrics,
                new DlqPublisher(jetStream, adapterConnection, metrics));

        assertThat(processor.canProcess(deployedModel))
                .as("the processor recognises what the converter produced")
                .isTrue();
        processor.registerChannelModel(deployedModel, "", eventRegistry,
                mock(EventRepositoryService.class), false);
        adapterConnection.flush(Duration.ofSeconds(5));

        // 4. An event on the subject the definition named.
        String payload = "{\"orderId\":\"A-1001\"}";
        publisherConnection.publish("order.new", payload.getBytes(StandardCharsets.UTF_8));
        publisherConnection.flush(Duration.ofSeconds(5));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(eventRegistry.getReceivedEvents()).hasSize(1);
            InboundEvent event = eventRegistry.getReceivedEvents().get(0);
            assertThat(event.getBody()).isEqualTo(payload);
        });
    }

    /**
     * The queue group has to survive the JSON too. It is the setting that makes a multi-node
     * Flowable cluster handle each event once, and it was silently dropped on the JetStream path
     * until earlier in this branch — a value that is accepted and discarded is worse than one that
     * does not exist.
     */
    @Test
    void channelDefinitionFromJson_carriesQueueGroupIntoTheModel() throws Exception {
        EventRegistryEngineConfiguration engineConfiguration = new EventRegistryEngineConfiguration();
        ChannelJsonConverter converter = engineConfiguration.getChannelJsonConverter();
        converter.addInboundChannelModelClass("nats", NatsInboundChannelModel.class);

        NatsInboundChannelModel model = (NatsInboundChannelModel) converter.convertToChannelModel("""
                { "key": "orderQueued", "channelType": "inbound", "type": "nats",
                  "deserializerType": "json",
                  "channelEventKeyDetection": { "fixedValue": "orderEvent" },
                  "subject": "order.queued",
                  "queueGroup": "order-service",
                  "jetstream": true,
                  "durableName": "order-service-durable" }
                """);

        assertThat(model.getSubject()).isEqualTo("order.queued");
        assertThat(model.getQueueGroup()).isEqualTo("order-service");
        assertThat(model.isJetstream()).isTrue();
        assertThat(model.getDurableName()).isEqualTo("order-service-durable");
    }
}
