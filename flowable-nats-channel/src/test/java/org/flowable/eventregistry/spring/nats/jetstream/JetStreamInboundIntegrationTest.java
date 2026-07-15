package org.flowable.eventregistry.spring.nats.jetstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Nats;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;
import org.flowable.eventregistry.api.EventRegistry;
import org.flowable.eventregistry.api.EventRegistryEvent;
import org.flowable.eventregistry.api.EventRegistryEventConsumer;
import org.flowable.eventregistry.api.InboundEvent;
import org.flowable.eventregistry.api.InboundEventProcessor;
import org.flowable.eventregistry.api.OutboundEventProcessor;
import org.flowable.eventregistry.api.runtime.EventInstance;
import org.flowable.eventregistry.model.ChannelModel;
import org.flowable.eventregistry.model.InboundChannelModel;
import org.flowable.eventregistry.spring.nats.channel.NatsInboundChannelModel;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class JetStreamInboundIntegrationTest {

    @Container
    static GenericContainer<?> natsContainer = new GenericContainer<>("nats:2.10-alpine")
            .withCommand("--jetstream")
            .withExposedPorts(4222);

    private Connection connection;
    private JetStream jetStream;
    private JetStreamManagement jsm;
    private NatsChannelMetrics metrics;
    private DlqPublisher dlqPublisher;

    @BeforeEach
    void setUp() throws Exception {
        String url = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);
        connection = Nats.connect(url);
        jetStream = connection.jetStream();
        jsm = connection.jetStreamManagement();
        metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        dlqPublisher = new DlqPublisher(jetStream, connection, metrics);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void inbound_receivesAndAcks() throws Exception {
        // Create stream
        jsm.addStream(StreamConfiguration.builder()
                .name("TEST")
                .subjects("test.inbound")
                .build());

        // Create adapter
        NatsInboundChannelModel channelModel = new NatsInboundChannelModel();
        channelModel.setKey("test-inbound");
        channelModel.setSubject("test.inbound");

        CopyOnWriteArrayList<InboundEvent> receivedEvents = new CopyOnWriteArrayList<>();
        EventRegistry registry = new StubEventRegistry() {
            @Override
            public void eventReceived(InboundChannelModel model, InboundEvent event) {
                receivedEvents.add(event);
            }
        };

        JetStreamInboundEventChannelAdapter adapter = new JetStreamInboundEventChannelAdapter(
                connection, jetStream, "test.inbound", 5, "dlq.test.inbound", metrics, "test-inbound", dlqPublisher);
        adapter.setEventRegistry(registry);
        adapter.setInboundChannelModel(channelModel);
        adapter.subscribe();

        try {
            // Publish message
            byte[] data = "{\"orderId\":\"123\"}".getBytes(StandardCharsets.UTF_8);
            jetStream.publish("test.inbound", data);
            connection.flush(Duration.ofSeconds(2));

            // Await event received
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(receivedEvents).hasSize(1);
                assertThat(receivedEvents.get(0).getBody()).isEqualTo("{\"orderId\":\"123\"}");
            });
        } finally {
            adapter.unsubscribe();
        }
    }

    @Test
    void inbound_processingError_redelivers() throws Exception {
        // Create stream
        jsm.addStream(StreamConfiguration.builder()
                .name("TEST-REDELIVER")
                .subjects("test.redeliver")
                .build());

        NatsInboundChannelModel channelModel = new NatsInboundChannelModel();
        channelModel.setKey("test-redeliver");
        channelModel.setSubject("test.redeliver");

        // Custom registry: throws on first call, succeeds on second
        AtomicInteger callCount = new AtomicInteger(0);
        CopyOnWriteArrayList<InboundEvent> receivedEvents = new CopyOnWriteArrayList<>();
        EventRegistry registry = new StubEventRegistry() {
            @Override
            public void eventReceived(InboundChannelModel model, InboundEvent event) {
                int count = callCount.incrementAndGet();
                if (count == 1) {
                    throw new RuntimeException("Simulated processing error");
                }
                receivedEvents.add(event);
            }
        };

        JetStreamInboundEventChannelAdapter adapter = new JetStreamInboundEventChannelAdapter(
                connection, jetStream, "test.redeliver", 5, null, metrics, "test-redeliver", dlqPublisher);
        adapter.setEventRegistry(registry);
        adapter.setInboundChannelModel(channelModel);
        adapter.subscribe();

        try {
            byte[] data = "{\"retry\":true}".getBytes(StandardCharsets.UTF_8);
            jetStream.publish("test.redeliver", data);
            connection.flush(Duration.ofSeconds(2));

            // Message should eventually succeed on retry (after backoff)
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                assertThat(receivedEvents).hasSize(1);
                assertThat(receivedEvents.get(0).getBody()).isEqualTo("{\"retry\":true}");
            });
            assertThat(callCount.get()).isGreaterThanOrEqualTo(2);
        } finally {
            adapter.unsubscribe();
        }
    }

    @Test
    void inbound_maxDeliverExceeded_sentToDlq() throws Exception {
        // Create stream for the main subject
        jsm.addStream(StreamConfiguration.builder()
                .name("TEST-DLQ")
                .subjects("test.dlq")
                .build());

        // Create stream to capture DLQ messages
        jsm.addStream(StreamConfiguration.builder()
                .name("TEST-DLQ-SINK")
                .subjects("dlq.test.dlq")
                .build());

        NatsInboundChannelModel channelModel = new NatsInboundChannelModel();
        channelModel.setKey("test-dlq");
        channelModel.setSubject("test.dlq");

        // Registry that ALWAYS throws -- forces repeated redelivery
        EventRegistry registry = new StubEventRegistry() {
            @Override
            public void eventReceived(InboundChannelModel model, InboundEvent event) {
                throw new RuntimeException("Always fails");
            }
        };

        // maxDeliver=2: adapter DLQ threshold; NATS consumer configured with maxDeliver=3 (threshold+1)
        // Delivery 1: deliveryCount=1, 1>2 false → process → fail → nakWithDelay(1s)
        // Delivery 2: deliveryCount=2, 2>2 false → process → fail → nakWithDelay(2s)
        // Delivery 3: deliveryCount=3, 3>2 true → message routed to DLQ subject
        JetStreamInboundEventChannelAdapter adapter = new JetStreamInboundEventChannelAdapter(
                connection, jetStream, "test.dlq", 2, "dlq.test.dlq", metrics, "test-dlq", dlqPublisher);
        adapter.setEventRegistry(registry);
        adapter.setInboundChannelModel(channelModel);
        adapter.subscribe();

        try {
            byte[] data = "{\"poison\":true}".getBytes(StandardCharsets.UTF_8);
            jetStream.publish("test.dlq", data);
            connection.flush(Duration.ofSeconds(2));

            // Subscribe to DLQ stream to verify message arrived
            PullSubscribeOptions pullOpts = PullSubscribeOptions.builder()
                    .stream("TEST-DLQ-SINK")
                    .configuration(ConsumerConfiguration.builder()
                            .ackPolicy(io.nats.client.api.AckPolicy.None)
                            .build())
                    .build();
            JetStreamSubscription dlqSub = jetStream.subscribe("dlq.test.dlq", pullOpts);

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                List<io.nats.client.Message> dlqMessages = dlqSub.fetch(1, Duration.ofSeconds(1));
                assertThat(dlqMessages).isNotEmpty();
                String dlqBody = new String(dlqMessages.get(0).getData(), StandardCharsets.UTF_8);
                assertThat(dlqBody).isEqualTo("{\"poison\":true}");
            });
        } finally {
            adapter.unsubscribe();
        }
    }

    /**
     * Base stub implementing all EventRegistry methods as no-ops.
     * Test-specific overrides provide custom behavior.
     */
    static abstract class StubEventRegistry implements EventRegistry {
        @Override public void eventReceived(InboundChannelModel channelModel, String rawEvent) {}
        @Override public void setInboundEventProcessor(InboundEventProcessor p) {}
        @Override public void setOutboundEventProcessor(OutboundEventProcessor p) {}
        @Override public OutboundEventProcessor getSystemOutboundEventProcessor() { return null; }
        @Override public void setSystemOutboundEventProcessor(OutboundEventProcessor p) {}
        @Override public void registerEventRegistryEventConsumer(EventRegistryEventConsumer c) {}
        @Override public void removeFlowableEventRegistryEventConsumer(EventRegistryEventConsumer c) {}
        @Override public String generateKey(Map<String, Object> data) { return null; }
        @Override public void sendEventToConsumers(EventRegistryEvent event) {}
        @Override public void sendSystemEventOutbound(EventInstance eventInstance) {}
        @Override public void sendEventOutbound(EventInstance eventInstance, Collection<ChannelModel> channels) {}
    }
}
