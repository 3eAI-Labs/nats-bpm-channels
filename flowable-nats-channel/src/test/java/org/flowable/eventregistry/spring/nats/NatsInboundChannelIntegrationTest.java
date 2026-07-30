package org.flowable.eventregistry.spring.nats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.flowable.eventregistry.api.InboundEvent;
import org.flowable.eventregistry.spring.nats.channel.NatsInboundChannelModel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class NatsInboundChannelIntegrationTest {

    @Container
    static GenericContainer<?> natsContainer = new GenericContainer<>("nats:2.10-alpine")
            .withExposedPorts(4222);

    private Connection adapterConnection;
    private Connection publisherConnection;

    @BeforeEach
    void setUp() throws IOException, InterruptedException {
        String natsUrl = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);
        Options options = new Options.Builder().server(natsUrl).build();
        adapterConnection = Nats.connect(options);
        publisherConnection = Nats.connect(options);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (adapterConnection != null) {
            adapterConnection.close();
        }
        if (publisherConnection != null) {
            publisherConnection.close();
        }
    }

    @Test
    void inboundChannel_receivesAndProcessesEvent() throws Exception {
        // Arrange
        String subject = "test.inbound";
        EventRegistryStub eventRegistryStub = new EventRegistryStub();

        NatsInboundChannelModel channelModel = new NatsInboundChannelModel();
        channelModel.setKey("testInbound");
        channelModel.setSubject(subject);

        NatsInboundEventChannelAdapter adapter = new NatsInboundEventChannelAdapter(
                adapterConnection, subject, null);
        adapter.setInboundChannelModel(channelModel);
        adapter.setEventRegistry(eventRegistryStub);
        adapter.subscribe();

        // Flush to ensure subscription is active on the server
        adapterConnection.flush(Duration.ofSeconds(2));

        String payload = "{\"orderId\":42,\"type\":\"created\"}";

        // Act
        publisherConnection.publish(subject, payload.getBytes(StandardCharsets.UTF_8));
        publisherConnection.flush(Duration.ofSeconds(2));

        // Assert
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(eventRegistryStub.getReceivedEvents()).hasSize(1);
            InboundEvent event = eventRegistryStub.getReceivedEvents().get(0);
            assertThat(event.getBody()).isEqualTo(payload);
        });

        adapter.unsubscribe();
    }

    @Test
    void inboundChannel_withQueueGroup_loadBalances() throws Exception {
        // Arrange
        String subject = "test.queue";
        String queueGroup = "test-workers";
        EventRegistryStub stub1 = new EventRegistryStub();
        EventRegistryStub stub2 = new EventRegistryStub();

        NatsInboundChannelModel channelModel1 = new NatsInboundChannelModel();
        channelModel1.setKey("queueAdapter1");
        channelModel1.setSubject(subject);
        channelModel1.setQueueGroup(queueGroup);

        NatsInboundChannelModel channelModel2 = new NatsInboundChannelModel();
        channelModel2.setKey("queueAdapter2");
        channelModel2.setSubject(subject);
        channelModel2.setQueueGroup(queueGroup);

        NatsInboundEventChannelAdapter adapter1 = new NatsInboundEventChannelAdapter(
                adapterConnection, subject, queueGroup);
        adapter1.setInboundChannelModel(channelModel1);
        adapter1.setEventRegistry(stub1);
        adapter1.subscribe();

        // Create a separate connection for second adapter
        String natsUrl = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);
        Connection adapter2Connection = Nats.connect(new Options.Builder().server(natsUrl).build());

        NatsInboundEventChannelAdapter adapter2 = new NatsInboundEventChannelAdapter(
                adapter2Connection, subject, queueGroup);
        adapter2.setInboundChannelModel(channelModel2);
        adapter2.setEventRegistry(stub2);
        adapter2.subscribe();

        // Flush to ensure both subscriptions are active
        adapterConnection.flush(Duration.ofSeconds(2));
        adapter2Connection.flush(Duration.ofSeconds(2));

        String payload = "{\"orderId\":99}";

        // Act — publish a single message
        publisherConnection.publish(subject, payload.getBytes(StandardCharsets.UTF_8));
        publisherConnection.flush(Duration.ofSeconds(2));

        // Assert — exactly one of the two adapters should receive it
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            int totalReceived = stub1.getReceivedEvents().size() + stub2.getReceivedEvents().size();
            assertThat(totalReceived).isEqualTo(1);
        });

        // Verify only one adapter got the message (load balancing)
        assertThat(stub1.getReceivedEvents().size() + stub2.getReceivedEvents().size()).isEqualTo(1);

        adapter1.unsubscribe();
        adapter2.unsubscribe();
        adapter2Connection.close();
    }
}
