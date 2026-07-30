package org.flowable.eventregistry.spring.nats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CopyOnWriteArrayList;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class NatsOutboundChannelIntegrationTest {

    @Container
    static GenericContainer<?> natsContainer = new GenericContainer<>("nats:2.10-alpine")
            .withExposedPorts(4222);

    private Connection adapterConnection;
    private Connection subscriberConnection;

    @BeforeEach
    void setUp() throws IOException, InterruptedException {
        String natsUrl = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);
        Options options = new Options.Builder().server(natsUrl).build();
        adapterConnection = Nats.connect(options);
        subscriberConnection = Nats.connect(options);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (adapterConnection != null) {
            adapterConnection.close();
        }
        if (subscriberConnection != null) {
            subscriberConnection.close();
        }
    }

    @Test
    void outboundChannel_publishesEvent() throws Exception {
        // Arrange
        String subject = "test.outbound";
        CopyOnWriteArrayList<String> receivedMessages = new CopyOnWriteArrayList<>();

        // Subscribe first to capture messages
        subscriberConnection.createDispatcher()
                .subscribe(subject, message -> {
                    receivedMessages.add(new String(message.getData(), StandardCharsets.UTF_8));
                });

        // Flush to ensure subscription is propagated before publishing
        subscriberConnection.flush(Duration.ofSeconds(2));

        NatsOutboundEventChannelAdapter adapter = new NatsOutboundEventChannelAdapter(
                adapterConnection, subject);

        String payload = "{\"orderId\":77,\"status\":\"shipped\"}";

        // Act
        adapter.sendEvent(payload, Collections.emptyMap());
        adapterConnection.flush(Duration.ofSeconds(2));

        // Assert
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(receivedMessages).hasSize(1);
            assertThat(receivedMessages.get(0)).isEqualTo(payload);
        });
    }
}
