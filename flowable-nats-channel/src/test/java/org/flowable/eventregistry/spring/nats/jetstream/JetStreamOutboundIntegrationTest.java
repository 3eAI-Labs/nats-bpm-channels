package org.flowable.eventregistry.spring.nats.jetstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Nats;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class JetStreamOutboundIntegrationTest {

    @Container
    static GenericContainer<?> natsContainer = new GenericContainer<>("nats:2.10-alpine")
            .withCommand("--jetstream")
            .withExposedPorts(4222);

    private Connection connection;
    private JetStream jetStream;
    private JetStreamManagement jsm;
    private NatsChannelMetrics metrics;

    @BeforeEach
    void setUp() throws Exception {
        String url = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);
        connection = Nats.connect(url);
        jetStream = connection.jetStream();
        jsm = connection.jetStreamManagement();
        metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void outbound_publishesToStream() throws Exception {
        // Create stream
        jsm.addStream(StreamConfiguration.builder()
                .name("TEST-OUT")
                .subjects("test.outbound")
                .build());

        // Create outbound adapter and send event
        JetStreamOutboundEventChannelAdapter adapter = new JetStreamOutboundEventChannelAdapter(
                jetStream, "test.outbound", metrics, "test-outbound");

        String jsonBody = "{\"eventType\":\"order.created\",\"orderId\":\"456\"}";
        Map<String, Object> headers = Map.of("X-Trace-Id", "trace-abc-123", "X-Source", "test");
        adapter.sendEvent(jsonBody, headers);

        // Subscribe to verify the message was published
        PullSubscribeOptions pullOpts = PullSubscribeOptions.builder()
                .stream("TEST-OUT")
                .configuration(ConsumerConfiguration.builder()
                        .ackPolicy(AckPolicy.None)
                        .build())
                .build();
        JetStreamSubscription sub = jetStream.subscribe("test.outbound", pullOpts);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            List<io.nats.client.Message> messages = sub.fetch(1, Duration.ofSeconds(1));
            assertThat(messages).isNotEmpty();
            io.nats.client.Message msg = messages.get(0);
            String body = new String(msg.getData(), StandardCharsets.UTF_8);
            assertThat(body).isEqualTo(jsonBody);
            assertThat(msg.getHeaders()).isNotNull();
            assertThat(msg.getHeaders().getLast("X-Trace-Id")).isEqualTo("trace-abc-123");
            assertThat(msg.getHeaders().getLast("X-Source")).isEqualTo("test");
        });
    }
}
