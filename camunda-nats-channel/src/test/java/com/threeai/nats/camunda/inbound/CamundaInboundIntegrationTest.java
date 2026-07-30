package com.threeai.nats.camunda.inbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.runtime.MessageCorrelationBuilder;
import org.camunda.bpm.engine.runtime.MessageCorrelationResult;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class CamundaInboundIntegrationTest {

    @Container
    static GenericContainer<?> natsContainer = new GenericContainer<>("nats:2.10-alpine")
            .withCommand("--jetstream")
            .withExposedPorts(4222);

    private Connection connection;
    private JetStream jetStream;
    private JetStreamManagement jsm;
    private NatsChannelMetrics metrics;
    private DlqPublisher dlqPublisher;
    private RuntimeService runtimeService;
    private MessageCorrelationBuilder correlationBuilder;

    @BeforeEach
    void setUp() throws Exception {
        String url = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);
        connection = Nats.connect(url);
        jetStream = connection.jetStream();
        jsm = connection.jetStreamManagement();
        metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        dlqPublisher = new DlqPublisher(jetStream, connection, metrics);

        runtimeService = mock(RuntimeService.class);
        correlationBuilder = mock(MessageCorrelationBuilder.class);
        when(runtimeService.createMessageCorrelation(any())).thenReturn(correlationBuilder);
        when(correlationBuilder.processInstanceBusinessKey(any())).thenReturn(correlationBuilder);
        when(correlationBuilder.setVariables(anyMap())).thenReturn(correlationBuilder);
        when(correlationBuilder.correlateWithResult()).thenReturn(mock(MessageCorrelationResult.class));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void inbound_coreNats_correlatesMessage() throws Exception {
        SubscriptionConfig config = new SubscriptionConfig();
        config.setSubject("camunda.test.core");
        config.setMessageName("TestMessage");

        NatsMessageCorrelationSubscriber subscriber =
                new NatsMessageCorrelationSubscriber(connection, runtimeService, config, metrics);
        subscriber.subscribe();

        try {
            connection.publish("camunda.test.core",
                    "{\"orderId\":\"123\"}".getBytes(StandardCharsets.UTF_8));
            connection.flush(Duration.ofSeconds(2));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    verify(runtimeService).createMessageCorrelation("TestMessage"));
        } finally {
            subscriber.unsubscribe();
        }
    }

    @Test
    void inbound_jetStream_acksAfterCorrelation() throws Exception {
        jsm.addStream(StreamConfiguration.builder()
                .name("CAMUNDA-TEST")
                .subjects("camunda.test.js")
                .build());

        SubscriptionConfig config = new SubscriptionConfig();
        config.setSubject("camunda.test.js");
        config.setMessageName("JsTestMessage");
        config.setJetstream(true);
        config.setMaxDeliver(5);
        config.setDlqSubject("dlq.camunda.test.js");

        JetStreamMessageCorrelationSubscriber subscriber =
                new JetStreamMessageCorrelationSubscriber(
                        connection, jetStream, runtimeService, config, metrics, dlqPublisher);
        subscriber.subscribe();

        try {
            jetStream.publish("camunda.test.js",
                    "{\"orderId\":\"456\"}".getBytes(StandardCharsets.UTF_8));
            connection.flush(Duration.ofSeconds(2));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                verify(runtimeService).createMessageCorrelation("JsTestMessage");
                verify(correlationBuilder).correlateWithResult();
            });
        } finally {
            subscriber.unsubscribe();
        }
    }

    @Test
    void inbound_jetStream_maxDeliverExceeded_dlq() throws Exception {
        jsm.addStream(StreamConfiguration.builder()
                .name("CAMUNDA-DLQ")
                .subjects("camunda.test.dlq")
                .build());

        jsm.addStream(StreamConfiguration.builder()
                .name("CAMUNDA-DLQ-SINK")
                .subjects("dlq.camunda.test.dlq")
                .build());

        // RuntimeService always throws to force redeliveries
        RuntimeService failingRuntimeService = mock(RuntimeService.class);
        MessageCorrelationBuilder failingBuilder = mock(MessageCorrelationBuilder.class);
        when(failingRuntimeService.createMessageCorrelation(any())).thenReturn(failingBuilder);
        when(failingBuilder.processInstanceBusinessKey(any())).thenReturn(failingBuilder);
        when(failingBuilder.setVariables(anyMap())).thenReturn(failingBuilder);
        when(failingBuilder.correlateWithResult()).thenThrow(new RuntimeException("Always fails"));

        SubscriptionConfig config = new SubscriptionConfig();
        config.setSubject("camunda.test.dlq");
        config.setMessageName("DlqTestMessage");
        config.setJetstream(true);
        config.setMaxDeliver(2);
        config.setDlqSubject("dlq.camunda.test.dlq");

        JetStreamMessageCorrelationSubscriber subscriber =
                new JetStreamMessageCorrelationSubscriber(
                        connection, jetStream, failingRuntimeService, config, metrics, dlqPublisher);
        subscriber.subscribe();

        try {
            jetStream.publish("camunda.test.dlq",
                    "{\"poison\":true}".getBytes(StandardCharsets.UTF_8));
            connection.flush(Duration.ofSeconds(2));

            // Subscribe to DLQ stream
            PullSubscribeOptions pullOpts = PullSubscribeOptions.builder()
                    .stream("CAMUNDA-DLQ-SINK")
                    .configuration(ConsumerConfiguration.builder()
                            .ackPolicy(AckPolicy.None)
                            .build())
                    .build();
            JetStreamSubscription dlqSub = jetStream.subscribe("dlq.camunda.test.dlq", pullOpts);

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                List<io.nats.client.Message> dlqMessages = dlqSub.fetch(1, Duration.ofSeconds(1));
                assertThat(dlqMessages).isNotEmpty();
                String dlqBody = new String(dlqMessages.get(0).getData(), StandardCharsets.UTF_8);
                assertThat(dlqBody).isEqualTo("{\"poison\":true}");
            });
        } finally {
            subscriber.unsubscribe();
        }
    }
}
