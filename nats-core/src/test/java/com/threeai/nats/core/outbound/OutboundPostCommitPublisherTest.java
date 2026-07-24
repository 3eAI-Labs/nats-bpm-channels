package com.threeai.nats.core.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Real NATS JetStream (Testcontainers) — best-effort path, zero DB involvement (mirrors
 *  basamak-2 {@code HistoryPostCommitPublisherTest}). */
@Testcontainers
class OutboundPostCommitPublisherTest {

    private static GenericContainer<?> natsContainer;
    private static io.nats.client.Connection natsConnection;
    private static JetStream jetStream;

    @BeforeAll
    static void startContainer() throws Exception {
        natsContainer = new GenericContainer<>("nats:2.10-alpine").withCommand("--jetstream").withExposedPorts(4222);
        natsContainer.start();
        natsConnection = Nats.connect("nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222));
        jetStream = natsConnection.jetStream();

        JetStreamManagement jsm = natsConnection.jetStreamManagement();
        jsm.addStream(StreamConfiguration.builder()
                .name("EVENTS_BULK_TEST")
                .subjects("events.>")
                .retentionPolicy(RetentionPolicy.Limits)
                .storageType(StorageType.File)
                .build());
    }

    @AfterAll
    static void stopContainer() throws Exception {
        natsConnection.close();
        natsContainer.stop();
    }

    @Test
    void publish_draft_landsOnSubjectWithHeaders() throws Exception {
        OutboundPostCommitPublisher publisher = new OutboundPostCommitPublisher(jetStream, null);
        OutboundMessageDraft draft = new OutboundMessageDraft("camunda", "order.created", "proc-bulk-1", "biz-1",
                "trace-1", "events.camunda.order.created.proc-bulk-1", "{\"foo\":1}".getBytes(StandardCharsets.UTF_8));

        publisher.publish(draft);

        JetStreamManagement jsm = natsConnection.jetStreamManagement();
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable("test-consumer-" + UUID.randomUUID().toString().replace("-", ""))
                .filterSubject("events.camunda.order.created.proc-bulk-1")
                .build();
        jsm.addOrUpdateConsumer("EVENTS_BULK_TEST", cc);
        JetStreamSubscription sub = jetStream.subscribe("events.camunda.order.created.proc-bulk-1",
                PullSubscribeOptions.bind("EVENTS_BULK_TEST", cc.getDurable()));
        List<Message> messages = sub.fetch(1, Duration.ofSeconds(5));

        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).getSubject()).isEqualTo("events.camunda.order.created.proc-bulk-1");
        assertThat(new String(messages.get(0).getData(), StandardCharsets.UTF_8)).isEqualTo("{\"foo\":1}");
    }

    @Test
    void publish_withMetrics_incrementsPostCommitPublishedCounter() throws Exception {
        NatsChannelMetrics metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        OutboundPostCommitPublisher publisher = new OutboundPostCommitPublisher(jetStream, metrics);
        OutboundMessageDraft draft = new OutboundMessageDraft("camunda", "order.created", "proc-bulk-metrics",
                "biz-1", "trace-1", "events.camunda.order.created.proc-bulk-metrics",
                "{}".getBytes(StandardCharsets.UTF_8));

        publisher.publish(draft);

        assertThat(metrics.outboundPostCommitPublishedCount("order.created").count()).isEqualTo(1.0);
    }

    @Test
    void publish_jetStreamUnavailable_doesNotThrow_atMostOnceLossAccepted() throws Exception {
        io.nats.client.Connection isolatedConnection =
                Nats.connect("nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222));
        JetStream isolatedJetStream = isolatedConnection.jetStream();
        isolatedConnection.close(); // now every publish on this JetStream will fail

        OutboundPostCommitPublisher publisher = new OutboundPostCommitPublisher(isolatedJetStream, null);
        OutboundMessageDraft draft = new OutboundMessageDraft("camunda", "order.created", "proc-bulk-2", null,
                "trace-2", "events.camunda.order.created.proc-bulk-2", "{}".getBytes(StandardCharsets.UTF_8));

        assertThatCode(() -> publisher.publish(draft)).doesNotThrowAnyException();
    }
}
