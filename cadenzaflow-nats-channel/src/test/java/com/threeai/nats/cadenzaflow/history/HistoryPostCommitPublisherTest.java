package com.threeai.nats.cadenzaflow.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import com.threeai.nats.core.history.HistoryClassNames;
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
import io.nats.client.support.NatsJetStreamConstants;
import org.cadenzaflow.bpm.engine.impl.history.event.HistoricActivityInstanceEventEntity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Real NATS JetStream (Testcontainers) — bulk path, zero DB involvement (BR-HDL-004). */
@Testcontainers
class HistoryPostCommitPublisherTest {

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
                .name("HISTORY_BULK_TEST")
                .subjects("history.>")
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
    void publish_actinstEvent_landsOnHistorySubjectWithHeaders() throws Exception {
        HistoryPostCommitPublisher publisher = new HistoryPostCommitPublisher(jetStream, null);
        HistoricActivityInstanceEventEntity event = new HistoricActivityInstanceEventEntity();
        String eventId = UUID.randomUUID().toString();
        event.setId(eventId);
        event.setEventType("start");
        event.setProcessInstanceId("proc-bulk-1");
        event.setActivityId("task1");

        publisher.publish(event, HistoryClassNames.ACTINST, "cadenzaflow");

        JetStreamManagement jsm = natsConnection.jetStreamManagement();
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable("test-consumer-" + UUID.randomUUID().toString().replace("-", ""))
                .filterSubject("history.cadenzaflow.ACTINST.proc-bulk-1")
                .build();
        jsm.addOrUpdateConsumer("HISTORY_BULK_TEST", cc);
        JetStreamSubscription sub = jetStream.subscribe("history.cadenzaflow.ACTINST.proc-bulk-1",
                PullSubscribeOptions.bind("HISTORY_BULK_TEST", cc.getDurable()));
        List<Message> messages = sub.fetch(1, Duration.ofSeconds(5));

        assertThat(messages).hasSize(1);
        Message msg = messages.get(0);
        assertThat(msg.getHeaders().getFirst(NatsJetStreamConstants.MSG_ID_HDR)).isEqualTo(eventId + ":start");
        assertThat(msg.getSubject()).isEqualTo("history.cadenzaflow.ACTINST.proc-bulk-1");
        assertThat(new String(msg.getData(), java.nio.charset.StandardCharsets.UTF_8)).contains("\"activityId\":\"task1\"");
    }

    @Test
    void publish_jetStreamUnavailable_doesNotThrow_atMostOnceLossAccepted() {
        io.nats.client.Connection isolatedConnection = null;
        try {
            isolatedConnection = Nats.connect(
                    "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222));
            JetStream isolatedJetStream = isolatedConnection.jetStream();
            isolatedConnection.close(); // now every publish on this JetStream will fail

            HistoryPostCommitPublisher publisher = new HistoryPostCommitPublisher(isolatedJetStream, null);
            HistoricActivityInstanceEventEntity event = new HistoricActivityInstanceEventEntity();
            event.setId(UUID.randomUUID().toString());
            event.setEventType("start");
            event.setProcessInstanceId("proc-bulk-2");

            org.assertj.core.api.Assertions.assertThatCode(
                    () -> publisher.publish(event, HistoryClassNames.ACTINST, "cadenzaflow")).doesNotThrowAnyException();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
