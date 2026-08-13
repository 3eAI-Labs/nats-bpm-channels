package com.threeai.nats.history.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The projection service is deployed with more than one instance for availability, and each runs a
 * {@link HistoryDlqInspectionSubscriptionRegistrar}. Its durable was fixed
 * ({@code history-dlq-inspection}) with no deliver group, which makes the consumer exclusive: the
 * first instance bound it and every other one failed to start with {@code [SUB-90012]}, taking its
 * whole Spring context down.
 *
 * <p>The registrar's own unit test could not see this — it mocks {@code JetStream}, and a mocked
 * broker enforces no binding rules. Two real registrars against a real server is the smallest setup
 * that can express the property at all.
 */
@Testcontainers
class HistoryDlqInspectionMultiInstanceIntegrationTest {

    private static final String DLQ_SUBJECT = "dlq.history.cadenzaflow.userOperationLog";

    private static GenericContainer<?> natsContainer;
    private static String natsUrl;

    private Connection instanceOneConnection;
    private Connection instanceTwoConnection;
    private HistoryDlqInspectionSubscriptionRegistrar instanceOne;
    private HistoryDlqInspectionSubscriptionRegistrar instanceTwo;

    @BeforeAll
    static void startContainer() throws Exception {
        natsContainer = new GenericContainer<>("nats:2.10-alpine").withCommand("--jetstream").withExposedPorts(4222);
        natsContainer.start();
        natsUrl = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);

        try (Connection provisioning = Nats.connect(natsUrl)) {
            provisioning.jetStreamManagement().addStream(StreamConfiguration.builder()
                    .name("DLQ_HISTORY_MULTI_INSTANCE_TEST")
                    .subjects("dlq.history.>")
                    .retentionPolicy(RetentionPolicy.Limits)
                    .storageType(StorageType.File)
                    .build());
        }
    }

    @AfterAll
    static void stopContainer() {
        natsContainer.stop();
    }

    @AfterEach
    void closeInstances() throws Exception {
        for (HistoryDlqInspectionSubscriptionRegistrar registrar
                : new HistoryDlqInspectionSubscriptionRegistrar[] { instanceOne, instanceTwo }) {
            if (registrar != null) {
                registrar.destroy();
            }
        }
        for (Connection connection : new Connection[] { instanceOneConnection, instanceTwoConnection }) {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    void secondInstance_startsWithoutFailingItsContext() throws Exception {
        HistoryDlqInspectionConsumer consumer = mock(HistoryDlqInspectionConsumer.class);

        instanceOneConnection = Nats.connect(natsUrl);
        instanceOne = new HistoryDlqInspectionSubscriptionRegistrar(
                instanceOneConnection, instanceOneConnection.jetStream(), consumer);
        instanceOne.afterPropertiesSet();

        instanceTwoConnection = Nats.connect(natsUrl);
        instanceTwo = new HistoryDlqInspectionSubscriptionRegistrar(
                instanceTwoConnection, instanceTwoConnection.jetStream(), consumer);

        assertThatCode(() -> instanceTwo.afterPropertiesSet()).doesNotThrowAnyException();
    }

    @Test
    void bothInstancesRunning_eachDlqRecordInspectedOnce() throws Exception {
        int records = 15;
        CountDownLatch inspected = new CountDownLatch(records);
        AtomicInteger inspections = new AtomicInteger();

        HistoryDlqInspectionConsumer consumer = mock(HistoryDlqInspectionConsumer.class);
        doAnswer(invocation -> {
            inspections.incrementAndGet();
            inspected.countDown();
            ((Message) invocation.getArgument(0)).ack();
            return null;
        }).when(consumer).onMessage(org.mockito.ArgumentMatchers.any(Message.class));

        instanceOneConnection = Nats.connect(natsUrl);
        instanceOne = new HistoryDlqInspectionSubscriptionRegistrar(
                instanceOneConnection, instanceOneConnection.jetStream(), consumer);
        instanceOne.afterPropertiesSet();

        instanceTwoConnection = Nats.connect(natsUrl);
        instanceTwo = new HistoryDlqInspectionSubscriptionRegistrar(
                instanceTwoConnection, instanceTwoConnection.jetStream(), consumer);
        instanceTwo.afterPropertiesSet();

        JetStream publisher = instanceOneConnection.jetStream();
        for (int i = 0; i < records; i++) {
            publisher.publish(DLQ_SUBJECT, ("{\"seq\":" + i + "}").getBytes(StandardCharsets.UTF_8));
        }

        assertThat(inspected.await(30, TimeUnit.SECONDS))
                .as("all %d DLQ records inspected within 30s", records)
                .isTrue();
        Thread.sleep(2000);
        assertThat(inspections.get()).as("no record inspected by more than one instance").isEqualTo(records);
    }
}
