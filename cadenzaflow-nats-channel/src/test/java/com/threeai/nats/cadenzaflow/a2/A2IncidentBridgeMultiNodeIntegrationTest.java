package com.threeai.nats.cadenzaflow.a2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.threeai.nats.core.metrics.NatsChannelMetrics;
import com.threeai.nats.core.resilience.DlqBridgeCircuitBreakerFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.NatsJetStreamConstants;
import org.cadenzaflow.bpm.engine.ExternalTaskService;
import org.cadenzaflow.bpm.engine.exception.NotFoundException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The incident bridge has the same multi-node obligation as the completion bridge: every engine
 * node runs one, and each DLQ message must raise its incident exactly once.
 *
 * <p>Its failure mode is the opposite of {@code [SUB-90012]} and considerably quieter. The bridge
 * was subscribed with no durable name at all, which makes the consumer ephemeral and therefore
 * private to each node — so nothing fails, every node simply receives every message on
 * {@code dlq.jobs.>} and calls {@code handleFailure(...)} for it. Three nodes did the same work
 * three times, two of those attempts landing on an already-resolved task and being swallowed as
 * idempotent. Nothing in a log or a metric distinguishes that from healthy operation.
 *
 * <p>It stayed invisible because the completion bridge's exclusive durable meant a second node
 * could never start in the first place. Fixing that is what makes this reachable.
 */
@Testcontainers
class A2IncidentBridgeMultiNodeIntegrationTest {

    private static final String DLQ_SUBJECT = "dlq.jobs.order-fulfillment";
    private static final String SENTINEL_WORKER_ID = "a2-jetstream-bridge";

    private static GenericContainer<?> natsContainer;
    private static String natsUrl;

    private Connection nodeOneConnection;
    private Connection nodeTwoConnection;
    private A2IncidentBridge nodeOneBridge;
    private A2IncidentBridge nodeTwoBridge;

    @BeforeAll
    static void startContainer() throws Exception {
        natsContainer = new GenericContainer<>("nats:2.10-alpine").withCommand("--jetstream").withExposedPorts(4222);
        natsContainer.start();
        natsUrl = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);

        try (Connection provisioning = Nats.connect(natsUrl)) {
            provisioning.jetStreamManagement().addStream(StreamConfiguration.builder()
                    .name("DLQ_JOBS_MULTINODE_TEST")
                    .subjects("dlq.jobs.>")
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
    void closeNodes() throws Exception {
        for (A2IncidentBridge bridge : new A2IncidentBridge[] { nodeOneBridge, nodeTwoBridge }) {
            if (bridge != null) {
                bridge.unsubscribe();
            }
        }
        for (Connection connection : new Connection[] { nodeOneConnection, nodeTwoConnection }) {
            if (connection != null) {
                connection.close();
            }
        }
        purgeStream();
    }

    @Test
    void secondNode_subscribingIncidentBridge_doesNotFail() throws Exception {
        ExternalTaskService externalTaskService = mock(ExternalTaskService.class);

        nodeOneConnection = Nats.connect(natsUrl);
        nodeOneBridge = bridgeFor(nodeOneConnection, externalTaskService);
        nodeOneBridge.subscribe();

        nodeTwoConnection = Nats.connect(natsUrl);
        nodeTwoBridge = bridgeFor(nodeTwoConnection, externalTaskService);

        assertThatCode(() -> nodeTwoBridge.subscribe()).doesNotThrowAnyException();
    }

    /** The property that actually matters: one incident per DLQ message, not one per node. */
    @Test
    void bothNodesSubscribed_eachDlqMessageRaisesOneIncident() throws Exception {
        int dlqMessages = 20;
        CountDownLatch handled = new CountDownLatch(dlqMessages);
        AtomicInteger handleFailureInvocations = new AtomicInteger();
        Set<String> handledTaskIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

        ExternalTaskService externalTaskService = mock(ExternalTaskService.class);
        doAnswer(invocation -> {
            handleFailureInvocations.incrementAndGet();
            handledTaskIds.add(invocation.getArgument(0));
            handled.countDown();
            return null;
        }).when(externalTaskService).handleFailure(anyString(), anyString(), anyString(), anyString(),
                anyInt(), anyLong());

        nodeOneConnection = Nats.connect(natsUrl);
        nodeOneBridge = bridgeFor(nodeOneConnection, externalTaskService);
        nodeOneBridge.subscribe();

        nodeTwoConnection = Nats.connect(natsUrl);
        nodeTwoBridge = bridgeFor(nodeTwoConnection, externalTaskService);
        nodeTwoBridge.subscribe();

        JetStream publisher = nodeOneConnection.jetStream();
        for (int i = 0; i < dlqMessages; i++) {
            Headers headers = new Headers();
            headers.add(NatsJetStreamConstants.MSG_ID_HDR, "dlq-task-" + i);
            publisher.publish(NatsMessage.builder()
                    .subject(DLQ_SUBJECT)
                    .headers(headers)
                    .data("{\"type\":\"SUCCESS\"}".getBytes(StandardCharsets.UTF_8))
                    .build());
        }

        assertThat(handled.await(30, TimeUnit.SECONDS))
                .as("all %d DLQ messages handled within 30s", dlqMessages)
                .isTrue();

        // Duplicate delivery to the other node arrives after the latch has already opened.
        Thread.sleep(2000);

        assertThat(handledTaskIds).as("every DLQ task id handled").hasSize(dlqMessages);
        assertThat(handleFailureInvocations.get())
                .as("no DLQ message handled by more than one node")
                .isEqualTo(dlqMessages);
    }

    private void purgeStream() throws Exception {
        try (Connection connection = Nats.connect(natsUrl)) {
            JetStreamManagement jsm = connection.jetStreamManagement();
            jsm.purgeStream("DLQ_JOBS_MULTINODE_TEST");
        }
    }

    private A2IncidentBridge bridgeFor(Connection connection, ExternalTaskService externalTaskService)
            throws Exception {
        NatsChannelMetrics metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        return new A2IncidentBridge(connection, connection.jetStream(), externalTaskService, SENTINEL_WORKER_ID,
                incidentConfig(), DlqBridgeCircuitBreakerFactory.create("cb-incident-bridge-test",
                        new SimpleMeterRegistry(), NotFoundException.class), metrics);
    }

    /** Mirrors the incident-bridge config {@code A2SubscriptionRegistrar.afterPropertiesSet()} builds. */
    private A2ConsumerConfig incidentConfig() {
        A2ConsumerConfig config = new A2ConsumerConfig();
        config.setSubject("dlq.jobs.>");
        config.setMessageName("a2-incident-bridge");
        config.setDurableName("a2-incident-bridge");
        config.setDeliverGroup("a2-incident-bridge");
        config.setAckWaitSeconds(30);
        config.setMaxDeliver(4);
        return config;
    }
}
