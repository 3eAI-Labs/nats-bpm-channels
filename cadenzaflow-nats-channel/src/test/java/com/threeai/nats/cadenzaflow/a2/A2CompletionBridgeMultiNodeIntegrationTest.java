package com.threeai.nats.cadenzaflow.a2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Multi-node A2 completion: every engine node in a cluster binds the SAME reply consumer, and each
 * reply must be handled by exactly one of them.
 *
 * <p>The unit-level {@code A2CompletionBridgeTest} mocks {@code JetStream}, so it can assert what
 * arguments {@code subscribe(...)} receives but never what JetStream does with them — a mocked
 * broker enforces no consumer-binding rules at all. That gap let a single-node-only subscription
 * ship: a durable push consumer with no deliver group is exclusive, so the second node to start
 * gets {@code [SUB-90012] Consumer is already bound to a subscription} and its Spring context dies
 * ({@code A2SubscriptionRegistrar} is an {@code InitializingBean}). Real JetStream is therefore not
 * an optional extra here — it is the only thing that can express the property under test.
 *
 * <p>Both bridges use their own {@link Connection}, as two engine JVMs would; sharing one
 * connection would not exercise the same server-side binding path.
 */
@Testcontainers
class A2CompletionBridgeMultiNodeIntegrationTest {

    private static final String TOPIC = "order-fulfillment";
    private static final String REPLY_SUBJECT = "jobs." + TOPIC + ".reply";
    private static final String SENTINEL_WORKER_ID = "a2-jetstream-bridge";

    private static GenericContainer<?> natsContainer;
    private static String natsUrl;

    private Connection nodeOneConnection;
    private Connection nodeTwoConnection;
    private A2CompletionBridge nodeOneBridge;
    private A2CompletionBridge nodeTwoBridge;

    @BeforeAll
    static void startContainer() throws Exception {
        natsContainer = new GenericContainer<>("nats:2.10-alpine").withCommand("--jetstream").withExposedPorts(4222);
        natsContainer.start();
        natsUrl = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);

        try (Connection provisioning = Nats.connect(natsUrl)) {
            JetStreamManagement jsm = provisioning.jetStreamManagement();
            jsm.addStream(StreamConfiguration.builder()
                    .name("A2_REPLY_MULTINODE_TEST")
                    .subjects("jobs.*.reply")
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
        for (A2CompletionBridge bridge : new A2CompletionBridge[] { nodeOneBridge, nodeTwoBridge }) {
            if (bridge != null) {
                bridge.unsubscribe();
            }
        }
        for (Connection connection : new Connection[] { nodeOneConnection, nodeTwoConnection }) {
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * The startup property: a second node binding the same durable must not be rejected. This is
     * the exact failure that blocks a 3-node engine cluster today.
     */
    @Test
    void secondNode_bindingSameDurable_subscribesWithoutError() throws Exception {
        ExternalTaskService externalTaskService = mock(ExternalTaskService.class);

        nodeOneConnection = Nats.connect(natsUrl);
        nodeOneBridge = bridgeFor(nodeOneConnection, externalTaskService);
        nodeOneBridge.subscribe();

        nodeTwoConnection = Nats.connect(natsUrl);
        nodeTwoBridge = bridgeFor(nodeTwoConnection, externalTaskService);

        assertThatCode(() -> nodeTwoBridge.subscribe()).doesNotThrowAnyException();
    }

    /**
     * The runtime property: with both nodes bound, each reply must reach {@code complete(...)}
     * exactly once. Per-node durable names would also let both nodes start, but would fan every
     * reply out to both — so "both nodes started" alone is not evidence the fix is correct.
     */
    @Test
    void bothNodesBound_eachReplyCompletedExactlyOnce() throws Exception {
        int replyCount = 30;
        CountDownLatch completions = new CountDownLatch(replyCount);
        AtomicInteger completeInvocations = new AtomicInteger();
        Set<String> completedTaskIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

        ExternalTaskService externalTaskService = mock(ExternalTaskService.class);
        doAnswer(invocation -> {
            completeInvocations.incrementAndGet();
            completedTaskIds.add(invocation.getArgument(0));
            completions.countDown();
            return null;
        }).when(externalTaskService).complete(anyString(), anyString(), anyMap());

        nodeOneConnection = Nats.connect(natsUrl);
        nodeOneBridge = bridgeFor(nodeOneConnection, externalTaskService);
        nodeOneBridge.subscribe();

        nodeTwoConnection = Nats.connect(natsUrl);
        nodeTwoBridge = bridgeFor(nodeTwoConnection, externalTaskService);
        nodeTwoBridge.subscribe();

        JetStream publisher = nodeOneConnection.jetStream();
        for (int i = 0; i < replyCount; i++) {
            Headers headers = new Headers();
            headers.add(NatsJetStreamConstants.MSG_ID_HDR, "task-" + i);
            publisher.publish(NatsMessage.builder()
                    .subject(REPLY_SUBJECT)
                    .headers(headers)
                    .data("{\"type\":\"SUCCESS\"}".getBytes(StandardCharsets.UTF_8))
                    .build());
        }

        assertThat(completions.await(30, TimeUnit.SECONDS))
                .as("all %d replies handled within 30s", replyCount)
                .isTrue();

        // A quiet period: a fan-out bug delivers the same reply to the other node too, which would
        // push the count past replyCount only AFTER the latch has already opened.
        Thread.sleep(2000);

        assertThat(completedTaskIds).as("every task id completed").hasSize(replyCount);
        assertThat(completeInvocations.get())
                .as("no reply handled by more than one node")
                .isEqualTo(replyCount);
    }

    private A2CompletionBridge bridgeFor(Connection connection, ExternalTaskService externalTaskService)
            throws Exception {
        JetStream jetStream = connection.jetStream();
        NatsChannelMetrics metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        DlqPublisher dlqPublisher = new DlqPublisher(jetStream, connection, metrics);
        return new A2CompletionBridge(connection, jetStream, externalTaskService, SENTINEL_WORKER_ID,
                replyConsumerConfig(), dlqPublisher, metrics);
    }

    /**
     * Mirrors {@code A2SubscriptionRegistrar.replyConsumerConfigFor(...)} — same durable AND same
     * deliver group on every node. {@code A2SubscriptionRegistrarTest} owns the assertion that the
     * registrar really produces these two values; this class owns what JetStream then does with
     * them. Keep the two in step: drop the deliver group here and this test goes back to proving
     * the single-node behaviour.
     */
    private A2ConsumerConfig replyConsumerConfig() {
        A2ConsumerConfig config = new A2ConsumerConfig();
        config.setSubject(REPLY_SUBJECT);
        config.setMessageName(TOPIC);
        config.setDurableName("a2-completion-" + TOPIC);
        config.setDeliverGroup("a2-completion-" + TOPIC);
        config.setAckWaitSeconds(30);
        config.setMaxDeliver(4);
        config.setDlqSubject("dlq.jobs." + TOPIC);
        return config;
    }
}
