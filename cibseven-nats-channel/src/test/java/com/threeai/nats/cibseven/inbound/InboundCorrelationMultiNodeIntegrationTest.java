package com.threeai.nats.cibseven.inbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
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
import io.nats.client.Nats;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import org.cibseven.bpm.engine.RuntimeService;
import org.cibseven.bpm.engine.runtime.MessageCorrelationBuilder;
import org.cibseven.bpm.engine.runtime.MessageCorrelationResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Inbound message correlation across an engine cluster. A message that correlates to a process
 * instance must correlate ONCE, however many engine nodes are subscribed — correlating the same
 * message on three nodes means the process advances three times, or three instances start.
 *
 * <p>Both transports had the same gap and failed differently. On JetStream, a configured durable
 * name with no deliver group is exclusive, so the second node could not start at all
 * ({@code [SUB-90012]}); with no durable, each node got a private consumer and every node saw every
 * message. On core NATS there is no durable to speak of, so it was always the second case — silent
 * duplicate correlation, no error anywhere. Flowable's own adapter has taken a queue group since it
 * was written ({@code NatsInboundEventChannelAdapter}); the Camunda-lineage adapters did not carry
 * it over.
 */
@Testcontainers
class InboundCorrelationMultiNodeIntegrationTest {

    private static final String CORE_SUBJECT = "orders.core.created";
    private static final String JS_SUBJECT = "orders.js.created";

    private static GenericContainer<?> natsContainer;
    private static String natsUrl;

    private Connection nodeOneConnection;
    private Connection nodeTwoConnection;
    private AutoCloseable nodeOneSubscriber;
    private AutoCloseable nodeTwoSubscriber;

    @BeforeAll
    static void startContainer() throws Exception {
        natsContainer = new GenericContainer<>("nats:2.10-alpine").withCommand("--jetstream").withExposedPorts(4222);
        natsContainer.start();
        natsUrl = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);

        try (Connection provisioning = Nats.connect(natsUrl)) {
            provisioning.jetStreamManagement().addStream(StreamConfiguration.builder()
                    .name("INBOUND_MULTINODE_TEST")
                    .subjects(JS_SUBJECT)
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
        for (AutoCloseable subscriber : new AutoCloseable[] { nodeOneSubscriber, nodeTwoSubscriber }) {
            if (subscriber != null) {
                subscriber.close();
            }
        }
        for (Connection connection : new Connection[] { nodeOneConnection, nodeTwoConnection }) {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    void coreNats_twoNodes_eachMessageCorrelatedOnce() throws Exception {
        int messages = 20;
        CorrelationCounter counter = new CorrelationCounter(messages);

        nodeOneConnection = Nats.connect(natsUrl);
        NatsMessageCorrelationSubscriber one = coreSubscriber(nodeOneConnection, counter.runtimeService());
        one.subscribe();
        nodeOneSubscriber = one::unsubscribe;

        nodeTwoConnection = Nats.connect(natsUrl);
        NatsMessageCorrelationSubscriber two = coreSubscriber(nodeTwoConnection, counter.runtimeService());
        two.subscribe();
        nodeTwoSubscriber = two::unsubscribe;

        for (int i = 0; i < messages; i++) {
            nodeOneConnection.publish(CORE_SUBJECT, headersWithKey("order-" + i),
                    "{}".getBytes(StandardCharsets.UTF_8));
        }
        nodeOneConnection.flush(java.time.Duration.ofSeconds(5));

        counter.assertExactlyOncePerMessage(messages);
    }

    @Test
    void jetStream_secondNodeBindsSameDurable_andEachMessageCorrelatedOnce() throws Exception {
        int messages = 20;
        CorrelationCounter counter = new CorrelationCounter(messages);

        nodeOneConnection = Nats.connect(natsUrl);
        JetStreamMessageCorrelationSubscriber one = jetStreamSubscriber(nodeOneConnection, counter.runtimeService());
        one.subscribe();
        nodeOneSubscriber = one::unsubscribe;

        nodeTwoConnection = Nats.connect(natsUrl);
        JetStreamMessageCorrelationSubscriber two = jetStreamSubscriber(nodeTwoConnection, counter.runtimeService());
        assertThatCode(two::subscribe).doesNotThrowAnyException();
        nodeTwoSubscriber = two::unsubscribe;

        JetStream publisher = nodeOneConnection.jetStream();
        for (int i = 0; i < messages; i++) {
            publisher.publish(NatsMessage.builder()
                    .subject(JS_SUBJECT)
                    .headers(headersWithKey("order-" + i))
                    .data("{}".getBytes(StandardCharsets.UTF_8))
                    .build());
        }

        counter.assertExactlyOncePerMessage(messages);
    }

    private Headers headersWithKey(String businessKey) {
        Headers headers = new Headers();
        headers.add("X-Business-Key", businessKey);
        return headers;
    }

    private NatsMessageCorrelationSubscriber coreSubscriber(Connection connection, RuntimeService runtimeService) {
        return new NatsMessageCorrelationSubscriber(connection, runtimeService, coreConfig(),
                new NatsChannelMetrics(new SimpleMeterRegistry()));
    }

    private JetStreamMessageCorrelationSubscriber jetStreamSubscriber(Connection connection,
            RuntimeService runtimeService) throws Exception {
        JetStream jetStream = connection.jetStream();
        NatsChannelMetrics metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        return new JetStreamMessageCorrelationSubscriber(connection, jetStream, runtimeService, jetStreamConfig(),
                metrics, new DlqPublisher(jetStream, connection, metrics));
    }

    private SubscriptionConfig coreConfig() {
        SubscriptionConfig config = new SubscriptionConfig();
        config.setSubject(CORE_SUBJECT);
        config.setMessageName("orderCreatedCore");
        config.setBusinessKeyHeader("X-Business-Key");
        return config;
    }

    private SubscriptionConfig jetStreamConfig() {
        SubscriptionConfig config = new SubscriptionConfig();
        config.setSubject(JS_SUBJECT);
        config.setMessageName("orderCreatedJs");
        config.setBusinessKeyHeader("X-Business-Key");
        config.setJetstream(true);
        config.setDurableName("inbound-orderCreatedJs");
        config.setDlqSubject("dlq." + JS_SUBJECT);
        return config;
    }

    /** Counts correlations across both nodes; the mocked engine is shared, as a real one would be. */
    private static final class CorrelationCounter {

        private final CountDownLatch latch;
        private final AtomicInteger correlations = new AtomicInteger();
        private final Set<String> businessKeys = Collections.newSetFromMap(new ConcurrentHashMap<>());
        private final RuntimeService runtimeService = mock(RuntimeService.class);

        CorrelationCounter(int expected) {
            this.latch = new CountDownLatch(expected);
            MessageCorrelationBuilder builder = mock(MessageCorrelationBuilder.class);
            when(runtimeService.createMessageCorrelation(anyString())).thenReturn(builder);
            // handleMessage chains createMessageCorrelation(..).setVariables(..) and only then
            // processInstanceBusinessKey(..); an unstubbed link returns null and nothing correlates.
            when(builder.processInstanceBusinessKey(anyString())).thenAnswer(invocation -> {
                correlations.incrementAndGet();
                businessKeys.add(invocation.getArgument(0));
                latch.countDown();
                return builder;
            });
            when(builder.setVariables(org.mockito.ArgumentMatchers.anyMap())).thenReturn(builder);
            when(builder.correlateWithResult()).thenReturn(mock(MessageCorrelationResult.class));
        }

        RuntimeService runtimeService() {
            return runtimeService;
        }

        void assertExactlyOncePerMessage(int expected) throws InterruptedException {
            assertThat(latch.await(30, TimeUnit.SECONDS))
                    .as("all %d messages correlated within 30s", expected)
                    .isTrue();
            // A fan-out bug delivers to the other node too, arriving after the latch opens.
            Thread.sleep(2000);
            assertThat(businessKeys).as("every business key correlated").hasSize(expected);
            assertThat(correlations.get()).as("no message correlated by more than one node").isEqualTo(expected);
        }
    }
}
