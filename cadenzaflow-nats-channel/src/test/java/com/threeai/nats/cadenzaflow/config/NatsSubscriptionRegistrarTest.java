package com.threeai.nats.cadenzaflow.config;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import com.threeai.nats.cadenzaflow.inbound.SubscriptionConfig;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.PushSubscribeOptions;
import org.cadenzaflow.bpm.engine.RuntimeService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Fans out {@code spring.nats.cadenzaflow.subscriptions[*]} entries to either a real {@link
 * com.threeai.nats.cadenzaflow.inbound.JetStreamMessageCorrelationSubscriber} or a real {@link
 * com.threeai.nats.cadenzaflow.inbound.NatsMessageCorrelationSubscriber} depending on {@code
 * jetstream} — those two ARE real objects here (not mocked), so their own {@code subscribe()}
 * runs against the mocked {@link JetStream}/{@link Connection}, proving the fan-out wiring itself.
 */
class NatsSubscriptionRegistrarTest {

    private CadenzaFlowNatsProperties properties;
    private Connection connection;
    private JetStream jetStream;
    private JetStreamStreamManager streamManager;
    private RuntimeService runtimeService;
    private NatsChannelMetrics metrics;
    private DlqPublisher dlqPublisher;
    private Dispatcher dispatcher;

    private NatsSubscriptionRegistrar registrar;

    @BeforeEach
    void setUp() {
        properties = new CadenzaFlowNatsProperties();
        connection = mock(Connection.class);
        jetStream = mock(JetStream.class);
        streamManager = mock(JetStreamStreamManager.class);
        runtimeService = mock(RuntimeService.class);
        metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        dlqPublisher = new DlqPublisher(jetStream, connection, metrics);
        dispatcher = mock(Dispatcher.class);
        when(connection.createDispatcher()).thenReturn(dispatcher);

        registrar = new NatsSubscriptionRegistrar(properties, connection, jetStream, streamManager,
                runtimeService, metrics, dlqPublisher);
    }

    private SubscriptionConfig jetStreamConfig(String subject, boolean autoCreateStream, String streamName) {
        SubscriptionConfig config = new SubscriptionConfig();
        config.setSubject(subject);
        config.setMessageName("OrderReceived");
        config.setJetstream(true);
        config.setAutoCreateStream(autoCreateStream);
        config.setStreamName(streamName);
        return config;
    }

    private SubscriptionConfig coreConfig(String subject) {
        SubscriptionConfig config = new SubscriptionConfig();
        config.setSubject(subject);
        config.setMessageName("OrderReceived");
        config.setJetstream(false);
        return config;
    }

    @Test
    void afterPropertiesSet_noSubscriptions_noOp() throws Exception {
        registrar.afterPropertiesSet();

        verify(jetStream, never()).subscribe(anyString(), any(), any(), org.mockito.ArgumentMatchers.anyBoolean(),
                any(PushSubscribeOptions.class));
        verify(dispatcher, never()).subscribe(anyString(), any(io.nats.client.MessageHandler.class));
    }

    @Test
    void afterPropertiesSet_jetstreamSubscription_autoCreateStreamDisabled_skipsEnsureStream() throws Exception {
        properties.setSubscriptions(List.of(jetStreamConfig("order.new", false, null)));

        registrar.afterPropertiesSet();

        verify(streamManager, never()).ensureStream(anyString(), anyString(), any(Connection.class));
        verify(jetStream).subscribe(eq("order.new"), any(), any(), eq(false), any(PushSubscribeOptions.class));
    }

    @Test
    void afterPropertiesSet_jetstreamSubscription_autoCreateStreamEnabled_ensuresStreamFirst() throws Exception {
        properties.setSubscriptions(List.of(jetStreamConfig("order.new", true, "ORDERS")));

        registrar.afterPropertiesSet();

        verify(streamManager).ensureStream("ORDERS", "order.new", connection);
        verify(jetStream).subscribe(eq("order.new"), any(), any(), eq(false), any(PushSubscribeOptions.class));
    }

    @Test
    void afterPropertiesSet_jetstreamSubscription_autoCreateStreamEnabledButNoStreamName_skipsEnsureStream()
            throws Exception {
        properties.setSubscriptions(List.of(jetStreamConfig("order.new", true, null)));

        registrar.afterPropertiesSet();

        verify(streamManager, never()).ensureStream(anyString(), anyString(), any(Connection.class));
    }

    @Test
    void afterPropertiesSet_coreNatsSubscription_registersDispatcherSubscription() {
        properties.setSubscriptions(List.of(coreConfig("order.legacy")));

        registrar.afterPropertiesSet();

        verify(dispatcher).subscribe(eq("order.legacy"), any(io.nats.client.MessageHandler.class));
    }

    @Test
    void afterPropertiesSet_mixedSubscriptions_bothPathsRegistered() throws Exception {
        properties.setSubscriptions(List.of(jetStreamConfig("order.new", false, null), coreConfig("order.legacy")));

        registrar.afterPropertiesSet();

        verify(jetStream).subscribe(eq("order.new"), any(), any(), eq(false), any(PushSubscribeOptions.class));
        verify(dispatcher).subscribe(eq("order.legacy"), any(io.nats.client.MessageHandler.class));
    }

    @Test
    void destroy_unsubscribesBothCoreAndJetStreamSubscribers() throws Exception {
        properties.setSubscriptions(List.of(jetStreamConfig("order.new", false, null), coreConfig("order.legacy")));
        registrar.afterPropertiesSet();

        registrar.destroy();

        // Both subscriber types share the SAME mocked Connection -> createDispatcher() returns
        // the SAME dispatcher mock -> JetStreamMessageCorrelationSubscriber#unsubscribe drains it,
        // NatsMessageCorrelationSubscriber#unsubscribe closes it via the connection.
        verify(dispatcher).drain(java.time.Duration.ofSeconds(10));
        verify(connection).closeDispatcher(dispatcher);
    }

    @Test
    void destroy_noSubscriptions_isNoOp_doesNotThrow() {
        org.assertj.core.api.Assertions.assertThatCode(registrar::destroy).doesNotThrowAnyException();
    }
}
