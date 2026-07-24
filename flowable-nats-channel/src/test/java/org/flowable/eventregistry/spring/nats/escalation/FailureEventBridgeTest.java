package org.flowable.eventregistry.spring.nats.escalation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;

import com.threeai.nats.core.metrics.NatsChannelMetrics;
import com.threeai.nats.core.resilience.DlqBridgeCircuitBreakerFactory;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsJetStreamMetaData;
import org.flowable.common.engine.api.FlowableException;
import org.flowable.eventregistry.api.EventRegistry;
import org.flowable.eventregistry.impl.EventRegistryEngineConfiguration;
import org.flowable.eventregistry.model.InboundChannelModel;
import org.flowable.eventregistry.spring.nats.NatsChannelDefinitionProcessor;
import org.flowable.eventregistry.spring.nats.NatsInboundEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FailureEventBridgeTest {

    private EventRegistry eventRegistry;
    private NatsChannelDefinitionProcessor channelModelLookup;
    private NatsChannelMetrics metrics;
    private CircuitBreaker circuitBreaker;
    private EventRegistryEngineConfiguration eventRegistryEngineConfiguration;
    private FailureEventBridge bridge;
    private InboundChannelModel channelModel;

    @BeforeEach
    void setUp() {
        eventRegistry = mock(EventRegistry.class);
        channelModelLookup = mock(NatsChannelDefinitionProcessor.class);
        metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        circuitBreaker = DlqBridgeCircuitBreakerFactory.create("cb-failure-event-bridge-flowable-test", null);
        eventRegistryEngineConfiguration = mock(EventRegistryEngineConfiguration.class);

        channelModel = new InboundChannelModel();
        channelModel.setKey("orderChannel");
        when(channelModelLookup.findBySubject("order.new")).thenReturn(Optional.of(channelModel));

        bridge = new FailureEventBridge(mock(Connection.class), mock(JetStream.class), "dlq.>",
                eventRegistry, channelModelLookup, circuitBreaker, metrics, eventRegistryEngineConfiguration);
    }

    @Test
    void subscribe_registersCorrelationMissConsumerOnEngineConfiguration() {
        Connection connection = mock(Connection.class);
        when(connection.createDispatcher()).thenReturn(mock(io.nats.client.Dispatcher.class));
        JetStream jetStream = mock(JetStream.class);
        FailureEventBridge subscribingBridge = new FailureEventBridge(connection, jetStream, "dlq.>",
                eventRegistry, channelModelLookup, circuitBreaker, metrics, eventRegistryEngineConfiguration);

        subscribingBridge.subscribe();

        verify(eventRegistryEngineConfiguration).setNonMatchingEventConsumer(
                any(FailureEventCorrelationMissConsumer.class));
    }

    @Test
    void subscribe_engineConfigurationAbsent_doesNotThrow() {
        Connection connection = mock(Connection.class);
        when(connection.createDispatcher()).thenReturn(mock(io.nats.client.Dispatcher.class));
        JetStream jetStream = mock(JetStream.class);
        FailureEventBridge subscribingBridge = new FailureEventBridge(connection, jetStream, "dlq.>",
                eventRegistry, channelModelLookup, circuitBreaker, metrics, null);

        assertThatCode(subscribingBridge::subscribe).doesNotThrowAnyException();
    }

    @Test
    void handleDlqMessage_success_correlatesAndAcks() {
        Message msg = dlqMessage("order.new", "{\"orderId\":1}", 1);

        bridge.handleDlqMessage(msg);

        verify(eventRegistry).eventReceived(org.mockito.ArgumentMatchers.eq(channelModel), any(NatsInboundEvent.class));
        verify(msg).ack();
    }

    @Test
    void handleDlqMessage_a2ReservedSubject_isSkippedAndAcked() {
        Message msg = dlqMessage("jobs.order-fulfillment", "{}", 1);

        bridge.handleDlqMessage(msg);

        verify(eventRegistry, never()).eventReceived(any(InboundChannelModel.class), any(NatsInboundEvent.class));
        verify(msg).ack();
    }

    @Test
    void handleDlqMessage_noRegisteredChannelModel_naks() {
        Message msg = dlqMessage("unknown.subject", "{}", 1);

        bridge.handleDlqMessage(msg);

        verify(eventRegistry, never()).eventReceived(any(InboundChannelModel.class), any(NatsInboundEvent.class));
        verify(msg).nakWithDelay(any(Duration.class));
    }

    @Test
    void handleDlqMessage_noMatchingSubscription_acksWithCorrelationMissMetric() {
        Message msg = dlqMessage("order.new", "{\"orderId\":1}", 1);
        doThrow(new FlowableException("no waiting subscription"))
                .when(eventRegistry).eventReceived(any(InboundChannelModel.class), any(NatsInboundEvent.class));

        bridge.handleDlqMessage(msg);

        verify(msg).ack();
        assertThat(metrics.failureEventCorrelationMissCount("orderChannel").count()).isEqualTo(1.0);
    }

    @Test
    void handleDlqMessage_downstreamFailure_naks() {
        Message msg = dlqMessage("order.new", "{\"orderId\":1}", 1);
        doThrow(new RuntimeException("event registry db down"))
                .when(eventRegistry).eventReceived(any(InboundChannelModel.class), any(NatsInboundEvent.class));

        bridge.handleDlqMessage(msg);

        verify(msg, never()).ack();
        verify(msg).nakWithDelay(any(Duration.class));
    }

    // --- Sentinel Phase 5.5 (round 2) coverage: subscribe()/unsubscribe() error paths ---

    @Test
    void subscribe_jetStreamSubscribeThrows_wrapsInFlowableException() throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.createDispatcher()).thenReturn(mock(Dispatcher.class));
        JetStream jetStream = mock(JetStream.class);
        when(jetStream.subscribe(anyString(), any(Dispatcher.class), any(MessageHandler.class),
                org.mockito.ArgumentMatchers.anyBoolean(), any(PushSubscribeOptions.class)))
                .thenThrow(new java.io.IOException("simulated broker failure"));
        FailureEventBridge subscribingBridge = new FailureEventBridge(connection, jetStream, "dlq.>",
                eventRegistry, channelModelLookup, circuitBreaker, metrics, eventRegistryEngineConfiguration);

        assertThatThrownBy(subscribingBridge::subscribe)
                .isInstanceOf(FlowableException.class)
                .hasMessageContaining("dlq.>");
    }

    @Test
    void subscribeThenUnsubscribe_cleanLifecycle_drainsAndShutsDownWithoutException() throws Exception {
        Connection connection = mock(Connection.class);
        Dispatcher dispatcher = mock(Dispatcher.class);
        when(connection.createDispatcher()).thenReturn(dispatcher);
        JetStream jetStream = mock(JetStream.class);
        when(jetStream.subscribe(anyString(), any(Dispatcher.class), any(MessageHandler.class),
                org.mockito.ArgumentMatchers.anyBoolean(), any(PushSubscribeOptions.class)))
                .thenReturn(mock(JetStreamSubscription.class));
        FailureEventBridge subscribingBridge = new FailureEventBridge(connection, jetStream, "dlq.>",
                eventRegistry, channelModelLookup, circuitBreaker, metrics, eventRegistryEngineConfiguration);
        subscribingBridge.subscribe();

        assertThatCode(subscribingBridge::unsubscribe).doesNotThrowAnyException();

        verify(dispatcher).drain(Duration.ofSeconds(10));
    }

    @Test
    void unsubscribe_dispatcherDrainThrows_logsWarnAndStillShutsDownExecutor() throws Exception {
        Connection connection = mock(Connection.class);
        Dispatcher dispatcher = mock(Dispatcher.class);
        when(connection.createDispatcher()).thenReturn(dispatcher);
        doThrow(new RuntimeException("drain failed")).when(dispatcher).drain(any(Duration.class));
        JetStream jetStream = mock(JetStream.class);
        when(jetStream.subscribe(anyString(), any(Dispatcher.class), any(MessageHandler.class),
                org.mockito.ArgumentMatchers.anyBoolean(), any(PushSubscribeOptions.class)))
                .thenReturn(mock(JetStreamSubscription.class));
        FailureEventBridge subscribingBridge = new FailureEventBridge(connection, jetStream, "dlq.>",
                eventRegistry, channelModelLookup, circuitBreaker, metrics, eventRegistryEngineConfiguration);
        subscribingBridge.subscribe();

        assertThatCode(subscribingBridge::unsubscribe).doesNotThrowAnyException();

        verify(dispatcher).drain(Duration.ofSeconds(10));
    }

    // --- handleDlqMessage: circuit-breaker OPEN / missing-headers edge cases ---

    @Test
    void handleDlqMessage_circuitBreakerOpen_naksWithBackoff_doesNotAck() {
        // Real resilience4j circuit breaker, forced OPEN — proves the CallNotPermittedException
        // branch fails fast without ever calling eventRegistry.eventReceived(...).
        circuitBreaker.transitionToOpenState();
        Message msg = dlqMessage("order.new", "{\"orderId\":1}", 1);

        bridge.handleDlqMessage(msg);

        verify(eventRegistry, never()).eventReceived(any(InboundChannelModel.class), any(NatsInboundEvent.class));
        verify(msg, never()).ack();
        verify(msg).nakWithDelay(any(Duration.class));
    }

    @Test
    void handleDlqMessage_nullHeaders_treatedAsNoOriginalSubject_naks() {
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn("{}".getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(null); // entire headers block missing, not just the one header
        when(msg.getSubject()).thenReturn("dlq.unknown");
        NatsJetStreamMetaData metaData = mock(NatsJetStreamMetaData.class);
        when(metaData.deliveredCount()).thenReturn(1L);
        when(msg.metaData()).thenReturn(metaData);

        bridge.handleDlqMessage(msg);

        verify(eventRegistry, never()).eventReceived(any(InboundChannelModel.class), any(NatsInboundEvent.class));
        verify(msg, never()).ack();
        verify(msg).nakWithDelay(any(Duration.class));
    }

    @Test
    void handleDlqMessage_metaDataThrows_backoffFallsBackToDeliveryCountOne() {
        // deliveryCountOf()'s catch(Exception) branch — msg.metaData() throws (e.g. a message
        // that reached this bridge without JetStream delivery metadata attached).
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn("{}".getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(null);
        when(msg.getSubject()).thenReturn("dlq.unknown");
        when(msg.metaData()).thenThrow(new IllegalStateException("not a JetStream message"));

        bridge.handleDlqMessage(msg);

        // Falls back to deliveryCount=1 -> calculateBackoff(1) == 1s (not the 30s cap).
        verify(msg).nakWithDelay(Duration.ofSeconds(1));
    }

    private Message dlqMessage(String originalSubject, String body, long deliveryCount) {
        Headers headers = new Headers();
        headers.add("X-Cadenzaflow-Dlq-Original-Subject", originalSubject);
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn(body.getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(headers);
        when(msg.getSubject()).thenReturn("dlq." + originalSubject);
        NatsJetStreamMetaData metaData = mock(NatsJetStreamMetaData.class);
        when(metaData.deliveredCount()).thenReturn(deliveryCount);
        when(msg.metaData()).thenReturn(metaData);
        return msg;
    }
}
