package org.flowable.eventregistry.spring.nats.escalation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
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
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsJetStreamMetaData;
import org.flowable.common.engine.api.FlowableException;
import org.flowable.eventregistry.api.EventRegistry;
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
    private FailureEventBridge bridge;
    private InboundChannelModel channelModel;

    @BeforeEach
    void setUp() {
        eventRegistry = mock(EventRegistry.class);
        channelModelLookup = mock(NatsChannelDefinitionProcessor.class);
        metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        circuitBreaker = DlqBridgeCircuitBreakerFactory.create("cb-failure-event-bridge-flowable-test", null);

        channelModel = new InboundChannelModel();
        channelModel.setKey("orderChannel");
        when(channelModelLookup.findBySubject("order.new")).thenReturn(Optional.of(channelModel));

        bridge = new FailureEventBridge(mock(Connection.class), mock(JetStream.class), "dlq.>",
                eventRegistry, channelModelLookup, circuitBreaker, metrics);
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
