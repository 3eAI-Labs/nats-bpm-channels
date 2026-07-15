package org.flowable.eventregistry.spring.nats.jetstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsJetStreamMetaData;
import io.nats.client.impl.NatsMessage;
import org.flowable.eventregistry.api.EventRegistry;
import org.flowable.eventregistry.spring.nats.NatsInboundEvent;
import org.flowable.eventregistry.spring.nats.channel.NatsInboundChannelModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class JetStreamInboundEventChannelAdapterTest {

    private Connection connection;
    private JetStream jetStream;
    private EventRegistry eventRegistry;
    private NatsInboundChannelModel channelModel;
    private DlqPublisher dlqPublisher;
    private JetStreamInboundEventChannelAdapter adapter;

    @BeforeEach
    void setUp() {
        connection = mock(Connection.class);
        jetStream = mock(JetStream.class);
        eventRegistry = mock(EventRegistry.class);
        dlqPublisher = new DlqPublisher(jetStream, connection, new NatsChannelMetrics(new SimpleMeterRegistry()));

        channelModel = new NatsInboundChannelModel();
        channelModel.setKey("orderChannel");
        channelModel.setSubject("order.new");

        adapter = new JetStreamInboundEventChannelAdapter(
                connection, jetStream, "order.new", 5, "order.dlq", null, "orderChannel", dlqPublisher);
        adapter.setInboundChannelModel(channelModel);
        adapter.setEventRegistry(eventRegistry);
    }

    @Test
    void handleMessage_success_acksMessage() {
        Message msg = createMockMessage("{\"orderId\":1}", null, 1);

        adapter.handleMessage(msg);

        verify(eventRegistry).eventReceived(eq(channelModel), any(NatsInboundEvent.class));
        verify(msg).ack();
    }

    @Test
    void handleMessage_error_naksWithDelay() {
        Message msg = createMockMessage("{\"bad\":true}", null, 1);
        doThrow(new RuntimeException("processing error"))
                .when(eventRegistry).eventReceived(any(), any(NatsInboundEvent.class));

        adapter.handleMessage(msg);

        verify(msg).nakWithDelay(Duration.ofSeconds(1));
    }

    @Test
    void handleMessage_error_backoffExponential() {
        // delivery 1 -> 1s, 2 -> 2s, 3 -> 4s, 4 -> 8s, 5 -> 16s, 6 -> 30s (cap)
        verifyBackoffForDelivery(1, Duration.ofSeconds(1));
        verifyBackoffForDelivery(2, Duration.ofSeconds(2));
        verifyBackoffForDelivery(3, Duration.ofSeconds(4));
        verifyBackoffForDelivery(4, Duration.ofSeconds(8));
        verifyBackoffForDelivery(5, Duration.ofSeconds(16));
        verifyBackoffForDelivery(6, Duration.ofSeconds(30));
        verifyBackoffForDelivery(10, Duration.ofSeconds(30));
    }

    @Test
    void handleMessage_error_metadataFails_fallsBackToPlainNak() {
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn("{\"orderId\":1}".getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(null);
        when(msg.getSubject()).thenReturn("order.new");
        when(msg.metaData()).thenThrow(new IllegalStateException("not a JetStream message"));
        doThrow(new RuntimeException("processing error"))
                .when(eventRegistry).eventReceived(any(), any(NatsInboundEvent.class));

        adapter.handleMessage(msg);

        verify(msg).nak();
    }

    @Test
    void handleMessage_maxDeliverExceeded_publishesToDlqAndAcks() throws Exception {
        Message msg = createMockMessage("{\"orderId\":99}", null, 6);

        adapter.handleMessage(msg);

        verify(jetStream).publish(any(NatsMessage.class));
        verify(msg).ack();
        verify(eventRegistry, never()).eventReceived(any(), any(NatsInboundEvent.class));
    }

    @Test
    void handleMessage_dlqJetStreamFails_fallbackToCoreNats_stillAcks() throws Exception {
        Message msg = createMockMessage("{\"orderId\":99}", null, 6);
        when(jetStream.publish(any(NatsMessage.class))).thenThrow(new IOException("JS unavailable"));

        adapter.handleMessage(msg);

        verify(connection).publish(eq("order.dlq"), any(Headers.class), any(byte[].class));
        verify(msg).ack();
    }

    @Test
    void handleMessage_dlqBothFail_naksInsteadOfAck() throws Exception {
        Message msg = createMockMessage("{\"orderId\":99}", null, 6);
        when(jetStream.publish(any(NatsMessage.class))).thenThrow(new IOException("JS unavailable"));
        doThrow(new RuntimeException("core NATS down"))
                .when(connection).publish(any(String.class), any(Headers.class), any(byte[].class));

        adapter.handleMessage(msg);

        verify(msg, never()).ack();
        verify(msg).nakWithDelay(any(Duration.class));
    }

    @Test
    void handleMessage_emptyBody_routesToDlqAndAcks() throws Exception {
        Message msg = createMockMessage("", null, 1);

        adapter.handleMessage(msg);

        verify(jetStream).publish(any(NatsMessage.class));
        verify(msg).ack();
        verify(eventRegistry, never()).eventReceived(any(), any(NatsInboundEvent.class));
    }

    @Test
    void handleMessage_emptyBody_dlqSubjectMissing_naksInsteadOfDiscarding() {
        JetStreamInboundEventChannelAdapter noDlqAdapter = new JetStreamInboundEventChannelAdapter(
                connection, jetStream, "order.new", 5, null, null, "orderChannel", dlqPublisher);
        noDlqAdapter.setInboundChannelModel(channelModel);
        noDlqAdapter.setEventRegistry(eventRegistry);

        Message msg = createMockMessage("", null, 1);

        noDlqAdapter.handleMessage(msg);

        verify(msg, never()).ack();
        verify(msg).nakWithDelay(any(Duration.class));
        verify(eventRegistry, never()).eventReceived(any(), any(NatsInboundEvent.class));
    }

    @Test
    void handleMessage_dlqDisabled_naksWithoutPublish() throws Exception {
        JetStreamInboundEventChannelAdapter noDlqAdapter = new JetStreamInboundEventChannelAdapter(
                connection, jetStream, "order.new", 5, null, null, "orderChannel", dlqPublisher);
        noDlqAdapter.setInboundChannelModel(channelModel);
        noDlqAdapter.setEventRegistry(eventRegistry);

        Message msg = createMockMessage("{\"orderId\":99}", null, 6);

        noDlqAdapter.handleMessage(msg);

        verify(msg, never()).ack();
        verify(jetStream, never()).publish(any(NatsMessage.class));
        verify(eventRegistry, never()).eventReceived(any(), any(NatsInboundEvent.class));
    }

    @Test
    void handleMessage_propagatesTraceIdToMdc() {
        Headers headers = new Headers();
        headers.add("X-Trace-Id", "trace-xyz-123");
        Message msg = createMockMessage("{\"orderId\":1}", headers, 1);

        // Capture the MDC value during eventRegistry.eventReceived()
        final String[] capturedTraceId = {null};
        org.mockito.Mockito.doAnswer(invocation -> {
            capturedTraceId[0] = MDC.get("trace_id");
            return null;
        }).when(eventRegistry).eventReceived(any(), any(NatsInboundEvent.class));

        adapter.handleMessage(msg);

        assertThat(capturedTraceId[0]).isEqualTo("trace-xyz-123");
        // MDC should be cleaned after processing
        assertThat(MDC.get("trace_id")).isNull();
    }

    @Test
    void handleMessage_standardTraceHeaderTakesPrecedenceOverLegacy() {
        Headers headers = new Headers();
        headers.add("X-Cadenzaflow-Trace-Id", "trace-standard");
        headers.add("X-Trace-Id", "trace-legacy");
        Message msg = createMockMessage("{\"orderId\":1}", headers, 1);

        final String[] capturedTraceId = {null};
        org.mockito.Mockito.doAnswer(invocation -> {
            capturedTraceId[0] = MDC.get("trace_id");
            return null;
        }).when(eventRegistry).eventReceived(any(), any(NatsInboundEvent.class));

        adapter.handleMessage(msg);

        assertThat(capturedTraceId[0]).isEqualTo("trace-standard");
    }

    // --- helpers ---

    private Message createMockMessage(String body, Headers headers, long deliveryCount) {
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn(body.getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(headers);
        when(msg.getSubject()).thenReturn("order.new");

        NatsJetStreamMetaData metaData = mock(NatsJetStreamMetaData.class);
        when(metaData.deliveredCount()).thenReturn(deliveryCount);
        when(msg.metaData()).thenReturn(metaData);

        return msg;
    }

    private void verifyBackoffForDelivery(long deliveryCount, Duration expectedDelay) {
        Message msg = createMockMessage("{\"fail\":true}", null, deliveryCount);
        doThrow(new RuntimeException("error"))
                .when(eventRegistry).eventReceived(any(), any(NatsInboundEvent.class));

        // Use a fresh adapter for isolation per delivery count if maxDeliver not exceeded
        JetStreamInboundEventChannelAdapter freshAdapter = new JetStreamInboundEventChannelAdapter(
                connection, jetStream, "order.new", 5, "order.dlq", null, "orderChannel", dlqPublisher);
        freshAdapter.setInboundChannelModel(channelModel);
        freshAdapter.setEventRegistry(eventRegistry);

        // Only test backoff for deliveries within maxDeliver
        if (deliveryCount <= 5) {
            freshAdapter.handleMessage(msg);
            verify(msg).nakWithDelay(expectedDelay);
        } else {
            // For deliveries exceeding maxDeliver, use adapter with higher maxDeliver
            JetStreamInboundEventChannelAdapter highMaxAdapter = new JetStreamInboundEventChannelAdapter(
                    connection, jetStream, "order.new", 100, "order.dlq", null, "orderChannel", dlqPublisher);
            highMaxAdapter.setInboundChannelModel(channelModel);
            highMaxAdapter.setEventRegistry(eventRegistry);
            highMaxAdapter.handleMessage(msg);
            verify(msg).nakWithDelay(expectedDelay);
        }
    }
}
