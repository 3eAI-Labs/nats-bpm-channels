package org.flowable.eventregistry.spring.nats.jetstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import com.threeai.nats.core.dlq.DlqPublishOutcome;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.dlq.DlqReason;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.api.PublishAck;
import org.flowable.common.engine.api.FlowableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class JetStreamOutboundEventChannelAdapterTest {

    private JetStream jetStream;
    private JetStreamOutboundEventChannelAdapter adapter;

    @BeforeEach
    void setUp() {
        jetStream = mock(JetStream.class);
        adapter = new JetStreamOutboundEventChannelAdapter(jetStream, "order.completed", null, "orderChannel");
    }

    @Test
    void sendEvent_publishesToJetStream() throws Exception {
        PublishAck ack = mock(PublishAck.class);
        when(ack.getSeqno()).thenReturn(42L);
        when(jetStream.publish(any(Message.class))).thenReturn(ack);

        adapter.sendEvent("{\"orderId\":123}", Collections.emptyMap());

        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        verify(jetStream).publish(captor.capture());
        Message published = captor.getValue();
        assertThat(published.getSubject()).isEqualTo("order.completed");
        assertThat(new String(published.getData(), StandardCharsets.UTF_8)).isEqualTo("{\"orderId\":123}");
    }

    @Test
    void sendEvent_propagatesHeaders() throws Exception {
        PublishAck ack = mock(PublishAck.class);
        when(ack.getSeqno()).thenReturn(1L);
        when(jetStream.publish(any(Message.class))).thenReturn(ack);

        adapter.sendEvent("{\"orderId\":456}", Map.of("X-Trace-Id", "trace-abc", "X-Source", "test"));

        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        verify(jetStream).publish(captor.capture());
        Message published = captor.getValue();
        assertThat(published.getHeaders()).isNotNull();
        assertThat(published.getHeaders().getLast("X-Trace-Id")).isEqualTo("trace-abc");
        assertThat(published.getHeaders().getLast("X-Source")).isEqualTo("test");
    }

    @Test
    void sendEvent_publishFails_throwsFlowableException() throws Exception {
        when(jetStream.publish(any(Message.class)))
                .thenThrow(new IOException("connection lost"));

        assertThatThrownBy(() -> adapter.sendEvent("{\"orderId\":789}", Collections.emptyMap()))
                .isInstanceOf(FlowableException.class)
                .hasMessageContaining("orderChannel")
                .hasMessageContaining("order.completed");
    }

    // --- D-G' (docs/09-outbound-handoff.md) DLQ hardening ---

    @Test
    void sendEvent_publishFails_dlqConfigured_routesToDlqInsteadOfThrowing() throws Exception {
        when(jetStream.publish(any(Message.class))).thenThrow(new IOException("connection lost"));
        DlqPublisher dlqPublisher = mock(DlqPublisher.class);
        when(dlqPublisher.publish(any(Message.class), anyString(), any(DlqReason.class), anyString(), anyString()))
                .thenReturn(DlqPublishOutcome.PUBLISHED_JETSTREAM);
        JetStreamOutboundEventChannelAdapter hardenedAdapter = new JetStreamOutboundEventChannelAdapter(
                jetStream, "order.completed", null, "orderChannel", dlqPublisher, "dlq.order.completed");

        assertThatCode(() -> hardenedAdapter.sendEvent("{\"orderId\":123}", Collections.emptyMap()))
                .doesNotThrowAnyException();

        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        verify(dlqPublisher).publish(captor.capture(), eq("dlq.order.completed"),
                eq(DlqReason.OUTBOUND_PUBLISH_FAILED), eq("order.completed"), eq("orderChannel"));
        assertThat(captor.getValue().getSubject()).isEqualTo("order.completed");
    }

    @Test
    void sendEvent_publishFails_dlqPublishAlsoFails_throwsFlowableException() throws Exception {
        when(jetStream.publish(any(Message.class))).thenThrow(new IOException("connection lost"));
        DlqPublisher dlqPublisher = mock(DlqPublisher.class);
        when(dlqPublisher.publish(any(Message.class), anyString(), any(DlqReason.class), anyString(), anyString()))
                .thenReturn(DlqPublishOutcome.FAILED_NO_DLQ_SUBJECT);
        JetStreamOutboundEventChannelAdapter hardenedAdapter = new JetStreamOutboundEventChannelAdapter(
                jetStream, "order.completed", null, "orderChannel", dlqPublisher, null);

        assertThatThrownBy(() -> hardenedAdapter.sendEvent("{\"orderId\":123}", Collections.emptyMap()))
                .isInstanceOf(FlowableException.class)
                .hasMessageContaining("order.completed");
    }

    // --- Phase-review FINDING-002 (MINOR — observability): flowableOutbound* metrics wired ---

    @Test
    void sendEvent_publishSucceeds_incrementsFlowableOutboundPublishedCount() throws Exception {
        PublishAck ack = mock(PublishAck.class);
        when(ack.getSeqno()).thenReturn(1L);
        when(jetStream.publish(any(Message.class))).thenReturn(ack);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        NatsChannelMetrics metrics = new NatsChannelMetrics(registry);
        JetStreamOutboundEventChannelAdapter metricAdapter = new JetStreamOutboundEventChannelAdapter(
                jetStream, "order.completed", metrics, "orderChannel");

        metricAdapter.sendEvent("{\"orderId\":123}", Collections.emptyMap());

        assertThat(registry.find("nats.flowable.outbound.published")
                .tag("subject", "order.completed").tag("channel", "orderChannel").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.find("nats.flowable.outbound.dlq_routed").counter()).isNull();
    }

    @Test
    void sendEvent_dlqRoutedSuccessfully_incrementsFlowableOutboundDlqRoutedCount_notPublishedCount() throws Exception {
        when(jetStream.publish(any(Message.class))).thenThrow(new IOException("connection lost"));
        DlqPublisher dlqPublisher = mock(DlqPublisher.class);
        when(dlqPublisher.publish(any(Message.class), anyString(), any(DlqReason.class), anyString(), anyString()))
                .thenReturn(DlqPublishOutcome.PUBLISHED_JETSTREAM);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        NatsChannelMetrics metrics = new NatsChannelMetrics(registry);
        JetStreamOutboundEventChannelAdapter metricAdapter = new JetStreamOutboundEventChannelAdapter(
                jetStream, "order.completed", metrics, "orderChannel", dlqPublisher, "dlq.order.completed");

        metricAdapter.sendEvent("{\"orderId\":123}", Collections.emptyMap());

        assertThat(registry.find("nats.flowable.outbound.dlq_routed")
                .tag("subject", "order.completed").tag("channel", "orderChannel").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.find("nats.flowable.outbound.published").counter()).isNull();
    }
}
