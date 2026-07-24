package org.flowable.eventregistry.spring.nats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import com.threeai.nats.core.dlq.DlqPublishOutcome;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.dlq.DlqReason;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.Message;
import org.flowable.common.engine.api.FlowableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class NatsOutboundEventChannelAdapterTest {

    private Connection connection;
    private NatsOutboundEventChannelAdapter adapter;

    @BeforeEach
    void setUp() {
        connection = mock(Connection.class);
        adapter = new NatsOutboundEventChannelAdapter(connection, "order.completed");
    }

    @Test
    void sendEvent_publishesMessage() {
        when(connection.getStatus()).thenReturn(Connection.Status.CONNECTED);

        adapter.sendEvent("{\"orderId\":123}", Collections.emptyMap());

        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        verify(connection).publish(captor.capture());
        Message published = captor.getValue();
        assertThat(published.getSubject()).isEqualTo("order.completed");
        assertThat(new String(published.getData(), StandardCharsets.UTF_8)).isEqualTo("{\"orderId\":123}");
    }

    @Test
    void sendEvent_withHeaders_publishesMessageWithHeaders() {
        when(connection.getStatus()).thenReturn(Connection.Status.CONNECTED);

        adapter.sendEvent("{\"orderId\":456}", Map.of("X-Trace-Id", "trace-123"));

        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        verify(connection).publish(captor.capture());
        Message published = captor.getValue();
        assertThat(published.getSubject()).isEqualTo("order.completed");
        assertThat(published.getHeaders()).isNotNull();
        assertThat(published.getHeaders().getLast("X-Trace-Id")).isEqualTo("trace-123");
    }

    // --- Sentinel Phase 5.5 (round 2): core-publish throws, no DLQ configured ---

    @Test
    void sendEvent_corePublishThrows_noDlqPublisherConfigured_throwsFlowableException() {
        when(connection.getStatus()).thenReturn(Connection.Status.CONNECTED);
        doThrow(new RuntimeException("broker unreachable")).when(connection).publish(any(Message.class));
        // `adapter` (from setUp) uses the 2-arg constructor -- dlqPublisher is null, so the
        // failure path has no custody-transfer option and must surface the FlowableException.

        assertThatThrownBy(() -> adapter.sendEvent("{\"orderId\":123}", Collections.emptyMap()))
                .isInstanceOf(FlowableException.class)
                .hasMessageContaining("publish failed")
                .hasMessageContaining("order.completed")
                .hasCauseInstanceOf(RuntimeException.class);
    }

    @Test
    void sendEvent_connectionClosed_throwsFlowableException() {
        when(connection.getStatus()).thenReturn(Connection.Status.CLOSED);

        assertThatThrownBy(() -> adapter.sendEvent("{\"orderId\":123}", Collections.emptyMap()))
                .isInstanceOf(FlowableException.class)
                .hasMessageContaining("order.completed");
    }

    // --- D-G' (docs/09-outbound-handoff.md) DLQ hardening ---

    @Test
    void sendEvent_connectionClosed_dlqConfigured_routesToDlqInsteadOfThrowing() {
        when(connection.getStatus()).thenReturn(Connection.Status.CLOSED);
        DlqPublisher dlqPublisher = mock(DlqPublisher.class);
        when(dlqPublisher.publish(any(Message.class), anyString(), any(DlqReason.class), anyString(), anyString()))
                .thenReturn(DlqPublishOutcome.PUBLISHED_JETSTREAM);
        NatsOutboundEventChannelAdapter hardenedAdapter = new NatsOutboundEventChannelAdapter(
                connection, "order.completed", "orderChannel", dlqPublisher, "dlq.order.completed");

        assertThatCode(() -> hardenedAdapter.sendEvent("{\"orderId\":123}", Collections.emptyMap()))
                .doesNotThrowAnyException();

        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        verify(dlqPublisher).publish(captor.capture(), eq("dlq.order.completed"),
                eq(DlqReason.OUTBOUND_PUBLISH_FAILED), eq("order.completed"), eq("orderChannel"));
        assertThat(captor.getValue().getSubject()).isEqualTo("order.completed");
    }

    @Test
    void sendEvent_connectionClosed_dlqPublishAlsoFails_throwsFlowableException() {
        when(connection.getStatus()).thenReturn(Connection.Status.CLOSED);
        DlqPublisher dlqPublisher = mock(DlqPublisher.class);
        when(dlqPublisher.publish(any(Message.class), anyString(), any(DlqReason.class), anyString(), anyString()))
                .thenReturn(DlqPublishOutcome.FAILED_BOTH_PUBLISH);
        NatsOutboundEventChannelAdapter hardenedAdapter = new NatsOutboundEventChannelAdapter(
                connection, "order.completed", "orderChannel", dlqPublisher, "dlq.order.completed");

        assertThatThrownBy(() -> hardenedAdapter.sendEvent("{\"orderId\":123}", Collections.emptyMap()))
                .isInstanceOf(FlowableException.class)
                .hasMessageContaining("order.completed");
    }

    @Test
    void sendEvent_corePublishThrows_dlqConfigured_routesToDlq() {
        when(connection.getStatus()).thenReturn(Connection.Status.CONNECTED);
        doThrow(new RuntimeException("broker unreachable")).when(connection).publish(any(Message.class));
        DlqPublisher dlqPublisher = mock(DlqPublisher.class);
        when(dlqPublisher.publish(any(Message.class), anyString(), any(DlqReason.class), anyString(), anyString()))
                .thenReturn(DlqPublishOutcome.PUBLISHED_CORE_FALLBACK);
        NatsOutboundEventChannelAdapter hardenedAdapter = new NatsOutboundEventChannelAdapter(
                connection, "order.completed", "orderChannel", dlqPublisher, "dlq.order.completed");

        assertThatCode(() -> hardenedAdapter.sendEvent("{\"orderId\":123}", Collections.emptyMap()))
                .doesNotThrowAnyException();
    }

    // --- Phase-review FINDING-002 (MINOR — observability): flowableOutbound* metrics wired ---

    @Test
    void sendEvent_publishSucceeds_incrementsFlowableOutboundPublishedCount() {
        when(connection.getStatus()).thenReturn(Connection.Status.CONNECTED);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        NatsChannelMetrics metrics = new NatsChannelMetrics(registry);
        NatsOutboundEventChannelAdapter metricAdapter = new NatsOutboundEventChannelAdapter(
                connection, "order.completed", "orderChannel", null, null, metrics);

        metricAdapter.sendEvent("{\"orderId\":123}", Collections.emptyMap());

        assertThat(registry.find("nats.flowable.outbound.published")
                .tag("subject", "order.completed").tag("channel", "orderChannel").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.find("nats.flowable.outbound.dlq_routed").counter()).isNull();
    }

    @Test
    void sendEvent_dlqRoutedSuccessfully_incrementsFlowableOutboundDlqRoutedCount_notPublishedCount() {
        when(connection.getStatus()).thenReturn(Connection.Status.CLOSED);
        DlqPublisher dlqPublisher = mock(DlqPublisher.class);
        when(dlqPublisher.publish(any(Message.class), anyString(), any(DlqReason.class), anyString(), anyString()))
                .thenReturn(DlqPublishOutcome.PUBLISHED_JETSTREAM);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        NatsChannelMetrics metrics = new NatsChannelMetrics(registry);
        NatsOutboundEventChannelAdapter metricAdapter = new NatsOutboundEventChannelAdapter(
                connection, "order.completed", "orderChannel", dlqPublisher, "dlq.order.completed", metrics);

        metricAdapter.sendEvent("{\"orderId\":123}", Collections.emptyMap());

        assertThat(registry.find("nats.flowable.outbound.dlq_routed")
                .tag("subject", "order.completed").tag("channel", "orderChannel").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.find("nats.flowable.outbound.published").counter()).isNull();
    }

    @Test
    void sendEvent_publishFailsAndDlqFails_noMetricIncremented() {
        when(connection.getStatus()).thenReturn(Connection.Status.CLOSED);
        DlqPublisher dlqPublisher = mock(DlqPublisher.class);
        when(dlqPublisher.publish(any(Message.class), anyString(), any(DlqReason.class), anyString(), anyString()))
                .thenReturn(DlqPublishOutcome.FAILED_BOTH_PUBLISH);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        NatsChannelMetrics metrics = new NatsChannelMetrics(registry);
        NatsOutboundEventChannelAdapter metricAdapter = new NatsOutboundEventChannelAdapter(
                connection, "order.completed", "orderChannel", dlqPublisher, "dlq.order.completed", metrics);

        assertThatThrownBy(() -> metricAdapter.sendEvent("{\"orderId\":123}", Collections.emptyMap()))
                .isInstanceOf(FlowableException.class);

        assertThat(registry.find("nats.flowable.outbound.published").counter()).isNull();
        assertThat(registry.find("nats.flowable.outbound.dlq_routed").counter()).isNull();
    }
}
