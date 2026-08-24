package com.threeai.nats.core.shard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.threeai.nats.core.dlq.DlqPublishOutcome;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.dlq.DlqReason;
import com.threeai.nats.core.headers.BpmHeaders;
import com.threeai.nats.core.metrics.NatsChannelMetrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.NatsJetStreamConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** docs/13 D-C v5 slice 2: key resolution order, derived Msg-Id, custody, DLQ path. */
class ShardRouterTest {

    private JetStream jetStream;
    private DlqPublisher dlqPublisher;
    private SimpleMeterRegistry registry;
    private ShardRouter router;
    private final ShardRouteConfig route = new ShardRouteConfig("evt.order.accept", "orderId");

    @BeforeEach
    void setUp() {
        jetStream = mock(JetStream.class);
        dlqPublisher = mock(DlqPublisher.class);
        registry = new SimpleMeterRegistry();
        router = new ShardRouter(mock(Connection.class), jetStream, new ShardTopology(2, 0),
                List.of(route),
                (payload, field) -> payload.contains("\"" + field + "\":\"ORD-123\"") ? "ORD-123" : null,
                dlqPublisher, new NatsChannelMetrics(registry), 30);
    }

    private Message message(String body, Headers headers) {
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn(body.getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(headers);
        when(msg.getSubject()).thenReturn("evt.order.accept");
        when(msg.metaData()).thenThrow(new IllegalStateException("not JS in unit test"));
        return msg;
    }

    @Test
    void headerKey_routesToHashedShard_withDerivedMsgId_andAcks() throws Exception {
        Headers h = new Headers();
        h.put(BpmHeaders.BUSINESS_KEY, "ORD-123");
        h.put(NatsJetStreamConstants.MSG_ID_HDR, "orig-1");
        Message msg = message("{}", h);

        router.handle(msg, route);

        ArgumentCaptor<NatsMessage> published = ArgumentCaptor.forClass(NatsMessage.class);
        verify(jetStream).publish(published.capture());
        // frozen vector: ORD-123 -> shard 1 of 2
        assertThat(published.getValue().getSubject()).isEqualTo("shard.1.evt.order.accept");
        assertThat(published.getValue().getHeaders().getFirst(NatsJetStreamConstants.MSG_ID_HDR))
                .isEqualTo("orig-1.s1");
        assertThat(published.getValue().getHeaders().getFirst(BpmHeaders.BUSINESS_KEY))
                .isEqualTo("ORD-123");
        verify(msg).ack();
        assertThat(registry.counter("nats.shard.routed",
                "subject", "evt.order.accept", "target_shard", "1").count()).isEqualTo(1.0);
    }

    @Test
    void payloadFallback_usedWhenHeaderAbsent_andPinnedAsHeader() throws Exception {
        Message msg = message("{\"orderId\":\"ORD-123\"}", new Headers());

        router.handle(msg, route);

        ArgumentCaptor<NatsMessage> published = ArgumentCaptor.forClass(NatsMessage.class);
        verify(jetStream).publish(published.capture());
        assertThat(published.getValue().getSubject()).isEqualTo("shard.1.evt.order.accept");
        // payload-derived key is pinned as a header for the shard side
        assertThat(published.getValue().getHeaders().getFirst(BpmHeaders.BUSINESS_KEY))
                .isEqualTo("ORD-123");
        verify(msg).ack();
    }

    @Test
    void missingKey_goesToShardDlq_notNak_whenDlqPublishSucceeds() throws Exception {
        when(dlqPublisher.publish(any(), eq("dlq.shard.evt.order.accept"),
                eq(DlqReason.SHARD_KEY_MISSING), eq("evt.order.accept"), any()))
                .thenReturn(DlqPublishOutcome.PUBLISHED_JETSTREAM);
        Message msg = message("{\"other\":1}", new Headers());

        router.handle(msg, route);

        verify(jetStream, never()).publish(any(NatsMessage.class));
        verify(msg).ack();
        assertThat(registry.counter("nats.shard.key_missing",
                "subject", "evt.order.accept").count()).isEqualTo(1.0);
    }

    @Test
    void missingKey_dlqFailure_naks_custodyKept() throws Exception {
        when(dlqPublisher.publish(any(), any(), any(), any(), any()))
                .thenReturn(DlqPublishOutcome.FAILED_BOTH_PUBLISH);
        Message msg = message("{\"other\":1}", new Headers());

        router.handle(msg, route);

        verify(msg, never()).ack();
        verify(msg).nakWithDelay(any(java.time.Duration.class));
    }

    @Test
    void publishRejected_naksWithBackoff_neverAcks() throws Exception {
        when(jetStream.publish(any(NatsMessage.class)))
                .thenThrow(new java.io.IOException("maximum bytes exceeded")); // discard=new reject
        Headers h = new Headers();
        h.put(BpmHeaders.BUSINESS_KEY, "ORD-123");
        Message msg = message("{}", h);

        router.handle(msg, route);

        verify(msg, never()).ack();
        verify(msg).nakWithDelay(any(java.time.Duration.class));
        assertThat(registry.counter("nats.shard.publish_reject",
                "subject", "evt.order.accept").count()).isEqualTo(1.0);
    }

    @Test
    void durableName_isSubjectDerivedAndSafe() {
        assertThat(route.durableName()).isEqualTo("shard-router-evt-order-accept");
    }
}
