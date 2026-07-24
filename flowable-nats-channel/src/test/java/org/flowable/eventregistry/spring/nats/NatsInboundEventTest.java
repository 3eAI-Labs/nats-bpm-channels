package org.flowable.eventregistry.spring.nats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import io.nats.client.Message;
import io.nats.client.impl.Headers;
import org.junit.jupiter.api.Test;

/**
 * Sentinel Phase 5.5 (round 2): {@link NatsInboundEvent} had no dedicated unit test — it was only
 * exercised indirectly through the inbound adapters' {@code handleMessage} paths, which never call
 * {@link NatsInboundEvent#getRawEvent()} or assert on {@link NatsInboundEvent#getHeaders()}
 * contents directly. These are real assertions on the actual InboundEvent contract Flowable's
 * event-registry engine relies on (correlation-key extraction reads {@code getHeaders()}).
 */
class NatsInboundEventTest {

    @Test
    void getRawEvent_returnsOriginalMessageInstance() {
        Message message = mock(Message.class);
        when(message.getData()).thenReturn("{}".getBytes(StandardCharsets.UTF_8));
        when(message.getHeaders()).thenReturn(null);

        NatsInboundEvent event = new NatsInboundEvent(message);

        assertThat(event.getRawEvent()).isSameAs(message);
    }

    @Test
    void getBody_dataPresent_decodesUtf8() {
        Message message = mock(Message.class);
        when(message.getData()).thenReturn("{\"orderId\":42}".getBytes(StandardCharsets.UTF_8));
        when(message.getHeaders()).thenReturn(null);

        NatsInboundEvent event = new NatsInboundEvent(message);

        assertThat(event.getBody()).isEqualTo("{\"orderId\":42}");
    }

    @Test
    void getBody_dataNull_isNull() {
        Message message = mock(Message.class);
        when(message.getData()).thenReturn(null);
        when(message.getHeaders()).thenReturn(null);

        NatsInboundEvent event = new NatsInboundEvent(message);

        assertThat(event.getBody()).isNull();
    }

    @Test
    void getHeaders_nullMessageHeaders_returnsEmptyMap() {
        Message message = mock(Message.class);
        when(message.getData()).thenReturn("{}".getBytes(StandardCharsets.UTF_8));
        when(message.getHeaders()).thenReturn(null);

        NatsInboundEvent event = new NatsInboundEvent(message);

        assertThat(event.getHeaders()).isEmpty();
    }

    @Test
    void getHeaders_emptyMessageHeaders_returnsEmptyMap() {
        Message message = mock(Message.class);
        when(message.getData()).thenReturn("{}".getBytes(StandardCharsets.UTF_8));
        when(message.getHeaders()).thenReturn(new Headers());

        NatsInboundEvent event = new NatsInboundEvent(message);

        assertThat(event.getHeaders()).isEmpty();
    }

    @Test
    void getHeaders_populatedHeaders_extractsLastValuePerKey() {
        Headers headers = new Headers();
        headers.add("X-Trace-Id", "trace-1");
        headers.add("X-Cadenzaflow-Dlq-Original-Subject", "order.new");
        // Multiple values for the same key -- getHeaders() must surface the LAST one, matching
        // extractHeaders()'s use of Headers#getLast (mirrors NATS's own "last wins" header semantics).
        headers.add("X-Retry-Count", "1");
        headers.add("X-Retry-Count", "2");

        Message message = mock(Message.class);
        when(message.getData()).thenReturn("{}".getBytes(StandardCharsets.UTF_8));
        when(message.getHeaders()).thenReturn(headers);

        NatsInboundEvent event = new NatsInboundEvent(message);
        Map<String, Object> extracted = event.getHeaders();

        assertThat(extracted)
                .containsEntry("X-Trace-Id", "trace-1")
                .containsEntry("X-Cadenzaflow-Dlq-Original-Subject", "order.new")
                .containsEntry("X-Retry-Count", "2");
    }

    @Test
    void getHeaders_returnedMapIsUnmodifiable() {
        Headers headers = new Headers();
        headers.add("X-Trace-Id", "trace-1");
        Message message = mock(Message.class);
        when(message.getData()).thenReturn("{}".getBytes(StandardCharsets.UTF_8));
        when(message.getHeaders()).thenReturn(headers);

        NatsInboundEvent event = new NatsInboundEvent(message);
        Map<String, Object> extracted = event.getHeaders();

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> extracted.put("X-New", "value"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
