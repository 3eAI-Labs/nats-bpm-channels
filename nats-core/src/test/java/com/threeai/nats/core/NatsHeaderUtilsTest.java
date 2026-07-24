package com.threeai.nats.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.Map;

import io.nats.client.Message;
import io.nats.client.impl.Headers;
import org.junit.jupiter.api.Test;

class NatsHeaderUtilsTest {

    @Test
    void toNatsHeaders_nullMap_returnsNull() {
        assertThat(NatsHeaderUtils.toNatsHeaders(null)).isNull();
    }

    @Test
    void toNatsHeaders_emptyMap_returnsNull() {
        assertThat(NatsHeaderUtils.toNatsHeaders(Map.of())).isNull();
    }

    @Test
    void toNatsHeaders_populatedMap_addsEveryEntryAsString() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("trace-id", "trace-1");
        source.put("retry-count", 3);

        Headers headers = NatsHeaderUtils.toNatsHeaders(source);

        assertThat(headers).isNotNull();
        assertThat(headers.getFirst("trace-id")).isEqualTo("trace-1");
        assertThat(headers.getFirst("retry-count")).isEqualTo("3");
    }

    @Test
    void toNatsHeaders_nullValueEntry_isSkipped() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("trace-id", "trace-1");
        source.put("business-key", null);

        Headers headers = NatsHeaderUtils.toNatsHeaders(source);

        assertThat(headers).isNotNull();
        assertThat(headers.containsKey("business-key")).isFalse();
        assertThat(headers.getFirst("trace-id")).isEqualTo("trace-1");
    }

    @Test
    void extractHeader_headersNull_returnsNull() {
        Message msg = mock(Message.class);
        when(msg.getHeaders()).thenReturn(null);

        assertThat(NatsHeaderUtils.extractHeader(msg, "trace-id")).isNull();
    }

    @Test
    void extractHeader_keyMissing_returnsNull() {
        Message msg = mock(Message.class);
        when(msg.getHeaders()).thenReturn(new Headers());

        assertThat(NatsHeaderUtils.extractHeader(msg, "trace-id")).isNull();
    }

    @Test
    void extractHeader_keyPresent_returnsLastValue() {
        Headers headers = new Headers();
        headers.add("trace-id", "first");
        headers.add("trace-id", "second");
        Message msg = mock(Message.class);
        when(msg.getHeaders()).thenReturn(headers);

        assertThat(NatsHeaderUtils.extractHeader(msg, "trace-id")).isEqualTo("second");
    }
}
