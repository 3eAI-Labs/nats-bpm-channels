package com.threeai.nats.core.headers;

import static org.assertj.core.api.Assertions.assertThat;

import io.nats.client.impl.Headers;
import org.junit.jupiter.api.Test;

class BpmHeadersTest {

    @Test
    void build_allPresent_addsAllThreeHeaders() {
        Headers headers = BpmHeaders.build("trace-1", "biz-7", "idem-x");

        assertThat(headers.getLast(BpmHeaders.TRACE_ID)).isEqualTo("trace-1");
        assertThat(headers.getLast(BpmHeaders.BUSINESS_KEY)).isEqualTo("biz-7");
        assertThat(headers.getLast(BpmHeaders.IDEMPOTENCY_KEY)).isEqualTo("idem-x");
        assertThat(headers.size()).isEqualTo(3);
    }

    @Test
    void build_nullBusinessKey_skipsThatHeaderOnly() {
        Headers headers = BpmHeaders.build("trace-1", null, "idem-x");

        assertThat(headers.containsKey(BpmHeaders.BUSINESS_KEY)).isFalse();
        assertThat(headers.containsKey(BpmHeaders.TRACE_ID)).isTrue();
        assertThat(headers.containsKey(BpmHeaders.IDEMPOTENCY_KEY)).isTrue();
    }

    @Test
    void build_blankValues_areSkipped() {
        Headers headers = BpmHeaders.build("  ", "", "idem-x");

        assertThat(headers.containsKey(BpmHeaders.TRACE_ID)).isFalse();
        assertThat(headers.containsKey(BpmHeaders.BUSINESS_KEY)).isFalse();
        assertThat(headers.containsKey(BpmHeaders.IDEMPOTENCY_KEY)).isTrue();
    }

    @Test
    void build_allNull_returnsEmptyHeaders() {
        Headers headers = BpmHeaders.build(null, null, null);

        assertThat(headers).isNotNull();
        assertThat(headers.size()).isZero();
    }

    @Test
    void headerNames_matchSpecExactly() {
        assertThat(BpmHeaders.TRACE_ID).isEqualTo("X-Cadenzaflow-Trace-Id");
        assertThat(BpmHeaders.BUSINESS_KEY).isEqualTo("X-Cadenzaflow-Business-Key");
        assertThat(BpmHeaders.IDEMPOTENCY_KEY).isEqualTo("X-Cadenzaflow-Idempotency-Key");
    }
}
