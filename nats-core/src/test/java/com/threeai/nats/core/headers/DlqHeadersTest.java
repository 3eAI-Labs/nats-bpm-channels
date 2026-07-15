package com.threeai.nats.core.headers;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DlqHeadersTest {

    @Test
    void headerNames_matchSpecExactly() {
        assertThat(DlqHeaders.ORIGINAL_SUBJECT).isEqualTo("X-Cadenzaflow-Dlq-Original-Subject");
        assertThat(DlqHeaders.DELIVERY_COUNT).isEqualTo("X-Cadenzaflow-Dlq-Delivery-Count");
        assertThat(DlqHeaders.REASON).isEqualTo("X-Cadenzaflow-Dlq-Reason");
        assertThat(DlqHeaders.TIMESTAMP).isEqualTo("X-Cadenzaflow-Dlq-Timestamp");
    }
}
