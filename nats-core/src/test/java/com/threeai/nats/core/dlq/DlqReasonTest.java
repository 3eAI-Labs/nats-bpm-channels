package com.threeai.nats.core.dlq;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DlqReasonTest {

    @Test
    void headerValue_matchesExceptionCode() {
        assertThat(DlqReason.DELIVERY_BUDGET_EXCEEDED.headerValue()).isEqualTo("BUS_REPLY_DELIVERY_BUDGET_EXCEEDED");
        assertThat(DlqReason.EMPTY_MESSAGE_BODY.headerValue()).isEqualTo("VAL_EMPTY_MESSAGE_BODY");
    }
}
