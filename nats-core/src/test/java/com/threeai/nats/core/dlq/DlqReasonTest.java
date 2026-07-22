package com.threeai.nats.core.dlq;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DlqReasonTest {

    @Test
    void headerValue_matchesExceptionCode() {
        assertThat(DlqReason.DELIVERY_BUDGET_EXCEEDED.headerValue()).isEqualTo("BUS_REPLY_DELIVERY_BUDGET_EXCEEDED");
        assertThat(DlqReason.EMPTY_MESSAGE_BODY.headerValue()).isEqualTo("VAL_EMPTY_MESSAGE_BODY");
        assertThat(DlqReason.INVALID_REPLY_TYPE.headerValue()).isEqualTo("VAL_INVALID_REPLY_TYPE");
        assertThat(DlqReason.HISTORY_DELIVERY_BUDGET_EXCEEDED.headerValue()).isEqualTo("BUS_HISTORY_DELIVERY_BUDGET_EXCEEDED");
        assertThat(DlqReason.HISTORY_SCHEMA_DRIFT.headerValue()).isEqualTo("SYS_PROJECTION_SCHEMA_DRIFT");
        assertThat(DlqReason.OUTBOUND_PUBLISH_FAILED.headerValue()).isEqualTo("SYS_OUTBOUND_PUBLISH_FAILED");
    }
}
