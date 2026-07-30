package com.threeai.nats.core.history;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HistoryHeadersTest {

    @Test
    void headerNames_matchAsyncapiContract() {
        assertThat(HistoryHeaders.ENGINE_ID).isEqualTo("X-Cadenzaflow-History-Engine-Id");
        assertThat(HistoryHeaders.CLASS).isEqualTo("X-Cadenzaflow-History-Class");
        assertThat(HistoryHeaders.EVENT_TYPE).isEqualTo("X-Cadenzaflow-History-Event-Type");
        assertThat(HistoryHeaders.EVENT_ID).isEqualTo("X-Cadenzaflow-History-Event-Id");
        assertThat(HistoryHeaders.PROCESS_INSTANCE_ID).isEqualTo("X-Cadenzaflow-History-Process-Instance-Id");
        assertThat(HistoryHeaders.EVENT_TIME).isEqualTo("X-Cadenzaflow-History-Event-Time");
    }
}
