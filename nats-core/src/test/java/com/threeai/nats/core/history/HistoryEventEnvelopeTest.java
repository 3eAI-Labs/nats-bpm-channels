package com.threeai.nats.core.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;

class HistoryEventEnvelopeTest {

    private HistoryEventEnvelope newEnvelope(long streamSequence) {
        return new HistoryEventEnvelope("camunda", "OP_LOG", "create", "hist-evt-1",
                "proc-inst-1", "biz-key-1", streamSequence, Instant.parse("2026-07-20T00:00:00Z"), "{}");
    }

    @Test
    void dedupId_isHistoryEventIdColonEventType() {
        assertThat(newEnvelope(0).dedupId()).isEqualTo("hist-evt-1:create");
    }

    @Test
    void subject_isInstanceKeyedHistorySubject() {
        assertThat(newEnvelope(0).subject()).isEqualTo("history.camunda.OP_LOG.proc-inst-1");
    }

    @Test
    void withStreamSequence_replacesOnlyStreamSequence() {
        HistoryEventEnvelope original = newEnvelope(0);

        HistoryEventEnvelope withSeq = original.withStreamSequence(42L);

        assertThat(withSeq.streamSequence()).isEqualTo(42L);
        assertThat(withSeq.engineId()).isEqualTo(original.engineId());
        assertThat(withSeq.historyEventId()).isEqualTo(original.historyEventId());
        assertThat(withSeq.payload()).isEqualTo(original.payload());
        assertThat(original.streamSequence()).isZero();
    }
}
