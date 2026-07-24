package com.threeai.nats.cibseven.a2;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;

import io.nats.client.Message;
import io.nats.client.impl.Headers;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Sentinel Phase 5.5 QA fix (reply discriminator, Levent karari 2026-07-15) — {@link
 * A2ReplyPayloadDecoder#classify(Message)} now reads the mandatory {@code type} field instead of
 * the old errorCode-presence heuristic; missing/unknown values return {@link
 * java.util.Optional#empty()} so the caller can route to the DLQ.
 *
 * <p>Sentinel Phase 6 follow-up fix F-1 (Levent karari 2026-07-15) — field extraction now parses
 * the body with Jackson and reads top-level fields only, so a same-named key nested inside
 * another field cannot shadow the wire-critical {@code type} discriminator (see {@code
 * classify_nestedObjectContainsSameNamedTypeKey_topLevelTypeWins}).
 */
class A2ReplyPayloadDecoderTest {

    @Test
    void classify_success_returnsSuccess() {
        Message msg = message("{\"type\":\"SUCCESS\",\"result\":\"ok\"}");

        assertThat(A2ReplyPayloadDecoder.classify(msg)).contains(ReplyType.SUCCESS);
    }

    @Test
    void classify_bpmnError_returnsBpmnError() {
        Message msg = message("{\"type\":\"BPMN_ERROR\",\"errorCode\":\"INSUFFICIENT_FUNDS\"}");

        assertThat(A2ReplyPayloadDecoder.classify(msg)).contains(ReplyType.BPMN_ERROR);
    }

    @Test
    void classify_transient_returnsTransient() {
        Message msg = message("{\"type\":\"TRANSIENT\",\"errorMessage\":\"timeout\"}");

        assertThat(A2ReplyPayloadDecoder.classify(msg)).contains(ReplyType.TRANSIENT);
    }

    @Test
    void classify_missingTypeField_returnsEmpty() {
        Message msg = message("{\"result\":\"ok\"}");

        assertThat(A2ReplyPayloadDecoder.classify(msg)).isEmpty();
    }

    @Test
    void classify_unknownTypeValue_returnsEmpty() {
        Message msg = message("{\"type\":\"WHO_KNOWS\"}");

        assertThat(A2ReplyPayloadDecoder.classify(msg)).isEmpty();
    }

    @Test
    void classify_emptyBody_returnsEmpty() {
        Message msg = message("");

        assertThat(A2ReplyPayloadDecoder.classify(msg)).isEmpty();
    }

    @Test
    void classify_lowerCaseTypeValue_returnsEmpty() {
        // Enum values are case-sensitive on purpose — the wire contract fixes the exact strings.
        Message msg = message("{\"type\":\"success\"}");

        assertThat(A2ReplyPayloadDecoder.classify(msg)).isEmpty();
    }

    /**
     * Sentinel Phase 6 follow-up fix F-1 (Levent karari 2026-07-15) — a same-named {@code type}
     * key nested inside another top-level object field (asyncapi permits nested objects,
     * {@code additionalProperties: true}) must NOT shadow the wire-critical top-level
     * discriminator. The old depth-unaware string-search parser would have matched the nested
     * {@code "type":"BPMN_ERROR"} first and misclassified this reply.
     */
    @Test
    void classify_nestedObjectContainsSameNamedTypeKey_topLevelTypeWins() {
        Message msg = message("{\"data\":{\"type\":\"BPMN_ERROR\"},\"type\":\"SUCCESS\"}");

        assertThat(A2ReplyPayloadDecoder.classify(msg)).contains(ReplyType.SUCCESS);
    }

    /** Malformed JSON must degrade to {@link java.util.Optional#empty()}, never throw. */
    @Test
    void classify_malformedJson_returnsEmpty() {
        Message msg = message("{\"type\":\"SUCCESS\"");

        assertThat(A2ReplyPayloadDecoder.classify(msg)).isEmpty();
    }

    /** A JSON array body (not an object) must degrade to {@link java.util.Optional#empty()}, never throw. */
    @Test
    void classify_jsonArrayBody_returnsEmpty() {
        Message msg = message("[\"type\",\"SUCCESS\"]");

        assertThat(A2ReplyPayloadDecoder.classify(msg)).isEmpty();
    }

    @Test
    void errorCodeOf_extractsField() {
        Message msg = message("{\"type\":\"BPMN_ERROR\",\"errorCode\":\"INSUFFICIENT_FUNDS\"}");

        assertThat(A2ReplyPayloadDecoder.errorCodeOf(msg)).isEqualTo("INSUFFICIENT_FUNDS");
    }

    @Test
    void retriesOf_missingField_defaultsToZero() {
        Message msg = message("{\"type\":\"TRANSIENT\"}");

        assertThat(A2ReplyPayloadDecoder.retriesOf(msg)).isEqualTo(0);
    }

    @Test
    void retriesOf_presentField_parsesInteger() {
        Message msg = message("{\"type\":\"TRANSIENT\",\"retries\":3}");

        assertThat(A2ReplyPayloadDecoder.retriesOf(msg)).isEqualTo(3);
    }

    @Test
    void variablesOf_wrapsRawBodyUnderNatsPayload() {
        Message msg = message("{\"type\":\"SUCCESS\",\"result\":\"ok\"}");

        assertThat(A2ReplyPayloadDecoder.variablesOf(msg))
                .containsEntry("natsPayload", "{\"type\":\"SUCCESS\",\"result\":\"ok\"}");
    }

    private Message message(String body) {
        Message msg = Mockito.mock(Message.class);
        Mockito.when(msg.getData()).thenReturn(body.getBytes(StandardCharsets.UTF_8));
        Mockito.when(msg.getHeaders()).thenReturn(new Headers());
        return msg;
    }
}
