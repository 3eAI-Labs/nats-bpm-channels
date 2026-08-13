package com.threeai.nats.cibseven.a2;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;

import io.nats.client.Message;
import io.nats.client.impl.Headers;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * QA review fix (reply discriminator, decision 2026-07-15) — {@link
 * A2ReplyPayloadDecoder#classify(Message)} now reads the mandatory {@code type} field instead of
 * the old errorCode-presence heuristic; missing/unknown values return {@link
 * java.util.Optional#empty()} so the caller can route to the DLQ.
 *
 * <p>Post-review follow-up fix F-1 (decision 2026-07-15) — field extraction now parses
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
     * Post-review follow-up fix F-1 (decision 2026-07-15) — a same-named {@code type}
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

        assertThat(A2ReplyPayloadDecoder.variablesOf(msg, A2Properties.ReplyPayloadVariable.WHEN_PRESENT))
                .containsEntry("natsPayload", "{\"type\":\"SUCCESS\",\"result\":\"ok\"}");
    }

    /**
     * A reply that carries only the discriminator has no worker result in it — {@code type} is
     * this layer's routing signal, not data the process asked for. Writing it anyway cost one
     * process-variable row plus one history row per completion; in a 100k-task run that was
     * 120k rows nothing ever read.
     */
    @Test
    void variablesOf_whenPresent_bareDiscriminator_writesNothing() {
        Message msg = message("{\"type\":\"SUCCESS\"}");

        assertThat(A2ReplyPayloadDecoder.variablesOf(msg, A2Properties.ReplyPayloadVariable.WHEN_PRESENT))
                .isEmpty();
    }

    /** One unrecognised field is still a worker result: pass the whole body, {@code type} included. */
    @Test
    void variablesOf_whenPresent_anyExtraField_passesWholeBodyThrough() {
        Message msg = message("{\"type\":\"SUCCESS\",\"orderId\":\"A-123\"}");

        assertThat(A2ReplyPayloadDecoder.variablesOf(msg, A2Properties.ReplyPayloadVariable.WHEN_PRESENT))
                .containsEntry("natsPayload", "{\"type\":\"SUCCESS\",\"orderId\":\"A-123\"}");
    }

    /** ALWAYS restores the pre-0.8.1 behaviour for anyone whose process reads the bare body. */
    @Test
    void variablesOf_always_bareDiscriminator_stillWritten() {
        Message msg = message("{\"type\":\"SUCCESS\"}");

        assertThat(A2ReplyPayloadDecoder.variablesOf(msg, A2Properties.ReplyPayloadVariable.ALWAYS))
                .containsEntry("natsPayload", "{\"type\":\"SUCCESS\"}");
    }

    @Test
    void variablesOf_never_writesNothingEvenWithBusinessData() {
        Message msg = message("{\"type\":\"SUCCESS\",\"orderId\":\"A-123\"}");

        assertThat(A2ReplyPayloadDecoder.variablesOf(msg, A2Properties.ReplyPayloadVariable.NEVER))
                .isEmpty();
    }

    /**
     * The emptiness check must never be the reason a result is dropped: a body this layer cannot
     * parse is passed through rather than assumed empty.
     */
    @Test
    void variablesOf_whenPresent_unparseableBody_passedThroughNotDropped() {
        Message msg = message("not json at all");

        assertThat(A2ReplyPayloadDecoder.variablesOf(msg, A2Properties.ReplyPayloadVariable.WHEN_PRESENT))
                .containsEntry("natsPayload", "not json at all");
    }

    private Message message(String body) {
        Message msg = Mockito.mock(Message.class);
        Mockito.when(msg.getData()).thenReturn(body.getBytes(StandardCharsets.UTF_8));
        Mockito.when(msg.getHeaders()).thenReturn(new Headers());
        return msg;
    }
}
