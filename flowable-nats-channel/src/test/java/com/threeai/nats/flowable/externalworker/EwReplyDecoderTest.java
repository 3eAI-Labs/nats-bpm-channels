package com.threeai.nats.flowable.externalworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Date;

import com.threeai.nats.core.dlq.DlqReason;
import io.nats.client.Message;
import org.junit.jupiter.api.Test;

/** D-C'v2 matrix + top-level-only discriminator reads. */
class EwReplyDecoderTest {

    private static Message msg(String json) {
        Message m = mock(Message.class);
        when(m.getData()).thenReturn(json.getBytes(StandardCharsets.UTF_8));
        return m;
    }

    @Test
    void success_withTypedVariables_decodes() throws Exception {
        var reply = EwReplyDecoder.decode(msg("""
                {"type":"SUCCESS","jobId":"j1","variables":{
                  "action":{"value":"approve","type":"String"},
                  "count":{"value":7,"type":"Integer"},
                  "when":{"value":"2026-08-22T10:15:30+03:00","type":"Date"},
                  "raw":{"value":"aGk=","type":"Bytes"},
                  "untyped":{"value":true}}}"""));

        assertThat(reply.type()).isEqualTo(EwReplyType.SUCCESS);
        assertThat(reply.jobId()).isEqualTo("j1");
        assertThat(reply.variables())
                .containsEntry("action", "approve")
                .containsEntry("count", 7)
                .containsEntry("untyped", true);
        assertThat(reply.variables().get("when")).isInstanceOf(Date.class);
        assertThat((byte[]) reply.variables().get("raw")).isEqualTo("hi".getBytes());
    }

    @Test
    void nestedTypeField_cannotShadowDiscriminator() {
        assertThatThrownBy(() -> EwReplyDecoder.decode(
                msg("{\"data\":{\"type\":\"SUCCESS\"},\"jobId\":\"j1\"}")))
                .isInstanceOf(InvalidEwReplyException.class)
                .extracting(e -> ((InvalidEwReplyException) e).reason())
                .isEqualTo(DlqReason.INVALID_REPLY_TYPE);
    }

    @Test
    void localVariables_onAnyReply_isUnsupportedShape() {
        assertThatThrownBy(() -> EwReplyDecoder.decode(
                msg("{\"type\":\"SUCCESS\",\"jobId\":\"j1\",\"localVariables\":{}}")))
                .extracting(e -> ((InvalidEwReplyException) e).reason())
                .isEqualTo(DlqReason.UNSUPPORTED_REPLY_SHAPE);
    }

    @Test
    void bpmnError_withoutErrorCode_rejected() {
        assertThatThrownBy(() -> EwReplyDecoder.decode(msg("{\"type\":\"BPMN_ERROR\",\"jobId\":\"j1\"}")))
                .extracting(e -> ((InvalidEwReplyException) e).reason())
                .isEqualTo(DlqReason.INVALID_REPLY_TYPE);
    }

    @Test
    void bpmnError_errorMessage_degradesIntoWellKnownVariable() throws Exception {
        var reply = EwReplyDecoder.decode(msg(
                "{\"type\":\"BPMN_ERROR\",\"jobId\":\"j1\",\"errorCode\":\"failed\",\"errorMessage\":\"boom\"}"));
        assertThat(reply.variables()).containsEntry(EwReplyDecoder.ERROR_MESSAGE_VARIABLE, "boom");
        assertThat(reply.errorCode()).isEqualTo("failed");
    }

    @Test
    void transient_variables_droppedLoudly() throws Exception {
        var reply = EwReplyDecoder.decode(msg("""
                {"type":"TRANSIENT","jobId":"j1","errorMessage":"db down","retries":2,
                 "variables":{"x":{"value":1,"type":"Integer"}}}"""));
        assertThat(reply.transientVariablesDropped()).isTrue();
        assertThat(reply.variables()).isEmpty();
        assertThat(reply.retries()).isEqualTo(2);
    }

    @Test
    void objectType_rejected_asInvalidVariables() {
        assertThatThrownBy(() -> EwReplyDecoder.decode(msg(
                "{\"type\":\"SUCCESS\",\"jobId\":\"j1\",\"variables\":{\"o\":{\"value\":\"{}\",\"type\":\"Object\"}}}")))
                .extracting(e -> ((InvalidEwReplyException) e).reason())
                .isEqualTo(DlqReason.INVALID_REPLY_VARIABLES);
    }

    @Test
    void untypedContainer_rejected() {
        assertThatThrownBy(() -> EwReplyDecoder.decode(msg(
                "{\"type\":\"SUCCESS\",\"jobId\":\"j1\",\"variables\":{\"c\":{\"value\":{\"a\":1}}}}")))
                .extracting(e -> ((InvalidEwReplyException) e).reason())
                .isEqualTo(DlqReason.INVALID_REPLY_VARIABLES);
    }
}
