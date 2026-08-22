package com.threeai.nats.cadenzaflow.a2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

import com.threeai.nats.cadenzaflow.a2.A2ReplyPayloadDecoder.ReplyVariables;
import io.nats.client.Message;
import org.cadenzaflow.bpm.engine.variable.value.BooleanValue;
import org.cadenzaflow.bpm.engine.variable.value.BytesValue;
import org.cadenzaflow.bpm.engine.variable.value.DateValue;
import org.cadenzaflow.bpm.engine.variable.value.IntegerValue;
import org.cadenzaflow.bpm.engine.variable.value.ObjectValue;
import org.cadenzaflow.bpm.engine.variable.value.StringValue;
import org.cadenzaflow.bpm.engine.variable.value.TypedValue;
import org.junit.jupiter.api.Test;

/**
 * Engine-parity reply variables (reply-variables increment, decision 2026-08-19): the structured
 * {@code variables}/{@code localVariables} contract, its type universe, and its refusal modes.
 * The legacy opaque-passthrough behaviour has its own coverage in {@link A2CompletionBridgeTest};
 * here it appears only where the two contracts meet.
 */
class A2ReplyVariablesTest {

    // --- the structured contract ---------------------------------------------------------------

    @Test
    void untypedScalars_mapToNaturalJavaTypes() throws Exception {
        ReplyVariables reply = decode("""
                {"type":"SUCCESS","variables":{
                  "action":{"value":"yes"},
                  "approved":{"value":true},
                  "count":{"value":42},
                  "big":{"value":far_long},
                  "ratio":{"value":0.5}}}""".replace("far_long", String.valueOf(1L + Integer.MAX_VALUE)));

        assertThat(reply.structured()).isTrue();
        assertThat(typedValue(reply, "action").getValue()).isEqualTo("yes");
        assertThat(typedValue(reply, "approved").getValue()).isEqualTo(true);
        assertThat(typedValue(reply, "count").getValue()).isEqualTo(42);
        assertThat(typedValue(reply, "big").getValue()).isEqualTo(1L + Integer.MAX_VALUE);
        assertThat(typedValue(reply, "ratio").getValue()).isEqualTo(0.5d);
    }

    @Test
    void typedValues_produceTheDeclaredEngineType() throws Exception {
        ReplyVariables reply = decode("""
                {"type":"SUCCESS","variables":{
                  "s":{"value":"x","type":"String"},
                  "b":{"value":false,"type":"Boolean"},
                  "i":{"value":7,"type":"Integer"},
                  "d":{"value":"2026-08-19T12:00:00Z","type":"Date"},
                  "raw":{"value":"aGVsbG8=","type":"Bytes"},
                  "nil":{"type":"Null"}}}""");

        assertThat(typedValue(reply, "s")).isInstanceOf(StringValue.class);
        assertThat(typedValue(reply, "b")).isInstanceOf(BooleanValue.class);
        assertThat(typedValue(reply, "i")).isInstanceOf(IntegerValue.class);
        assertThat(((DateValue) typedValue(reply, "d")).getValue())
                .isEqualTo(Date.from(java.time.Instant.parse("2026-08-19T12:00:00Z")));
        assertThat(((BytesValue) typedValue(reply, "raw")).getValue())
                .isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
        assertThat(typedValue(reply, "nil").getValue()).isNull();
    }

    @Test
    void serializedObject_carriesFormatAndTypeName() throws Exception {
        ReplyVariables reply = decode("""
                {"type":"SUCCESS","variables":{
                  "order":{"value":"{\\"id\\":1}","type":"Object",
                           "valueInfo":{"serializationDataFormat":"application/json",
                                        "objectTypeName":"com.acme.Order"}}}}""");

        ObjectValue order = (ObjectValue) typedValue(reply, "order");
        assertThat(order.getValueSerialized()).isEqualTo("{\"id\":1}");
        assertThat(order.getSerializationDataFormat()).isEqualTo("application/json");
        assertThat(order.getObjectTypeName()).isEqualTo("com.acme.Order");
    }

    @Test
    void transientFlag_travelsThroughValueInfo() throws Exception {
        ReplyVariables reply = decode("""
                {"type":"SUCCESS","variables":{
                  "tmp":{"value":"x","type":"String","valueInfo":{"transient":true}}}}""");

        assertThat(typedValue(reply, "tmp").isTransient()).isTrue();
    }

    @Test
    void localVariables_decodeIntoTheirOwnScope() throws Exception {
        ReplyVariables reply = decode("""
                {"type":"SUCCESS",
                 "variables":{"v1":{"value":1}},
                 "localVariables":{"scratch":{"value":"tmp"}}}""");

        assertThat(reply.variables()).containsOnlyKeys("v1");
        assertThat(reply.localVariables()).containsOnlyKeys("scratch");
    }

    /**
     * A worker that speaks the structured contract has said exactly what it wants written — the
     * opaque natsPayload passthrough must NOT run next to it, or the same bytes get stored twice.
     */
    @Test
    void structuredReply_suppressesTheOpaquePassthrough() throws Exception {
        ReplyVariables reply = decode("""
                {"type":"SUCCESS","somethingElse":"data","variables":{"v1":{"value":1}}}""");

        assertThat(reply.variables()).containsOnlyKeys("v1");
        assertThat(reply.variables()).doesNotContainKey("natsPayload");
    }

    @Test
    void legacyReply_keepsThePassthroughContract() throws Exception {
        ReplyVariables reply = decode("{\"type\":\"SUCCESS\",\"orderId\":\"A-1\"}");

        assertThat(reply.structured()).isFalse();
        assertThat(reply.variables()).containsOnlyKeys("natsPayload");
        assertThat(reply.localVariables()).isEmpty();
    }

    // --- refusal modes: broken variables must throw, never degrade ------------------------------

    @Test
    void variablesNotAnObject_isRefused() {
        assertThatThrownBy(() -> decode("{\"type\":\"SUCCESS\",\"variables\":[1,2]}"))
                .isInstanceOf(InvalidReplyVariablesException.class)
                .hasMessageContaining("'variables' must be a JSON object");
    }

    @Test
    void entryNotAnObject_isRefused() {
        assertThatThrownBy(() -> decode("{\"type\":\"SUCCESS\",\"variables\":{\"v1\":5}}"))
                .isInstanceOf(InvalidReplyVariablesException.class)
                .hasMessageContaining("{value, type?, valueInfo?}");
    }

    @Test
    void unsupportedType_isRefusedByName() {
        assertThatThrownBy(() -> decode(
                "{\"type\":\"SUCCESS\",\"variables\":{\"f\":{\"value\":\"x\",\"type\":\"File\"}}}"))
                .isInstanceOf(InvalidReplyVariablesException.class)
                .hasMessageContaining("unsupported type 'File'");
    }

    @Test
    void containerValueWithoutType_isRefusedNotGuessed() {
        assertThatThrownBy(() -> decode(
                "{\"type\":\"SUCCESS\",\"variables\":{\"m\":{\"value\":{\"a\":1}}}}"))
                .isInstanceOf(InvalidReplyVariablesException.class)
                .hasMessageContaining("no type");
    }

    @Test
    void undecodableTypedValue_isRefused() {
        assertThatThrownBy(() -> decode(
                "{\"type\":\"SUCCESS\",\"variables\":{\"d\":{\"value\":\"not-a-date\",\"type\":\"Date\"}}}"))
                .isInstanceOf(InvalidReplyVariablesException.class)
                .hasMessageContaining("cannot be decoded as 'Date'");
    }

    @Test
    void objectWithoutSerializationDataFormat_isRefused() {
        assertThatThrownBy(() -> decode(
                "{\"type\":\"SUCCESS\",\"variables\":{\"o\":{\"value\":\"{}\",\"type\":\"Object\"}}}"))
                .isInstanceOf(InvalidReplyVariablesException.class)
                .hasMessageContaining("serializationDataFormat");
    }

    // --- fixtures --------------------------------------------------------------------------------

    private ReplyVariables decode(String body) throws InvalidReplyVariablesException {
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn(body.getBytes(StandardCharsets.UTF_8));
        return A2ReplyPayloadDecoder.replyVariablesOf(msg, A2Properties.ReplyPayloadVariable.WHEN_PRESENT);
    }

    private TypedValue typedValue(ReplyVariables reply, String name) {
        Object value = reply.variables().get(name);
        assertThat(value).as("variable '%s'", name).isInstanceOf(TypedValue.class);
        return (TypedValue) value;
    }
}
