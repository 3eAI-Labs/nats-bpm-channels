package com.threeai.nats.core.outbound;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import com.threeai.nats.core.headers.BpmHeaders;
import com.threeai.nats.core.headers.OutboundHeaders;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.NatsJetStreamConstants;
import org.junit.jupiter.api.Test;

class OutboundWireMessageFactoryTest {

    @Test
    void buildPayload_identityOnly_noVariables() {
        byte[] payload = OutboundWireMessageFactory.buildPayload("camunda", "order.created", "proc-1", "biz-1", Map.of());

        String json = new String(payload, StandardCharsets.UTF_8);
        assertThat(json).contains("\"engineId\":\"camunda\"")
                .contains("\"messageType\":\"order.created\"")
                .contains("\"processInstanceId\":\"proc-1\"")
                .contains("\"businessKey\":\"biz-1\"")
                .doesNotContain("variables");
    }

    @Test
    void buildPayload_withVariables_includesVariablesObject() {
        Map<String, Object> variables = new LinkedHashMap<>();
        variables.put("amount", 42);
        variables.put("currency", "EUR");

        byte[] payload = OutboundWireMessageFactory.buildPayload("camunda", "order.created", "proc-1", null, variables);

        String json = new String(payload, StandardCharsets.UTF_8);
        assertThat(json).contains("\"variables\":{\"amount\":42,\"currency\":\"EUR\"}")
                .doesNotContain("businessKey");
    }

    @Test
    void buildPayload_escapesSpecialCharacters() {
        byte[] payload = OutboundWireMessageFactory.buildPayload("camunda", "order.created", "proc-1", "biz\"1", Map.of());

        String json = new String(payload, StandardCharsets.UTF_8);
        assertThat(json).contains("\"businessKey\":\"biz\\\"1\"");
    }

    @Test
    void buildPayload_variableWithNullValue_emittedAsJsonNullLiteral() {
        Map<String, Object> variables = new LinkedHashMap<>();
        variables.put("cancelledReason", null);

        byte[] payload = OutboundWireMessageFactory.buildPayload("camunda", "order.created", "proc-1", null, variables);

        String json = new String(payload, StandardCharsets.UTF_8);
        assertThat(json).contains("\"variables\":{\"cancelledReason\":null}");
    }

    @Test
    void buildPayload_variableWithBooleanValue_emittedUnquoted() {
        Map<String, Object> variables = new LinkedHashMap<>();
        variables.put("approved", true);

        byte[] payload = OutboundWireMessageFactory.buildPayload("camunda", "order.created", "proc-1", null, variables);

        String json = new String(payload, StandardCharsets.UTF_8);
        assertThat(json).contains("\"variables\":{\"approved\":true}");
    }

    @Test
    void buildPayload_engineIdNull_escapesToEmptyStringWithoutThrowing() {
        byte[] payload = OutboundWireMessageFactory.buildPayload(null, "order.created", "proc-1", null, Map.of());

        String json = new String(payload, StandardCharsets.UTF_8);
        assertThat(json).contains("\"engineId\":\"\"");
    }

    @Test
    void buildPayload_controlCharactersInBusinessKey_escapedPerJsonRules() {
        // Sub-0x20 control byte (SOH) built via char-concat (not a source-level unicode escape),
        // so no raw control byte has to be embedded literally in this source file.
        char controlByte = (char) 1;
        StringBuilder raw = new StringBuilder();
        raw.append("line1\nline2\ttabbed\rcr\\backslash\"quote").append(controlByte).append("ctrl");
        String rawWithControlChars = raw.toString();

        byte[] payload = OutboundWireMessageFactory.buildPayload("camunda", "order.created", "proc-1",
                rawWithControlChars, Map.of());

        String json = new String(payload, StandardCharsets.UTF_8);
        String expectedControlEscape = String.format("\\u%04x", (int) controlByte);
        assertThat(json).contains("\\n").contains("\\t").contains("\\r").contains("\\\\").contains("\\\"")
                .contains(expectedControlEscape);
        // Round-trip sanity: none of the raw control bytes leak through unescaped.
        assertThat(json).doesNotContain("line1\nline2");
    }

    @Test
    void buildMessage_withExplicitDedupId_setsSubjectHeadersAndPayload() {
        OutboundMessageDraft draft = new OutboundMessageDraft("camunda", "order.created", "proc-1", "biz-1",
                "trace-1", "events.camunda.order.created.proc-1", "{}".getBytes(StandardCharsets.UTF_8));

        NatsMessage msg = OutboundWireMessageFactory.buildMessage(draft, "dedup-1");

        assertThat(msg.getSubject()).isEqualTo("events.camunda.order.created.proc-1");
        assertThat(msg.getHeaders().getFirst(NatsJetStreamConstants.MSG_ID_HDR)).isEqualTo("dedup-1");
        assertThat(msg.getHeaders().getFirst(BpmHeaders.TRACE_ID)).isEqualTo("trace-1");
        assertThat(msg.getHeaders().getFirst(BpmHeaders.BUSINESS_KEY)).isEqualTo("biz-1");
        assertThat(msg.getHeaders().getFirst(OutboundHeaders.ENGINE_ID)).isEqualTo("camunda");
        assertThat(msg.getHeaders().getFirst(OutboundHeaders.MESSAGE_TYPE)).isEqualTo("order.created");
        assertThat(msg.getHeaders().getFirst(OutboundHeaders.PROCESS_INSTANCE_ID)).isEqualTo("proc-1");
        assertThat(msg.getData()).isEqualTo("{}".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void buildMessage_noExplicitDedupId_mintsFreshUuidPerCall() {
        OutboundMessageDraft draft = new OutboundMessageDraft("camunda", "order.created", "proc-1", null,
                "trace-1", "events.camunda.order.created.proc-1", "{}".getBytes(StandardCharsets.UTF_8));

        NatsMessage first = OutboundWireMessageFactory.buildMessage(draft);
        NatsMessage second = OutboundWireMessageFactory.buildMessage(draft);

        String firstDedup = first.getHeaders().getFirst(NatsJetStreamConstants.MSG_ID_HDR);
        String secondDedup = second.getHeaders().getFirst(NatsJetStreamConstants.MSG_ID_HDR);
        assertThat(firstDedup).isNotBlank();
        assertThat(secondDedup).isNotBlank().isNotEqualTo(firstDedup);
    }
}
