package com.threeai.nats.camunda.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import com.threeai.nats.core.headers.BpmHeaders;
import com.threeai.nats.core.history.HistoryHeaders;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.NatsJetStreamConstants;
import org.junit.jupiter.api.Test;

class HistoryWireMessageFactoryTest {

    @Test
    void build_setsSubjectHeadersAndDedupId() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("userId", "u1");

        NatsMessage msg = HistoryWireMessageFactory.build("camunda", "OP_LOG", "hist-evt-1", "create",
                "proc-1", "biz-1", fields, null);

        assertThat(msg.getSubject()).isEqualTo("history.camunda.OP_LOG.proc-1");
        assertThat(msg.getHeaders().getFirst(NatsJetStreamConstants.MSG_ID_HDR)).isEqualTo("hist-evt-1:create");
        assertThat(msg.getHeaders().getFirst(HistoryHeaders.ENGINE_ID)).isEqualTo("camunda");
        assertThat(msg.getHeaders().getFirst(HistoryHeaders.CLASS)).isEqualTo("OP_LOG");
        assertThat(msg.getHeaders().getFirst(HistoryHeaders.EVENT_TYPE)).isEqualTo("create");
        assertThat(msg.getHeaders().getFirst(HistoryHeaders.EVENT_ID)).isEqualTo("hist-evt-1");
        assertThat(msg.getHeaders().getFirst(HistoryHeaders.PROCESS_INSTANCE_ID)).isEqualTo("proc-1");
        assertThat(msg.getHeaders().getFirst(BpmHeaders.BUSINESS_KEY)).isEqualTo("biz-1");
        assertThat(new String(msg.getData(), StandardCharsets.UTF_8)).contains("\"userId\":\"u1\"");
    }

    @Test
    void build_blankBusinessKey_headerOmitted() {
        NatsMessage msg = HistoryWireMessageFactory.build("camunda", "ACTINST", "hist-evt-2", "start",
                "proc-2", null, Map.of(), null);

        assertThat(msg.getHeaders().getFirst(BpmHeaders.BUSINESS_KEY)).isNull();
    }

    @Test
    void encodePayload_withLargePayload_addsBase64Key() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("a", "b");

        String json = HistoryWireMessageFactory.encodePayload(fields, "hello".getBytes(StandardCharsets.UTF_8));

        assertThat(json).contains("\"a\":\"b\"");
        assertThat(json).contains("_largePayloadBase64");
    }

    @Test
    void encodePayload_withoutLargePayload_noBase64Key() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("a", "b");

        String json = HistoryWireMessageFactory.encodePayload(fields, null);

        assertThat(json).doesNotContain("_largePayloadBase64");
    }

    @Test
    void encodePayloadFromRawFieldsJson_mergesLargePayload() {
        String rawJson = "{\"userId\":\"u1\"}";

        String result = HistoryWireMessageFactory.encodePayloadFromRawFieldsJson(rawJson,
                "extra".getBytes(StandardCharsets.UTF_8));

        assertThat(result).contains("\"userId\":\"u1\"");
        assertThat(result).contains("_largePayloadBase64");
    }

    @Test
    void encodePayloadFromRawFieldsJson_noLargePayload_passthroughFields() {
        String rawJson = "{\"userId\":\"u1\"}";

        String result = HistoryWireMessageFactory.encodePayloadFromRawFieldsJson(rawJson, null);

        assertThat(result).contains("\"userId\":\"u1\"");
        assertThat(result).doesNotContain("_largePayloadBase64");
    }
}
