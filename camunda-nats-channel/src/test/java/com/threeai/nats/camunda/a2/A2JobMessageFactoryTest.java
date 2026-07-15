package com.threeai.nats.camunda.a2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import io.nats.client.impl.NatsMessage;
import org.camunda.bpm.engine.impl.persistence.entity.ExternalTaskEntity;
import org.junit.jupiter.api.Test;

/**
 * Sentinel Phase 5.5 QA fix (item 5, Levent karari 2026-07-15) — allowlisted variable-capture
 * wire serialization: allowlist EMPTY (default, identity-only envelope unchanged), allowlist
 * POPULATED (variables object appended), and allowlist naming a NON-EXISTENT variable
 * (upstream-omitted by {@code A2ExternalTaskBehavior.captureAllowlistedVariables}, so simply
 * absent from the map handed here — covered from this layer as "map without that key").
 */
class A2JobMessageFactoryTest {

    @Test
    void build_noVariablesOverload_identityEnvelopeOnly() {
        ExternalTaskEntity task = mockTask("task-1", "order-fulfillment", "biz-1");

        NatsMessage msg = A2JobMessageFactory.build(task);

        assertThat(bodyOf(msg)).isEqualTo(
                "{\"externalTaskId\":\"task-1\",\"topic\":\"order-fulfillment\",\"businessKey\":\"biz-1\"}");
    }

    @Test
    void build_emptyCapturedVariablesMap_identityEnvelopeOnly() {
        ExternalTaskEntity task = mockTask("task-2", "order-fulfillment", "biz-2");

        NatsMessage msg = A2JobMessageFactory.build(task, Map.of());

        assertThat(bodyOf(msg)).isEqualTo(
                "{\"externalTaskId\":\"task-2\",\"topic\":\"order-fulfillment\",\"businessKey\":\"biz-2\"}");
    }

    @Test
    void build_populatedCapturedVariables_appendsVariablesObject() {
        ExternalTaskEntity task = mockTask("task-3", "payment-capture", "biz-3");
        Map<String, Object> variables = new LinkedHashMap<>();
        variables.put("amount", 42);
        variables.put("currency", "EUR");

        NatsMessage msg = A2JobMessageFactory.build(task, variables);

        assertThat(bodyOf(msg)).isEqualTo(
                "{\"externalTaskId\":\"task-3\",\"topic\":\"payment-capture\",\"businessKey\":\"biz-3\","
                        + "\"variables\":{\"amount\":42,\"currency\":\"EUR\"}}");
    }

    @Test
    void build_capturedVariableWithNullValue_serializesJsonNull() {
        ExternalTaskEntity task = mockTask("task-4", "order-fulfillment", "biz-4");
        Map<String, Object> variables = new LinkedHashMap<>();
        variables.put("optionalField", null);

        NatsMessage msg = A2JobMessageFactory.build(task, variables);

        assertThat(bodyOf(msg)).contains("\"optionalField\":null");
    }

    @Test
    void build_capturedVariableWithQuoteInValue_escapesJson() {
        ExternalTaskEntity task = mockTask("task-5", "order-fulfillment", "biz-5");
        Map<String, Object> variables = new LinkedHashMap<>();
        variables.put("note", "says \"hello\"");

        NatsMessage msg = A2JobMessageFactory.build(task, variables);

        assertThat(bodyOf(msg)).contains("\"note\":\"says \\\"hello\\\"\"");
    }

    @Test
    void build_nullCapturedVariables_treatedAsEmpty() {
        ExternalTaskEntity task = mockTask("task-6", "order-fulfillment", "biz-6");

        NatsMessage msg = A2JobMessageFactory.build(task, null);

        assertThat(bodyOf(msg)).doesNotContain("variables");
    }

    private ExternalTaskEntity mockTask(String id, String topic, String businessKey) {
        ExternalTaskEntity task = mock(ExternalTaskEntity.class);
        when(task.getId()).thenReturn(id);
        when(task.getTopicName()).thenReturn(topic);
        when(task.getBusinessKey()).thenReturn(businessKey);
        return task;
    }

    private String bodyOf(NatsMessage msg) {
        return new String(msg.getData(), StandardCharsets.UTF_8);
    }
}
