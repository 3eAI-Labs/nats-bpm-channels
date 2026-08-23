package com.threeai.nats.camunda.eventing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

class EventingPayloadMapperTest {

    // --- typedVariables: EventPayloadTypes dictionary mapping (docs/12 D-C v2) ---

    @Test
    void typedVariables_convertsEveryDictionaryType() {
        JsonNode root = EventingPayloadMapper.parse(
                "{\"s\":\"txt\",\"i\":42,\"l\":9999999999,\"d\":1.5,\"b\":true,\"j\":{\"nested\":1}}");
        Map<String, String> types = new LinkedHashMap<>();
        types.put("s", "string");
        types.put("i", "integer");
        types.put("l", "long");
        types.put("d", "double");
        types.put("b", "boolean");
        types.put("j", "json");

        Map<String, Object> vars = EventingPayloadMapper.typedVariables(root, types);

        assertThat(vars)
                .containsEntry("s", "txt")
                .containsEntry("i", 42)
                .containsEntry("l", 9999999999L)
                .containsEntry("d", 1.5)
                .containsEntry("b", true)
                // json type stays a raw String in v1 (docs/12 D-A v2)
                .containsEntry("j", "{\"nested\":1}");
    }

    @Test
    void typedVariables_absentOrNullFields_areSkippedNotNulled() {
        JsonNode root = EventingPayloadMapper.parse("{\"present\":\"x\",\"explicitNull\":null}");
        Map<String, String> types = Map.of(
                "present", "string", "explicitNull", "string", "absent", "integer");

        Map<String, Object> vars = EventingPayloadMapper.typedVariables(root, types);

        assertThat(vars).containsOnlyKeys("present");
    }

    @Test
    void typedVariables_nonObjectPayload_yieldsEmptyMap() {
        assertThat(EventingPayloadMapper.typedVariables(
                EventingPayloadMapper.parse("[1,2,3]"), Map.of("a", "string"))).isEmpty();
        assertThat(EventingPayloadMapper.typedVariables(
                EventingPayloadMapper.parse("not json"), Map.of("a", "string"))).isEmpty();
    }

    // --- correlationValues: missing field is a PERMANENT failure ---

    @Test
    void correlationValues_typedMatchValues() {
        JsonNode root = EventingPayloadMapper.parse("{\"orderId\":123,\"region\":\"eu\"}");
        Map<String, String> types = new LinkedHashMap<>();
        types.put("orderId", "long");
        types.put("region", "string");

        Map<String, Object> values = EventingPayloadMapper.correlationValues(root, types);

        assertThat(values).containsEntry("orderId", 123L).containsEntry("region", "eu");
    }

    @Test
    void correlationValues_missingField_throwsPermanentFailure() {
        JsonNode root = EventingPayloadMapper.parse("{\"other\":1}");

        assertThatThrownBy(() -> EventingPayloadMapper.correlationValues(root, Map.of("orderId", "long")))
                .isInstanceOf(EventingPayloadMapper.MissingCorrelationValueException.class)
                .hasMessageContaining("VAL_EVENTING_MISSING_CORRELATION_VALUE")
                .hasMessageContaining("orderId");
    }

    @Test
    void correlationValues_unparseablePayload_throwsPermanentFailure() {
        assertThatThrownBy(() -> EventingPayloadMapper.correlationValues(
                EventingPayloadMapper.parse("garbage"), Map.of("orderId", "long")))
                .isInstanceOf(EventingPayloadMapper.MissingCorrelationValueException.class);
    }

    // --- topLevelScalarAsText: the extractJsonField replacement (D-F v2 behavior change) ---

    @Test
    void topLevelScalarAsText_numericField_returnsValueNotNextFieldName() {
        // The retired indexOf scan returned "name" (the NEXT field's name) for this input —
        // the latent numeric-field bug this change fixes.
        assertThat(EventingPayloadMapper.topLevelScalarAsText(
                "{\"orderId\":123,\"name\":\"x\"}", "orderId")).isEqualTo("123");
    }

    @Test
    void topLevelScalarAsText_nestedField_noLongerFound() {
        // The retired scan was depth-unaware and DID find this — documented breaking change.
        assertThat(EventingPayloadMapper.topLevelScalarAsText(
                "{\"outer\":{\"orderId\":\"abc\"}}", "orderId")).isNull();
    }

    @Test
    void topLevelScalarAsText_containerNullOrUnparseable_returnsNull() {
        assertThat(EventingPayloadMapper.topLevelScalarAsText("{\"f\":{\"a\":1}}", "f")).isNull();
        assertThat(EventingPayloadMapper.topLevelScalarAsText("{\"f\":null}", "f")).isNull();
        assertThat(EventingPayloadMapper.topLevelScalarAsText("not json", "f")).isNull();
        assertThat(EventingPayloadMapper.topLevelScalarAsText("{\"g\":1}", "f")).isNull();
    }

    @Test
    void topLevelScalarAsText_booleanAndStringScalars() {
        assertThat(EventingPayloadMapper.topLevelScalarAsText("{\"f\":true}", "f")).isEqualTo("true");
        assertThat(EventingPayloadMapper.topLevelScalarAsText("{\"f\":\"v\"}", "f")).isEqualTo("v");
    }
}
