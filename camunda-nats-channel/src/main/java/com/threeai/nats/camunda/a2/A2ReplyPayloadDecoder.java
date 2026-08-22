package com.threeai.nats.camunda.a2;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Message;
import org.camunda.bpm.engine.variable.Variables;

/**
 * Decodes {@code jobs.<topic>.reply} messages per the asyncapi {@code JobSuccessReply} /
 * {@code JobBpmnErrorReply} / {@code JobTransientFailureReply} schemas.
 *
 * <p><b>Reply discriminator (QA review fix, decision 2026-07-15):</b> the
 * original errorCode-presence heuristic (classify by {@code Content-Type} first, then by whether
 * an {@code errorCode} field was present) was an implementation choice, not an explicitly
 * mandated wire rule — and it silently misclassified any {@code TransientFailurePayload} that
 * happened to include an {@code errorCode}-shaped field, or any reply missing a body field it
 * expected. The wire contract now mandates a {@code type: SUCCESS|BPMN_ERROR|TRANSIENT}
 * discriminator field on EVERY reply payload (asyncapi {@code JobSuccessPayload} /
 * {@code BpmnErrorPayload} / {@code TransientFailurePayload}, all JSON — {@code JobSuccessReply}'s
 * {@code contentType} changed from {@code application/octet-stream} to {@code application/json}
 * to carry it). {@link #classify(Message)} reads ONLY this field; a missing or unrecognized value
 * returns {@link Optional#empty()}, which {@link A2CompletionBridge} routes to the DLQ as
 * {@code VAL_INVALID_REPLY_TYPE} ({@code DlqReason#INVALID_REPLY_TYPE}) instead of guessing.
 *
 * <p><b>Top-level-only JSON parsing (post-review follow-up fix F-1, decision
 * 2026-07-15):</b> field extraction used to be a depth-unaware string search
 * ({@code json.indexOf("\"" + fieldName + "\"")}) that could match a same-named key nested
 * inside an object-valued field (asyncapi permits nested objects, e.g. {@code BpmnErrorPayload
 * .variables}, {@code additionalProperties: true}) — a payload shaped like
 * {@code {"data":{"type":"BPMN_ERROR"},"type":"SUCCESS"}} could misread the nested {@code type}
 * as the wire-critical top-level discriminator. Field extraction now parses the body into a
 * Jackson {@link JsonNode} tree once per call and reads ONLY direct children of the root object
 * ({@link JsonNode#get(String)} never descends into nested objects/arrays), so a same-named key
 * nested inside any field can no longer shadow the top-level value. Malformed or non-object JSON
 * still degrades to {@code null}/defaults rather than throwing — this class never throws;
 * callers route missing/invalid data to the DLQ instead of receiving an exception.
 */
final class A2ReplyPayloadDecoder {

    private static final String TYPE_FIELD = "type";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private A2ReplyPayloadDecoder() {
    }

    /** @return the reply's declared type, or {@link Optional#empty()} if missing/unrecognized (routes to DLQ). */
    static Optional<ReplyType> classify(Message msg) {
        String typeValue = topLevelField(bodyAsString(msg), TYPE_FIELD);
        if (typeValue == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(ReplyType.valueOf(typeValue.trim()));
        } catch (IllegalArgumentException unknownType) {
            return Optional.empty();
        }
    }

    /**
     * Full structured variable-map deserialization is tenant-defined (asyncapi
     * {@code OpaqueBusinessPayload}) and out of this repo's contract — the raw payload is passed
     * through under a single {@code natsPayload} variable, consistent with the existing
     * {@code JetStreamMessageCorrelationSubscriber} convention.
     *
     * <p>Under {@link A2Properties.ReplyPayloadVariable#WHEN_PRESENT} (the default) a body whose
     * only top-level field is {@code type} yields no variable at all: the discriminator is this
     * layer's own routing signal, not a worker result, so persisting it buys the process nothing
     * and costs a variable row plus a history row per completion. Any additional field — however
     * this layer fails to understand it — means the worker returned something, and the whole body
     * is passed through unchanged, {@code type} included.
     */
    /**
     * The variables a reply asks the engine to write, split by scope.
     *
     * <p>{@code structured} distinguishes the two contracts this decoder now speaks. {@code false}
     * = the legacy opaque contract: {@code variables} holds at most the single {@code natsPayload}
     * passthrough entry, governed by {@link A2Properties.ReplyPayloadVariable}. {@code true} = the
     * engine-parity contract (reply carries a top-level {@code variables} and/or
     * {@code localVariables} object): entries are named, typed engine variables, and the opaque
     * passthrough does not run — a worker that speaks the structured contract has said exactly
     * what it wants written, and writing {@code natsPayload} next to it would double-store the
     * same bytes.
     */
    record ReplyVariables(Map<String, Object> variables, Map<String, Object> localVariables,
            boolean structured) {
    }

    /**
     * Engine-parity variable decoding (reply-variables increment, decision 2026-08-19).
     *
     * <p>The value format mirrors the engine's own REST wire shape ({@code CompleteExternalTaskDto}
     * / {@code VariableValueDto}): each entry is {@code name -> {value, type?, valueInfo?}}. A
     * missing {@code type} yields an untyped scalar the engine resolves itself, exactly as the
     * REST API does. Supported types: {@code String, Boolean, Integer, Long, Short, Double, Date}
     * (ISO-8601), {@code Null, Bytes} (base64), {@code Object} (pre-serialized, with
     * {@code valueInfo.serializationDataFormat}/{@code objectTypeName}). {@code File/Json/Xml} are
     * NOT supported — File's REST semantics are multipart, and Json/Xml are Spin types this module
     * does not depend on; a reply naming them is invalid rather than silently coerced.
     *
     * <p>This method throws where the rest of the class deliberately does not, because the failure
     * modes differ: a malformed {@code errorMessage} degrades a log line, a malformed variable
     * changes what the process does next. The bridge routes the exception to the DLQ as
     * {@code VAL_INVALID_REPLY_VARIABLES}.
     */
    static ReplyVariables replyVariablesOf(Message msg, A2Properties.ReplyPayloadVariable mode)
            throws InvalidReplyVariablesException {
        String body = bodyAsString(msg);
        JsonNode root = parsedObjectOrNull(body);
        boolean structured = root != null && (root.has("variables") || root.has("localVariables"));
        if (!structured) {
            return new ReplyVariables(variablesOf(msg, mode), Map.of(), false);
        }
        return new ReplyVariables(
                convertVariables(root.get("variables"), "variables"),
                convertVariables(root.get("localVariables"), "localVariables"),
                true);
    }

    private static Map<String, Object> convertVariables(JsonNode node, String fieldName)
            throws InvalidReplyVariablesException {
        if (node == null || node.isNull()) {
            return Map.of();
        }
        if (!node.isObject()) {
            throw new InvalidReplyVariablesException(
                    "'" + fieldName + "' must be a JSON object, got " + node.getNodeType());
        }
        Map<String, Object> variables = new LinkedHashMap<>();
        for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = it.next();
            variables.put(entry.getKey(), convertValue(entry.getKey(), entry.getValue(), fieldName));
        }
        return variables;
    }

    private static Object convertValue(String name, JsonNode dto, String fieldName)
            throws InvalidReplyVariablesException {
        if (dto == null || !dto.isObject()) {
            throw new InvalidReplyVariablesException("Variable '" + name + "' in '" + fieldName
                    + "' must be an object shaped {value, type?, valueInfo?}");
        }
        JsonNode valueNode = dto.get("value");
        JsonNode typeNode = dto.get("type");
        boolean isTransient = dto.path("valueInfo").path("transient").asBoolean(false);
        String type = typeNode != null && typeNode.isTextual() ? typeNode.asText() : null;

        try {
            if (type == null) {
                return untypedScalar(name, valueNode, isTransient);
            }
            boolean valueIsNull = valueNode == null || valueNode.isNull();
            return switch (type) {
                case "String" -> Variables.stringValue(valueIsNull ? null : requireText(name, valueNode),
                        isTransient);
                case "Boolean" -> Variables.booleanValue(valueIsNull ? null
                        : requireBoolean(name, valueNode), isTransient);
                case "Integer" -> Variables.integerValue(valueIsNull ? null
                        : requireNumber(name, valueNode).intValue(), isTransient);
                case "Long" -> Variables.longValue(valueIsNull ? null
                        : requireNumber(name, valueNode).longValue(), isTransient);
                case "Short" -> Variables.shortValue(valueIsNull ? null
                        : requireNumber(name, valueNode).shortValue(), isTransient);
                case "Double" -> Variables.doubleValue(valueIsNull ? null
                        : requireNumber(name, valueNode).doubleValue(), isTransient);
                case "Date" -> Variables.dateValue(valueIsNull ? null
                        : Date.from(Instant.parse(requireText(name, valueNode))), isTransient);
                case "Null" -> Variables.untypedNullValue(isTransient);
                case "Bytes" -> Variables.byteArrayValue(valueIsNull ? null
                        : Base64.getDecoder().decode(requireText(name, valueNode)), isTransient);
                case "Object" -> serializedObject(name, valueNode, dto.path("valueInfo"), isTransient);
                default -> throw new InvalidReplyVariablesException("Variable '" + name
                        + "' has unsupported type '" + type
                        + "' (supported: String, Boolean, Integer, Long, Short, Double, Date, Null,"
                        + " Bytes, Object)");
            };
        } catch (IllegalArgumentException | DateTimeParseException badValue) {
            throw new InvalidReplyVariablesException(
                    "Variable '" + name + "' value cannot be decoded as '" + type + "'", badValue);
        }
    }

    /**
     * No declared type: only JSON scalars are accepted, mapped to the natural Java type and left
     * for the engine to resolve — the same contract as the REST API's typeless
     * {@code VariableValueDto}. A container value without a type is refused rather than guessed:
     * an untyped Map would head into Java serialization, which no caller can have meant.
     */
    private static Object untypedScalar(String name, JsonNode valueNode, boolean isTransient)
            throws InvalidReplyVariablesException {
        if (valueNode == null || valueNode.isNull()) {
            return Variables.untypedNullValue(isTransient);
        }
        if (valueNode.isContainerNode()) {
            throw new InvalidReplyVariablesException("Variable '" + name
                    + "' has an object/array value but no type — declare type Object with"
                    + " valueInfo.serializationDataFormat, or send a scalar");
        }
        Object scalar;
        if (valueNode.isTextual()) {
            scalar = valueNode.asText();
        } else if (valueNode.isBoolean()) {
            scalar = valueNode.asBoolean();
        } else if (valueNode.isIntegralNumber()) {
            scalar = valueNode.canConvertToInt() ? (Object) valueNode.intValue()
                    : (Object) valueNode.longValue();
        } else {
            scalar = valueNode.doubleValue();
        }
        return Variables.untypedValue(scalar, isTransient);
    }

    private static Object serializedObject(String name, JsonNode valueNode, JsonNode valueInfo,
            boolean isTransient) throws InvalidReplyVariablesException {
        if (valueNode == null || !valueNode.isTextual()) {
            throw new InvalidReplyVariablesException("Variable '" + name
                    + "' of type Object must carry its serialized form as a string value");
        }
        String dataFormat = valueInfo.path("serializationDataFormat").asText(null);
        if (dataFormat == null) {
            throw new InvalidReplyVariablesException("Variable '" + name
                    + "' of type Object requires valueInfo.serializationDataFormat");
        }
        // setTransient last: it narrows the builder to TypedValueBuilder, which no longer knows
        // the SerializedObjectValueBuilder-only methods like objectTypeName.
        var builder = Variables.serializedObjectValue(valueNode.asText())
                .serializationDataFormat(dataFormat);
        String objectTypeName = valueInfo.path("objectTypeName").asText(null);
        if (objectTypeName != null) {
            builder = builder.objectTypeName(objectTypeName);
        }
        return builder.setTransient(isTransient).create();
    }

    private static String requireText(String name, JsonNode node) throws InvalidReplyVariablesException {
        if (!node.isTextual()) {
            throw new InvalidReplyVariablesException("Variable '" + name + "' value must be a string");
        }
        return node.asText();
    }

    private static Boolean requireBoolean(String name, JsonNode node) throws InvalidReplyVariablesException {
        if (!node.isBoolean()) {
            throw new InvalidReplyVariablesException("Variable '" + name + "' value must be a boolean");
        }
        return node.asBoolean();
    }

    private static JsonNode requireNumber(String name, JsonNode node) throws InvalidReplyVariablesException {
        if (!node.isNumber()) {
            throw new InvalidReplyVariablesException("Variable '" + name + "' value must be a number");
        }
        return node;
    }

    /** @return the body parsed as a JSON object, or {@code null} if blank/malformed/non-object. */
    private static JsonNode parsedObjectOrNull(String body) {
        if (body == null || body.isBlank()) {
            return null;
        }
        try {
            JsonNode root = OBJECT_MAPPER.readTree(body);
            return root != null && root.isObject() ? root : null;
        } catch (JsonProcessingException malformed) {
            return null;
        }
    }

    static Map<String, Object> variablesOf(Message msg, A2Properties.ReplyPayloadVariable mode) {
        Map<String, Object> variables = new HashMap<>();
        if (mode == A2Properties.ReplyPayloadVariable.NEVER) {
            return variables;
        }
        String body = bodyAsString(msg);
        if (mode == A2Properties.ReplyPayloadVariable.WHEN_PRESENT && carriesOnlyDiscriminator(body)) {
            return variables;
        }
        variables.put("natsPayload", body);
        return variables;
    }

    /**
     * True when the body parses as an object whose single field is {@code type}. A body that does
     * not parse, or is not an object, is NOT treated as discriminator-only — this method must
     * never be the reason a worker's result is dropped, so anything it cannot positively identify
     * as empty is passed through.
     */
    private static boolean carriesOnlyDiscriminator(String body) {
        if (body == null || body.isBlank()) {
            return false;
        }
        JsonNode root;
        try {
            root = OBJECT_MAPPER.readTree(body);
        } catch (JsonProcessingException malformed) {
            return false;
        }
        return root != null && root.isObject() && root.size() == 1 && root.has("type");
    }

    static String errorCodeOf(Message msg) {
        return topLevelField(bodyAsString(msg), "errorCode");
    }

    static String errorMessageOf(Message msg) {
        return topLevelField(bodyAsString(msg), "errorMessage");
    }

    static String errorDetailsOf(Message msg) {
        return topLevelField(bodyAsString(msg), "errorDetails");
    }

    static int retriesOf(Message msg) {
        String retries = topLevelField(bodyAsString(msg), "retries");
        try {
            return retries != null ? Integer.parseInt(retries) : 0;
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static String bodyAsString(Message msg) {
        byte[] data = msg.getData();
        return data != null ? new String(data, StandardCharsets.UTF_8) : "";
    }

    /**
     * Reads {@code fieldName} from the JSON root object ONLY — {@link JsonNode#get(String)}
     * looks up a direct child of the root, never descending into nested objects/arrays, so a
     * same-named key inside a nested value (e.g. {@code variables.type}) cannot shadow the
     * top-level field. Returns {@code null} for a missing field, an explicit JSON {@code null},
     * a non-object body, or malformed JSON — this method never throws.
     */
    private static String topLevelField(String json, String fieldName) {
        if (json == null || json.isBlank()) {
            return null;
        }
        JsonNode root;
        try {
            root = OBJECT_MAPPER.readTree(json);
        } catch (JsonProcessingException malformed) {
            return null;
        }
        if (root == null || !root.isObject()) {
            return null;
        }
        JsonNode value = root.get(fieldName);
        if (value == null || value.isNull()) {
            return null;
        }
        return value.isContainerNode() ? value.toString() : value.asText();
    }
}
