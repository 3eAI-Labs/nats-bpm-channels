package com.threeai.nats.flowable.externalworker;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Base64;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.threeai.nats.core.dlq.DlqReason;
import io.nats.client.Message;

/**
 * Decodes an {@code ewjobs.<topic>.reply} payload against the D-C'v2 matrix (docs/11).
 *
 * <p>Same reply grammar as A2 ({@code type} discriminator, top-level-only field reads so a
 * nested duplicate can never shadow it, engine-REST value shape
 * {@code name -> {value, type?, valueInfo?}}) — with the Flowable 7.1.0 capability matrix
 * enforced here:
 * <ul>
 *   <li>{@code localVariables} on ANY reply → {@link DlqReason#UNSUPPORTED_REPLY_SHAPE} (the
 *       completion API has no local-variables parameter at all);</li>
 *   <li>{@code BPMN_ERROR} requires {@code errorCode}; {@code errorMessage} is DEGRADED into the
 *       well-known {@code natsErrorMessage} variable (the builder takes only the code — silent
 *       drop would contradict the A2 rationale);</li>
 *   <li>{@code TRANSIENT} accepts {@code variables} but the bridge WARNS and drops them (the
 *       failure builder has no variables parameter; the failure path routes no gateway).</li>
 * </ul>
 *
 * <p>Identity comes from the envelope {@code jobId} field — NOT from {@code Nats-Msg-Id}, which
 * is the composite generation-scoped dedup key (decision NEW-2).
 */
final class EwReplyDecoder {

    /** Degrade target for BPMN_ERROR errorMessage (D-C'v2). */
    static final String ERROR_MESSAGE_VARIABLE = "natsErrorMessage";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private EwReplyDecoder() {
    }

    record DecodedReply(EwReplyType type, String jobId, Map<String, Object> variables,
            String errorCode, String errorMessage, String errorDetails, int retries,
            boolean transientVariablesDropped) {
    }

    static DecodedReply decode(Message msg) throws InvalidEwReplyException {
        JsonNode root;
        try {
            root = MAPPER.readTree(new String(msg.getData(), StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new InvalidEwReplyException(DlqReason.INVALID_REPLY_TYPE, "reply body is not JSON");
        }
        if (root == null || !root.isObject()) {
            throw new InvalidEwReplyException(DlqReason.INVALID_REPLY_TYPE, "reply body is not a JSON object");
        }

        EwReplyType type = classify(root);

        if (root.has("localVariables")) {
            throw new InvalidEwReplyException(DlqReason.UNSUPPORTED_REPLY_SHAPE,
                    "localVariables cannot be expressed through the Flowable completion API");
        }

        String jobId = textOrNull(root, "jobId");
        Map<String, Object> variables = decodeVariables(root.get("variables"));

        String errorCode = textOrNull(root, "errorCode");
        String errorMessage = textOrNull(root, "errorMessage");
        String errorDetails = textOrNull(root, "errorDetails");
        int retries = root.hasNonNull("retries") && root.get("retries").canConvertToInt()
                ? root.get("retries").asInt() : 0;

        boolean transientVariablesDropped = false;
        switch (type) {
            case BPMN_ERROR -> {
                if (errorCode == null || errorCode.isEmpty()) {
                    throw new InvalidEwReplyException(DlqReason.INVALID_REPLY_TYPE,
                            "BPMN_ERROR reply is missing errorCode");
                }
                if (errorMessage != null && !errorMessage.isEmpty()) {
                    variables.put(ERROR_MESSAGE_VARIABLE, errorMessage);
                }
            }
            case TRANSIENT -> {
                if (!variables.isEmpty()) {
                    transientVariablesDropped = true;
                    variables.clear();
                }
            }
            case SUCCESS -> { /* variables pass through */ }
        }
        return new DecodedReply(type, jobId, variables, errorCode, errorMessage, errorDetails,
                retries, transientVariablesDropped);
    }

    /** Top-level-only read — a nested {"data":{"type":...}} can never shadow the discriminator. */
    private static EwReplyType classify(JsonNode root) throws InvalidEwReplyException {
        JsonNode typeNode = root.get("type");
        if (typeNode == null || !typeNode.isTextual()) {
            throw new InvalidEwReplyException(DlqReason.INVALID_REPLY_TYPE,
                    "mandatory top-level 'type' field is missing or not a string");
        }
        try {
            return EwReplyType.valueOf(typeNode.asText());
        } catch (IllegalArgumentException e) {
            throw new InvalidEwReplyException(DlqReason.INVALID_REPLY_TYPE,
                    "'type' must be SUCCESS|BPMN_ERROR|TRANSIENT, was: " + typeNode.asText());
        }
    }

    private static Map<String, Object> decodeVariables(JsonNode varsNode) throws InvalidEwReplyException {
        Map<String, Object> out = new LinkedHashMap<>();
        if (varsNode == null || varsNode.isNull()) {
            return out;
        }
        if (!varsNode.isObject()) {
            throw new InvalidEwReplyException(DlqReason.INVALID_REPLY_VARIABLES,
                    "'variables' is not a JSON object");
        }
        var fields = varsNode.fields();
        while (fields.hasNext()) {
            var entry = fields.next();
            JsonNode valueDto = entry.getValue();
            if (valueDto == null || !valueDto.isObject()) {
                throw new InvalidEwReplyException(DlqReason.INVALID_REPLY_VARIABLES,
                        "variable '" + entry.getKey() + "' is not a {value, type?} object");
            }
            out.put(entry.getKey(), decodeValue(entry.getKey(), valueDto));
        }
        return out;
    }

    private static Object decodeValue(String name, JsonNode dto) throws InvalidEwReplyException {
        JsonNode value = dto.get("value");
        JsonNode typeNode = dto.get("type");
        if (typeNode == null || typeNode.isNull()) {
            // Untyped: JSON-native scalars only (A2 parity — a container without a type is refused).
            if (value == null || value.isNull()) {
                return null;
            }
            if (value.isTextual()) {
                return value.asText();
            }
            if (value.isBoolean()) {
                return value.asBoolean();
            }
            if (value.isIntegralNumber()) {
                return value.canConvertToInt() ? (Object) value.asInt() : (Object) value.asLong();
            }
            if (value.isFloatingPointNumber()) {
                return value.asDouble();
            }
            throw new InvalidEwReplyException(DlqReason.INVALID_REPLY_VARIABLES,
                    "variable '" + name + "' has a container value but no type");
        }
        String type = typeNode.asText();
        try {
            return switch (type) {
                case "Null" -> null;
                case "String" -> value == null || value.isNull() ? null : value.asText();
                case "Boolean" -> value.asBoolean();
                case "Integer" -> value.asInt();
                case "Long" -> value.asLong();
                case "Short" -> (short) value.asInt();
                case "Double" -> value.asDouble();
                case "Date" -> parseIso(value.asText());
                case "Bytes" -> Base64.getDecoder().decode(value.asText());
                default -> throw new InvalidEwReplyException(DlqReason.INVALID_REPLY_VARIABLES,
                        "variable '" + name + "' has unsupported type '" + type
                                + "' (Object/File/Json/Xml are not expressible through the Flowable"
                                + " completion builder — demand-driven extension)");
            };
        } catch (InvalidEwReplyException e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidEwReplyException(DlqReason.INVALID_REPLY_VARIABLES,
                    "variable '" + name + "' value cannot be decoded as " + type);
        }
    }

    private static Date parseIso(String text) throws ParseException {
        return Date.from(java.time.OffsetDateTime.parse(text).toInstant());
    }

    private static String textOrNull(JsonNode root, String field) {
        JsonNode n = root.get(field);
        return n != null && n.isTextual() ? n.asText() : null;
    }
}
