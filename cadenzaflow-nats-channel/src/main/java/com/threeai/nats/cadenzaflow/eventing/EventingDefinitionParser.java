package com.threeai.nats.cadenzaflow.eventing;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.threeai.nats.core.config.NamespaceValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Strict parser for the portable {@code .event}/{@code .channel} JSON subset (docs/12 D-A v2).
 *
 * <p>Rejection over silent degradation (the repo rule: "a setting that is accepted and
 * discarded is worse than one that does not exist"): unsupported key-detection forms REJECT
 * ({@code VAL_EVENTING_UNSUPPORTED_FEATURE}); a portable channel without {@code queueGroup}
 * REJECTS ({@code VAL_EVENTING_QUEUE_GROUP_REQUIRED}, F-6); reserved subject roots REJECT
 * through {@link NamespaceValidator} (D-F v2). Known-but-unapplied fields ({@code subject}'s
 * outbound twin, {@code deliverPolicy}, {@code ackWaitSeconds}, {@code isFullPayload},
 * {@code metaParameter}, header payloads) WARN once per definition and are ignored — same rule
 * the doc sets for Flowable's outbound {@code subject}.
 *
 * <p>Lineage-only knobs live under {@code extension} (Flowable's official escape hatch — its
 * strict channel databind rejects unknown top-level fields): {@code extension.messageName}.
 */
public final class EventingDefinitionParser {

    private static final Logger log = LoggerFactory.getLogger(EventingDefinitionParser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern SAFE_TOKEN = Pattern.compile("^[A-Za-z0-9_-]+$");
    private static final List<String> IGNORED_CHANNEL_FIELDS =
            List.of("deliverPolicy", "ackWaitSeconds");
    private static final List<String> IGNORED_EVENT_PAYLOAD_FLAGS =
            List.of("header", "isFullPayload", "metaParameter");

    private EventingDefinitionParser() {
    }

    public static EventDefinition parseEvent(String resourceName, String json) {
        JsonNode root = readObject(resourceName, json);
        String key = requiredText(root, "key", resourceName);
        assertSafeToken(key, "event key", resourceName);

        Map<String, String> correlation = new LinkedHashMap<>();
        readTypedParams(root.get("correlationParameters"), correlation, resourceName, "correlationParameters");
        Map<String, String> payload = new LinkedHashMap<>();
        readTypedParams(root.get("payload"), payload, resourceName, "payload");

        String messageName = null;
        JsonNode extension = root.get("extension");
        if (extension != null && extension.hasNonNull("messageName")) {
            messageName = extension.get("messageName").asText();
        }
        return new EventDefinition(key,
                root.hasNonNull("name") ? root.get("name").asText() : key,
                Map.copyOf(correlation), Map.copyOf(payload), messageName);
    }

    public static ChannelResource parseChannel(String resourceName, String json) {
        JsonNode root = readObject(resourceName, json);
        String key = requiredText(root, "key", resourceName);
        assertSafeToken(key, "channel key", resourceName);

        String channelType = requiredText(root, "channelType", resourceName);
        if ("outbound".equals(channelType)) {
            return parseOutboundChannel(resourceName, root, key);
        }
        if (!"inbound".equals(channelType)) {
            throw new EventingDefinitionException(EventingDefinitionException.CODE_UNSUPPORTED,
                    resourceName + ": channelType '" + channelType
                            + "' — supported: inbound, outbound");
        }
        String type = requiredText(root, "type", resourceName);
        if (!"nats".equals(type)) {
            throw new EventingDefinitionException(EventingDefinitionException.CODE_UNSUPPORTED,
                    resourceName + ": channel type '" + type + "' is not this adapter's");
        }
        if (root.has("deliverGroup")) {
            throw new EventingDefinitionException(EventingDefinitionException.CODE_INVALID,
                    resourceName + ": 'deliverGroup' is forbidden in portable files — use"
                            + " 'queueGroup' (the shared field name; docs/12 D-A v2)");
        }

        String subject = requiredText(root, "subject", resourceName);
        NamespaceValidator.assertNotReservedForA2(subject, resourceName);
        NamespaceValidator.assertNotReservedForOutbound(subject, resourceName);
        NamespaceValidator.assertNotReservedForExternalWorker(subject, resourceName);

        JsonNode detection = root.get("channelEventKeyDetection");
        if (detection == null || !detection.isObject()) {
            throw new EventingDefinitionException(EventingDefinitionException.CODE_INVALID,
                    resourceName + ": channelEventKeyDetection.fixedValue is required (v1: one"
                            + " subject carries one event type)");
        }
        for (String unsupported : List.of("jsonField", "jsonPointerExpression",
                "xmlXPathExpression", "delegateExpression")) {
            if (detection.has(unsupported)) {
                throw new EventingDefinitionException(EventingDefinitionException.CODE_UNSUPPORTED,
                        resourceName + ": channelEventKeyDetection." + unsupported
                                + " — v1 supports fixedValue only");
            }
        }
        String eventKey = detection.hasNonNull("fixedValue") ? detection.get("fixedValue").asText() : null;
        if (eventKey == null || eventKey.isEmpty()) {
            throw new EventingDefinitionException(EventingDefinitionException.CODE_INVALID,
                    resourceName + ": channelEventKeyDetection.fixedValue is required");
        }

        String queueGroup = root.hasNonNull("queueGroup") ? root.get("queueGroup").asText() : null;
        if (queueGroup == null || queueGroup.isEmpty()) {
            throw new EventingDefinitionException(
                    EventingDefinitionException.CODE_QUEUE_GROUP_REQUIRED,
                    resourceName + ": queueGroup is mandatory in portable channel files —"
                            + " Flowable has no default (null means per-node duplicate delivery"
                            + " there); migrating from YAML? use correlation-<messageName>");
        }

        for (String ignored : IGNORED_CHANNEL_FIELDS) {
            if (root.has(ignored)) {
                log.warn("Channel definition field is read but not applied on the Camunda lineage",
                        kv("resource", resourceName), kv("field", ignored));
            }
        }

        return new ChannelDefinition(key, subject, eventKey, queueGroup,
                root.path("jetstream").asBoolean(false),
                root.hasNonNull("durableName") ? root.get("durableName").asText() : null,
                root.hasNonNull("maxDeliver") ? root.get("maxDeliver").asInt() : 5,
                root.hasNonNull("dlqSubject") ? root.get("dlqSubject").asText() : null,
                root.path("autoCreateStream").asBoolean(false),
                root.hasNonNull("streamName") ? root.get("streamName").asText() : null);
    }

    /**
     * Outbound channel (docs/12 D-D v2, partial parity): carries ONLY classification +
     * allowlist, under {@code extension} (the official escape hatch — these are lineage
     * concepts Flowable's format does not model). {@code subject} is Flowable-mandatory but
     * engine-governed here: read, WARNed once, never applied. {@code extension.messageType}
     * is REQUIRED — without it the definition would be completely inert on the lineage, and
     * the repo rule is rejection over silent discard.
     */
    private static OutboundOverlayDefinition parseOutboundChannel(String resourceName,
            JsonNode root, String key) {
        String type = requiredText(root, "type", resourceName);
        if (!"nats".equals(type)) {
            throw new EventingDefinitionException(EventingDefinitionException.CODE_UNSUPPORTED,
                    resourceName + ": channel type '" + type + "' is not this adapter's");
        }
        if (root.hasNonNull("subject")) {
            log.warn("Outbound channel 'subject' is engine-governed on the Camunda lineage —"
                            + " read but not applied (docs/12 D-D v2)",
                    kv("resource", resourceName), kv("subject", root.get("subject").asText()));
        }
        if (root.has("serializerType")) {
            log.warn("Channel definition field is read but not applied on the Camunda lineage",
                    kv("resource", resourceName), kv("field", "serializerType"));
        }

        JsonNode extension = root.get("extension");
        String messageType = extension != null && extension.hasNonNull("messageType")
                ? extension.get("messageType").asText() : null;
        if (messageType == null || messageType.isEmpty()) {
            throw new EventingDefinitionException(EventingDefinitionException.CODE_INVALID,
                    resourceName + ": extension.messageType is required on outbound channels —"
                            + " without it the definition carries nothing the lineage applies"
                            + " (subject is engine-governed; docs/12 D-D v2)");
        }

        boolean critical = extension.path("critical").asBoolean(false);
        List<String> allowlist = new java.util.ArrayList<>();
        JsonNode allowlistNode = extension.get("variableAllowlist");
        if (allowlistNode != null && !allowlistNode.isNull()) {
            if (!allowlistNode.isArray()) {
                throw new EventingDefinitionException(EventingDefinitionException.CODE_INVALID,
                        resourceName + ": extension.variableAllowlist must be an array of"
                                + " variable names");
            }
            for (JsonNode entry : allowlistNode) {
                allowlist.add(entry.asText());
            }
        }
        return new OutboundOverlayDefinition(key, messageType, critical, allowlist);
    }

    private static void readTypedParams(JsonNode array, Map<String, String> target,
            String resourceName, String field) {
        if (array == null || array.isNull()) {
            return;
        }
        if (!array.isArray()) {
            throw new EventingDefinitionException(EventingDefinitionException.CODE_INVALID,
                    resourceName + ": '" + field + "' must be an array");
        }
        for (JsonNode entry : array) {
            String name = entry.hasNonNull("name") ? entry.get("name").asText() : null;
            String type = entry.hasNonNull("type") ? entry.get("type").asText() : null;
            if (name == null || type == null) {
                throw new EventingDefinitionException(EventingDefinitionException.CODE_INVALID,
                        resourceName + ": every " + field + " entry needs name and type");
            }
            if (!EventDefinition.TYPE_DICTIONARY.contains(type)) {
                throw new EventingDefinitionException(EventingDefinitionException.CODE_UNSUPPORTED,
                        resourceName + ": " + field + " type '" + type
                                + "' is outside the EventPayloadTypes dictionary "
                                + EventDefinition.TYPE_DICTIONARY);
            }
            for (String flag : IGNORED_EVENT_PAYLOAD_FLAGS) {
                if (entry.has(flag)) {
                    log.warn("Event payload flag is read but not applied on the Camunda lineage",
                            kv("resource", resourceName), kv("flag", flag));
                }
            }
            target.put(name, type);
        }
    }

    private static JsonNode readObject(String resourceName, String json) {
        try {
            JsonNode root = MAPPER.readTree(json);
            if (root == null || !root.isObject()) {
                throw new EventingDefinitionException(EventingDefinitionException.CODE_INVALID,
                        resourceName + ": not a JSON object");
            }
            return root;
        } catch (EventingDefinitionException e) {
            throw e;
        } catch (Exception e) {
            throw new EventingDefinitionException(EventingDefinitionException.CODE_INVALID,
                    resourceName + ": unreadable JSON — " + e.getMessage());
        }
    }

    private static String requiredText(JsonNode root, String field, String resourceName) {
        JsonNode n = root.get(field);
        if (n == null || !n.isTextual() || n.asText().isEmpty()) {
            throw new EventingDefinitionException(EventingDefinitionException.CODE_INVALID,
                    resourceName + ": '" + field + "' is required");
        }
        return n.asText();
    }

    private static void assertSafeToken(String value, String what, String resourceName) {
        if (!SAFE_TOKEN.matcher(value).matches()) {
            throw new EventingDefinitionException(EventingDefinitionException.CODE_INVALID,
                    resourceName + ": " + what + " '" + value + "' is not a safe token"
                            + " (^[A-Za-z0-9_-]+$)");
        }
    }
}
