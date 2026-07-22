package com.threeai.nats.core.outbound;

import java.util.regex.Pattern;

import com.threeai.nats.core.exception.InvalidOutboundMessageTypeException;

/**
 * Builds the basamak-4 outbound-handoff subject scheme (docs/09-outbound-handoff.md D-E'):
 * {@code events.<engineId>.<type>.<processInstanceId>} — a direct projection of the basamak-2
 * history subject shape ({@code history.<engineId>.<class>.<processInstanceId>}), instance-keyed
 * so all outbound messages for the SAME process instance land on the SAME subject (sequence
 * preserved). {@code events.*} is reserved against tenant-defined channel collisions by
 * {@link com.threeai.nats.core.config.NamespaceValidator#assertNotReservedForOutbound}.
 *
 * <p><b>Phase-review FINDING-003 (MINOR — robustness):</b> {@code messageType} is tenant-defined,
 * free-form input, and it is embedded as a single subject TOKEN — a literal {@code .} would add
 * an extra subject hierarchy level, and {@code *}/{@code >}/whitespace are NATS wildcard/reserved
 * characters — any of these would silently produce a malformed or wildcard-colliding subject
 * rather than failing loudly. {@code engineId} (fixed, code-controlled constant) and {@code
 * processInstanceId} (engine-generated) are NOT tenant free-form input and are therefore not
 * subject to this check.
 */
public final class OutboundSubjectBuilder {

    private static final String PREFIX = "events.";

    /** Letters, digits, underscore, hyphen only — excludes NATS subject-reserved '.'/'*'/'>' and whitespace. */
    private static final Pattern SAFE_MESSAGE_TYPE = Pattern.compile("^[A-Za-z0-9_-]+$");

    private OutboundSubjectBuilder() {
    }

    public static String build(String engineId, String messageType, String processInstanceId) {
        requireNonBlank(engineId, "engineId");
        requireNonBlank(messageType, "messageType");
        requireNonBlank(processInstanceId, "processInstanceId");
        requireSafeMessageType(messageType);
        return PREFIX + engineId + "." + messageType + "." + processInstanceId;
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank when building an outbound-handoff subject");
        }
    }

    private static void requireSafeMessageType(String messageType) {
        if (!SAFE_MESSAGE_TYPE.matcher(messageType).matches()) {
            throw new InvalidOutboundMessageTypeException(messageType);
        }
    }
}
