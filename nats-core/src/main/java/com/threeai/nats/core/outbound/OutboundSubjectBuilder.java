package com.threeai.nats.core.outbound;

/**
 * Builds the basamak-4 outbound-handoff subject scheme (docs/09-outbound-handoff.md D-E'):
 * {@code events.<engineId>.<type>.<processInstanceId>} — a direct projection of the basamak-2
 * history subject shape ({@code history.<engineId>.<class>.<processInstanceId>}), instance-keyed
 * so all outbound messages for the SAME process instance land on the SAME subject (sequence
 * preserved). {@code events.*} is reserved against tenant-defined channel collisions by
 * {@link com.threeai.nats.core.config.NamespaceValidator#assertNotReservedForOutbound}.
 */
public final class OutboundSubjectBuilder {

    private static final String PREFIX = "events.";

    private OutboundSubjectBuilder() {
    }

    public static String build(String engineId, String messageType, String processInstanceId) {
        requireNonBlank(engineId, "engineId");
        requireNonBlank(messageType, "messageType");
        requireNonBlank(processInstanceId, "processInstanceId");
        return PREFIX + engineId + "." + messageType + "." + processInstanceId;
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank when building an outbound-handoff subject");
        }
    }
}
