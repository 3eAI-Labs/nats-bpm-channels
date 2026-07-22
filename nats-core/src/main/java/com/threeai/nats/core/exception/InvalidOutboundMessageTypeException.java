package com.threeai.nats.core.exception;

/**
 * Thrown by {@link com.threeai.nats.core.outbound.OutboundSubjectBuilder} when a tenant-supplied
 * outbound message type is not safe to embed as a single NATS subject token
 * (docs/09-outbound-handoff.md D-E', phase-review FINDING-003). {@code messageType} is the ONLY
 * subject segment that is tenant/free-form input — {@code engineId} is a fixed, code-controlled
 * constant and {@code processInstanceId} is engine-generated — so this is the only segment that
 * needs runtime validation before {@code events.<engineId>.<type>.<processInstanceId>} is built.
 *
 * <p>A message type containing {@code .} (extra subject hierarchy level), {@code *}/{@code >}
 * (NATS wildcard tokens), or whitespace would otherwise silently produce a malformed or
 * wildcard-colliding subject rather than failing loudly — this exception makes that failure mode
 * explicit and controlled (VAL_OUTBOUND_MESSAGE_TYPE_INVALID) instead.
 */
public class InvalidOutboundMessageTypeException extends RuntimeException {

    private static final String CODE = "VAL_OUTBOUND_MESSAGE_TYPE_INVALID";

    private final String messageType;

    public InvalidOutboundMessageTypeException(String messageType) {
        super("Outbound message type '" + messageType + "' is not a safe NATS subject token "
                + "(allowed: letters, digits, '_', '-' only; no '.', '*', '>', or whitespace) (" + CODE + ")");
        this.messageType = messageType;
    }

    public String getCode() {
        return CODE;
    }

    public String getMessageType() {
        return messageType;
    }
}
