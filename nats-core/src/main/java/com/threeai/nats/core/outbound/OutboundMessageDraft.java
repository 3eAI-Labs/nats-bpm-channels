package com.threeai.nats.core.outbound;

/**
 * Fully-resolved outbound message, ready either for direct publish (best-effort path) or for
 * persistence into {@code outbound_message_outbox} (critical path). {@code payload} is the
 * already-serialized wire body ({@link OutboundWireMessageFactory#buildPayload}) — built ONCE at
 * classification time so the critical path's tx-in write and the eventual relay publish never
 * re-derive it from process-variable state.
 */
public record OutboundMessageDraft(
        String engineId,
        String messageType,
        String processInstanceId,
        String businessKey,
        String traceId,
        String subject,
        byte[] payload) {
}
