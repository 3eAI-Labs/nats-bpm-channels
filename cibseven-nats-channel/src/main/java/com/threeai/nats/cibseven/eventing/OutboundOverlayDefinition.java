package com.threeai.nats.cibseven.eventing;

import java.util.List;

/**
 * Outbound partial parity (docs/12 D-D v2): the definition carries ONLY classification +
 * allowlist. The Flowable-mandatory {@code subject} is read but never applied on the Camunda
 * lineage — subject grammar stays engine-governed ({@code OutboundSubjectBuilder}) — and the
 * parser WARNs once per definition about it. Overlay semantics: definitions overlay the YAML
 * ({@code spring.nats.outbound.*}) PER message-type key; for a covered key the definition fully
 * defines the classification ({@code critical}, default false = best-effort) and the variable
 * allowlist (default empty = identity-only envelope); when the definition disappears the key
 * reverts to YAML.
 */
public record OutboundOverlayDefinition(
        String key,
        String messageType,
        boolean critical,
        List<String> variableAllowlist) implements ChannelResource {

    public OutboundOverlayDefinition {
        variableAllowlist = List.copyOf(variableAllowlist);
    }
}
