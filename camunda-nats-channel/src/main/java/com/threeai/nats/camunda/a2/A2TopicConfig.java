package com.threeai.nats.camunda.a2;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Thin wrapper exposing the configured A2-topic set as an immutable {@link Set} — shared by
 * {@link A2BpmnParseListener} (parse-time swap decision) and {@link A2OrphanSweep}
 * (fetchable-parity query filter), LLD 03_classes/2_camunda_a2.md §3.1.
 *
 * <p>Also exposes the per-topic {@code variableAllowlist} (Sentinel Phase 5.5 QA fix, item 5) —
 * {@link A2BpmnParseListener} resolves it once at parse-time (same pattern as
 * {@link UmbrellaLockResolver#resolveMillis}) and hands it to {@link A2ExternalTaskBehavior}.
 */
public class A2TopicConfig {

    private final Set<String> a2Topics;
    private final Map<String, List<String>> variableAllowlist;

    public A2TopicConfig(A2Properties properties) {
        this.a2Topics = Set.copyOf(new LinkedHashSet<>(properties.getTopics()));
        this.variableAllowlist = Map.copyOf(new HashMap<>(properties.getVariableAllowlist()));
    }

    public Set<String> a2Topics() {
        return a2Topics;
    }

    public boolean isA2Topic(String topic) {
        return a2Topics.contains(topic);
    }

    /** @return the configured allowlist for {@code topic}, or an empty list (default — no capture) if unconfigured. */
    public List<String> variableAllowlistFor(String topic) {
        return variableAllowlist.getOrDefault(topic, List.of());
    }
}
