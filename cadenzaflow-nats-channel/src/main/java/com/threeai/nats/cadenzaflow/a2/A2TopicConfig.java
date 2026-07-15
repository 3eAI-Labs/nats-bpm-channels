package com.threeai.nats.cadenzaflow.a2;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Thin wrapper exposing the configured A2-topic set as an immutable {@link Set} — shared by
 * {@link A2BpmnParseListener} (parse-time swap decision) and {@link A2OrphanSweep}
 * (fetchable-parity query filter), LLD 03_classes/2_camunda_a2.md §3.1.
 */
public class A2TopicConfig {

    private final Set<String> a2Topics;

    public A2TopicConfig(A2Properties properties) {
        this.a2Topics = Set.copyOf(new LinkedHashSet<>(properties.getTopics()));
    }

    public Set<String> a2Topics() {
        return a2Topics;
    }

    public boolean isA2Topic(String topic) {
        return a2Topics.contains(topic);
    }
}
