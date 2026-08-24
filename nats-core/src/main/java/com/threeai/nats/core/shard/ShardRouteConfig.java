package com.threeai.nats.core.shard;

/**
 * One routed external-inbound subject (docs/13 D-C v5). {@code businessKeyField} is the
 * optional top-level payload fallback; the {@code X-Cadenzaflow-Business-Key} header always
 * wins. The durable/queue-group name is derived from the subject (docs/11 rule: durable ==
 * deliverGroup, both always set).
 */
public record ShardRouteConfig(String subject, String businessKeyField) {

    public ShardRouteConfig {
        if (subject == null || subject.isBlank()) {
            throw new IllegalArgumentException("[VAL_SHARD_CONFIG] route subject must be set");
        }
    }

    /** Durable-safe name: {@code shard-router-<subject with unsafe chars dashed>}. */
    public String durableName() {
        return "shard-router-" + subject.replaceAll("[^A-Za-z0-9_-]", "-");
    }
}
