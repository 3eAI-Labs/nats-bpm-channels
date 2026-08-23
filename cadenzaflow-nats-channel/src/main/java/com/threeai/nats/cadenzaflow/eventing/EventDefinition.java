package com.threeai.nats.cadenzaflow.eventing;

import java.util.List;
import java.util.Map;

/**
 * Parsed {@code .event} artifact — the portable subset of Flowable's event model
 * (docs/12 D-A v2). {@code payloadTypes} uses the {@code EventPayloadTypes} dictionary
 * ({@code string, integer, long, double, boolean, json}); the lineage maps {@code json} to a
 * raw String variable in v1.
 *
 * <p>{@code messageName} comes from {@code extension.messageName} when present, otherwise the
 * {@code key == messageName} convention applies.
 */
public record EventDefinition(
        String key,
        String name,
        Map<String, String> correlationParameterTypes,
        Map<String, String> payloadTypes,
        String messageName) {

    public static final List<String> TYPE_DICTIONARY =
            List.of("string", "integer", "long", "double", "boolean", "json");

    public String resolvedMessageName() {
        return messageName != null && !messageName.isEmpty() ? messageName : key;
    }
}
