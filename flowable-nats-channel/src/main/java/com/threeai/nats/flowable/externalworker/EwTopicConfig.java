package com.threeai.nats.flowable.externalworker;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Validated, immutable view of the configured external-worker topic set.
 *
 * <p><b>Decision D-H' (docs/11):</b> every topic is validated against the F-003 token rule
 * {@code ^[A-Za-z0-9_-]+$} at CONFIG LOAD TIME, and violations THROW. The topic becomes a
 * subject token ({@code ewjobs.<topic>}, {@code ewjobs.<topic>.reply},
 * {@code dlq.ewjobs.<topic>}) and a durable/deliver-group name
 * ({@code flw-ew-completion-<topic>}); a {@code .}, {@code *}, {@code >} or whitespace in it
 * would produce a malformed or wildcard-colliding subject and a broken durable. A2 shipped
 * without this check (gap found 2026-08-22, fixed separately as a boot WARN there).
 */
public class EwTopicConfig {

    /** F-003 token rule — same pattern as {@code OutboundSubjectBuilder.SAFE_MESSAGE_TYPE}. */
    private static final Pattern SAFE_TOPIC = Pattern.compile("^[A-Za-z0-9_-]+$");

    private final Set<String> topics;

    public EwTopicConfig(EwProperties properties) {
        Set<String> validated = new LinkedHashSet<>();
        for (String topic : properties.getTopics()) {
            if (topic == null || !SAFE_TOPIC.matcher(topic).matches()) {
                throw new InvalidEwTopicException(topic);
            }
            validated.add(topic);
        }
        this.topics = Set.copyOf(validated);
    }

    public Set<String> topics() {
        return topics;
    }

    public boolean isDispatchTopic(String topic) {
        return topics.contains(topic);
    }
}
