package com.threeai.nats.cadenzaflow.eventing;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.threeai.nats.cadenzaflow.inbound.SubscriptionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The definition-path registration authority (docs/12 D-E v2): a dynamic registry keyed by
 * {@code tenantId#definitionKey} (Flowable {@code resolveKey} precedent — stable across
 * redeploys, unlike deploymentId). Idempotent: re-registering an unchanged definition is a
 * no-op; a changed one atomically replaces its predecessor (unregister -> register, single
 * durable per key, gate G3). Injectivity (F-4b): {@code durableName} and
 * {@code (subject, queueGroup)} must be unique across the active set — a later-arriving
 * conflicting definition is rejected with {@code VAL_EVENTING_DURABLE_CONFLICT} and does not
 * disturb the incumbent.
 *
 * <p>Thread-safety: all mutators are synchronized; the reconciler's single-flight rule (F-2)
 * makes contention rare, but the locations bootstrap path may race the first pass.
 */
public class EventingRegistry {

    public static final String CODE_DURABLE_CONFLICT = "VAL_EVENTING_DURABLE_CONFLICT";

    private static final Logger log = LoggerFactory.getLogger(EventingRegistry.class);

    private record Registration(EventDefinition event, ChannelDefinition channel,
            String fingerprint, SubscriberHandle handle) {
    }

    private final SubscriberFactory subscriberFactory;
    private final Map<String, Registration> active = new LinkedHashMap<>();

    public EventingRegistry(SubscriberFactory subscriberFactory) {
        this.subscriberFactory = subscriberFactory;
    }

    /** @return true if a (re)subscription happened, false for the idempotent no-op. */
    public synchronized boolean register(String key, EventDefinition event, ChannelDefinition channel) {
        String fingerprint = DefinitionTranslator.fingerprint(event, channel);
        Registration existing = active.get(key);
        if (existing != null && existing.fingerprint().equals(fingerprint)) {
            return false;
        }
        assertInjective(key, channel);
        if (existing != null) {
            // Replace: close the old subscription BEFORE binding the new one — same durable,
            // second concurrent bind would be [SUB-90012].
            closeQuietly(key, existing);
            active.remove(key);
        }
        SubscriptionConfig config = DefinitionTranslator.translate(event, channel);
        SubscriberHandle handle = subscriberFactory.subscribe(config, event);
        active.put(key, new Registration(event, channel, fingerprint, handle));
        log.info("Eventing definition registered", kv("key", key),
                kv("subject", channel.subject()), kv("queue_group", channel.queueGroup()));
        return true;
    }

    public synchronized boolean unregister(String key) {
        Registration existing = active.remove(key);
        if (existing == null) {
            return false;
        }
        closeQuietly(key, existing);
        log.info("Eventing definition unregistered", kv("key", key));
        return true;
    }

    public synchronized Set<String> activeKeys() {
        return Set.copyOf(active.keySet());
    }

    public synchronized void close() {
        for (Map.Entry<String, Registration> e : active.entrySet()) {
            closeQuietly(e.getKey(), e.getValue());
        }
        active.clear();
    }

    private void assertInjective(String candidateKey, ChannelDefinition candidate) {
        for (Map.Entry<String, Registration> e : active.entrySet()) {
            if (e.getKey().equals(candidateKey)) {
                continue;
            }
            ChannelDefinition incumbent = e.getValue().channel();
            if (candidate.durableName() != null
                    && candidate.durableName().equals(incumbent.durableName())) {
                throw new EventingDefinitionException(CODE_DURABLE_CONFLICT,
                        "definition '" + candidateKey + "' declares durableName '"
                                + candidate.durableName() + "' already owned by '" + e.getKey() + "'");
            }
            if (candidate.subject().equals(incumbent.subject())
                    && candidate.queueGroup().equals(incumbent.queueGroup())) {
                throw new EventingDefinitionException(CODE_DURABLE_CONFLICT,
                        "definition '" + candidateKey + "' declares (subject, queueGroup) ("
                                + candidate.subject() + ", " + candidate.queueGroup()
                                + ") already owned by '" + e.getKey() + "'");
            }
        }
    }

    private void closeQuietly(String key, Registration registration) {
        try {
            registration.handle().unsubscribe();
        } catch (Exception e) {
            log.warn("Error closing eventing subscription", kv("key", key), e);
        }
    }
}
