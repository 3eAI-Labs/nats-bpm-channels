package com.threeai.nats.camunda.eventing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.threeai.nats.camunda.inbound.SubscriptionConfig;
import org.junit.jupiter.api.Test;

/** docs/12 D-E v2: idempotent keyed lifecycle, single durable per key, injectivity. */
class EventingRegistryTest {

    private static EventDefinition event(String key) {
        return new EventDefinition(key, key, Map.of(), Map.of(), null);
    }

    private static ChannelDefinition channel(String key, String subject, String qg, String durable) {
        return new ChannelDefinition(key, subject, "evt-" + key, qg, durable != null,
                durable, 5, null, false, null);
    }

    /** Recording fake: tracks subscribe order and closes. */
    static class RecordingFactory implements SubscriberFactory {
        final List<String> subscribed = new ArrayList<>();
        final List<String> closed = new ArrayList<>();

        @Override
        public SubscriberHandle subscribe(SubscriptionConfig config, EventDefinition event) {
            String id = config.getSubject() + "/" + config.getDeliverGroup();
            subscribed.add(id);
            return () -> closed.add(id);
        }
    }

    @Test
    void register_isIdempotent_forUnchangedDefinition() {
        RecordingFactory factory = new RecordingFactory();
        EventingRegistry registry = new EventingRegistry(factory);

        assertThat(registry.register("t#k1", event("e1"), channel("c1", "s.one", "g1", null))).isTrue();
        assertThat(registry.register("t#k1", event("e1"), channel("c1", "s.one", "g1", null))).isFalse();
        assertThat(factory.subscribed).hasSize(1);
        assertThat(factory.closed).isEmpty();
    }

    @Test
    void changedDefinition_replacesAtomically_oldClosedBeforeNewBind() {
        RecordingFactory factory = new RecordingFactory();
        EventingRegistry registry = new EventingRegistry(factory);
        registry.register("t#k1", event("e1"), channel("c1", "s.one", "g1", null));

        assertThat(registry.register("t#k1", event("e1"), channel("c1", "s.one", "g2", null))).isTrue();
        assertThat(factory.closed).containsExactly("s.one/g1");
        assertThat(factory.subscribed).containsExactly("s.one/g1", "s.one/g2");
        assertThat(registry.activeKeys()).containsExactly("t#k1");
    }

    @Test
    void durableConflict_rejectsLateArrival_keepsIncumbent() {
        RecordingFactory factory = new RecordingFactory();
        EventingRegistry registry = new EventingRegistry(factory);
        registry.register("t#k1", event("e1"), channel("c1", "s.one", "g1", "dur-1"));

        assertThatThrownBy(() ->
                registry.register("t#k2", event("e2"), channel("c2", "s.two", "g2", "dur-1")))
                .isInstanceOf(EventingDefinitionException.class)
                .hasMessageContaining("VAL_EVENTING_DURABLE_CONFLICT");
        assertThat(registry.activeKeys()).containsExactly("t#k1");
        assertThat(factory.subscribed).hasSize(1);
    }

    @Test
    void subjectQueueGroupConflict_rejected() {
        RecordingFactory factory = new RecordingFactory();
        EventingRegistry registry = new EventingRegistry(factory);
        registry.register("t#k1", event("e1"), channel("c1", "s.one", "g1", null));

        assertThatThrownBy(() ->
                registry.register("t#k2", event("e2"), channel("c2", "s.one", "g1", null)))
                .isInstanceOf(EventingDefinitionException.class)
                .hasMessageContaining("(s.one, g1)");
    }

    @Test
    void unregister_closes_andForgetsKey() {
        RecordingFactory factory = new RecordingFactory();
        EventingRegistry registry = new EventingRegistry(factory);
        registry.register("t#k1", event("e1"), channel("c1", "s.one", "g1", null));

        assertThat(registry.unregister("t#k1")).isTrue();
        assertThat(factory.closed).containsExactly("s.one/g1");
        assertThat(registry.unregister("t#k1")).isFalse();
        assertThat(registry.activeKeys()).isEmpty();
    }

    @Test
    void translator_mapsQueueGroupToDeliverGroup_andMessageNameConvention() {
        EventDefinition withExt = new EventDefinition("orderEvent", "n", Map.of(), Map.of(), "OrderMessage");
        SubscriptionConfig config = DefinitionTranslator.translate(withExt,
                channel("c1", "order.new", "eventing-orders", "dur-x"));
        assertThat(config.getMessageName()).isEqualTo("OrderMessage");
        assertThat(config.getDeliverGroup()).isEqualTo("eventing-orders");
        assertThat(config.getSubject()).isEqualTo("order.new");
        assertThat(config.getDurableName()).isEqualTo("dur-x");
        assertThat(config.isJetstream()).isTrue();
    }
}
