package com.threeai.nats.cibseven.shard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.threeai.nats.cibseven.inbound.SubscriptionConfig;
import com.threeai.nats.core.exception.TopicNamespaceCollisionException;
import com.threeai.nats.core.shard.ShardTopology;

import org.junit.jupiter.api.Test;

/** docs/13 §2.4 slice 5: prefixes, Y-9 ordering, T-8 RED, autoCreate off. */
class ShardSubscriptionRewriterTest {

    private final ShardTopology shard1 = new ShardTopology(2, 1);

    private SubscriptionConfig config(String subject) {
        SubscriptionConfig config = new SubscriptionConfig();
        config.setSubject(subject);
        config.setMessageName("OrderMessage");
        config.setJetstream(true);
        return config;
    }

    @Test
    void rewrite_prefixesSubjectAndDurable_deliverGroupFollows() {
        SubscriptionConfig config = config("evt.order.accept");
        config.setDurableName("er-orders");

        ShardSubscriptionRewriter.rewrite(config, shard1);

        assertThat(config.getSubject()).isEqualTo("shard.1.evt.order.accept");
        assertThat(config.getDurableName()).isEqualTo("s1-er-orders");
        assertThat(config.resolveDeliverGroup()).isEqualTo("s1-er-orders");
    }

    @Test
    void rewrite_noDurable_usesResolvedDeliverGroupAsBase() {
        SubscriptionConfig config = config("evt.order.accept");

        ShardSubscriptionRewriter.rewrite(config, shard1);

        // base = correlation-<messageName> (the legacy default), shard-prefixed
        assertThat(config.getDurableName()).isEqualTo("s1-correlation-OrderMessage");
    }

    @Test
    void rewrite_reservationCheckedOnOriginalSubject_beforePrefixing() {
        // Y-9: a subject ALREADY in the shard namespace is a collision, not double-prefixed
        assertThatThrownBy(() -> ShardSubscriptionRewriter.rewrite(config("shard.0.evt.x"), shard1))
                .isInstanceOf(TopicNamespaceCollisionException.class);
    }

    @Test
    void rewrite_coreNats_rejected_T8() {
        SubscriptionConfig config = config("evt.order.accept");
        config.setJetstream(false);

        assertThatThrownBy(() -> ShardSubscriptionRewriter.rewrite(config, shard1))
                .hasMessageContaining("VAL_SHARD_JETSTREAM_REQUIRED");
    }

    @Test
    void rewrite_autoCreateStream_forcedOffWithWarn() {
        SubscriptionConfig config = config("evt.order.accept");
        config.setAutoCreateStream(true);
        config.setStreamName("ORDERS");

        ShardSubscriptionRewriter.rewrite(config, shard1);

        assertThat(config.isAutoCreateStream()).isFalse();
        assertThat(config.getStreamName()).isNull();
    }
}
