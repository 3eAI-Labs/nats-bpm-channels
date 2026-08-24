package com.threeai.nats.core.shard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import com.threeai.nats.core.config.NamespaceValidator;
import com.threeai.nats.core.exception.TopicNamespaceCollisionException;

class ShardSubjectsTest {

    @Test
    void grammar() {
        assertThat(ShardSubjects.scoped(2, "evt.order.accept")).isEqualTo("shard.2.evt.order.accept");
        assertThat(ShardSubjects.dlq("evt.order.accept")).isEqualTo("dlq.shard.evt.order.accept");
        assertThat(ShardSubjects.shardWildcard(2)).isEqualTo("shard.2.>");
    }

    @Test
    void reservation_throwsOnShardRoots() {
        assertThatThrownBy(() -> NamespaceValidator.assertNotReservedForSharding("shard.0.x", "test"))
                .isInstanceOf(TopicNamespaceCollisionException.class)
                .hasMessageContaining("shard.");
        assertThatThrownBy(() -> NamespaceValidator.assertNotReservedForSharding("dlq.shard.x", "test"))
                .isInstanceOf(TopicNamespaceCollisionException.class)
                .hasMessageContaining("dlq.shard.");
    }

    @Test
    void reservation_allowsOrdinarySubjects() {
        NamespaceValidator.assertNotReservedForSharding("evt.order.accept", "test");
        NamespaceValidator.assertNotReservedForSharding("sharding.metrics", "test"); // no dot-boundary false positive... prefix is 'shard.'
        NamespaceValidator.assertNotReservedForSharding(null, "test");
    }
}
