package com.threeai.nats.core.shard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class ShardTopologyTest {

    // --- D-E: the hash is FROZEN. These vectors were computed with an INDEPENDENT
    // --- implementation (python hashlib, big-endian int32, floorMod). If any of these
    // --- ever fails, the wire contract broke — that is a release blocker, not a test fix.

    @Test
    void frozenVectors_crossImplementation() {
        assertThat(new ShardTopology(2, 0).shardOf("ORD-123")).isEqualTo(1);
        assertThat(new ShardTopology(3, 0).shardOf("ORD-123")).isEqualTo(1);
        assertThat(new ShardTopology(8, 0).shardOf("ORD-123")).isEqualTo(5);
        assertThat(new ShardTopology(8, 0).shardOf("ORD-124")).isEqualTo(7);
        // non-ASCII key: UTF-8 bytes are the contract, not platform charset
        assertThat(new ShardTopology(8, 0).shardOf("café-42")).isEqualTo(3);
        assertThat(new ShardTopology(2, 0).shardOf("a")).isEqualTo(0);
    }

    @Test
    void owns_matchesShardOf() {
        ShardTopology s1 = new ShardTopology(2, 1);
        assertThat(s1.owns("ORD-123")).isTrue();  // vector: shard 1
        assertThat(new ShardTopology(2, 0).owns("ORD-123")).isFalse();
    }

    @Test
    void distribution_isRoughlyUniform() {
        ShardTopology topology = new ShardTopology(4, 0);
        Map<Integer, Integer> counts = new HashMap<>();
        for (int i = 0; i < 4000; i++) {
            counts.merge(topology.shardOf("key-" + i), 1, Integer::sum);
        }
        // 4000 keys over 4 shards: each within [800, 1200] is ample for SHA-256
        counts.values().forEach(c -> assertThat(c).isBetween(800, 1200));
    }

    @Test
    void invalidConfig_throws() {
        assertThatThrownBy(() -> new ShardTopology(0, 0))
                .hasMessageContaining("VAL_SHARD_CONFIG");
        assertThatThrownBy(() -> new ShardTopology(2, 2))
                .hasMessageContaining("VAL_SHARD_CONFIG");
        assertThatThrownBy(() -> new ShardTopology(2, -1))
                .hasMessageContaining("VAL_SHARD_CONFIG");
    }

    @Test
    void nullOrEmptyKey_throws() {
        ShardTopology topology = new ShardTopology(2, 0);
        assertThatThrownBy(() -> topology.shardOf(null))
                .hasMessageContaining("VAL_SHARD_BUSINESS_KEY_REQUIRED");
        assertThatThrownBy(() -> topology.shardOf(""))
                .hasMessageContaining("VAL_SHARD_BUSINESS_KEY_REQUIRED");
    }
}
