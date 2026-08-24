package com.threeai.nats.core.shard;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** T-1: ONE normalize rule — strip .dlq first, then .s<ownShardId>. */
class ShardMsgIdTest {

    @Test
    void derive_appendsShardSuffix() {
        assertThat(ShardMsgId.derive("task-77", 0)).isEqualTo("task-77.s0");
    }

    @Test
    void normalize_stripsOwnShardSuffix() {
        assertThat(ShardMsgId.normalize("task-77.s0", 0)).isEqualTo("task-77");
    }

    @Test
    void normalize_bridgedThenDeadLettered_stripsBothInOrder() {
        // DlqPublisher appends .dlq to whatever id it received (round-4 §a): <id>.s0.dlq
        assertThat(ShardMsgId.normalize("task-77.s0.dlq", 0)).isEqualTo("task-77");
    }

    @Test
    void normalize_alienShardSuffix_notStripped() {
        // not ours to interpret — the wrong-shard case surfaces as NotFound, observably
        assertThat(ShardMsgId.normalize("task-77.s1", 0)).isEqualTo("task-77.s1");
    }

    @Test
    void normalize_plainAndDlqOnlyAndNull() {
        assertThat(ShardMsgId.normalize("task-77", 0)).isEqualTo("task-77");
        assertThat(ShardMsgId.normalize("task-77.dlq", 0)).isEqualTo("task-77");
        assertThat(ShardMsgId.normalize(null, 0)).isNull();
    }
}
