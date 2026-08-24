package com.threeai.nats.core.shard;

/**
 * The shard subject grammar (docs/13 §2.4): reserved root {@code shard.} + DLQ twin
 * {@code dlq.shard.} — v1 has NO fleet token (deliberate; adding one is a breaking wire
 * change owned by the parallel-fleet design). Reservation checks run on the ORIGINAL
 * subject BEFORE any rewrite (Y-9 ordering rule).
 */
public final class ShardSubjects {

    public static final String SHARD_PREFIX = "shard.";
    public static final String SHARD_DLQ_PREFIX = "dlq.shard.";

    private ShardSubjects() {
    }

    /** {@code shard.<id>.<subject>} — the routed twin of an inbound subject. */
    public static String scoped(int shardId, String subject) {
        return SHARD_PREFIX + shardId + "." + subject;
    }

    /** {@code dlq.shard.<subject>} — the router's DLQ; escapes ALL incident bridges (T-2). */
    public static String dlq(String subject) {
        return SHARD_DLQ_PREFIX + subject;
    }

    /** {@code shard.<id>.>} — the subject set one shard's stream must capture (G8). */
    public static String shardWildcard(int shardId) {
        return SHARD_PREFIX + shardId + ".>";
    }
}
