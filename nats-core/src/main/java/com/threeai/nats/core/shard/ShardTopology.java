package com.threeai.nats.core.shard;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * The shard topology of a single fleet (docs/13 D-D v4: N is FIXED for the fleet's
 * lifetime; growth is a parallel-fleet procedure, deferred to its own design).
 *
 * <p><b>The hash is FROZEN (D-E):</b> {@code floorMod(int32BE(SHA-256(businessKey UTF-8)
 * [0..3]), shardCount)}. Worker SDKs in any language and load balancers must be able to
 * reproduce it byte-for-byte, so it is published in the USER_GUIDE with test vectors and
 * must never change within a fleet's lifetime. JDK-only primitives, no library hash.
 */
public final class ShardTopology {

    private final int shardCount;
    private final int shardId;

    public ShardTopology(int shardCount, int shardId) {
        if (shardCount < 1) {
            throw new IllegalArgumentException(
                    "[VAL_SHARD_CONFIG] shardCount must be >= 1 — got " + shardCount);
        }
        if (shardId < 0 || shardId >= shardCount) {
            throw new IllegalArgumentException("[VAL_SHARD_CONFIG] shardId must be within"
                    + " [0, shardCount) — got shardId=" + shardId + ", shardCount=" + shardCount);
        }
        this.shardCount = shardCount;
        this.shardId = shardId;
    }

    public int getShardCount() {
        return shardCount;
    }

    public int getShardId() {
        return shardId;
    }

    /** The frozen D-E hash. Throws on null/empty — a shard decision without a key is a bug. */
    public int shardOf(String businessKey) {
        if (businessKey == null || businessKey.isEmpty()) {
            throw new IllegalArgumentException(
                    "[VAL_SHARD_BUSINESS_KEY_REQUIRED] businessKey must be non-empty");
        }
        byte[] digest = sha256(businessKey.getBytes(StandardCharsets.UTF_8));
        int hash = ((digest[0] & 0xFF) << 24) | ((digest[1] & 0xFF) << 16)
                | ((digest[2] & 0xFF) << 8) | (digest[3] & 0xFF);
        return Math.floorMod(hash, shardCount);
    }

    public boolean owns(String businessKey) {
        return shardOf(businessKey) == shardId;
    }

    private static byte[] sha256(byte[] input) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(input);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable — mandated by every JVM", e);
        }
    }
}
