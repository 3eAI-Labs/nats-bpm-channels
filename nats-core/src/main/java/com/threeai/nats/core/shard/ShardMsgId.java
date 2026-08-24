package com.threeai.nats.core.shard;

/**
 * The single Msg-Id derive/normalize helper (docs/13 T-1 — ONE rule, ONE place).
 *
 * <p>Derivation: the router republishes with {@code <origMsgId>.s<shardId>} — never a
 * passthrough (round-1 PROBE-A: JetStream dedup is stream-scoped; a passthrough would be
 * silently swallowed when source and target subjects share a stream; the {@code DlqPublisher}
 * {@code .dlq} suffix precedent).
 *
 * <p>Normalization (shard-side consumers — completion AND incident bridges): strip one
 * optional {@code .dlq} suffix FIRST, then one optional {@code .s<ownShardId>} suffix.
 * The order mirrors real suffix stacking: {@code DlqPublisher} appends {@code .dlq} to
 * whatever id it received, so a bridged-then-dead-lettered reply carries
 * {@code <taskId>.s<id>.dlq} (round-4 §a verified this against DlqPublisher:88). Only the
 * OWN shard's suffix is stripped — an alien shard's suffix is not ours to interpret.
 */
public final class ShardMsgId {

    private static final String DLQ_SUFFIX = ".dlq";

    private ShardMsgId() {
    }

    /** Router republish id: {@code <origMsgId>.s<shardId>}. */
    public static String derive(String originalMsgId, int shardId) {
        return originalMsgId + ".s" + shardId;
    }

    /** Strips one optional {@code .dlq}, then one optional {@code .s<ownShardId>}. */
    public static String normalize(String msgId, int ownShardId) {
        if (msgId == null) {
            return null;
        }
        String result = msgId;
        if (result.endsWith(DLQ_SUFFIX)) {
            result = result.substring(0, result.length() - DLQ_SUFFIX.length());
        }
        String ownSuffix = ".s" + ownShardId;
        if (result.endsWith(ownSuffix)) {
            result = result.substring(0, result.length() - ownSuffix.length());
        }
        return result;
    }
}
