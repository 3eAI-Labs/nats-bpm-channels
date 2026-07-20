package com.threeai.nats.core.jetstream;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KV-lease leader election, one independent key per engine family (LLD-Q1, LLD 03_classes/1_nats_core_common.md §3.2,
 * ADR-0002). A single {@code a2-sweep-leader} bucket is shared by Camunda and CadenzaFlow, but
 * each engine family elects its own leader on its own {@code sweep-leader.<engineId>} key —
 * the two engine families never compete for the same key.
 *
 * <p><b>Basamak-2 reuse (`history-relay-leader` bucket, `08_config.md` §3):</b> {@link
 * HistoryOutboxRelay} needs the SAME leader-election mechanism against a DIFFERENT bucket
 * ({@code history-relay-leader}, not {@code a2-sweep-leader}) and a DIFFERENT key prefix
 * ({@code relay-leader.<engineId>}, not {@code sweep-leader.<engineId>}) — an intentionally
 * separate lease namespace per LLD ("basamak-1 a2-sweep-leader'DAN AYRI — farklı amaç"). The
 * original 6-arg constructor is preserved BYTE-FOR-BEHAVIOR-IDENTICAL for every existing
 * basamak-1 caller/test (defaults to the original bucket/key-prefix); a new 8-arg constructor
 * exposes bucket + key-prefix as parameters for basamak-2 reuse without copying this class.
 */
public class SweepLeaderLease {

    private static final Logger log = LoggerFactory.getLogger(SweepLeaderLease.class);
    private static final String DEFAULT_BUCKET = "a2-sweep-leader";
    private static final String DEFAULT_KEY_PREFIX = "sweep-leader.";

    private final Connection connection;
    private final String bucket;
    private final String key;
    private final String nodeId;
    private final Duration ttl;

    private volatile Long heldRevision;

    /**
     * @param jetStream  kept for LLD signature parity; leader-election uses the KV API on
     *                   {@code connection} directly (not exposed through {@link JetStream}).
     * @param kvManager  kept for LLD signature parity; bucket provisioning happens once at
     *                   bootstrap via {@link JetStreamKvManager#ensureBucket}, not per-lease.
     * @param connection live NATS connection used to obtain the KV handle.
     * @param engineId   {@code "camunda"} or {@code "cadenzaflow"} — engine-family identity,
     *                   determines the key namespace.
     * @param nodeId     identity of this engine-family replica (e.g. pod name) — becomes the KV
     *                   value if this replica wins leadership.
     * @param ttl        lease TTL, {@code 2*S} per ADR-0002 (informational here — actual
     *                   expiry is enforced by the bucket's own TTL configuration).
     */
    public SweepLeaderLease(JetStream jetStream, JetStreamKvManager kvManager, Connection connection,
            String engineId, String nodeId, Duration ttl) {
        this(jetStream, kvManager, connection, DEFAULT_BUCKET, DEFAULT_KEY_PREFIX, engineId, nodeId, ttl);
    }

    /**
     * Basamak-2 reuse overload — explicit {@code bucket}/{@code keyPrefix} (`08_config.md` §3).
     *
     * @param bucket    KV bucket name (e.g. {@code "history-relay-leader"})
     * @param keyPrefix key prefix, engineId is appended verbatim (e.g. {@code "relay-leader."})
     */
    public SweepLeaderLease(JetStream jetStream, JetStreamKvManager kvManager, Connection connection,
            String bucket, String keyPrefix, String engineId, String nodeId, Duration ttl) {
        this.connection = connection;
        this.bucket = bucket;
        this.key = keyPrefix + engineId;
        this.nodeId = nodeId;
        this.ttl = ttl;
    }

    /**
     * Idempotent — renews if this node is already the leader, otherwise attempts to take over.
     * Called once every S seconds (see {@code A2OrphanSweep.sweepCycle()}).
     */
    public boolean tryAcquireOrRenew() {
        KeyValue kv;
        try {
            kv = connection.keyValue(bucket);
        } catch (IOException connectionFailure) {
            log.warn("Sweep-leader lease unavailable — could not obtain KV handle",
                    kv("key", key), connectionFailure);
            markNotLeader();
            return false;
        }
        try {
            long rev = kv.create(key, nodeId.getBytes(StandardCharsets.UTF_8));
            markLeader(rev);
            return true;
        } catch (Exception createFailed) {
            return tryRenewExisting(kv);
        }
    }

    private boolean tryRenewExisting(KeyValue kv) {
        KeyValueEntry entry;
        try {
            entry = kv.get(key);
        } catch (Exception getFailed) {
            log.warn("Sweep-leader lease lookup failed", kv("key", key), getFailed);
            markNotLeader();
            return false;
        }
        if (entry == null || !nodeId.equals(new String(entry.getValue(), StandardCharsets.UTF_8))) {
            markNotLeader();
            return false;
        }
        try {
            long rev = kv.update(key, nodeId.getBytes(StandardCharsets.UTF_8), entry.getRevision());
            markLeader(rev);
            return true;
        } catch (Exception renewRace) {
            markNotLeader();
            return false;
        }
    }

    /** Records that this node holds the lease as of {@code revision}. */
    private void markLeader(long revision) {
        this.heldRevision = revision;
    }

    /**
     * Records that this node does NOT hold the lease. Called on every acquire/renew failure path
     * so that {@link #isLeader()} reflects current reality rather than a stale prior success —
     * without this, a node that HELD the lease and then lost it on a later renew would keep
     * reporting {@code isLeader() == true} forever, since {@code heldRevision} was only ever
     * written on success.
     */
    private void markNotLeader() {
        this.heldRevision = null;
    }

    public boolean isLeader() {
        return heldRevision != null;
    }

    public String getKey() {
        return key;
    }

    public Duration getTtl() {
        return ttl;
    }
}
