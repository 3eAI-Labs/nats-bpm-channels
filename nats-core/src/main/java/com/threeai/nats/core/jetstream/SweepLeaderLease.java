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
 */
public class SweepLeaderLease {

    private static final Logger log = LoggerFactory.getLogger(SweepLeaderLease.class);
    private static final String BUCKET = "a2-sweep-leader";

    private final Connection connection;
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
        this.connection = connection;
        this.key = "sweep-leader." + engineId;
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
            kv = connection.keyValue(BUCKET);
        } catch (IOException connectionFailure) {
            log.warn("Sweep-leader lease unavailable — could not obtain KV handle",
                    kv("key", key), connectionFailure);
            return false;
        }
        try {
            long rev = kv.create(key, nodeId.getBytes(StandardCharsets.UTF_8));
            this.heldRevision = rev;
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
            return false;
        }
        if (entry == null || !nodeId.equals(new String(entry.getValue(), StandardCharsets.UTF_8))) {
            return false;
        }
        try {
            long rev = kv.update(key, nodeId.getBytes(StandardCharsets.UTF_8), entry.getRevision());
            this.heldRevision = rev;
            return true;
        } catch (Exception renewRace) {
            return false;
        }
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
