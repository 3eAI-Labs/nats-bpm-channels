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
 * KV-lease leader election, one independent key per engine family (LLD-Q1,
 * ADR-0002). A single {@code a2-sweep-leader} bucket is shared by Camunda and CadenzaFlow, but
 * each engine family elects its own leader on its own {@code sweep-leader.<engineId>} key —
 * the two engine families never compete for the same key.
 *
 * <p><b>Increment 2 reuse ({@code history-relay-leader} bucket):</b> {@code
 * HistoryOutboxRelay} (lives in the downstream engine modules — camunda/cibseven/cadenzaflow —
 * which nats-core cannot see, hence NOT an {@code @link}) needs the SAME leader-election
 * mechanism against a DIFFERENT bucket
 * ({@code history-relay-leader}, not {@code a2-sweep-leader}) and a DIFFERENT key prefix
 * ({@code relay-leader.<engineId>}, not {@code sweep-leader.<engineId>}) — an intentionally
 * separate lease namespace per LLD (SEPARATE from increment 1's a2-sweep-leader — different
 * purpose). The original 6-arg constructor is preserved BYTE-FOR-BEHAVIOR-IDENTICAL for every
 * existing increment 1 caller/test (defaults to the original bucket/key-prefix); a new 8-arg
 * constructor exposes bucket + key-prefix as parameters for increment 2 reuse without copying
 * this class.
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
     * Last observed leadership state, for transition-only logging. The steady states are
     * deliberately quiet (a permanent non-leader logging every cycle would be noise), but every
     * TRANSITION is loud: in the 2026-08-20 sustained-load incident the live post-mortem misread
     * a working sweep as flatlined for 35 minutes, because leadership state was invisible on
     * every path (container-stats forensics later showed the sweep had been leader and cycling
     * throughout — see the multi-node flatline-repro reliability test in cibseven-nats-channel).
     * A diagnostic that says nothing about the state of the safety net it gates is the same
     * defect class the topology check exists for.
     */
    private volatile boolean wasLeader;

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
     * Increment 2 reuse overload — explicit {@code bucket}/{@code keyPrefix}.
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
            // connection.keyValue() declares only IOException (verified: the 2026-08-20 flatline
            // investigation suspected an uncaught JetStreamApiException here; the compiler
            // refutes it — that exception cannot escape this call).
            log.warn("Sweep-leader lease unavailable — could not obtain KV handle",
                    kv("key", key), connectionFailure);
            transition(false, "kv-handle-unavailable");
            return false;
        }
        try {
            long rev = kv.create(key, nodeId.getBytes(StandardCharsets.UTF_8));
            markLeader(rev);
            transition(true, "acquired");
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
            transition(false, "lease-lookup-failed");
            return false;
        }
        if (entry == null) {
            // Key vanished between the failed create and this get (TTL expiry race) — next
            // cycle's create will contend for it. Not an error, but a state worth seeing.
            transition(false, "lease-key-expired");
            return false;
        }
        String holder = new String(entry.getValue(), StandardCharsets.UTF_8);
        if (!nodeId.equals(holder)) {
            transition(false, "held-by-" + holder);
            return false;
        }
        try {
            long rev = kv.update(key, nodeId.getBytes(StandardCharsets.UTF_8), entry.getRevision());
            markLeader(rev);
            transition(true, "renewed");
            return true;
        } catch (Exception renewRace) {
            // Was silent — a leader losing its lease produced no signal anywhere, and everything
            // the lease gates (the orphan sweep) went silently dead with it.
            log.warn("Sweep-leader lease renew failed — leadership lost until re-acquired",
                    kv("key", key), renewRace);
            transition(false, "renew-failed");
            return false;
        }
    }

    /**
     * Records the new leadership state and logs the change exactly once per transition; steady
     * states stay quiet. Owns BOTH state pieces: {@code heldRevision} (what {@link #isLeader()}
     * reports) and {@code wasLeader} (what has been logged) — splitting them reintroduced the
     * stale-leader bug the {@code markNotLeader} javadoc warns about, and the existing unit tests
     * caught it within one run.
     */
    private void transition(boolean nowLeader, String reason) {
        if (!nowLeader) {
            markNotLeader();
        }
        if (nowLeader == wasLeader) {
            return;
        }
        wasLeader = nowLeader;
        if (nowLeader) {
            log.info("Sweep leadership GAINED", kv("key", key), kv("node", nodeId), kv("reason", reason));
        } else {
            log.warn("Sweep leadership LOST — everything this lease gates (orphan sweep) is now"
                    + " inactive on this node", kv("key", key), kv("node", nodeId), kv("reason", reason));
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
