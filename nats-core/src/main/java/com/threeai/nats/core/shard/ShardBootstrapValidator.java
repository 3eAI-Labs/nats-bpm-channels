package com.threeai.nats.core.shard;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StreamInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HARD gate of sharded boot (docs/13 §2.7, F-7/T-7): unlike the topology self-check
 * family ("reports; never fails a boot"), this bean exists ONLY when sharding is enabled and
 * THROWS — sharding must not run against an unvalidated topology, and an opt-in feature may
 * stop its own boot. Broker unreachable at validation time → THROW too (deliberate inversion
 * of the self-check contract; the 0.8.1 swallow-lesson was about the always-on path).
 *
 * <p>Checks, in order: (a) config (delegated to {@link ShardTopology}'s constructor +
 * T-8 JetStream-only correlation handled by the adapter before calling in); (b) per-shard
 * stream resolution BY CONTENT (the library never names streams — G8): exactly one stream
 * per shard, pairwise distinct (a stream capturing several shards would recreate the
 * fleet-wide blast radius the per-shard split exists to prevent), WorkQueue retention,
 * explicit caps, {@code discard=new} (T-3: default {@code old} ACKs and silently deletes),
 * {@code duplicate_window} above the redelivery horizon; (c) activation-skipped detection —
 * legacy durables still on the reply streams mean step (2) of the activation procedure was
 * skipped and boot would die later with a raw {@code [10100]}/{@code [SUB-90011]}.
 */
public class ShardBootstrapValidator {

    private static final Logger log = LoggerFactory.getLogger(ShardBootstrapValidator.class);

    /** Legacy durable families whose presence means the activation procedure was skipped. */
    private static final List<String> LEGACY_DURABLE_PREFIXES =
            List.of("a2-completion-", "flw-ew-completion-", "correlation-");
    private static final List<String> LEGACY_REPLY_SUBJECT_FILTERS =
            List.of("jobs.*.reply", "ewjobs.*.reply");

    private final Connection connection;
    private final ShardTopology topology;
    /** The redelivery horizon (seconds) the duplicate window must exceed — max over the
     * router consumer and every shard-scoped subscription: {@code ackWait × (maxDeliver+1)}. */
    private final long redeliveryHorizonSeconds;

    public ShardBootstrapValidator(Connection connection, ShardTopology topology,
            long redeliveryHorizonSeconds) {
        this.connection = connection;
        this.topology = topology;
        this.redeliveryHorizonSeconds = redeliveryHorizonSeconds;
    }

    /** Runs all checks; throws {@link IllegalStateException} with a SHARD-coded message. */
    public void validate() {
        JetStreamManagement jsm;
        try {
            jsm = connection.jetStreamManagement();
            jsm.getAccountStatistics(); // connectivity probe — fail fast, clearly
        } catch (Exception e) {
            throw new IllegalStateException("[SHARD-BOOT-BROKER] broker unreachable during"
                    + " shard bootstrap validation — sharding must not start unvalidated"
                    + " (deliberate THROW; see docs/13 T-7)", e);
        }
        Map<Integer, String> shardStreams = resolveShardStreams(jsm);
        validateStreamProperties(jsm, shardStreams);
        detectSkippedActivation(jsm);
        log.info("Shard bootstrap validation passed",
                kv("shard_count", topology.getShardCount()),
                kv("own_shard", topology.getShardId()),
                kv("streams", shardStreams));
    }

    private Map<Integer, String> resolveShardStreams(JetStreamManagement jsm) {
        Map<Integer, String> byShard = new HashMap<>();
        for (int shard = 0; shard < topology.getShardCount(); shard++) {
            List<String> streams;
            try {
                streams = jsm.getStreamNames(ShardSubjects.shardWildcard(shard));
            } catch (Exception e) {
                throw new IllegalStateException("[SHARD-BOOT-BROKER] stream lookup failed for"
                        + " shard " + shard, e);
            }
            if (streams.isEmpty()) {
                throw new IllegalStateException("[SHARD-BOOT-STREAM-MISSING] no stream captures"
                        + " '" + ShardSubjects.shardWildcard(shard) + "' — provision the"
                        + " per-shard stream (activation step 3) before starting shard nodes");
            }
            if (streams.size() > 1) {
                throw new IllegalStateException("[SHARD-BOOT-STREAM-OVERLAP] " + streams.size()
                        + " streams capture '" + ShardSubjects.shardWildcard(shard) + "' ("
                        + streams + ") — shard subjects must live in exactly one stream (G8)");
            }
            byShard.put(shard, streams.get(0));
        }
        // pairwise distinct: one stream per shard, never shared (blast-radius isolation)
        Map<String, List<Integer>> byStream = new HashMap<>();
        byShard.forEach((shard, stream) ->
                byStream.computeIfAbsent(stream, s -> new ArrayList<>()).add(shard));
        byStream.forEach((stream, shards) -> {
            if (shards.size() > 1) {
                throw new IllegalStateException("[SHARD-BOOT-STREAM-SHARED] stream '" + stream
                        + "' captures shards " + shards + " — a shared stream recreates the"
                        + " fleet-wide cap blast radius (docs/13 round-4 §b); provision one"
                        + " stream per shard");
            }
        });
        return byShard;
    }

    private void validateStreamProperties(JetStreamManagement jsm, Map<Integer, String> shardStreams) {
        for (Map.Entry<Integer, String> entry : shardStreams.entrySet()) {
            StreamInfo info;
            try {
                info = jsm.getStreamInfo(entry.getValue());
            } catch (Exception e) {
                throw new IllegalStateException("[SHARD-BOOT-BROKER] stream info failed for '"
                        + entry.getValue() + "'", e);
            }
            var config = info.getConfiguration();
            String at = "stream '" + entry.getValue() + "' (shard " + entry.getKey() + ")";
            if (config.getRetentionPolicy() != RetentionPolicy.WorkQueue) {
                throw new IllegalStateException("[SHARD-BOOT-STREAM-PROPS] " + at
                        + " retention is " + config.getRetentionPolicy()
                        + " — WorkQueue required (docs/13 F-3)");
            }
            if (config.getDiscardPolicy() != DiscardPolicy.New) {
                throw new IllegalStateException("[SHARD-BOOT-STREAM-PROPS] " + at
                        + " discard is " + config.getDiscardPolicy() + " — discard=new required"
                        + " (T-3: discard=old ACKs and silently DELETES at the cap)");
            }
            if (config.getMaxBytes() <= 0) {
                throw new IllegalStateException("[SHARD-BOOT-STREAM-PROPS] " + at
                        + " has no max_bytes cap — explicit caps required"
                        + " (LIMITS_STREAM_WITHOUT_SIZE_CAP lesson)");
            }
            Duration window = config.getDuplicateWindow();
            if (window == null || window.isZero()
                    || window.getSeconds() <= redeliveryHorizonSeconds) {
                throw new IllegalStateException("[SHARD-BOOT-STREAM-PROPS] " + at
                        + " duplicate_window (" + window + ") must exceed the redelivery"
                        + " horizon (" + redeliveryHorizonSeconds + "s = max ackWait ×"
                        + " (maxDeliver+1)) — the exactly-once carrier (docs/13 T-8)");
            }
        }
    }

    private void detectSkippedActivation(JetStreamManagement jsm) {
        for (String filter : LEGACY_REPLY_SUBJECT_FILTERS) {
            List<String> streams;
            try {
                streams = jsm.getStreamNames(filter);
            } catch (Exception e) {
                throw new IllegalStateException("[SHARD-BOOT-BROKER] legacy stream lookup"
                        + " failed for '" + filter + "'", e);
            }
            for (String stream : streams) {
                List<ConsumerInfo> consumers;
                try {
                    consumers = jsm.getConsumers(stream);
                } catch (Exception e) {
                    throw new IllegalStateException("[SHARD-BOOT-BROKER] consumer listing"
                            + " failed for stream '" + stream + "'", e);
                }
                for (ConsumerInfo consumer : consumers) {
                    String name = consumer.getName();
                    boolean legacy = LEGACY_DURABLE_PREFIXES.stream().anyMatch(name::startsWith);
                    if (legacy) {
                        throw new IllegalStateException("[SHARD-BOOT-ACTIVATION-SKIPPED] legacy"
                                + " durable '" + name + "' still exists on stream '" + stream
                                + "' — the activation procedure (docs/13 §2.3 step 2: delete"
                                + " ALL four legacy durable families) was skipped; starting"
                                + " sharded now would die later with a raw [10100]/[SUB-90011]");
                    }
                }
            }
        }
    }
}
