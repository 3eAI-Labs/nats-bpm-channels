package com.threeai.nats.cibseven.shard;

import static net.logstash.logback.argument.StructuredArguments.kv;

import com.threeai.nats.cibseven.inbound.SubscriptionConfig;
import com.threeai.nats.core.config.NamespaceValidator;
import com.threeai.nats.core.shard.ShardSubjects;
import com.threeai.nats.core.shard.ShardTopology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The subject/durable rewrite of sharded mode (docs/13 §2.4): applied at
 * subscription-build time to EVERY inbound correlation subscription — legacy YAML and the
 * ER-parity definition path alike. Order matters (Y-9): the reservation check runs on the
 * ORIGINAL subject BEFORE the shard prefix goes on.
 *
 * <p>Rules: subject → {@code shard.<ownId>.<subject>}; durable → {@code s<ownId>-<base>}
 * (portable definition files carry the BASE name — they structurally cannot carry a shard
 * id); deliverGroup follows the durable (docs/11 rule: both, always). {@code jetstream}
 * MUST already be true (T-8: core NATS has no delivery budget — enforced engine-instance-
 * wide for correlation subscriptions; outbound channels are unaffected).
 * {@code autoCreateStream} is forced OFF with one WARN — per-shard streams are provisioned
 * by the operator (activation step 3) and validated by the bootstrap validator; a
 * subscribe-time auto-create would collide with them as an overlapping-subject stream.
 */
public final class ShardSubscriptionRewriter {

    private static final Logger log = LoggerFactory.getLogger(ShardSubscriptionRewriter.class);

    private ShardSubscriptionRewriter() {
    }

    public static void rewrite(SubscriptionConfig config, ShardTopology topology) {
        NamespaceValidator.assertNotReservedForSharding(config.getSubject(),
                "sharded subscription rewrite"); // Y-9: original subject, BEFORE prefixing
        if (!config.isJetstream()) {
            throw new IllegalStateException("[VAL_SHARD_JETSTREAM_REQUIRED] sharding is enabled"
                    + " but correlation subscription on '" + config.getSubject() + "' has"
                    + " jetstream=false — core NATS has no delivery budget/redelivery, so every"
                    + " correlation subscription on a sharded engine must be JetStream"
                    + " (activation step 0; outbound channels are unaffected)");
        }
        if (config.isAutoCreateStream()) {
            log.warn("Sharded mode ignores autoCreateStream — per-shard streams are"
                    + " operator-provisioned (activation step 3) and validated at boot",
                    kv("subject", config.getSubject()));
            config.setAutoCreateStream(false);
            config.setStreamName(null);
        }
        config.setSubject(ShardSubjects.scoped(topology.getShardId(), config.getSubject()));
        String baseDurable = config.getDurableName() != null ? config.getDurableName()
                : config.resolveDeliverGroup();
        String shardedDurable = "s" + topology.getShardId() + "-" + baseDurable;
        config.setDurableName(shardedDurable);
        config.setDeliverGroup(shardedDurable);
    }
}
