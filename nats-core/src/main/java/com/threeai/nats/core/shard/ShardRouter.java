package com.threeai.nats.core.shard;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.threeai.nats.core.NatsHeaderUtils;
import com.threeai.nats.core.dlq.DlqPublishOutcome;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.dlq.DlqReason;
import com.threeai.nats.core.headers.BpmHeaders;
import com.threeai.nats.core.metrics.NatsChannelMetrics;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.NatsJetStreamConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The shard router (docs/13 D-C v5): stateless, runs on every node, queue-grouped across
 * the fleet — no leader. Consumes the configured external-inbound subjects over JetStream
 * (T-8: JetStream ONLY — the delivery budget, redelivery and DLQ semantics live there),
 * resolves the shard key (header first, then the injected top-level payload reader),
 * republishes to {@code shard.<target>.<subject>} with a DERIVED Msg-Id
 * ({@link ShardMsgId#derive} — never a passthrough, round-1 PROBE-A), and only THEN acks.
 *
 * <p><b>Custody (T-3):</b> a rejected republish (target stream at its cap with
 * {@code discard=new}, broker error) is NOT a loss: the source message is NAKed with
 * backoff, counted ({@code nats.shard.publish_reject}) and redelivered. A message with no
 * resolvable key goes to {@code dlq.shard.<subject>} ({@code VAL_SHARD_KEY_MISSING}) —
 * permanent, retrying cannot supply a key.
 */
public class ShardRouter {

    private static final Logger log = LoggerFactory.getLogger(ShardRouter.class);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);
    private static final String CHANNEL_TAG = "shard-router";

    private final Connection connection;
    private final JetStream jetStream;
    private final ShardTopology topology;
    private final List<ShardRouteConfig> routes;
    private final PayloadFieldReader payloadFieldReader;
    private final DlqPublisher dlqPublisher;
    private final NatsChannelMetrics metrics;
    private final long ackWaitSeconds;

    private Dispatcher dispatcher;
    private ExecutorService executor;

    public ShardRouter(Connection connection, JetStream jetStream, ShardTopology topology,
            List<ShardRouteConfig> routes, PayloadFieldReader payloadFieldReader,
            DlqPublisher dlqPublisher, NatsChannelMetrics metrics, long ackWaitSeconds) {
        this.connection = connection;
        this.jetStream = jetStream;
        this.topology = topology;
        this.routes = List.copyOf(routes);
        this.payloadFieldReader = payloadFieldReader;
        this.dlqPublisher = dlqPublisher;
        this.metrics = metrics;
        this.ackWaitSeconds = ackWaitSeconds;
    }

    public void subscribe() {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.dispatcher = connection.createDispatcher();
        for (ShardRouteConfig route : routes) {
            try {
                ConsumerConfiguration cc = ConsumerConfiguration.builder()
                        .ackWait(Duration.ofSeconds(ackWaitSeconds))
                        .durable(route.durableName())
                        .deliverGroup(route.durableName())
                        .build();
                PushSubscribeOptions opts = PushSubscribeOptions.builder().configuration(cc).build();
                jetStream.subscribe(route.subject(), route.durableName(), dispatcher,
                        msg -> executor.submit(() -> handle(msg, route)), false, opts);
                log.info("Shard router subscribed", kv("subject", route.subject()),
                        kv("durable", route.durableName()));
            } catch (Exception e) {
                // Fail loud: a route that cannot bind means external messages for it would
                // flow nowhere. The bootstrap validator has already checked stream topology,
                // so this is unexpected — propagate and let boot fail.
                throw new IllegalStateException("[SYS_SHARD_ROUTER_BIND] failed to subscribe"
                        + " route '" + route.subject() + "'", e);
            }
        }
    }

    public void unsubscribe() {
        if (dispatcher != null) {
            try {
                connection.closeDispatcher(dispatcher);
            } catch (Exception e) {
                log.warn("Error closing shard-router dispatcher", e);
            }
            dispatcher = null;
        }
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            executor = null;
        }
    }

    void handle(Message msg, ShardRouteConfig route) {
        try {
            String key = resolveKey(msg, route);
            if (key == null || key.isBlank()) {
                if (metrics != null) {
                    metrics.shardKeyMissingCount(route.subject()).increment();
                }
                routeToDlq(msg, route);
                return;
            }

            int target = topology.shardOf(key);
            Headers headers = copyHeaders(msg.getHeaders());
            headers.put(NatsJetStreamConstants.MSG_ID_HDR,
                    ShardMsgId.derive(originalMsgId(msg), target));
            if (NatsHeaderUtils.extractHeader(msg, BpmHeaders.BUSINESS_KEY) == null) {
                // key came from the payload — pin it as a header so the shard side (and any
                // later DLQ hop) never has to re-parse the body
                headers.put(BpmHeaders.BUSINESS_KEY, key);
            }

            jetStream.publish(NatsMessage.builder()
                    .subject(ShardSubjects.scoped(target, msg.getSubject()))
                    .headers(headers)
                    .data(msg.getData())
                    .build());

            if (metrics != null) {
                metrics.shardRouteCount(route.subject(), target).increment();
            }
            msg.ack();
        } catch (Exception e) {
            // T-3 custody: publish rejection (cap/discard=new/broker error) is not a loss —
            // nak, count, let redelivery retry. Never ack on failure.
            if (metrics != null) {
                metrics.shardPublishRejectCount(route.subject()).increment();
            }
            log.warn("Shard route publish failed — NAKing for redelivery (custody kept)",
                    kv("subject", msg.getSubject()), kv("route", route.subject()), e);
            nakWithBackoff(msg);
        }
    }

    private String resolveKey(Message msg, ShardRouteConfig route) {
        String fromHeader = NatsHeaderUtils.extractHeader(msg, BpmHeaders.BUSINESS_KEY);
        if (fromHeader != null && !fromHeader.isBlank()) {
            return fromHeader;
        }
        if (route.businessKeyField() != null && payloadFieldReader != null) {
            return payloadFieldReader.topLevelScalar(
                    new String(msg.getData(), StandardCharsets.UTF_8), route.businessKeyField());
        }
        return null;
    }

    private void routeToDlq(Message msg, ShardRouteConfig route) {
        DlqPublishOutcome outcome = dlqPublisher.publish(msg, ShardSubjects.dlq(msg.getSubject()),
                DlqReason.SHARD_KEY_MISSING, route.subject(), CHANNEL_TAG);
        switch (outcome) {
            case PUBLISHED_JETSTREAM, PUBLISHED_CORE_FALLBACK -> msg.ack();
            case FAILED_NO_DLQ_SUBJECT, FAILED_BOTH_PUBLISH -> nakWithBackoff(msg);
        }
    }

    private String originalMsgId(Message msg) {
        String id = NatsHeaderUtils.extractHeader(msg, NatsJetStreamConstants.MSG_ID_HDR);
        if (id != null && !id.isBlank()) {
            return id;
        }
        // Deterministic fallback: same source stream sequence on redelivery -> same derived
        // id -> target-stream dedup still holds.
        try {
            return msg.metaData().getStream() + "-" + msg.metaData().streamSequence();
        } catch (Exception e) {
            return msg.getSubject() + "-" + System.nanoTime(); // last resort: unique, no dedup
        }
    }

    private static Headers copyHeaders(Headers original) {
        Headers copy = new Headers();
        if (original != null) {
            original.forEach((k, values) -> values.forEach(v -> copy.add(k, v)));
        }
        return copy;
    }

    private void nakWithBackoff(Message msg) {
        long deliveryCount = 1;
        try {
            deliveryCount = msg.metaData().deliveredCount();
        } catch (Exception ignored) {
            // not a JetStream message in tests — plain backoff
        }
        long seconds = Math.min(MAX_BACKOFF.getSeconds(), 1L << Math.min(5, deliveryCount - 1));
        msg.nakWithDelay(Duration.ofSeconds(seconds));
    }
}
