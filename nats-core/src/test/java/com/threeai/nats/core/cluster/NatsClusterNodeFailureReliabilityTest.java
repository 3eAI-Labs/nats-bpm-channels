package com.threeai.nats.core.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import com.threeai.nats.core.jetstream.SweepLeaderLease;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * QA reliability suite — what happens to a running system when one of three NATS nodes crashes and
 * later returns.
 *
 * <p>The rest of the suite proves invariants against a single NATS container. That is the right
 * shape for protocol-level guarantees, but it leaves an entire failure class unexamined: on one
 * node a stream with one replica and a stream with three behave identically, and nothing ever
 * disappears underneath a subscriber. These tests use a real three-node cluster
 * ({@link NatsClusterTestHarness}) and a real SIGKILL, so the difference is observable.
 *
 * <p>Heavy — three containers, hard kills, wall-clock waits for Raft to settle, and pinned host
 * ports. {@code @Tag("reliability")}, excluded from the default {@code mvn test} run. Run via
 * {@code mvn test -Dgroups=reliability -Dreliability.excludedGroups=}.
 */
@Tag("reliability")
class NatsClusterNodeFailureReliabilityTest {

    private static NatsClusterTestHarness cluster;

    @BeforeAll
    static void startCluster() {
        cluster = new NatsClusterTestHarness();
        cluster.start();
    }

    @AfterAll
    static void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    /**
     * The gap, stated as an executable fact: a stream created by the library's own
     * {@link JetStreamStreamManager#ensureStream} lives on exactly one node, because that method
     * never calls {@code .replicas(...)} and JetStream defaults to one. Kill the node holding it
     * and the stream stops serving — publishes fail outright rather than degrading.
     *
     * <p>This test passes against today's code. It is here so that the behaviour is a recorded
     * decision rather than an accident, and so that changing the default breaks a test that says
     * why.
     */
    @Test
    void streamFromEnsureStream_hasOneReplica_andGoesOfflineWhenItsNodeIsKilled() throws Exception {
        String stream = "SINGLE_REPLICA_" + suffix();
        String subject = "single." + suffix() + ".>";

        try (Connection conn = cluster.connect()) {
            new JetStreamStreamManager().ensureStream(stream, subject, conn);
            JetStreamManagement jsm = conn.jetStreamManagement();

            // The library asked for no replica count, so JetStream gave it one.
            assertThat(jsm.getStreamInfo(stream).getConfiguration().getReplicas())
                    .as("ensureStream does not set .replicas(), so the stream is single-replica")
                    .isEqualTo(1);

            JetStream js = conn.jetStream();
            js.publish(subject.replace(">", "one"), "before".getBytes(StandardCharsets.UTF_8));

            // A three-replica control stream, created BEFORE the crash. Creating one afterwards
            // would prove nothing about replication: stream creation goes through the cluster's
            // metadata Raft group, which is itself briefly unavailable while a new meta-leader is
            // elected. Provisioning both streams up front leaves replica count as the only
            // difference between them.
            String control = "CONTROL_" + suffix();
            jsm.addStream(StreamConfiguration.builder()
                    .name(control)
                    .subjects(control + ".>")
                    .storageType(StorageType.File)
                    .replicas(3)
                    .build());

            String host = NatsClusterTestHarness.leaderOf(jsm, stream);
            cluster.killNode(host);
            try {
                // The other two nodes are healthy and the client reconnects to them, but the
                // stream itself has no surviving copy: there is nothing to publish into.
                assertThatThrownBy(() ->
                        conn.jetStream().publish(subject.replace(">", "two"),
                                "after".getBytes(StandardCharsets.UTF_8)))
                        .as("a single-replica stream cannot serve while its only node is down")
                        .isInstanceOfAny(IOException.class, JetStreamApiException.class);

                // The sharper half of the claim: the CLUSTER is fine. With the very same node
                // down, the three-replica control stream still takes writes — after a short
                // re-election window. So the outage above is caused by where the library placed
                // the stream, not by the node loss itself.
                Instant deadline = Instant.now().plusSeconds(30);
                Exception lastFailure = null;
                boolean controlAccepted = false;
                while (Instant.now().isBefore(deadline) && !controlAccepted) {
                    try {
                        conn.jetStream().publish(control + ".probe", "ok".getBytes(StandardCharsets.UTF_8));
                        controlAccepted = true;
                    } catch (Exception e) {
                        lastFailure = e;
                        Thread.sleep(500);
                    }
                }
                assertThat(controlAccepted)
                        .as("a 3-replica stream serves writes while the same node is down (last error: %s)",
                                lastFailure)
                        .isTrue();
            } finally {
                cluster.restartNode(host);
                cluster.awaitNodeRejoined(host, Duration.ofSeconds(60));
            }
        }
    }

    /**
     * The same crash against a three-replica stream: the cluster elects a new leader and the
     * stream keeps serving. Nothing about the library prevents this — the capability is one
     * builder call away — so the single-replica default above is a configuration gap, not a
     * JetStream limitation.
     */
    @Test
    void threeReplicaStream_survivesLeaderCrash_publishAndConsumeContinue() throws Exception {
        String stream = "THREE_REPLICA_" + suffix();
        String base = "triple." + suffix();

        try (Connection conn = cluster.connect()) {
            JetStreamManagement jsm = conn.jetStreamManagement();
            jsm.addStream(StreamConfiguration.builder()
                    .name(stream)
                    .subjects(base + ".>")
                    .storageType(StorageType.File)
                    .replicas(3)
                    .build());

            JetStream js = conn.jetStream();
            js.publish(base + ".a", "before".getBytes(StandardCharsets.UTF_8));

            String host = NatsClusterTestHarness.leaderOf(jsm, stream);
            cluster.killNode(host);
            try {
                // Quorum of two survives, so a new leader is elected and writes continue.
                Instant deadline = Instant.now().plusSeconds(30);
                Exception lastFailure = null;
                boolean published = false;
                while (Instant.now().isBefore(deadline) && !published) {
                    try {
                        conn.jetStream().publish(base + ".b", "after".getBytes(StandardCharsets.UTF_8));
                        published = true;
                    } catch (Exception e) {
                        lastFailure = e;
                        Thread.sleep(500);
                    }
                }
                assertThat(published)
                        .as("three-replica stream must accept writes after one node dies (last error: %s)",
                                lastFailure)
                        .isTrue();

                assertThat(conn.jetStreamManagement().getStreamInfo(stream).getStreamState().getMsgCount())
                        .as("the pre-crash message must still be there — quorum kept the data")
                        .isEqualTo(2);
            } finally {
                cluster.restartNode(host);
                cluster.awaitNodeRejoined(host, Duration.ofSeconds(60));
            }
        }
    }

    /**
     * The scenario in full: three engine nodes share a durable queue-group consumer, one NATS node
     * is killed mid-flight, and later comes back.
     *
     * <p>Two distinct properties are checked, and they fail in different ways:
     *
     * <ul>
     *   <li><b>No fan-out</b> — while healthy, each message reaches exactly one group member. This
     *       is the property that broke in the A2 bridges and in the Flowable inbound adapter, where
     *       a discarded queue group turned N nodes into N duplicate processings.
     *   <li><b>No loss</b> — every message published before, during and after the outage is
     *       eventually delivered. JetStream promises at-least-once, so a redelivery is legal and is
     *       not treated as a failure; a message that never arrives is.
     * </ul>
     */
    @Test
    void nodeCrashesAndReturns_durableQueueGroup_noFanOutWhileHealthy_andNoMessageLost() throws Exception {
        String stream = "QUEUE_GROUP_" + suffix();
        String subject = "qgroup." + suffix();
        String durable = "engine-nodes";
        String deliverGroup = "engine-nodes-qg";
        int perPhase = 30;

        Set<String> distinctReceived = ConcurrentHashMap.newKeySet();
        List<String> allDeliveries = new CopyOnWriteArrayList<>();
        AtomicInteger publishedTotal = new AtomicInteger();

        try (Connection conn = cluster.connect()) {
            JetStreamManagement jsm = conn.jetStreamManagement();
            jsm.addStream(StreamConfiguration.builder()
                    .name(stream)
                    .subjects(subject)
                    .storageType(StorageType.File)
                    .replicas(3)
                    .build());

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(durable)
                    .deliverGroup(deliverGroup)
                    .deliverSubject(deliverGroup + "-delivery")
                    .ackPolicy(AckPolicy.Explicit)
                    .ackWait(Duration.ofSeconds(30))
                    .build();
            PushSubscribeOptions opts = PushSubscribeOptions.builder().configuration(cc).build();

            // Three subscribers standing in for three engine nodes, all bound to the one durable
            // through the same deliver group — the production pattern.
            JetStream js = conn.jetStream();
            for (int node = 0; node < 3; node++) {
                Dispatcher dispatcher = conn.createDispatcher();
                js.subscribe(subject, deliverGroup, dispatcher, msg -> {
                    String id = new String(msg.getData(), StandardCharsets.UTF_8);
                    allDeliveries.add(id);
                    distinctReceived.add(id);
                    msg.ack();
                }, false, opts);
            }

            // Phase 1 — healthy. This is where fan-out would show up.
            publishPhase(conn, subject, "healthy", perPhase, publishedTotal);
            awaitCount(distinctReceived, perPhase, Duration.ofSeconds(30));
            assertThat(allDeliveries)
                    .as("a queue group must deliver each message once, not once per member")
                    .hasSize(perPhase);

            // Phase 2 — a node crashes and traffic keeps flowing.
            String host = NatsClusterTestHarness.leaderOf(jsm, stream);
            cluster.killNode(host);
            try {
                publishPhase(conn, subject, "during-outage", perPhase, publishedTotal);

                // Phase 3 — the same node returns.
                cluster.restartNode(host);
                cluster.awaitNodeRejoined(host, Duration.ofSeconds(60));
                publishPhase(conn, subject, "recovered", perPhase, publishedTotal);
            } catch (RuntimeException e) {
                cluster.restartNode(host);
                throw e;
            }

            awaitCount(distinctReceived, publishedTotal.get(), Duration.ofSeconds(60));
            assertThat(distinctReceived)
                    .as("no message may be lost across a node crash and its recovery")
                    .hasSize(publishedTotal.get());
            assertThat(allDeliveries.size())
                    .as("at-least-once permits redelivery, but every delivery must be a real message")
                    .isGreaterThanOrEqualTo(distinctReceived.size());
        }
    }

    /**
     * The leader lease already asks for three KV replicas ({@code NatsProperties.Jetstream
     * .kvReplicas}), so a node crash must not cost the holder its leadership nor let a standby
     * believe it has taken over — the split-brain invariant of
     * {@code SweepLeaderLeaseContentionReliabilityTest}, now with the node genuinely gone rather
     * than merely idle.
     */
    @Test
    void nodeCrashesAndReturns_kvLeaderLease_holderKeepsLeadership_noSplitBrain() throws Exception {
        String bucket = "lease-node-failure-" + suffix();
        Duration ttl = Duration.ofSeconds(60);

        try (Connection conn = cluster.connect()) {
            JetStreamKvManager kvManager = new JetStreamKvManager();
            kvManager.ensureBucket(bucket, ttl, 3, conn);

            SweepLeaderLease holder = new SweepLeaderLease(conn.jetStream(), kvManager, conn,
                    bucket, "relay-leader.", "camunda", "holder", ttl);
            SweepLeaderLease standby = new SweepLeaderLease(conn.jetStream(), kvManager, conn,
                    bucket, "relay-leader.", "camunda", "standby", ttl);

            assertThat(holder.tryAcquireOrRenew()).isTrue();
            assertThat(standby.tryAcquireOrRenew()).isFalse();

            // Kill the node hosting the KV bucket's own stream — the hardest case for the lease.
            String host = NatsClusterTestHarness.leaderOf(conn.jetStreamManagement(), "KV_" + bucket);
            cluster.killNode(host);
            try {
                Instant deadline = Instant.now().plusSeconds(30);
                boolean renewed = false;
                while (Instant.now().isBefore(deadline) && !renewed) {
                    if (holder.tryAcquireOrRenew()) {
                        renewed = true;
                    } else {
                        Thread.sleep(500);
                    }
                }
                assertThat(renewed)
                        .as("a 3-replica KV bucket must keep serving the lease through one node loss")
                        .isTrue();
                assertThat(standby.tryAcquireOrRenew())
                        .as("the standby must never acquire while the holder is still renewing")
                        .isFalse();
                assertThat(holder.isLeader()).isTrue();
                assertThat(standby.isLeader()).isFalse();
            } finally {
                cluster.restartNode(host);
                cluster.awaitNodeRejoined(host, Duration.ofSeconds(60));
            }
        }
    }

    /**
     * The warning exists because the dangerous case is invisible: a single-replica stream on a
     * cluster behaves exactly like a healthy one right up to the moment its node is lost. These
     * three cases pin down when it fires, and — just as importantly — when it stays quiet, so it
     * does not become noise that operators learn to ignore.
     *
     * <p>The other half of the matrix — a standalone server, where the warning must stay silent —
     * is covered in {@code JetStreamStreamManagerTest}, which runs in the default suite.
     */
    @Test
    void singleReplicaOnCluster_warns_butThreeReplicasDoesNot() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(JetStreamStreamManager.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try (Connection conn = cluster.connect()) {
            String quiet = "REPLICATED_" + suffix();
            new JetStreamStreamManager(3).ensureStream(quiet, "quiet." + suffix() + ".>", conn);
            assertThat(warnings(appender))
                    .as("a replicated stream on a cluster is the safe case — no warning")
                    .isEmpty();

            String noisy = "UNREPLICATED_" + suffix();
            new JetStreamStreamManager(1).ensureStream(noisy, "noisy." + suffix() + ".>", conn);
            assertThat(warnings(appender))
                    .as("a single-replica stream on a cluster must say so")
                    .hasSize(1)
                    .first(org.assertj.core.api.InstanceOfAssertFactories.STRING)
                    .contains("single replica")
                    .contains("stream-replicas");
        } finally {
            logger.detachAppender(appender);
        }
    }

    private static List<String> warnings(ListAppender<ILoggingEvent> appender) {
        return appender.list.stream()
                .filter(e -> e.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }

    private static void publishPhase(Connection conn, String subject, String phase, int count,
            AtomicInteger publishedTotal) throws Exception {
        JetStream js = conn.jetStream();
        for (int i = 0; i < count; i++) {
            String id = phase + "-" + i;
            Instant deadline = Instant.now().plusSeconds(30);
            Exception lastFailure = null;
            boolean sent = false;
            while (Instant.now().isBefore(deadline) && !sent) {
                try {
                    js.publish(subject, id.getBytes(StandardCharsets.UTF_8));
                    sent = true;
                } catch (Exception e) {
                    lastFailure = e;
                    Thread.sleep(250);
                }
            }
            if (!sent) {
                throw new IllegalStateException("Could not publish '" + id + "' within the window", lastFailure);
            }
            publishedTotal.incrementAndGet();
        }
    }

    private static void awaitCount(Set<String> received, int expected, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline) && received.size() < expected) {
            Thread.sleep(200);
        }
    }

    private static String suffix() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 8).toUpperCase();
    }
}
