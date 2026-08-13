package com.threeai.nats.core.cluster;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

/**
 * A real three-node NATS JetStream cluster for reliability tests, with the ability to hard-kill a
 * node and bring the SAME node back.
 *
 * <p>Every other Testcontainers test in this repository runs a single {@code nats:2.10-alpine}
 * with {@code --jetstream}. That is enough to exercise protocol behaviour but it cannot distinguish
 * a stream with one replica from a stream with three — on one node both work identically. The
 * difference only becomes observable when a node dies, which is what this harness makes possible.
 *
 * <h2>Why not {@code GenericContainer.stop()}/{@code start()}</h2>
 *
 * <p>{@code GenericContainer.stop()} routes through {@code ResourceReaper.stopAndRemoveContainer}
 * and nulls {@code containerId}; a following {@code start()} creates a <em>brand new</em> container
 * with an empty filesystem. That models "node replaced with a blank one", not "node came back".
 * JetStream's whole recovery story depends on the returning node still having its store directory,
 * so this harness drives Docker directly: {@code killContainerCmd} (SIGKILL — a crash, not a
 * graceful shutdown) and {@code startContainerCmd} on the same container id, whose writable layer
 * — and therefore {@code /data} — survives.
 *
 * <h2>Host ports move when a node restarts</h2>
 *
 * <p>Docker assigns a new published port every time a container starts, so the address the test
 * JVM holds for a killed node is dead once it returns. Pinning the ports would freeze them, but
 * makes the harness order-dependent and prone to "address already in use" whenever an earlier run
 * has not released them — so the port is re-read from Docker after each restart instead.
 *
 * <p>Tolerating the staleness was tried first and does not work. It looks safe for a single
 * failure — the client only needs a quorum, and one node is down at a time — but each test in a
 * class restarts a node, and the dead addresses accumulate until none of the three resolve. The
 * refresh in {@link #restartNode} is what keeps the harness usable across more than one test.
 */
final class NatsClusterTestHarness implements AutoCloseable {

    private static final String[] SERVER_NAMES = {"n1", "n2", "n3"};
    private static final int CLIENT_PORT = 4222;
    private static final int ROUTE_PORT = 6222;
    private static final String PROBE_STREAM = "REJOIN_PROBE";

    private final Network network;
    private final Map<String, GenericContainer<?>> nodesByServerName = new LinkedHashMap<>();
    private final List<String> serverUrls = new ArrayList<>(SERVER_NAMES.length);
    private final Map<String, String> hostsByServerName = new LinkedHashMap<>();
    private final DockerClient docker = DockerClientFactory.instance().client();

    NatsClusterTestHarness() {
        this.network = Network.newNetwork();
        String routes = "nats://n1:" + ROUTE_PORT + ",nats://n2:" + ROUTE_PORT + ",nats://n3:" + ROUTE_PORT;

        for (String serverName : SERVER_NAMES) {
            GenericContainer<?> node = new GenericContainer<>("nats:2.10-alpine")
                    .withNetwork(network)
                    .withNetworkAliases(serverName)
                    .withExposedPorts(CLIENT_PORT)
                    .withCommand(
                            "--server_name", serverName,
                            "--jetstream",
                            "--store_dir", "/data",
                            "--cluster_name", "reliability-cluster",
                            "--cluster", "nats://0.0.0.0:" + ROUTE_PORT,
                            "--routes", routes);
            nodesByServerName.put(serverName, node);
        }
    }

    /** Starts all three nodes and blocks until the cluster can actually place a 3-replica stream. */
    void start() {
        nodesByServerName.values().forEach(GenericContainer::start);
        for (String serverName : SERVER_NAMES) {
            GenericContainer<?> node = nodesByServerName.get(serverName);
            hostsByServerName.put(serverName, node.getHost());
            serverUrls.add("nats://" + node.getHost() + ":" + node.getMappedPort(CLIENT_PORT));
        }
        awaitClusterReady(Duration.ofSeconds(60));
        createProbeStream();
    }

    /**
     * A three-replica stream whose only job is to be a stable Raft group that
     * {@link #awaitNodeRejoined} can watch converge.
     */
    private void createProbeStream() {
        try (Connection conn = connect()) {
            conn.jetStreamManagement().addStream(StreamConfiguration.builder()
                    .name(PROBE_STREAM)
                    .subjects(PROBE_STREAM + ".>")
                    .storageType(StorageType.File)
                    .replicas(3)
                    .build());
        } catch (Exception e) {
            throw new IllegalStateException("Could not create the rejoin probe stream", e);
        }
    }

    /**
     * Every node's host address, in server-name order. Handed to the client as a complete server
     * list rather than relying on cluster discovery: a discovered peer advertises its
     * container-internal address, which the host JVM cannot reach.
     *
     * <p>Resolved once at startup. An address for a node that is later killed and restarted goes
     * stale — see the class javadoc for why that costs nothing.
     */
    List<String> serverUrls() {
        return List.copyOf(serverUrls);
    }

    /**
     * A connection pinned to the addresses above. {@code ignoreDiscoveredServers} keeps the client
     * from wandering onto unreachable internal addresses; the short reconnect wait keeps failover
     * inside a test's patience.
     */
    Connection connect() throws Exception {
        Options options = new Options.Builder()
                .servers(serverUrls().toArray(new String[0]))
                .ignoreDiscoveredServers()
                .maxReconnects(-1)
                .reconnectWait(Duration.ofMillis(250))
                .connectionTimeout(Duration.ofSeconds(5))
                .build();
        return Nats.connect(options);
    }

    /** SIGKILL — the node crashes without flushing or leaving the cluster politely. */
    void killNode(String serverName) {
        docker.killContainerCmd(containerIdOf(serverName)).exec();
    }

    /**
     * Brings the same container back, store directory and all, and re-reads the host port Docker
     * has just assigned it so the client's server list stays resolvable.
     */
    void restartNode(String serverName) {
        String containerId = containerIdOf(serverName);
        docker.startContainerCmd(containerId).exec();

        Ports.Binding[] bindings = docker.inspectContainerCmd(containerId).exec()
                .getNetworkSettings().getPorts().getBindings()
                .get(new ExposedPort(CLIENT_PORT));
        if (bindings == null || bindings.length == 0) {
            throw new IllegalStateException("No host binding for " + serverName + " after restart");
        }
        int index = List.of(SERVER_NAMES).indexOf(serverName);
        serverUrls.set(index, "nats://" + hostsByServerName.get(serverName)
                + ":" + bindings[0].getHostPortSpec());
    }

    /**
     * Blocks until {@code serverName} is a current (caught-up) peer of the long-lived probe
     * stream — a stronger signal than "the container is running again", which is true well before
     * the node has replayed what it missed.
     *
     * <p>The probe stream is created once, at cluster start, and deliberately outlives every
     * node kill. An earlier version created a fresh probe on each poll and read its cluster info
     * immediately: Raft had no time to mark peers current before the stream was deleted and
     * replaced, so the check could never succeed. Convergence needs a stable Raft group to
     * converge <em>on</em>.
     */
    void awaitNodeRejoined(String serverName, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        Exception last = null;
        try (Connection conn = connect()) {
            JetStreamManagement jsm = conn.jetStreamManagement();
            while (Instant.now().isBefore(deadline)) {
                try {
                    var cluster = jsm.getStreamInfo(PROBE_STREAM).getClusterInfo();
                    if (cluster != null && cluster.getReplicas() != null
                            && cluster.getReplicas().stream()
                                    .anyMatch(r -> serverName.equals(r.getName()) && r.isCurrent())) {
                        // Caught up on an existing Raft group is not the same as available for a
                        // new one: the metadata layer's placement view lags behind, and a stream
                        // created in that window fails with "no suitable peers for placement,
                        // peer offline [10005]". Wait for both before calling the node back.
                        awaitClusterReady(Duration.between(Instant.now(), deadline));
                        return;
                    }
                } catch (Exception e) {
                    last = e;
                    // Metadata plane may still be re-electing — expected while the node catches up.
                }
                Thread.sleep(500);
            }
        }
        throw new IllegalStateException(
                "Node " + serverName + " did not rejoin within " + timeout, last);
    }

    /** The server name (n1/n2/n3) currently hosting a stream's leader. */
    static String leaderOf(JetStreamManagement jsm, String streamName) throws Exception {
        var cluster = jsm.getStreamInfo(streamName).getClusterInfo();
        if (cluster == null || cluster.getLeader() == null) {
            throw new IllegalStateException("Stream '" + streamName + "' reports no cluster leader");
        }
        return cluster.getLeader();
    }

    private String containerIdOf(String serverName) {
        GenericContainer<?> node = nodesByServerName.get(serverName);
        if (node == null) {
            throw new IllegalArgumentException("Unknown node: " + serverName);
        }
        return node.getContainerId();
    }

    /**
     * The cluster is ready when it can place a 3-replica stream — a stricter and more useful
     * signal than "the client port accepts connections", which is true long before the Raft
     * groups have formed.
     */
    private void awaitClusterReady(Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        Exception last = null;
        while (Instant.now().isBefore(deadline)) {
            try (Connection conn = connect()) {
                String probe = "READY_PROBE_" + UUID.randomUUID().toString().replace("-", "");
                JetStreamManagement jsm = conn.jetStreamManagement();
                jsm.addStream(StreamConfiguration.builder()
                        .name(probe)
                        .subjects(probe + ".>")
                        .storageType(StorageType.Memory)
                        .replicas(3)
                        .build());
                jsm.deleteStream(probe);
                return;
            } catch (Exception e) {
                last = e;
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted awaiting cluster", ie);
                }
            }
        }
        throw new IllegalStateException("Cluster not ready within " + timeout, last);
    }

    @Override
    public void close() {
        nodesByServerName.values().forEach(node -> {
            try {
                node.stop();
            } catch (Exception ignored) {
                // Best-effort teardown; a killed node may already be gone.
            }
        });
        try {
            network.close();
        } catch (Exception ignored) {
            // Best-effort teardown.
        }
    }
}
