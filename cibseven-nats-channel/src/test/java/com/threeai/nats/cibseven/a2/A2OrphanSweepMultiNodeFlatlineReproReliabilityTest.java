package com.threeai.nats.cibseven.a2;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.sql.DataSource;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.jetstream.SweepLeaderLease;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import org.cibseven.bpm.engine.ExternalTaskService;
import org.cibseven.bpm.engine.ProcessEngine;
import org.cibseven.bpm.engine.ProcessEngineConfiguration;
import org.cibseven.bpm.engine.RuntimeService;
import org.cibseven.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

/**
 * Multi-node incident-shape trap distilled from the 2026-08-20 sustained-load run, in the exact
 * production shape the single-node {@link A2OrphanSweepOutageRecoveryIntegrationTest} cannot
 * model: R1 work stream whose home node is the victim, R3 lease bucket, TWO contending sweeps.
 *
 * <p>The incident (stats.csv/backlog.csv forensics): work streams were deliberately R1 and all
 * lived on nats-2; the {@code a2-sweep-leader} bucket was R3. nats-2 OOMed at 00:44:27Z (34,138
 * tasks were born during the outage with their fast-path publish failed), returned with its
 * store at 01:02:17Z, then OOMed AGAIN at 01:06:40Z — permanently. The live post-mortem read
 * the ending (34,138 stranded, no visible sweep activity) as a silent sweep flatline. The
 * container-stats forensics that followed showed the opposite: the sweep was leader and cycling
 * the whole time — growing grind cycles on engine-1 during the outage (fixed-delay CPU/RSS
 * signature at 00:52:36/00:55:09/00:58:28), a recovery-window republish, and ~160s-period grind
 * cycles on engine-2 across the entire final plateau. The stranding was environmental: the
 * republished messages were queued FIFO behind the 59,467 pre-outage messages, the drain ran at
 * ~226/s, and the second OOM killed the R1 stream — republished messages inside it — before the
 * queue got that far. No library defect; the misread was possible because every lease/cycle
 * state was invisible pre-fix.
 *
 * <p>This test pins the three contracts that diagnosis leaned on, against real infrastructure
 * (three-node cluster, SIGKILL + same-store restarts, real {@code scheduleWithFixedDelay}
 * schedulers whose error handling replicates the registrar's hardened wrapper — {@code catch
 * (Throwable)}, because an escaped throwable cancels a periodic task silently and forever, which
 * the phase-4 {@code isDone()} asserts pin): (1) leadership survives a one-node outage
 * on the R3 bucket; (2) the first cycles after a same-store return republish every orphan;
 * (3) on a plateau with the stream home dead, cycles stay loud and the scheduled tasks stay
 * alive. Any silent-death regression in any of the three fails here, with the post-fix
 * GAINED/LOST logs and {@code nats.a2.sweep.cycles{outcome}} counters saying which.
 */
@Tag("reliability")
class A2OrphanSweepMultiNodeFlatlineReproReliabilityTest {

    private static final Logger log =
            LoggerFactory.getLogger(A2OrphanSweepMultiNodeFlatlineReproReliabilityTest.class);

    private static final String TOPIC = "sweep-repro-topic";
    private static final String STREAM = "JOBS";
    private static final String SENTINEL = "a2-jetstream-bridge";
    private static final String[] SERVER_NAMES = {"n1", "n2", "n3"};
    private static final int CLIENT_PORT = 4222;
    private static final int ROUTE_PORT = 6222;
    private static final Duration SWEEP_PERIOD = Duration.ofSeconds(2);
    /** 4×S rather than the ADR-0002 2×S: cycles that grind through failing publishes outlast a
     * 2×S lease at test speeds, and lease churn mid-grind would blur which phase an assert
     * failure belongs to. The production ratio is exercised by the contention reliability test. */
    private static final Duration LEASE_TTL = Duration.ofSeconds(8);
    private static final int ORPHANS = 5;

    private Network network;
    private final Map<String, GenericContainer<?>> nodesByServerName = new LinkedHashMap<>();
    private DockerClient docker;

    private io.nats.client.Connection adminNats;
    private io.nats.client.Connection natsA;
    private io.nats.client.Connection natsB;
    private ProcessEngine processEngine;
    private DataSource dataSource;
    private NatsChannelMetrics metricsA;
    private NatsChannelMetrics metricsB;
    private ScheduledExecutorService schedulerA;
    private ScheduledExecutorService schedulerB;
    private ScheduledFuture<?> sweepTaskA;
    private ScheduledFuture<?> sweepTaskB;
    private String victim;

    @BeforeEach
    void setUp() throws Exception {
        network = Network.newNetwork();
        docker = DockerClientFactory.instance().client();
        String routes = "nats://n1:" + ROUTE_PORT + ",nats://n2:" + ROUTE_PORT + ",nats://n3:" + ROUTE_PORT;

        // Host ports are pinned BEFORE the containers start: Docker hands out a fresh published
        // port on every container start, and the whole point of the recovery phases is that the
        // returning node is reachable at the SAME address, so the reconnect + re-acquire +
        // republish chain is the library's own doing, not the test re-wiring connections.
        Map<String, Integer> pinnedPorts = new LinkedHashMap<>();
        for (String serverName : SERVER_NAMES) {
            try (ServerSocket socket = new ServerSocket(0)) {
                pinnedPorts.put(serverName, socket.getLocalPort());
            }
        }
        for (String serverName : SERVER_NAMES) {
            int hostPort = pinnedPorts.get(serverName);
            GenericContainer<?> node = new GenericContainer<>("nats:2.10-alpine")
                    .withNetwork(network)
                    .withNetworkAliases(serverName)
                    .withExposedPorts(CLIENT_PORT)
                    .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                            new PortBinding(Ports.Binding.bindPort(hostPort), new ExposedPort(CLIENT_PORT))))
                    .withCommand(
                            "--server_name", serverName,
                            "--jetstream",
                            "--store_dir", "/data",
                            "--cluster_name", "flatline-repro",
                            "--cluster", "nats://0.0.0.0:" + ROUTE_PORT,
                            "--routes", routes);
            node.start();
            nodesByServerName.put(serverName, node);
        }

        adminNats = connect(allServerUrls(pinnedPorts));
        awaitClusterReady(Duration.ofSeconds(60));

        JetStreamManagement jsm = adminNats.jetStreamManagement();
        jsm.addStream(StreamConfiguration.builder()
                .name(STREAM)
                .subjects("jobs.>")
                .storageType(StorageType.File)
                .replicas(1)
                .build());
        // The victim is wherever JetStream homed the R1 stream — same "by chance" placement that
        // put every R1 stream on nats-2 in production, made deterministic by just asking.
        victim = jsm.getStreamInfo(STREAM).getClusterInfo().getLeader();
        String healthy = List.of(SERVER_NAMES).stream()
                .filter(name -> !name.equals(victim)).findFirst().orElseThrow();
        log.info("R1 {} stream homed on {} (the victim); engine connections pinned to {}",
                STREAM, victim, healthy);

        String healthyUrl = "nats://localhost:" + pinnedPorts.get(healthy);
        natsA = connect(List.of(healthyUrl));
        natsB = connect(List.of(healthyUrl));
        JetStream jetStreamA = natsA.jetStream();
        JetStream jetStreamB = natsB.jetStream();

        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket("a2-sweep-leader", LEASE_TTL, 3, adminNats);
        SweepLeaderLease leaseA = new SweepLeaderLease(jetStreamA, kvManager, natsA,
                "cibseven", "engine-a", LEASE_TTL);
        SweepLeaderLease leaseB = new SweepLeaderLease(jetStreamB, kvManager, natsB,
                "cibseven", "engine-b", LEASE_TTL);

        A2Properties properties = new A2Properties();
        properties.setTopics(List.of(TOPIC));
        A2TopicConfig topicConfig = new A2TopicConfig(properties);
        UmbrellaLockResolver lockResolver = new UmbrellaLockResolver(properties);
        UmbrellaLockValidator lockValidator = new UmbrellaLockValidator(properties, lockResolver);
        lockValidator.afterPropertiesSet();
        A2BpmnParseListener parseListener = new A2BpmnParseListener(topicConfig,
                properties.getSentinelWorkerId(), lockResolver,
                org.mockito.Mockito.mock(A2PostCommitPublisher.class)); // fast path deliberately dead: every task is an orphan

        JdbcDataSource h2 = new JdbcDataSource();
        h2.setUrl("jdbc:h2:mem:sweep-flatline-repro-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("sa");
        dataSource = h2;

        ProcessEngineConfigurationImpl config = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneInMemProcessEngineConfiguration();
        config.setDataSource(dataSource);
        config.setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE);
        config.setJobExecutorActivate(false);
        config.setPreParseListeners(new java.util.ArrayList<>(List.of(parseListener)));
        processEngine = config.buildProcessEngine();

        metricsA = new NatsChannelMetrics(new SimpleMeterRegistry());
        metricsB = new NatsChannelMetrics(new SimpleMeterRegistry());
        // Two sweeps against ONE engine/DB: production ran one engine per node over a shared
        // Postgres, but the sweep only touches the engine through the command API — what the
        // flatline needs modelled is the LEASE contention between two node identities.
        A2OrphanSweep sweepA = new A2OrphanSweep(processEngine, leaseA, jetStreamA, topicConfig,
                SENTINEL, lockResolver, metricsA, lockValidator);
        A2OrphanSweep sweepB = new A2OrphanSweep(processEngine, leaseB, jetStreamB, topicConfig,
                SENTINEL, lockResolver, metricsB, lockValidator);

        schedulerA = newSweepScheduler("a2-orphan-sweep-engine-a");
        schedulerB = newSweepScheduler("a2-orphan-sweep-engine-b");
        sweepTaskA = schedulerA.scheduleWithFixedDelay(registrarReplica(sweepA),
                SWEEP_PERIOD.toMillis(), SWEEP_PERIOD.toMillis(), TimeUnit.MILLISECONDS);
        sweepTaskB = schedulerB.scheduleWithFixedDelay(registrarReplica(sweepB),
                SWEEP_PERIOD.toMillis(), SWEEP_PERIOD.toMillis(), TimeUnit.MILLISECONDS);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (sweepTaskA != null) {
            sweepTaskA.cancel(false);
        }
        if (sweepTaskB != null) {
            sweepTaskB.cancel(false);
        }
        if (schedulerA != null) {
            schedulerA.shutdownNow();
        }
        if (schedulerB != null) {
            schedulerB.shutdownNow();
        }
        if (processEngine != null) {
            processEngine.close();
        }
        for (io.nats.client.Connection conn : new io.nats.client.Connection[] {natsA, natsB, adminNats}) {
            if (conn != null) {
                conn.close();
            }
        }
        nodesByServerName.values().forEach(node -> {
            try {
                node.stop();
            } catch (Exception ignored) {
                // A killed node may already be gone; teardown is best-effort.
            }
        });
        if (network != null) {
            network.close();
        }
    }

    @Test
    void doubleOutageOfTheR1StreamHomeUnderLeaseContentionMustNotFlatlineTheSweep() throws Exception {
        // --- phase 1: baseline — exactly this cluster, no failures, someone leads and cycles ----
        await("a sweep leader emerges and cycles", Duration.ofSeconds(30),
                () -> leaderCycles() >= 2);

        // --- phase 2: OOM #1 — victim dies, orphans are born while its stream is leaderless ----
        docker.killContainerCmd(containerIdOf(victim)).exec();
        log.info("phase 2: {} SIGKILLed (OOM #1)", victim);

        processEngine.getRepositoryService().createDeployment()
                .addString("sweep-flatline-repro.bpmn20.xml", PROCESS_XML)
                .deploy();
        RuntimeService runtimeService = processEngine.getRuntimeService();
        for (int i = 0; i < ORPHANS; i++) {
            runtimeService.startProcessInstanceByKey("sweepFlatlineReproProcess");
        }
        expireAllLocks();

        // Production mystery, assert #1: the lease bucket is R3 and keeps quorum through a
        // one-node outage, so leadership must SURVIVE — cycles keep counting leader on some node.
        // In the incident, zero sweep output during this exact window says it did not.
        double leaderCyclesBeforeOutageWindow = leaderCycles();
        await("leadership survives the one-node outage (R3 lease bucket keeps quorum)",
                Duration.ofSeconds(45),
                () -> leaderCycles() >= leaderCyclesBeforeOutageWindow + 2);
        assertThat(republished())
                .as("no republish can succeed while the R1 stream's home node is down")
                .isEqualTo(0.0);

        // --- phase 3: same node, same store, same address returns — THE recovery -----------------
        docker.startContainerCmd(containerIdOf(victim)).exec();
        log.info("phase 3: {} restarted with its store intact", victim);
        await("the R1 stream elects its home node back", Duration.ofSeconds(60),
                this::jobsStreamHasLeader);

        // Production mystery, assert #2: this is the 01:02:17–01:06:40 window the incident
        // wasted. Every orphan is lock-expired and native-fetchable; the first leader cycle
        // against a healthy cluster must republish all of them.
        await("every orphan is republished once the stream home is back", Duration.ofSeconds(60),
                () -> republished() == (double) ORPHANS);
        assertThat(adminNats.jetStreamManagement().getStreamInfo(STREAM).getStreamState().getMsgCount())
                .as("republished jobs are real messages on the real stream")
                .isEqualTo(ORPHANS);
        ExternalTaskService externalTaskService = processEngine.getExternalTaskService();
        assertThat(externalTaskService.createExternalTaskQuery().topicName(TOPIC).list())
                .allSatisfy(task -> assertThat(task.getWorkerId())
                        .as("republished rows are re-locked by the sentinel")
                        .isEqualTo(SENTINEL));

        // --- phase 4: OOM #2 — the victim dies again, permanently: the 37-minute plateau --------
        docker.killContainerCmd(containerIdOf(victim)).exec();
        log.info("phase 4: {} SIGKILLed again (OOM #2, permanent)", victim);
        expireAllLocks(); // rows fetchable again while their stream is dead: the plateau's exact DB state

        // Post-fix contract for the plateau: the sweep stays LOUD and ALIVE. Cycles keep
        // incrementing as leader, republishing fails without killing anything, and both
        // scheduled tasks survive — a done/cancelled future here is the silent-Error suspect
        // caught red-handed (scheduleWithFixedDelay cancels forever on any escaped throwable).
        double leaderCyclesBeforePlateau = leaderCycles();
        await("the sweep keeps cycling as leader against the dead stream home",
                Duration.ofSeconds(45),
                () -> leaderCycles() >= leaderCyclesBeforePlateau + 2);
        assertThat(sweepTaskA.isDone())
                .as("engine-a's scheduled sweep must still be alive on the plateau").isFalse();
        assertThat(sweepTaskB.isDone())
                .as("engine-b's scheduled sweep must still be alive on the plateau").isFalse();
        assertThat(republished())
                .as("nothing new can be republished onto a dead stream — and failing to do so "
                        + "must not be silent death")
                .isEqualTo((double) ORPHANS);
    }

    /** Replica of {@code A2SubscriptionRegistrar.runSweepCycleSafely} (hardened to
     * {@code catch (Throwable)} — an escaped {@code Error} would cancel the periodic task
     * silently and forever). The phase-4 {@code isDone()} asserts pin exactly that aliveness. */
    private static Runnable registrarReplica(A2OrphanSweep sweep) {
        return () -> {
            try {
                sweep.sweepCycle();
            } catch (Throwable t) {
                log.error("Uncaught throwable in A2 orphan-sweep cycle — cycle abandoned, task"
                        + " survives, will retry next cycle", t);
            }
        };
    }

    private static ScheduledExecutorService newSweepScheduler(String threadName) {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, threadName);
            t.setDaemon(true);
            return t;
        });
    }

    private double leaderCycles() {
        return metricsA.sweepCycleCount("leader").count() + metricsB.sweepCycleCount("leader").count();
    }

    private double republished() {
        return metricsA.sweepRepublishCount(TOPIC).count() + metricsB.sweepRepublishCount(TOPIC).count();
    }

    private boolean jobsStreamHasLeader() {
        try {
            var cluster = adminNats.jetStreamManagement().getStreamInfo(STREAM).getClusterInfo();
            return cluster != null && cluster.getLeader() != null;
        } catch (Exception notYet) {
            return false;
        }
    }

    private static void await(String what, Duration timeout, BooleanSupplier condition)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (!condition.getAsBoolean()) {
            if (Instant.now().isAfter(deadline)) {
                throw new AssertionError("Timed out after " + timeout + " waiting for: " + what);
            }
            Thread.sleep(250);
        }
    }

    private io.nats.client.Connection connect(List<String> urls) throws Exception {
        return Nats.connect(new Options.Builder()
                .servers(urls.toArray(new String[0]))
                .ignoreDiscoveredServers()
                .maxReconnects(-1)
                .reconnectWait(Duration.ofMillis(250))
                .connectionTimeout(Duration.ofSeconds(5))
                .build());
    }

    private List<String> allServerUrls(Map<String, Integer> pinnedPorts) {
        return pinnedPorts.values().stream().map(port -> "nats://localhost:" + port).toList();
    }

    private String containerIdOf(String serverName) {
        return nodesByServerName.get(serverName).getContainerId();
    }

    /** Ready = the cluster can actually place a 3-replica stream (same probe as the nats-core
     * harness) — true well after "the client port answers". */
    private void awaitClusterReady(Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        Exception last = null;
        while (Instant.now().isBefore(deadline)) {
            try {
                String probe = "READY_PROBE_" + UUID.randomUUID().toString().replace("-", "");
                JetStreamManagement jsm = adminNats.jetStreamManagement();
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
                Thread.sleep(500);
            }
        }
        throw new IllegalStateException("Cluster not ready within " + timeout, last);
    }

    private void expireAllLocks() throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "UPDATE ACT_RU_EXT_TASK SET LOCK_EXP_TIME_ = ?")) {
            stmt.setTimestamp(1, java.sql.Timestamp.from(java.time.Instant.now().minusSeconds(3600)));
            stmt.executeUpdate();
        }
    }

    private static final String PROCESS_XML = """
            <?xml version="1.0" encoding="UTF-8"?>
            <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                         xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                         targetNamespace="http://threeai.com/sweep-flatline-repro">
              <process id="sweepFlatlineReproProcess" isExecutable="true" camunda:historyTimeToLive="1">
                <startEvent id="start"/>
                <sequenceFlow id="f1" sourceRef="start" targetRef="work"/>
                <serviceTask id="work" camunda:type="external" camunda:topic="sweep-repro-topic"/>
                <sequenceFlow id="f2" sourceRef="work" targetRef="end"/>
                <endEvent id="end"/>
              </process>
            </definitions>
            """;
}
