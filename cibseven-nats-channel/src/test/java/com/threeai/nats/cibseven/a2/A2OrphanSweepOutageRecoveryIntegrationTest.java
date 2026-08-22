package com.threeai.nats.cibseven.a2;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.List;
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
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The sweep must survive a broker outage and republish every orphan once the broker returns —
 * this is the regression trap distilled from the 2026-08-20 sustained-load incident, in which
 * 34,138 instances whose fast-path publish failed during a broker OOM outage ended the run
 * stranded. (The live post-mortem initially read that as a silent sweep flatline; stats-csv
 * forensics later showed the sweep had been leader, cycling and republishing throughout — the
 * stranding was the R1 stream's home node dying a second, permanent time with the republished
 * messages still queued behind the pre-outage backlog. See the multi-node flatline-repro
 * reliability test in cibseven-nats-channel for the full incident shape.) The recovery contract
 * pinned here is unchanged by that correction: after a broker returns with its store, the first
 * cycle must republish every orphan, loudly, with logs.
 *
 * <p>End-to-end with no mocks on the recovery path: a REAL NATS broker (killed with SIGKILL and
 * restarted with its store directory intact — {@code GenericContainer.stop()} would delete the
 * container and lose the store, which models "node replaced", not "node returned"), a REAL
 * {@link SweepLeaderLease} over a real KV bucket, a REAL embedded engine with real
 * {@code ACT_RU_EXT_TASK} rows, and the sweep's real publish onto a real JetStream stream.
 *
 * <p>The host port is pinned before the container starts: Docker assigns a fresh published port
 * on every restart, and the jnats connection must be able to reconnect to the SAME address for
 * the recovery to be the library's own (reconnect + lease re-acquire + republish), not the
 * test's.
 */
@Testcontainers
class A2OrphanSweepOutageRecoveryIntegrationTest {

    private static final String TOPIC = "sweep-recovery-topic";
    private static final String STREAM = "JOBS";
    private static final String SENTINEL = "a2-jetstream-bridge";

    private GenericContainer<?> broker;
    private io.nats.client.Connection nats;
    private ProcessEngine processEngine;
    private DataSource dataSource;
    private NatsChannelMetrics metrics;
    private A2OrphanSweep sweep;

    @BeforeEach
    void setUp() throws Exception {
        int hostPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            hostPort = socket.getLocalPort();
        }
        broker = new GenericContainer<>("nats:2.10-alpine")
                .withCommand("--jetstream", "-sd", "/data")
                .withExposedPorts(4222)
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                        new PortBinding(Ports.Binding.bindPort(hostPort), new ExposedPort(4222))));
        broker.start();

        nats = Nats.connect(new Options.Builder()
                .server("nats://localhost:" + hostPort)
                .maxReconnects(-1)
                .reconnectWait(Duration.ofSeconds(1))
                .connectionTimeout(Duration.ofSeconds(5))
                .build());

        JetStreamManagement jsm = nats.jetStreamManagement();
        jsm.addStream(StreamConfiguration.builder()
                .name(STREAM)
                .subjects("jobs.>")
                .storageType(StorageType.File)
                .build());
        JetStream jetStream = nats.jetStream();

        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket("a2-sweep-leader", Duration.ofSeconds(240), 1, nats);
        SweepLeaderLease lease = new SweepLeaderLease(jetStream, kvManager, nats,
                "camunda", "outage-test-node", Duration.ofSeconds(240));

        JdbcDataSource h2 = new JdbcDataSource();
        h2.setUrl("jdbc:h2:mem:sweep-outage-test-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("sa");
        dataSource = h2;

        A2Properties properties = new A2Properties();
        properties.setTopics(List.of(TOPIC));
        A2TopicConfig topicConfig = new A2TopicConfig(properties);
        UmbrellaLockResolver lockResolver = new UmbrellaLockResolver(properties);
        UmbrellaLockValidator lockValidator = new UmbrellaLockValidator(properties, lockResolver);
        lockValidator.afterPropertiesSet();
        A2BpmnParseListener parseListener = new A2BpmnParseListener(topicConfig,
                properties.getSentinelWorkerId(), lockResolver,
                org.mockito.Mockito.mock(A2PostCommitPublisher.class)); // fast path deliberately dead: EVERY task is an orphan

        ProcessEngineConfigurationImpl config = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneInMemProcessEngineConfiguration();
        config.setDataSource(dataSource);
        config.setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE);
        config.setJobExecutorActivate(false);
        config.setPreParseListeners(new java.util.ArrayList<>(List.of(parseListener)));
        processEngine = config.buildProcessEngine();

        metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        sweep = new A2OrphanSweep(processEngine, lease, jetStream, topicConfig,
                SENTINEL, lockResolver, metrics, lockValidator);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (processEngine != null) {
            processEngine.close();
        }
        if (nats != null) {
            nats.close();
        }
        if (broker != null) {
            broker.stop();
        }
    }

    @Test
    void sweepRepublishesEveryOrphanAfterBrokerOutageAndReturn() throws Exception {
        // --- healthy baseline: leadership acquired, nothing to do -----------------------------
        sweep.sweepCycle();
        assertThat(metrics.sweepCycleCount("leader").count()).isEqualTo(1.0);

        // --- outage: SIGKILL, store intact ------------------------------------------------------
        DockerClient docker = DockerClientFactory.instance().client();
        docker.killContainerCmd(broker.getContainerId()).exec();

        // Orphans are born DURING the outage: rows exist in the engine DB, their fast-path
        // publish never happened (dead publisher above), and their birth locks are expired below
        // — exactly the population the 2026-08-20 flatline stranded.
        processEngine.getRepositoryService().createDeployment()
                .addString("sweep-outage.bpmn20.xml", PROCESS_XML)
                .deploy();
        RuntimeService runtimeService = processEngine.getRuntimeService();
        for (int i = 0; i < 3; i++) {
            runtimeService.startProcessInstanceByKey("sweepOutageProcess");
        }
        expireAllLocks();

        // A cycle during the outage must degrade cleanly: no exception, counted as not-leader
        // (the lease cannot reach KV), rows untouched.
        sweep.sweepCycle();
        assertThat(metrics.sweepCycleCount("not-leader").count()).isEqualTo(1.0);
        assertThat(metrics.sweepRepublishCount(TOPIC).count()).isEqualTo(0.0);

        // --- recovery: same container, same store, same address --------------------------------
        docker.startContainerCmd(broker.getContainerId()).exec();
        awaitReconnected();

        // THE regression assertion. If the sweep is dead after recovery — a wedged lease, a
        // cancelled scheduler, anything silent — the republish count stays 0 and this fails.
        sweep.sweepCycle();

        assertThat(metrics.sweepRepublishCount(TOPIC).count())
                .as("every orphan stranded by the outage must be republished on the first "
                        + "post-recovery cycle")
                .isEqualTo(3.0);
        assertThat(nats.jetStreamManagement().getStreamInfo(STREAM).getStreamState().getMsgCount())
                .as("republished jobs must be real messages on the real stream")
                .isEqualTo(3L);

        ExternalTaskService externalTaskService = processEngine.getExternalTaskService();
        assertThat(externalTaskService.createExternalTaskQuery().topicName(TOPIC).list())
                .allSatisfy(task -> assertThat(task.getWorkerId())
                        .as("republished rows are re-locked by the sentinel")
                        .isEqualTo(SENTINEL));
    }

    private void awaitReconnected() throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (nats.getStatus() != io.nats.client.Connection.Status.CONNECTED) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("NATS connection did not recover within 30s — status "
                        + nats.getStatus());
            }
            Thread.sleep(250);
        }
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
                         targetNamespace="http://threeai.com/sweep-outage">
              <process id="sweepOutageProcess" isExecutable="true" camunda:historyTimeToLive="1">
                <startEvent id="start"/>
                <sequenceFlow id="f1" sourceRef="start" targetRef="work"/>
                <serviceTask id="work" camunda:type="external" camunda:topic="sweep-recovery-topic"/>
                <sequenceFlow id="f2" sourceRef="work" targetRef="end"/>
                <endEvent id="end"/>
              </process>
            </definitions>
            """;
}
