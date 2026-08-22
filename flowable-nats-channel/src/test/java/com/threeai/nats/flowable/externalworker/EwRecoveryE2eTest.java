package com.threeai.nats.flowable.externalworker;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.jetstream.SweepLeaderLease;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.NatsMessage;
import org.flowable.engine.ProcessEngine;
import org.flowable.engine.ProcessEngineConfiguration;
import org.flowable.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.flowable.job.api.ExternalWorkerJob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Recovery-path acceptance (docs/11 §6): publish loss → engine reset releases the born lock →
 * the sweep re-acquires through the PUBLIC API with a fresh generation and re-publishes → a
 * worker completes against the NEW generation. Asserts the recovery bound with test-scale
 * values (L + resetInterval + S). Also the D-C'v2 BPMN_ERROR row end-to-end.
 */
@Testcontainers
class EwRecoveryE2eTest {

    private static final String SENTINEL = "flw-ew-bridge";

    private static final String BPMN = """
            <?xml version="1.0" encoding="UTF-8"?>
            <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                         xmlns:flowable="http://flowable.org/bpmn"
                         targetNamespace="ewtest">
              <process id="probeProcess" isExecutable="true">
                <startEvent id="start"/>
                <sequenceFlow id="f1" sourceRef="start" targetRef="ext"/>
                <serviceTask id="ext" flowable:type="external-worker" flowable:topic="probe"/>
                <sequenceFlow id="f2" sourceRef="ext" targetRef="end"/>
                <endEvent id="end"/>
              </process>
            </definitions>
            """;

    private static final String BPMN_ERROR_BOUNDARY = """
            <?xml version="1.0" encoding="UTF-8"?>
            <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                         xmlns:flowable="http://flowable.org/bpmn"
                         targetNamespace="ewtest">
              <process id="errorProcess" isExecutable="true">
                <startEvent id="start"/>
                <sequenceFlow id="f1" sourceRef="start" targetRef="ext"/>
                <serviceTask id="ext" flowable:type="external-worker" flowable:topic="probe"/>
                <boundaryEvent id="err" attachedToRef="ext">
                  <errorEventDefinition errorRef="orderFailed"/>
                </boundaryEvent>
                <sequenceFlow id="f2" sourceRef="ext" targetRef="endOk"/>
                <sequenceFlow id="f3" sourceRef="err" targetRef="endErr"/>
                <endEvent id="endOk"/>
                <endEvent id="endErr"/>
              </process>
              <error id="orderFailed" errorCode="orderFailed"/>
            </definitions>
            """;

    @Container
    static GenericContainer<?> natsContainer = new GenericContainer<>("nats:2.10-alpine")
            .withCommand("--jetstream")
            .withExposedPorts(4222);

    private Connection connection;
    private JetStream jetStream;
    private JetStreamManagement jsm;
    private ProcessEngine engine;
    private EwCompletionBridge bridge;

    @BeforeEach
    void setUp() throws Exception {
        String url = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);
        connection = Nats.connect(new Options.Builder().server(url).build());
        jetStream = connection.jetStream();
        jsm = connection.jetStreamManagement();
        jsm.addStream(StreamConfiguration.builder().name("FLW-EW-JOBS-REPLY")
                .retentionPolicy(RetentionPolicy.WorkQueue).subjects("ewjobs.*.reply").build());
        jsm.addStream(StreamConfiguration.builder().name("EW-DLQ")
                .retentionPolicy(RetentionPolicy.Limits).subjects("dlq.ewjobs.>").build());
        // FLW-EW-JOBS is deliberately NOT created here — the publish-loss test creates it late.
    }

    @AfterEach
    void tearDown() throws Exception {
        if (bridge != null) {
            bridge.unsubscribe();
        }
        if (engine != null) {
            engine.close();
        }
        for (String s : List.of("FLW-EW-JOBS", "FLW-EW-JOBS-REPLY", "EW-DLQ")) {
            try {
                jsm.deleteStream(s);
            } catch (Exception ignored) {
            }
        }
        if (connection != null) {
            connection.close();
        }
    }

    private static EwProperties unsafeShortLockProps() {
        EwProperties p = new EwProperties();
        p.setEnabled(true);
        p.setTopics(List.of("probe"));
        p.setLockDurationSeconds(2L);
        p.setAllowUnsafeLockDuration(true); // deliberate: test-scale L, floor bypassed loudly
        p.setSweepPeriodSeconds(2);
        return p;
    }

    private ProcessEngine buildEngine(EwProperties props, int resetIntervalMs) {
        ProcessEngineConfigurationImpl cfg = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneInMemProcessEngineConfiguration()
                        .setJdbcUrl("jdbc:h2:mem:ewr-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=1000")
                        .setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE)
                        .setAsyncExecutorActivate(true);
        cfg.setAsyncExecutorResetExpiredJobsInterval(resetIntervalMs);
        EwPostCommitPublisher publisher =
                new EwPostCommitPublisher(jetStream, null, new EwLockConfig(props));
        cfg.setCreateExternalWorkerJobInterceptor(new EwCreateJobInterceptor(
                new EwTopicConfig(props), new EwLockConfig(props), SENTINEL, publisher, null));
        return cfg.buildProcessEngine();
    }

    private void startBridge(EwProperties props) {
        EwConsumerConfig cc = new EwConsumerConfig("probe");
        cc.setAckWaitSeconds(props.getAckWaitSeconds());
        cc.setMaxDeliver(props.getMaxDeliver());
        bridge = new EwCompletionBridge(connection, jetStream, engine.getManagementService(),
                cc, SENTINEL, new DlqPublisher(jetStream, connection, null), null);
        bridge.subscribe();
    }

    private void awaitTrue(java.util.function.BooleanSupplier condition, long timeoutMs, String what) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError("Timed out waiting for: " + what);
    }

    /**
     * Publish loss → reset → sweep public re-acquire with a NEW generation → worker completes.
     * Recovery observed within the L + resetInterval + S bound (test-scale: 2s + 2s + 2s + margin).
     */
    @Test
    void publishLoss_recoversThroughResetAndSweep_withinBound() throws Exception {
        EwProperties p = unsafeShortLockProps();
        engine = buildEngine(p, 2000);
        engine.getRepositoryService().createDeployment()
                .addString("probe.bpmn20.xml", BPMN).deploy();
        startBridge(p);

        long startedAt = System.currentTimeMillis();
        // FLW-EW-JOBS stream absent -> the post-commit publish fails (WARN-only orphan).
        engine.getRuntimeService().startProcessInstanceByKey("probeProcess");
        ExternalWorkerJob born = engine.getManagementService().createExternalWorkerJobQuery().singleResult();
        String bornOwner = born.getLockOwner();
        assertThat(bornOwner).startsWith(SENTINEL + "#");

        // Engine reset thread releases the expired born lock (probe-proven behavior).
        awaitTrue(() -> {
            ExternalWorkerJob j = engine.getManagementService().createExternalWorkerJobQuery().singleResult();
            return j != null && j.getLockOwner() == null;
        }, 15000, "engine reset to release the expired sentinel lock");

        // Broker recovers: the stream now exists; the sweep re-acquires and re-publishes.
        jsm.addStream(StreamConfiguration.builder().name("FLW-EW-JOBS")
                .retentionPolicy(RetentionPolicy.WorkQueue).subjects("ewjobs.*").build());
        JetStreamSubscription worker = jetStream.subscribe("ewjobs.probe");

        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket("a2-sweep-leader", Duration.ofSeconds(4), 1, connection);
        SweepLeaderLease lease = new SweepLeaderLease(jetStream, kvManager, connection,
                "flowable", "test-node", Duration.ofSeconds(4));
        EwOrphanSweep sweep = new EwOrphanSweep(engine.getManagementService(),
                new EwTopicConfig(p), new EwLockConfig(p), SENTINEL, jetStream, lease, null, 100);
        sweep.runCycle();

        Message redispatched = worker.nextMessage(Duration.ofSeconds(10));
        assertThat(redispatched).isNotNull();
        String newNonce = redispatched.getHeaders().getFirst(EwHeaders.LOCK_NONCE);
        assertThat(SENTINEL + "#" + newNonce).isNotEqualTo(bornOwner); // fresh generation
        long recoveredAfterMs = System.currentTimeMillis() - startedAt;
        redispatched.ack();

        String payload = new String(redispatched.getData(), StandardCharsets.UTF_8);
        String jobId = payload.split("\"jobId\":\"")[1].split("\"")[0];
        jetStream.publish(NatsMessage.builder()
                .subject("ewjobs.probe.reply")
                .headers(new io.nats.client.impl.Headers().add(EwHeaders.LOCK_NONCE, newNonce))
                .data(("{\"type\":\"SUCCESS\",\"jobId\":\"" + jobId + "\"}").getBytes(StandardCharsets.UTF_8))
                .build());

        awaitTrue(() -> engine.getRuntimeService().createProcessInstanceQuery().count() == 0,
                15000, "instance completion through the recovered generation");
        // Derived SLO at test scale (L=2s, reset=2s, S~=one manual cycle) + generous CI margin.
        assertThat(recoveredAfterMs).isLessThan(30000);
    }

    /** D-C'v2 BPMN_ERROR row: code routes the boundary, errorMessage lands as natsErrorMessage. */
    @Test
    void bpmnErrorReply_routesBoundary_withDegradedErrorMessage() throws Exception {
        jsm.addStream(StreamConfiguration.builder().name("FLW-EW-JOBS")
                .retentionPolicy(RetentionPolicy.WorkQueue).subjects("ewjobs.*").build());
        EwProperties p = new EwProperties();
        p.setEnabled(true);
        p.setTopics(List.of("probe"));
        engine = buildEngine(p, 60000);
        engine.getRepositoryService().createDeployment()
                .addString("error.bpmn20.xml", BPMN_ERROR_BOUNDARY).deploy();
        startBridge(p);

        JetStreamSubscription worker = jetStream.subscribe("ewjobs.probe");
        engine.getRuntimeService().startProcessInstanceByKey("errorProcess");
        Message job = worker.nextMessage(Duration.ofSeconds(10));
        job.ack();
        String payload = new String(job.getData(), StandardCharsets.UTF_8);
        String jobId = payload.split("\"jobId\":\"")[1].split("\"")[0];
        String nonce = job.getHeaders().getFirst(EwHeaders.LOCK_NONCE);

        jetStream.publish(NatsMessage.builder()
                .subject("ewjobs.probe.reply")
                .headers(new io.nats.client.impl.Headers().add(EwHeaders.LOCK_NONCE, nonce))
                .data(("{\"type\":\"BPMN_ERROR\",\"jobId\":\"" + jobId
                        + "\",\"errorCode\":\"orderFailed\",\"errorMessage\":\"supplier down\"}")
                        .getBytes(StandardCharsets.UTF_8))
                .build());

        awaitTrue(() -> engine.getRuntimeService().createProcessInstanceQuery().count() == 0,
                15000, "boundary-error path to complete the instance");
        Object degraded = engine.getHistoryService().createHistoricVariableInstanceQuery()
                .variableName(EwReplyDecoder.ERROR_MESSAGE_VARIABLE).singleResult() != null
                ? engine.getHistoryService().createHistoricVariableInstanceQuery()
                        .variableName(EwReplyDecoder.ERROR_MESSAGE_VARIABLE).singleResult().getValue()
                : null;
        assertThat(degraded).isEqualTo("supplier down");
    }
}
