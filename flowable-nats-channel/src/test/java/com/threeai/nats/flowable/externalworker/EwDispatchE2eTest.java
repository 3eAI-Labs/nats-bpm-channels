package com.threeai.nats.flowable.externalworker;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.headers.DlqHeaders;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.PublishAck;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.NatsJetStreamConstants;
import org.flowable.engine.ProcessEngine;
import org.flowable.engine.ProcessEngineConfiguration;
import org.flowable.engine.delegate.DelegateExecution;
import org.flowable.engine.delegate.JavaDelegate;
import org.flowable.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.flowable.job.api.ExternalWorkerJob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Acceptance suite for the external-worker dispatch slice (docs/11 §6) — embedded Flowable
 * 7.1.0 + real JetStream. Covers the happy path, gate G2 (rollback publishes nothing), the
 * born-locked invariant (G1 proxy — SQL-level single-INSERT proof lives in the characterization
 * probe), the reviewer-prescribed tests (nonce-less reply → DLQ; generation-scoped Msg-Id not
 * swallowed by the duplicate window) and the multi-node double-bind absence.
 */
@Testcontainers
class EwDispatchE2eTest {

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

    private static final String BPMN_ROLLBACK = """
            <?xml version="1.0" encoding="UTF-8"?>
            <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                         xmlns:flowable="http://flowable.org/bpmn"
                         targetNamespace="ewtest">
              <process id="rollbackProcess" isExecutable="true">
                <startEvent id="start"/>
                <sequenceFlow id="f1" sourceRef="start" targetRef="fork"/>
                <parallelGateway id="fork"/>
                <sequenceFlow id="f2" sourceRef="fork" targetRef="ext"/>
                <serviceTask id="ext" flowable:type="external-worker" flowable:topic="probe"/>
                <sequenceFlow id="f3" sourceRef="fork" targetRef="boom"/>
                <serviceTask id="boom" flowable:class="com.threeai.nats.flowable.externalworker.EwDispatchE2eTest$Boom"/>
                <sequenceFlow id="f4" sourceRef="ext" targetRef="end"/>
                <sequenceFlow id="f5" sourceRef="boom" targetRef="end"/>
                <endEvent id="end"/>
              </process>
            </definitions>
            """;

    public static class Boom implements JavaDelegate {
        @Override
        public void execute(DelegateExecution execution) {
            throw new RuntimeException("rollback the whole start transaction");
        }
    }

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
        ensureStream("FLW-EW-JOBS", RetentionPolicy.WorkQueue, "ewjobs.*");
        ensureStream("FLW-EW-JOBS-REPLY", RetentionPolicy.WorkQueue, "ewjobs.*.reply");
        ensureStream("EW-DLQ", RetentionPolicy.Limits, "dlq.ewjobs.>");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (bridge != null) {
            bridge.unsubscribe();
            bridge = null;
        }
        if (engine != null) {
            engine.close();
            engine = null;
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

    private void ensureStream(String name, RetentionPolicy policy, String subject) throws Exception {
        jsm.addStream(StreamConfiguration.builder()
                .name(name).retentionPolicy(policy).subjects(subject).build());
    }

    private ProcessEngine buildEngine(EwProperties props) {
        ProcessEngineConfigurationImpl cfg = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneInMemProcessEngineConfiguration()
                        .setJdbcUrl("jdbc:h2:mem:ew-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=1000")
                        .setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE)
                        .setAsyncExecutorActivate(true);
        EwPostCommitPublisher publisher =
                new EwPostCommitPublisher(jetStream, null, new EwLockConfig(props));
        cfg.setCreateExternalWorkerJobInterceptor(new EwCreateJobInterceptor(
                new EwTopicConfig(props), new EwLockConfig(props), SENTINEL, publisher, null));
        return cfg.buildProcessEngine();
    }

    private static EwProperties props() {
        EwProperties p = new EwProperties();
        p.setEnabled(true);
        p.setTopics(List.of("probe"));
        return p;
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

    @Test
    void happyPath_bornLocked_dispatch_workerReply_completes() throws Exception {
        EwProperties p = props();
        engine = buildEngine(p);
        engine.getRepositoryService().createDeployment()
                .addString("probe.bpmn20.xml", BPMN).deploy();
        startBridge(p);

        JetStreamSubscription worker = jetStream.subscribe("ewjobs.probe");
        engine.getRuntimeService().startProcessInstanceByKey("probeProcess", "bk-42");

        Message job = worker.nextMessage(Duration.ofSeconds(10));
        assertThat(job).isNotNull();
        String payload = new String(job.getData(), StandardCharsets.UTF_8);
        assertThat(payload).contains("\"topic\":\"probe\"").contains("\"businessKey\":\"bk-42\"");
        String nonce = job.getHeaders().getFirst(EwHeaders.LOCK_NONCE);
        assertThat(nonce).isNotNull();
        assertThat(job.getHeaders().getFirst(NatsJetStreamConstants.MSG_ID_HDR)).endsWith("#" + nonce);
        job.ack();

        // Born-locked invariant (G1 proxy): the job row is sentinel-locked from birth.
        ExternalWorkerJob row = engine.getManagementService()
                .createExternalWorkerJobQuery().singleResult();
        assertThat(row.getLockOwner()).isEqualTo(SENTINEL + "#" + nonce);
        assertThat(row.getLockExpirationTime()).isNotNull();

        String jobId = extractJobId(payload);
        jetStream.publish(NatsMessage.builder()
                .subject("ewjobs.probe.reply")
                .headers(new io.nats.client.impl.Headers().add(EwHeaders.LOCK_NONCE, nonce))
                .data(("{\"type\":\"SUCCESS\",\"jobId\":\"" + jobId
                        + "\",\"variables\":{\"result\":{\"value\":\"ok\",\"type\":\"String\"}}}")
                        .getBytes(StandardCharsets.UTF_8))
                .build());

        awaitTrue(() -> engine.getRuntimeService().createProcessInstanceQuery().count() == 0,
                15000, "process instance to complete");
    }

    /** Gate G2: a rollback in the creating transaction publishes NOTHING. */
    @Test
    void rollback_publishesNothing() throws Exception {
        EwProperties p = props();
        engine = buildEngine(p);
        engine.getRepositoryService().createDeployment()
                .addString("rollback.bpmn20.xml", BPMN_ROLLBACK).deploy();

        try {
            engine.getRuntimeService().startProcessInstanceByKey("rollbackProcess");
            throw new AssertionError("start should have rolled back");
        } catch (RuntimeException expected) {
            // the Boom delegate rolled back the whole start transaction
        }

        JetStreamSubscription worker = jetStream.subscribe("ewjobs.probe");
        assertThat(worker.nextMessage(Duration.ofSeconds(2))).isNull();
        assertThat(engine.getManagementService().createExternalWorkerJobQuery().count()).isZero();
    }

    /** Reviewer-prescribed test #1 at E2E level: nonce-less reply is dead-lettered, job survives. */
    @Test
    void nonceLessReply_isDeadLettered_jobSurvives() throws Exception {
        EwProperties p = props();
        engine = buildEngine(p);
        engine.getRepositoryService().createDeployment()
                .addString("probe.bpmn20.xml", BPMN).deploy();
        startBridge(p);

        JetStreamSubscription worker = jetStream.subscribe("ewjobs.probe");
        JetStreamSubscription dlqSpy = jetStream.subscribe("dlq.ewjobs.probe");
        engine.getRuntimeService().startProcessInstanceByKey("probeProcess");
        Message job = worker.nextMessage(Duration.ofSeconds(10));
        job.ack();
        String jobId = extractJobId(new String(job.getData(), StandardCharsets.UTF_8));

        jetStream.publish(NatsMessage.builder()
                .subject("ewjobs.probe.reply")
                .data(("{\"type\":\"SUCCESS\",\"jobId\":\"" + jobId + "\"}").getBytes(StandardCharsets.UTF_8))
                .build());

        Message dlq = dlqSpy.nextMessage(Duration.ofSeconds(10));
        assertThat(dlq).isNotNull();
        assertThat(dlq.getHeaders().getFirst(DlqHeaders.REASON)).isEqualTo("VAL_MISSING_LOCK_NONCE");
        assertThat(engine.getManagementService().createExternalWorkerJobQuery().count()).isEqualTo(1);
    }

    /**
     * Reviewer-prescribed test #2: the generation-scoped Msg-Id. A same-generation duplicate is
     * deduplicated by the stream; a new generation (sweep re-publish) is NEVER swallowed.
     */
    @Test
    void generationScopedMsgId_survivesDuplicateWindow() throws Exception {
        PublishAck first = jetStream.publish(NatsMessage.builder().subject("ewjobs.probe")
                .headers(new io.nats.client.impl.Headers()
                        .add(NatsJetStreamConstants.MSG_ID_HDR, "j1#gen1"))
                .data("{}".getBytes(StandardCharsets.UTF_8)).build());
        PublishAck dup = jetStream.publish(NatsMessage.builder().subject("ewjobs.probe")
                .headers(new io.nats.client.impl.Headers()
                        .add(NatsJetStreamConstants.MSG_ID_HDR, "j1#gen1"))
                .data("{}".getBytes(StandardCharsets.UTF_8)).build());
        PublishAck newGen = jetStream.publish(NatsMessage.builder().subject("ewjobs.probe")
                .headers(new io.nats.client.impl.Headers()
                        .add(NatsJetStreamConstants.MSG_ID_HDR, "j1#gen2"))
                .data("{}".getBytes(StandardCharsets.UTF_8)).build());

        assertThat(dup.isDuplicate()).isTrue();
        assertThat(newGen.isDuplicate()).isFalse();
        assertThat(jsm.getStreamInfo("FLW-EW-JOBS").getStreamState().getMsgCount()).isEqualTo(2);
        assertThat(first.isDuplicate()).isFalse();
    }

    /** SUB-90012 lesson: a second node binding the same durable+deliverGroup must NOT fail. */
    @Test
    void secondBridge_sameDurable_bindsWithoutError() throws Exception {
        EwProperties p = props();
        engine = buildEngine(p);
        startBridge(p);

        EwConsumerConfig cc = new EwConsumerConfig("probe");
        EwCompletionBridge second = new EwCompletionBridge(connection, jetStream,
                engine.getManagementService(), cc, SENTINEL,
                new DlqPublisher(jetStream, connection, null), null);
        second.subscribe(); // must not throw [SUB-90012]
        second.unsubscribe();
    }

    private static String extractJobId(String payload) {
        int i = payload.indexOf("\"jobId\":\"") + 9;
        return payload.substring(i, payload.indexOf('"', i));
    }
}
