package com.threeai.nats.camunda.a2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

import com.threeai.nats.core.jetstream.SweepLeaderLease;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.JetStream;
import io.nats.client.impl.NatsMessage;
import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.ProcessEngineConfiguration;
import org.camunda.bpm.engine.RepositoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.externaltask.ExternalTask;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * DECISION_MATRIX Matrix 3 (sweep kararı), rows 2/3/4 — {@code A2OrphanSweep}'s
 * "fetchable-parity" predicate (native {@code ExternalTaskManager
 * .selectExternalTasksForTopics(...)}, the SAME query the classic {@code fetchAndLock} poller
 * uses) is a SQL {@code WHERE} clause the coder's Java code never branches on directly — the
 * existing {@code A2OrphanSweepTest} (Mockito-mocked {@code ExternalTaskManager}) can only prove
 * "given this list of candidates, the sweep re-locks and republishes them"; it CANNOT prove the
 * query itself correctly EXCLUDES retries-exhausted / suspended / still-fresh-locked rows, since
 * the mock always returns whatever list a test hands it.
 *
 * <p>This test closes that gap with a REAL embedded {@code ProcessEngine} (H2) and REAL
 * {@code ACT_RU_EXT_TASK} rows in all four DECISION_MATRIX Matrix-3 states, then runs
 * {@code A2OrphanSweep.sweepCycle()} against them for real and asserts exactly ONE republish
 * happens — for the genuinely orphaned row only.
 *
 * <p><b>[BLOCKING] Sentinel Phase 5.5 QA finding — FIXED (Sentinel Phase 5, 2026-07-15):</b>
 * running this test against the REAL engine used to reproducibly throw — {@code
 * A2OrphanSweep.fetchFetchableParity()} passed {@code instructions.values()} (a
 * {@code Map.values()} view, runtime type {@code java.util.HashMap$Values}) into Camunda's
 * {@code ExternalTaskManager.selectExternalTasksForTopics(...)}, whose MyBatis mapper
 * ({@code ExternalTask.xml}) evaluates the dynamic-SQL guard
 * {@code parameter.topics.size > 0} via OGNL reflection on the parameter's ACTUAL runtime class.
 * {@code HashMap$Values.size()} is a JDK-internal method the {@code java.base} module does not
 * open to unnamed modules by default (JPMS strong encapsulation, JDK 9+, enforced as a hard
 * denial since JDK 16) — every invocation threw
 * {@code java.lang.reflect.InaccessibleObjectException} unless the JVM is launched with
 * {@code --add-opens java.base/java.util=ALL-UNNAMED}. This project mandates Java 21
 * ({@code pom.xml} {@code java.version=21}); no {@code --add-opens} flag is set anywhere in this
 * repo's build or documented in {@code 99_deployment.md}. Reproduced independently of
 * Camunda/MyBatis (plain {@code HashMap.values()} + reflective {@code size()} call, same
 * exception) to rule out a test-artifact explanation.
 *
 * <p><b>Production impact (pre-fix):</b> {@code A2OrphanSweep.sweepCycle()}'s
 * {@code fetchFetchableParity()} call threw EVERY cycle on a standard JDK 21 launch; the
 * {@code catch (Exception e)} in {@code sweepCycle()} swallowed it as
 * {@code SYS_SWEEP_QUERY_FAILED} (plain ERROR, no CRITICAL/page per {@code ERROR_REGISTRY.md}
 * #13) and the cycle was skipped — meaning the ENTIRE orphan-sweep safety net
 * (ADR-0002/ADR-0003, the mechanism ADR-0001's umbrella-lock formula and NFR-R3's "invisible
 * orphan window &lt;= L" guarantee both depend on) silently never ran, indefinitely, in a
 * default deployment.
 *
 * <p><b>Fix applied:</b> {@code A2OrphanSweep.fetchFetchableParity()} now materializes a plain
 * {@code new java.util.ArrayList<>(instructions.values())} before crossing into the
 * MyBatis/OGNL-reflected engine API — the JPMS-restricted view type never reaches OGNL. This
 * test is re-enabled and is a real regression guard for the DECISION_MATRIX Matrix-3
 * skip-branches (retries-exhausted / suspended / still-fresh-locked rows), not a false positive.
 */
class A2OrphanSweepFetchableParityIntegrationTest {

    private static final String TOPIC = "sweep-parity-topic";

    private ProcessEngine processEngine;
    private DataSource dataSource;

    @BeforeEach
    void setUp() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setUrl("jdbc:h2:mem:sweep-parity-test-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("sa");
        dataSource = h2;

        A2Properties properties = new A2Properties();
        properties.setTopics(List.of(TOPIC));
        A2TopicConfig topicConfig = new A2TopicConfig(properties);
        UmbrellaLockResolver lockResolver = new UmbrellaLockResolver(properties);
        A2PostCommitPublisher noOpPublisher = mock(A2PostCommitPublisher.class); // post-commit publish irrelevant here
        A2BpmnParseListener parseListener = new A2BpmnParseListener(
                topicConfig, properties.getSentinelWorkerId(), lockResolver, noOpPublisher);

        ProcessEngineConfigurationImpl config = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneInMemProcessEngineConfiguration();
        config.setDataSource(dataSource);
        config.setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE);
        config.setJobExecutorActivate(false);
        config.setPreParseListeners(new java.util.ArrayList<>(List.of(parseListener)));

        processEngine = config.buildProcessEngine();
    }

    @AfterEach
    void tearDown() {
        if (processEngine != null) {
            processEngine.close();
        }
    }

    @Test
    void sweepCycle_realFourRowStates_republishesOnlyTheGenuineOrphan() throws Exception {
        RepositoryService repositoryService = processEngine.getRepositoryService();
        RuntimeService runtimeService = processEngine.getRuntimeService();
        ExternalTaskService externalTaskService = processEngine.getExternalTaskService();

        repositoryService.createDeployment()
                .addString("sweep-parity.bpmn20.xml", PROCESS_XML)
                .deploy();

        // Four process instances -> four ACT_RU_EXT_TASK rows, all born SENTINEL-locked with a
        // fresh (future) LOCK_EXP_TIME_ by A2ExternalTaskBehavior.
        String orphanInstanceId = runtimeService.startProcessInstanceByKey("sweepParityProcess",
                Map.of("scenario", "orphan")).getId();
        String retriesZeroInstanceId = runtimeService.startProcessInstanceByKey("sweepParityProcess",
                Map.of("scenario", "retries-zero")).getId();
        String suspendedInstanceId = runtimeService.startProcessInstanceByKey("sweepParityProcess",
                Map.of("scenario", "suspended")).getId();
        String freshLockInstanceId = runtimeService.startProcessInstanceByKey("sweepParityProcess",
                Map.of("scenario", "fresh-lock")).getId();

        String orphanTaskId = taskIdFor(externalTaskService, orphanInstanceId);
        String retriesZeroTaskId = taskIdFor(externalTaskService, retriesZeroInstanceId);
        String suspendedTaskId = taskIdFor(externalTaskService, suspendedInstanceId);
        String freshLockTaskId = taskIdFor(externalTaskService, freshLockInstanceId);

        // Row 1 (Matrix 3): genuinely orphaned -> LOCK_EXP_TIME_ in the past, everything else
        // default (RETRIES_ null, SUSPENSION_STATE_ null/active).
        expireLock(orphanTaskId);

        // Row 2 (Matrix 3): RETRIES_ = 0 (Cockpit-exhausted / already DLQ'd) -> must be SKIPPED
        // even though its lock also looks expired.
        expireLock(retriesZeroTaskId);
        setRetries(retriesZeroTaskId, 0);

        // Row 3 (Matrix 3): SUSPENSION_STATE_ = SUSPENDED (2) -> must be SKIPPED even though its
        // lock also looks expired.
        expireLock(suspendedTaskId);
        setSuspensionState(suspendedTaskId, 2);

        // Row 4 (Matrix 3): LOCK_EXP_TIME_ still fresh (untouched, in the future) -> normal
        // in-flight task, must be SKIPPED (not an orphan).
        // (freshLockTaskId intentionally left untouched.)

        JetStream jetStream = mock(JetStream.class);
        SweepLeaderLease leaderLease = mock(SweepLeaderLease.class);
        when(leaderLease.tryAcquireOrRenew()).thenReturn(true);
        NatsChannelMetrics metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        UmbrellaLockResolver lockResolver = new UmbrellaLockResolver(new A2PropertiesWithTopic(TOPIC));
        UmbrellaLockValidator lockValidator = new UmbrellaLockValidator(
                new A2PropertiesWithTopic(TOPIC), lockResolver);
        lockValidator.afterPropertiesSet();
        A2TopicConfig topicConfig = new A2TopicConfig(new A2PropertiesWithTopic(TOPIC));

        A2OrphanSweep sweep = new A2OrphanSweep(processEngine, leaderLease, jetStream, topicConfig,
                "a2-jetstream-bridge", lockResolver, metrics, lockValidator);

        sweep.sweepCycle();

        ArgumentCaptor<NatsMessage> captor = ArgumentCaptor.forClass(NatsMessage.class);
        verify(jetStream).publish(captor.capture());
        assertThat(captor.getValue().getHeaders().getLast("Nats-Msg-Id"))
                .as("Only the genuinely orphaned row (expired lock, retries left, not suspended) "
                        + "should ever be republished")
                .isEqualTo(orphanTaskId);
        assertThat(metrics.sweepRepublishCount(TOPIC).count()).isEqualTo(1.0);

        // Sanity: the other three tasks are untouched by the sweep (still fetchable/unfetchable
        // exactly as configured — no accidental relock/republish leaked to them).
        assertThat(externalTaskService.createExternalTaskQuery().externalTaskId(retriesZeroTaskId)
                .singleResult().getRetries()).isEqualTo(0);
        assertThat(externalTaskService.createExternalTaskQuery().externalTaskId(suspendedTaskId)
                .singleResult().isSuspended()).isTrue();
        assertThat(externalTaskService.createExternalTaskQuery().externalTaskId(freshLockTaskId)
                .singleResult().getWorkerId()).isEqualTo("a2-jetstream-bridge");
    }

    private String taskIdFor(ExternalTaskService externalTaskService, String processInstanceId) {
        List<ExternalTask> tasks = externalTaskService.createExternalTaskQuery()
                .processInstanceId(processInstanceId).list();
        assertThat(tasks).hasSize(1);
        return tasks.get(0).getId();
    }

    private void expireLock(String taskId) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "UPDATE ACT_RU_EXT_TASK SET LOCK_EXP_TIME_ = ? WHERE ID_ = ?")) {
            stmt.setTimestamp(1, java.sql.Timestamp.from(java.time.Instant.now().minusSeconds(3600)));
            stmt.setString(2, taskId);
            int updated = stmt.executeUpdate();
            assertThat(updated).isEqualTo(1);
        }
    }

    private void setRetries(String taskId, int retries) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "UPDATE ACT_RU_EXT_TASK SET RETRIES_ = ? WHERE ID_ = ?")) {
            stmt.setInt(1, retries);
            stmt.setString(2, taskId);
            stmt.executeUpdate();
        }
    }

    private void setSuspensionState(String taskId, int suspensionState) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "UPDATE ACT_RU_EXT_TASK SET SUSPENSION_STATE_ = ? WHERE ID_ = ?")) {
            stmt.setInt(1, suspensionState);
            stmt.setString(2, taskId);
            stmt.executeUpdate();
        }
    }

    /** Minimal single-topic {@link A2Properties}, reused for the sweep's own collaborators. */
    private static final class A2PropertiesWithTopic extends A2Properties {
        A2PropertiesWithTopic(String topic) {
            setTopics(List.of(topic));
        }
    }

    private static final String PROCESS_XML = """
            <?xml version="1.0" encoding="UTF-8"?>
            <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                         xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                         targetNamespace="http://threeai.com/sweep-parity">
              <process id="sweepParityProcess" isExecutable="true" camunda:historyTimeToLive="180">
                <startEvent id="start" />
                <sequenceFlow id="flow1" sourceRef="start" targetRef="a2Task" />
                <serviceTask id="a2Task" camunda:type="external" camunda:topic="sweep-parity-topic" />
                <sequenceFlow id="flow2" sourceRef="a2Task" targetRef="end" />
                <endEvent id="end" />
              </process>
            </definitions>
            """;
}
