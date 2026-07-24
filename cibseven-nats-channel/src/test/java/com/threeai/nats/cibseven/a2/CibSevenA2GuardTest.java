package com.threeai.nats.cibseven.a2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

import net.ttddyy.dsproxy.ExecutionInfo;
import net.ttddyy.dsproxy.QueryInfo;
import net.ttddyy.dsproxy.listener.QueryExecutionListener;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;
import org.cibseven.bpm.engine.ProcessEngine;
import org.cibseven.bpm.engine.ProcessEngineConfiguration;
import org.cibseven.bpm.engine.RepositoryService;
import org.cibseven.bpm.engine.RuntimeService;
import org.cibseven.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * TEST_SPECIFICATIONS.md (a) — flush-before-lock single-INSERT guard (ADR-0005 §2, BR-A2-002,
 * US-A2). Proves that {@link A2ExternalTaskBehavior#execute} produces exactly ONE
 * {@code INSERT ... ACT_RU_EXT_TASK} and ZERO follow-up {@code UPDATE} — i.e. the task is born
 * already SENTINEL-locked in a single flush, never locked via a second round-trip.
 *
 * <p>Unlike the existing {@code *IntegrationTest} classes (NATS Testcontainers, engine services
 * mocked), this guard test needs a REAL embedded {@code ProcessEngine} backed by H2 wrapped with
 * {@code datasource-proxy} so actual SQL statements can be counted — a genuinely new test
 * infrastructure, not a repeat of the existing pattern (per LLD TEST_SPECIFICATIONS.md (a)).
 */
class CibSevenA2GuardTest {

    private static final String TOPIC = "a2-guard-topic";

    private ProcessEngine processEngine;
    private SqlStatementCounter sqlCounter;

    @BeforeEach
    void setUp() {
        sqlCounter = new SqlStatementCounter();

        JdbcDataSource h2 = new JdbcDataSource();
        h2.setUrl("jdbc:h2:mem:a2-guard-test-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("sa");

        DataSource proxiedDataSource = ProxyDataSourceBuilder.create(h2)
                .name("a2-guard-test")
                .listener(sqlCounter)
                .build();

        A2Properties properties = new A2Properties();
        properties.setTopics(List.of(TOPIC));
        A2TopicConfig topicConfig = new A2TopicConfig(properties);
        UmbrellaLockResolver lockResolver = new UmbrellaLockResolver(properties);
        A2PostCommitPublisher publisher = mock(A2PostCommitPublisher.class);
        A2BpmnParseListener parseListener = new A2BpmnParseListener(
                topicConfig, properties.getSentinelWorkerId(), lockResolver, publisher);

        ProcessEngineConfigurationImpl config = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneInMemProcessEngineConfiguration();
        config.setDataSource(proxiedDataSource);
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
    void a2ExternalTaskBehavior_createsTaskWithSingleInsertNoFollowUpUpdate() {
        RepositoryService repositoryService = processEngine.getRepositoryService();
        RuntimeService runtimeService = processEngine.getRuntimeService();

        repositoryService.createDeployment()
                .addString("a2-guard.bpmn20.xml", A2_GUARD_PROCESS_XML)
                .deploy();

        sqlCounter.reset();

        runtimeService.startProcessInstanceByKey("a2GuardProcess");

        assertThat(sqlCounter.insertCount()).isEqualTo(1);
        assertThat(sqlCounter.updateCount()).isEqualTo(0);
    }

    private static final String A2_GUARD_PROCESS_XML = """
            <?xml version="1.0" encoding="UTF-8"?>
            <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                         xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                         targetNamespace="http://threeai.com/a2-guard">
              <process id="a2GuardProcess" isExecutable="true" camunda:historyTimeToLive="180">
                <startEvent id="start" />
                <sequenceFlow id="flow1" sourceRef="start" targetRef="a2Task" />
                <serviceTask id="a2Task" camunda:type="external" camunda:topic="a2-guard-topic" />
                <sequenceFlow id="flow2" sourceRef="a2Task" targetRef="end" />
                <endEvent id="end" />
              </process>
            </definitions>
            """;

    /**
     * Counts DML statements against {@code ACT_RU_EXT_TASK} only — every other table this
     * process touches (execution, process-instance, etc.) is irrelevant to this guard's claim.
     */
    static class SqlStatementCounter implements QueryExecutionListener {

        private final AtomicInteger inserts = new AtomicInteger();
        private final AtomicInteger updates = new AtomicInteger();

        void reset() {
            inserts.set(0);
            updates.set(0);
        }

        int insertCount() {
            return inserts.get();
        }

        int updateCount() {
            return updates.get();
        }

        @Override
        public void beforeQuery(ExecutionInfo executionInfo, List<QueryInfo> queryInfoList) {
            // no-op — counting happens after successful execution
        }

        @Override
        public void afterQuery(ExecutionInfo executionInfo, List<QueryInfo> queryInfoList) {
            for (QueryInfo queryInfo : queryInfoList) {
                String sql = queryInfo.getQuery();
                if (sql == null) {
                    continue;
                }
                String normalized = sql.trim().toUpperCase(Locale.ROOT);
                if (!normalized.contains("ACT_RU_EXT_TASK")) {
                    continue;
                }
                if (normalized.startsWith("INSERT")) {
                    inserts.incrementAndGet();
                } else if (normalized.startsWith("UPDATE")) {
                    updates.incrementAndGet();
                }
            }
        }
    }
}
