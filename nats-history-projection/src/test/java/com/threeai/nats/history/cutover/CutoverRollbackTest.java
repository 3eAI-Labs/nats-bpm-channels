package com.threeai.nats.history.cutover;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.Duration;

import com.threeai.nats.core.db.SqlMigrationRunner;
import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.history.cutover.ClassCutoverState.ConsistencyPath;
import com.threeai.nats.history.cutover.ClassCutoverState.CutoverState;
import io.nats.client.KeyValue;
import io.nats.client.Nats;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class CutoverRollbackTest {

    private static PostgreSQLContainer<?> postgres;
    private static GenericContainer<?> natsContainer;
    private static PGSimpleDataSource dataSource;
    private static io.nats.client.Connection natsConnection;
    private static ClassCutoverStateStore stateStore;
    private static JetStreamKvManager kvManager;

    @BeforeAll
    static void setUp() throws Exception {
        postgres = new PostgreSQLContainer<>("postgres:16");
        postgres.start();
        dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgres.getJdbcUrl());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/projection/V1__entity_lifecycle_tables.sql");
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/projection/V2__append_log_tables.sql");
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/projection/V3__control_plane_and_compliance.sql");
        SqlMigrationRunner.applyClasspathScript(dataSource, "db/migration/projection/V4__large_payload_content_addressing.sql");
        stateStore = new ClassCutoverStateStore(dataSource);

        natsContainer = new GenericContainer<>("nats:2.10-alpine").withCommand("--jetstream").withExposedPorts(4222);
        natsContainer.start();
        natsConnection = Nats.connect("nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222));
        kvManager = new JetStreamKvManager();
        kvManager.ensureBucket("history-cutover-state", Duration.ZERO, 1, natsConnection);
    }

    @AfterAll
    static void tearDown() throws Exception {
        natsConnection.close();
        natsContainer.stop();
        postgres.stop();
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection c = dataSource.getConnection(); java.sql.Statement stmt = c.createStatement()) {
            stmt.execute("TRUNCATE class_cutover_state");
        }
    }

    @Test
    void rollback_cutoverlanmisClass_returnsToDualRun_writesKvFalse() throws Exception {
        stateStore.findOrCreate("camunda", HistoryClassNames.OP_LOG, ConsistencyPath.AUDIT_CRITICAL, 7);
        stateStore.recordCleanCycle("camunda", HistoryClassNames.OP_LOG, 7, 0, CutoverState.N_GUN_TEMIZ);
        stateStore.markCutoverRequested("camunda", HistoryClassNames.OP_LOG);
        stateStore.markCutoverApplied("camunda", HistoryClassNames.OP_LOG, java.time.Instant.now());

        new CutoverRollback(stateStore, kvManager, natsConnection)
                .rollback("camunda", HistoryClassNames.OP_LOG, "ops-user-1", "incident-123 investigation");

        ClassCutoverState state = stateStore.find("camunda", HistoryClassNames.OP_LOG).orElseThrow();
        assertThat(state.state()).isEqualTo(CutoverState.DUAL_RUN);
        assertThat(state.cleanStreakDays()).isZero();
        assertThat(state.rollbackCount()).isEqualTo(1);
        assertThat(state.lastRollbackAt()).isNotNull();

        KeyValue kv = natsConnection.keyValue("history-cutover-state");
        assertThat(new String(kv.get("cutover.camunda.OP_LOG").getValue(), StandardCharsets.UTF_8)).isEqualTo("false");
    }

    @Test
    void rollback_multipleInvocations_incrementsRollbackCount() {
        stateStore.findOrCreate("camunda", HistoryClassNames.INCIDENT, ConsistencyPath.AUDIT_CRITICAL, 7);
        CutoverRollback rollback = new CutoverRollback(stateStore, kvManager, natsConnection);

        rollback.rollback("camunda", HistoryClassNames.INCIDENT, "ops-1", "reason-1");
        rollback.rollback("camunda", HistoryClassNames.INCIDENT, "ops-1", "reason-2");

        assertThat(stateStore.find("camunda", HistoryClassNames.INCIDENT).orElseThrow().rollbackCount()).isEqualTo(2);
    }
}
