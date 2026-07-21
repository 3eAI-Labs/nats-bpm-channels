package com.threeai.nats.camunda.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.threeai.nats.core.db.SqlMigrationRunner;
import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.core.history.PseudonymTokenGenerator;
import com.threeai.nats.core.vault.PseudonymizationVaultClient;
import com.threeai.nats.core.vault.VaultAccessAuditor;
import org.camunda.bpm.engine.impl.history.event.UserOperationLogEntryEventEntity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * CQ-1 (Levent, önerilen): real Postgres (Testcontainers) proof of the engine-side pseudonym-vault
 * write path — TWO physically separate Postgres instances (ARCH-Q2: engine/outbox DB and vault
 * DB), matching real deployment topology exactly (never the same schema/instance).
 */
@Testcontainers
class CompactHistoryOutboxWriterVaultIntegrationTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String COLUMN_ENCRYPTION_KEY = "test-column-key";

    private static PostgreSQLContainer<?> enginePostgres;
    private static PostgreSQLContainer<?> vaultPostgres;
    private static PGSimpleDataSource engineDataSource;
    private static PGSimpleDataSource vaultDataSource;

    @BeforeAll
    static void startContainers() {
        enginePostgres = new PostgreSQLContainer<>("postgres:16");
        enginePostgres.start();
        engineDataSource = new PGSimpleDataSource();
        engineDataSource.setUrl(enginePostgres.getJdbcUrl());
        engineDataSource.setUser(enginePostgres.getUsername());
        engineDataSource.setPassword(enginePostgres.getPassword());
        SqlMigrationRunner.applyClasspathScript(engineDataSource, "db/migration/history/V1__compact_history_outbox.sql");

        vaultPostgres = new PostgreSQLContainer<>("postgres:16");
        vaultPostgres.start();
        vaultDataSource = new PGSimpleDataSource();
        vaultDataSource.setUrl(vaultPostgres.getJdbcUrl());
        vaultDataSource.setUser(vaultPostgres.getUsername());
        vaultDataSource.setPassword(vaultPostgres.getPassword());
        SqlMigrationRunner.applyClasspathScript(vaultDataSource, "db/migration/vault/V1__pseudonym_map.sql");
    }

    @AfterAll
    static void stopContainers() {
        vaultPostgres.stop();
        enginePostgres.stop();
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection c = engineDataSource.getConnection(); var stmt = c.createStatement()) {
            stmt.execute("DELETE FROM compact_history_outbox");
        }
        try (Connection c = vaultDataSource.getConnection(); var stmt = c.createStatement()) {
            stmt.execute("TRUNCATE pseudonym_map, vault_access_audit");
        }
    }

    private HistoryClassificationProperties pseudonymizationOptInClassification() {
        HistoryClassificationProperties classification = new HistoryClassificationProperties();
        classification.setPseudonymizationOptIn(true);
        classification.setTenantKeyId("tenant-key-1");
        classification.setTenantKeyVersion(1);
        return classification;
    }

    @Test
    void write_pseudonymizationOptIn_realValueLandsInVault_tokenFlowsToOutbox_reidentifiable() throws Exception {
        VaultAccessAuditor auditor = new VaultAccessAuditor(vaultDataSource);
        PseudonymizationVaultClient vaultClient = new PseudonymizationVaultClient(vaultDataSource, auditor, COLUMN_ENCRYPTION_KEY);
        CompactHistoryOutboxWriter writer = new CompactHistoryOutboxWriter(
                engineDataSource, new PseudonymTokenGenerator(), pseudonymizationOptInClassification(), null, vaultClient);

        UserOperationLogEntryEventEntity event = new UserOperationLogEntryEventEntity();
        event.setId(UUID.randomUUID().toString());
        event.setEventType("UserOperationLog");
        event.setProcessInstanceId("proc-vault-1");
        event.setUserId("real-msisdn-905551112233");

        try (Connection connection = engineDataSource.getConnection()) {
            connection.setAutoCommit(false);
            writer.write(event, HistoryClassNames.OP_LOG, "camunda", connection);
            connection.commit();
        }

        // (1) The token -- not the raw value -- is what actually flows through the audit pipe
        // (compact_history_outbox -> relay -> HISTORY stream -> projection).
        String token = tokenFromOutboxRow(event.getId());
        assertThat(token).isNotBlank();

        // (2) Real value durably landed in the vault, downstream/async of the tx (ADR-0016) --
        // poll briefly since persistMapping is dispatched on a background executor.
        awaitVaultRow(token);

        // (3) Re-identification resolves via the map -- proves the SAME token in both places
        // maps back to the SAME real value (not just "some value").
        Optional<String> reidentified = vaultClient.reidentify(token, "compliance-officer-1", "KVKK subject access request", true);
        assertThat(reidentified).contains("real-msisdn-905551112233");
    }

    @Test
    void write_vaultUnreachable_auditFlowStillCompletes_outboxRowStillWritten() throws Exception {
        VaultAccessAuditor auditor = new VaultAccessAuditor(vaultDataSource);
        PseudonymizationVaultClient brokenVaultClient = new PseudonymizationVaultClient(
                unreachableDataSource(), auditor, COLUMN_ENCRYPTION_KEY);
        CompactHistoryOutboxWriter writer = new CompactHistoryOutboxWriter(
                engineDataSource, new PseudonymTokenGenerator(), pseudonymizationOptInClassification(), null, brokenVaultClient);

        UserOperationLogEntryEventEntity event = new UserOperationLogEntryEventEntity();
        event.setId(UUID.randomUUID().toString());
        event.setEventType("UserOperationLog");
        event.setProcessInstanceId("proc-vault-2");
        event.setUserId("real-msisdn-905559998877");

        // SYS_PSEUDONYM_VAULT_UNAVAILABLE happens async, in the background -- write() itself
        // must neither throw nor block on it (ADR-0016: vault unavailability never blocks audit).
        try (Connection connection = engineDataSource.getConnection()) {
            connection.setAutoCommit(false);
            writer.write(event, HistoryClassNames.OP_LOG, "camunda", connection);
            connection.commit();
        }

        String token = tokenFromOutboxRow(event.getId());
        assertThat(token).isNotBlank(); // the audit-critical outbox row committed regardless
    }

    private static PGSimpleDataSource unreachableDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[] {"127.0.0.1"});
        ds.setPortNumbers(new int[] {1}); // nothing listens here
        ds.setDatabaseName("unreachable");
        ds.setUser("nobody");
        return ds;
    }

    private String tokenFromOutboxRow(String historyEventId) throws Exception {
        try (Connection connection = engineDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "SELECT payload_scalar FROM compact_history_outbox WHERE history_event_id = ?")) {
            stmt.setString(1, historyEventId);
            try (ResultSet rs = stmt.executeQuery()) {
                assertThat(rs.next()).isTrue();
                Map<String, Object> payloadScalar = JSON.readValue(rs.getString("payload_scalar"), Map.class);
                return (String) payloadScalar.get("pseudonymToken");
            }
        }
    }

    /** Async vault write dispatch — poll for up to 5s rather than assert immediately. */
    private void awaitVaultRow(String token) throws Exception {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(5));
        while (Instant.now().isBefore(deadline)) {
            if (countVaultRows(token) > 0) {
                return;
            }
            Thread.sleep(50);
        }
        assertThat(countVaultRows(token)).isEqualTo(1L);
    }

    private long countVaultRows(String token) throws Exception {
        try (Connection c = vaultDataSource.getConnection();
             PreparedStatement stmt = c.prepareStatement("SELECT count(*) FROM pseudonym_map WHERE pseudonym_token = ?")) {
            stmt.setString(1, token);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }
}
