package com.threeai.nats.core.vault;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import javax.sql.DataSource;

import com.threeai.nats.core.history.exception.PseudonymVaultAccessDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vault-DB CLIENT — talks to the physically separate pseudonym-vault Postgres (`DB_SCHEMA.md`
 * §0/§3, ARCH-Q2). Never called from the audit-critical hot path directly; invoked
 * downstream/async from {@link com.threeai.nats.history.projection.HistoryProjectionConsumer}
 * (BA-Q5) once the projection row carrying {@code pseudonym_token} is committed.
 *
 * <p><b>CODER-NOTE (constructor, beyond the LLD sketch):</b> a trailing {@code String
 * columnEncryptionKey} parameter was added. {@code pgp_sym_encrypt}/{@code pgp_sym_decrypt}
 * require the actual pgcrypto symmetric key material — {@code
 * PseudonymVaultDataSourceProperties.vaultColumnEncryptionKeyRef} is documented as "OpenBao/deploy-secret
 * referansı" (a reference, not the secret itself, `08_config.md` §7); resolving that reference is
 * I/O performed by the config layer at bootstrap (same pattern as
 * {@code PseudonymTokenGenerator}'s CODER-NOTE), and the resolved key material is handed to this
 * client's constructor.
 */
public class PseudonymizationVaultClient {

    private static final Logger log = LoggerFactory.getLogger(PseudonymizationVaultClient.class);

    private final DataSource vaultDataSource;
    private final VaultAccessAuditor auditor;
    private final String columnEncryptionKey;

    public PseudonymizationVaultClient(DataSource vaultDataSource, VaultAccessAuditor auditor, String columnEncryptionKey) {
        this.vaultDataSource = vaultDataSource;
        this.auditor = auditor;
        this.columnEncryptionKey = columnEncryptionKey;
    }

    /**
     * INSERT {@code pseudonym_map} (idempotent — {@code ON CONFLICT (pseudonym_token) DO
     * NOTHING}, since the same deterministic token from the same real value is expected to
     * repeat). Writes {@code vault_access_audit} (operation=WRITE). Unreachable vault →
     * {@code SYS_PSEUDONYM_VAULT_UNAVAILABLE}, caller retries; audit-critical outbox/relay/NATS
     * flow is NEVER blocked by this (BA-Q5).
     */
    public void persistMapping(String pseudonymToken, String engineId, String realUserId, int tenantKeyVersion,
            String sourceClass) {
        String sql = "INSERT INTO pseudonym_map "
                + "(pseudonym_token, engine_id, tenant_key_version, real_user_id_encrypted, source_class) "
                + "VALUES (?,?,?, pgp_sym_encrypt(?, ?), ?) ON CONFLICT (pseudonym_token) DO NOTHING";
        try (Connection connection = vaultDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, pseudonymToken);
            stmt.setString(2, engineId);
            stmt.setInt(3, tenantKeyVersion);
            stmt.setString(4, realUserId);
            stmt.setString(5, columnEncryptionKey);
            stmt.setString(6, sourceClass);
            stmt.executeUpdate();
            auditor.record(pseudonymToken, "WRITE", "system:history-projection-consumer", null, true);
            log.info("Pseudonym mapping persisted", kv("source_class", sourceClass), kv("engine_id", engineId));
        } catch (SQLException e) {
            log.error("Pseudonym vault unavailable — mapping write failed, caller may retry",
                    kv("source_class", sourceClass), e);
            throw new IllegalStateException("SYS_PSEUDONYM_VAULT_UNAVAILABLE: failed to persist pseudonym mapping", e);
        }
    }

    /**
     * DELETE FROM {@code pseudonym_map} — hard delete, irreversible (ADR-0016). Called by {@code
     * ErasurePipeline} when an erasure request targets a pseudonymized audit-critical record.
     * Writes {@code vault_access_audit} (operation=DELETE) → {@code BUS_PSEUDONYM_MAP_ENTRY_DELETED}.
     */
    public void deleteMapping(String pseudonymToken, String requestedBy) {
        String sql = "DELETE FROM pseudonym_map WHERE pseudonym_token = ?";
        try (Connection connection = vaultDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, pseudonymToken);
            int affected = stmt.executeUpdate();
            auditor.record(pseudonymToken, "DELETE", requestedBy, null, true);
            log.info("Pseudonym map entry deleted (irreversible)", kv("requested_by", requestedBy),
                    kv("rows_affected", affected)); // BUS_PSEUDONYM_MAP_ENTRY_DELETED
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to delete pseudonym_map entry", e);
        }
    }

    /**
     * Rare, authorized re-identification — requires an explicit reason string, always audited
     * (operation=REIDENTIFY_ATTEMPT). Unauthorized caller → {@code
     * AUTH_PSEUDONYM_VAULT_ACCESS_DENIED} (CRITICAL, security-page) — the access-control decision
     * is made by the caller's own authz layer BEFORE this method is invoked; this method logs
     * {@code granted=false} too if invoked without authorization (defense-in-depth, DP-16).
     */
    public Optional<String> reidentify(String pseudonymToken, String requesterIdentity, String reason, boolean authorized) {
        auditor.record(pseudonymToken, "REIDENTIFY_ATTEMPT", requesterIdentity, reason, authorized);
        if (!authorized) {
            log.error("Unauthorized pseudonym re-identification attempt",
                    kv("requester_identity", requesterIdentity), kv("reason", reason));
            throw new PseudonymVaultAccessDeniedException(
                    "Re-identification denied — caller not authorized: " + requesterIdentity);
        }
        String sql = "SELECT pgp_sym_decrypt(real_user_id_encrypted, ?) AS real_user_id "
                + "FROM pseudonym_map WHERE pseudonym_token = ?";
        try (Connection connection = vaultDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, columnEncryptionKey);
            stmt.setString(2, pseudonymToken);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                auditor.record(pseudonymToken, "READ", requesterIdentity, reason, true);
                return Optional.ofNullable(rs.getString("real_user_id"));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to re-identify pseudonym token", e);
        }
    }
}
