package com.threeai.nats.core.vault;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * Writes {@code vault_access_audit} rows — every WRITE/READ/DELETE/REIDENTIFY_ATTEMPT (DP-16:
 * en-az-yetki + audit). Unauthorized attempts are logged too ({@code granted=false}) —
 * {@code AUTH_PSEUDONYM_VAULT_ACCESS_DENIED} trail (`DB_SCHEMA.md` §3).
 */
public class VaultAccessAuditor {

    private static final String INSERT_SQL =
            "INSERT INTO vault_access_audit (pseudonym_token, operation, accessor_identity, access_reason, granted) "
          + "VALUES (?,?,?,?,?)";

    private final DataSource vaultDataSource;

    public VaultAccessAuditor(DataSource vaultDataSource) {
        this.vaultDataSource = vaultDataSource;
    }

    public void record(String pseudonymToken, String operation, String accessorIdentity, String accessReason, boolean granted) {
        try (Connection connection = vaultDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(INSERT_SQL)) {
            stmt.setString(1, pseudonymToken);
            stmt.setString(2, operation);
            stmt.setString(3, accessorIdentity);
            stmt.setString(4, accessReason);
            stmt.setBoolean(5, granted);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to write vault_access_audit row for operation " + operation, e);
        }
    }
}
