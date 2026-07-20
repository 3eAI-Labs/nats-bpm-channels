package com.threeai.nats.history.vault;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code history.vault.datasource.*} (`08_config.md §7`) — the physically SEPARATE pseudonym
 * vault Postgres instance (ARCH-Q2, ADR-0016, DP-16). {@code vaultColumnEncryptionKeyRef} is a
 * reference (OpenBao/deploy-secret path), not the key material itself; resolving that reference
 * into the actual {@code pgcrypto} symmetric key is I/O performed once at config/bootstrap layer
 * (see {@link PseudonymizationVaultClient}'s constructor CODER-NOTE) and never logged (DP-1).
 */
@ConfigurationProperties(prefix = "history.vault.datasource")
public class PseudonymVaultDataSourceProperties {

    private String jdbcUrl;
    private String username;
    private String password;
    private String vaultColumnEncryptionKeyRef;

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getVaultColumnEncryptionKeyRef() {
        return vaultColumnEncryptionKeyRef;
    }

    public void setVaultColumnEncryptionKeyRef(String vaultColumnEncryptionKeyRef) {
        this.vaultColumnEncryptionKeyRef = vaultColumnEncryptionKeyRef;
    }
}
