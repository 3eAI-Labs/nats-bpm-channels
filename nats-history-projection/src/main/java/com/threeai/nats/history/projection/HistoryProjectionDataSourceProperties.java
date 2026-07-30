package com.threeai.nats.history.projection;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code history.projection.datasource.*} — the projection Postgres instance/schema
 * (ARCH-Q2/ADR-0011), physically SEPARATE from {@code history.vault.datasource.*}.
 *
 * <p><b>CODER-NOTE:</b> the config design mentions this prefix only in passing, in the vault
 * section's comment ("a SEPARATE Postgres instance (ARCH-Q2) — NOT shared with
 * {@code history.projection.datasource}"), and does not itself provide
 * a class stub for it (only {@code PseudonymVaultDataSourceProperties}, the vault side, is
 * sketched). This class mirrors that sketch's shape for the projection side — own connection
 * pooling, never Spring Boot's single auto-configured
 * {@code DataSource}.
 */
@ConfigurationProperties(prefix = "history.projection.datasource")
public class HistoryProjectionDataSourceProperties {

    private String jdbcUrl;
    private String username;
    private String password;

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
}
