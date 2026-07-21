package com.threeai.nats.history.projection;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code history.projection.datasource.*} — the projection Postgres instance/schema
 * (ARCH-Q2/ADR-0011, `DB_SCHEMA.md §2`), physically SEPARATE from {@code
 * history.vault.datasource.*} (`08_config.md §7`).
 *
 * <p><b>CODER-NOTE:</b> {@code 08_config.md} names this prefix in §7's comment ("AYRI Postgres
 * örneği (ARCH-Q2) — history.projection.datasource ile PAYLAŞILMAZ") but does not itself provide
 * a class stub for it (only {@code PseudonymVaultDataSourceProperties}, the vault side, is
 * sketched). This class mirrors that sketch's shape for the projection side — own connection
 * pooling (`CODING_GUIDELINES_JAVA.md §6.2`), never Spring Boot's single auto-configured
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
