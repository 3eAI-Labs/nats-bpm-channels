package com.threeai.nats.core.largepayload;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code history.large-variable.projection-datasource.*} — the RUNTIME-side connection pool
 * pointed at the SAME physical projection Postgres instance {@code nats-history-projection} owns
 * (`docs/08-large-variable-externalization.md` D-B'/D-D'). A separate pool (never sharing a
 * {@code DataSource} bean with the engine database), mirroring how {@code
 * PseudonymVaultDataSourceProperties} isolates the pseudonym-vault pool (ARCH-Q2 discipline) — the
 * physical database is shared/unified (that IS the point of D-B'/D-D'), the connection pool is not.
 */
@ConfigurationProperties(prefix = "history.large-variable.projection-datasource")
public class LargeVariableProjectionDataSourceProperties {

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
