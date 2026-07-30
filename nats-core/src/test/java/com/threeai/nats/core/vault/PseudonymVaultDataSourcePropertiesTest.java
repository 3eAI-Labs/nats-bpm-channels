package com.threeai.nats.core.vault;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/**
 * Real Spring Boot {@code @ConfigurationProperties} binding for {@code history.vault.datasource.*}
 * (ARCH-Q2, ADR-0016, DP-16 — the pseudonym vault's physically SEPARATE Postgres pool) — proves the
 * prefix and every field name (including {@code vaultColumnEncryptionKeyRef}, the one field whose
 * kebab-case mapping is least obvious) actually bind from a real property source.
 */
class PseudonymVaultDataSourcePropertiesTest {

    @Configuration
    @EnableConfigurationProperties(PseudonymVaultDataSourceProperties.class)
    static class TestConfig {
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfig.class);

    @Test
    void noProperties_fieldsRemainNull() {
        runner.run(context -> {
            PseudonymVaultDataSourceProperties props = context.getBean(PseudonymVaultDataSourceProperties.class);
            assertThat(props.getJdbcUrl()).isNull();
            assertThat(props.getUsername()).isNull();
            assertThat(props.getPassword()).isNull();
            assertThat(props.getVaultColumnEncryptionKeyRef()).isNull();
        });
    }

    @Test
    void kebabCaseProperties_bindOntoEveryField() {
        runner.withPropertyValues(
                "history.vault.datasource.jdbc-url=jdbc:postgresql://vault-db:5432/vault",
                "history.vault.datasource.username=vault-app",
                "history.vault.datasource.password=s3cr3t",
                "history.vault.datasource.vault-column-encryption-key-ref=openbao://secret/pseudonym-vault/column-key"
        ).run(context -> {
            PseudonymVaultDataSourceProperties props = context.getBean(PseudonymVaultDataSourceProperties.class);
            assertThat(props.getJdbcUrl()).isEqualTo("jdbc:postgresql://vault-db:5432/vault");
            assertThat(props.getUsername()).isEqualTo("vault-app");
            assertThat(props.getPassword()).isEqualTo("s3cr3t");
            assertThat(props.getVaultColumnEncryptionKeyRef()).isEqualTo("openbao://secret/pseudonym-vault/column-key");
        });
    }

    @Test
    void prefixIsHistoryVaultDatasource_confirmedViaAnnotation() {
        ConfigurationProperties annotation =
                PseudonymVaultDataSourceProperties.class.getAnnotation(ConfigurationProperties.class);
        assertThat(annotation.prefix()).isEqualTo("history.vault.datasource");
    }
}
