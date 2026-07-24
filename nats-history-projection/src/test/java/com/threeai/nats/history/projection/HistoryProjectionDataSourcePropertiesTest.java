package com.threeai.nats.history.projection;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/**
 * Real Spring Boot {@code @ConfigurationProperties} binding for {@code
 * history.projection.datasource.*} (ARCH-Q2/ADR-0011, DB_SCHEMA.md §2) — the projection Postgres
 * pool, physically SEPARATE from {@code history.vault.datasource.*} (mirrors
 * PseudonymVaultDataSourcePropertiesTest's pattern in nats-core).
 */
class HistoryProjectionDataSourcePropertiesTest {

    @Configuration
    @EnableConfigurationProperties(HistoryProjectionDataSourceProperties.class)
    static class TestConfig {
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfig.class);

    @Test
    void noProperties_fieldsRemainNull() {
        runner.run(context -> {
            HistoryProjectionDataSourceProperties props = context.getBean(HistoryProjectionDataSourceProperties.class);
            assertThat(props.getJdbcUrl()).isNull();
            assertThat(props.getUsername()).isNull();
            assertThat(props.getPassword()).isNull();
        });
    }

    @Test
    void kebabCaseProperties_bindOntoEveryField() {
        runner.withPropertyValues(
                "history.projection.datasource.jdbc-url=jdbc:postgresql://projection-db:5432/history",
                "history.projection.datasource.username=projection-app",
                "history.projection.datasource.password=s3cr3t"
        ).run(context -> {
            HistoryProjectionDataSourceProperties props = context.getBean(HistoryProjectionDataSourceProperties.class);
            assertThat(props.getJdbcUrl()).isEqualTo("jdbc:postgresql://projection-db:5432/history");
            assertThat(props.getUsername()).isEqualTo("projection-app");
            assertThat(props.getPassword()).isEqualTo("s3cr3t");
        });
    }

    @Test
    void prefixIsHistoryProjectionDatasource_confirmedViaAnnotation() {
        ConfigurationProperties annotation =
                HistoryProjectionDataSourceProperties.class.getAnnotation(ConfigurationProperties.class);
        assertThat(annotation.prefix()).isEqualTo("history.projection.datasource");
    }
}
