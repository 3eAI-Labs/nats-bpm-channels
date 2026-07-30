package com.threeai.nats.core.largepayload;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/**
 * Real Spring Boot {@code @ConfigurationProperties} binding for {@code
 * history.large-variable.projection-datasource.*} — proves the prefix and every field name
 * actually bind from a real property source (a plain {@code new ...(); setXxx(); getXxx();} unit
 * test would never catch a prefix/field-name typo, since the getter/setter pair would still
 * round-trip correctly on its own).
 */
class LargeVariableProjectionDataSourcePropertiesTest {

    @Configuration
    @EnableConfigurationProperties(LargeVariableProjectionDataSourceProperties.class)
    static class TestConfig {
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfig.class);

    @Test
    void noProperties_fieldsRemainNull() {
        runner.run(context -> {
            LargeVariableProjectionDataSourceProperties props =
                    context.getBean(LargeVariableProjectionDataSourceProperties.class);
            assertThat(props.getJdbcUrl()).isNull();
            assertThat(props.getUsername()).isNull();
            assertThat(props.getPassword()).isNull();
        });
    }

    @Test
    void kebabCaseProperties_bindOntoEveryField() {
        runner.withPropertyValues(
                "history.large-variable.projection-datasource.jdbc-url=jdbc:postgresql://projection-db:5432/projection",
                "history.large-variable.projection-datasource.username=projection-app",
                "history.large-variable.projection-datasource.password=s3cr3t"
        ).run(context -> {
            LargeVariableProjectionDataSourceProperties props =
                    context.getBean(LargeVariableProjectionDataSourceProperties.class);
            assertThat(props.getJdbcUrl()).isEqualTo("jdbc:postgresql://projection-db:5432/projection");
            assertThat(props.getUsername()).isEqualTo("projection-app");
            assertThat(props.getPassword()).isEqualTo("s3cr3t");
        });
    }

    @Test
    void prefixIsHistoryLargeVariableProjectionDatasource_confirmedViaAnnotation() {
        ConfigurationProperties annotation =
                LargeVariableProjectionDataSourceProperties.class.getAnnotation(ConfigurationProperties.class);
        assertThat(annotation.prefix()).isEqualTo("history.large-variable.projection-datasource");
    }
}
