package com.threeai.nats.history.projection;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/** Real Spring Boot binding for {@code history.projection.*} (08_config.md §4). */
class HistoryProjectionPropertiesTest {

    @Configuration
    @EnableConfigurationProperties(HistoryProjectionProperties.class)
    static class TestConfig {
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfig.class);

    @Test
    void noProperties_defaultsApply() {
        runner.run(context -> {
            HistoryProjectionProperties props = context.getBean(HistoryProjectionProperties.class);
            assertThat(props.getPartitionCount()).isEqualTo(8);
            assertThat(props.getPartitionAssignment()).isEmpty();
        });
    }

    @Test
    void kebabCaseProperties_bindOntoEveryField() {
        runner.withPropertyValues(
                "history.projection.partition-count=16",
                "history.projection.partition-assignment=0,1,2,3"
        ).run(context -> {
            HistoryProjectionProperties props = context.getBean(HistoryProjectionProperties.class);
            assertThat(props.getPartitionCount()).isEqualTo(16);
            assertThat(props.getPartitionAssignment()).containsExactly(0, 1, 2, 3);
        });
    }
}
