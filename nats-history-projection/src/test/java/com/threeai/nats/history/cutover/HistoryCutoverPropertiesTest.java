package com.threeai.nats.history.cutover;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/** Real Spring Boot binding for {@code history.cutover.*} (08_config.md §5, BR-HDL-005). */
class HistoryCutoverPropertiesTest {

    @Configuration
    @EnableConfigurationProperties(HistoryCutoverProperties.class)
    static class TestConfig {
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfig.class);

    @Test
    void noProperties_defaultVolumePriorityOrderApplies() {
        runner.run(context -> {
            HistoryCutoverProperties props = context.getBean(HistoryCutoverProperties.class);
            assertThat(props.getVolumePriorityOrder())
                    .startsWith("DETAIL", "VARINST", "ACTINST")
                    .contains("INCIDENT", "OP_LOG");
        });
    }

    @Test
    void kebabCaseProperty_overridesVolumePriorityOrder() {
        runner.withPropertyValues(
                "history.cutover.volume-priority-order=TASKINST,PROCINST"
        ).run(context -> {
            HistoryCutoverProperties props = context.getBean(HistoryCutoverProperties.class);
            assertThat(props.getVolumePriorityOrder()).containsExactly("TASKINST", "PROCINST");
        });
    }

    @Test
    void setVolumePriorityOrder_directly_roundTrips() {
        HistoryCutoverProperties props = new HistoryCutoverProperties();

        props.setVolumePriorityOrder(List.of("BATCH", "COMMENT"));

        assertThat(props.getVolumePriorityOrder()).containsExactly("BATCH", "COMMENT");
    }
}
