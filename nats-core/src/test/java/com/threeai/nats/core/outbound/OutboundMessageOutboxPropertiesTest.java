package com.threeai.nats.core.outbound;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/**
 * Real Spring Boot {@code @ConfigurationProperties} binding for {@code
 * spring.nats.outbound.outbox.*} — proves the prefix/field names actually bind, plus the
 * {@link OutboundMessageOutboxProperties#stuckThresholdSeconds()} derived-value business logic.
 */
class OutboundMessageOutboxPropertiesTest {

    @Configuration
    @EnableConfigurationProperties(OutboundMessageOutboxProperties.class)
    static class TestConfig {
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfig.class);

    @Test
    void noProperties_defaultsApply() {
        runner.run(context -> {
            OutboundMessageOutboxProperties props = context.getBean(OutboundMessageOutboxProperties.class);
            assertThat(props.getRelayCyclePeriodSeconds()).isEqualTo(30);
            assertThat(props.getStuckThresholdMultiplier()).isEqualTo(5);
            assertThat(props.stuckThresholdSeconds()).isEqualTo(150);
        });
    }

    @Test
    void kebabCaseProperties_bindOntoBothFields() {
        runner.withPropertyValues(
                "spring.nats.outbound.outbox.relay-cycle-period-seconds=10",
                "spring.nats.outbound.outbox.stuck-threshold-multiplier=3"
        ).run(context -> {
            OutboundMessageOutboxProperties props = context.getBean(OutboundMessageOutboxProperties.class);
            assertThat(props.getRelayCyclePeriodSeconds()).isEqualTo(10);
            assertThat(props.getStuckThresholdMultiplier()).isEqualTo(3);
            assertThat(props.stuckThresholdSeconds()).isEqualTo(30);
        });
    }

    @Test
    void prefixIsSpringNatsOutboundOutbox_confirmedViaAnnotation() {
        ConfigurationProperties annotation =
                OutboundMessageOutboxProperties.class.getAnnotation(ConfigurationProperties.class);
        assertThat(annotation.prefix()).isEqualTo("spring.nats.outbound.outbox");
    }
}
