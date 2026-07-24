package com.threeai.nats.history.governance;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/**
 * Real Spring Boot binding for {@code history.retention.*} (08_config.md §6), plus direct unit
 * coverage of {@link RetentionProperties#windowFor(String)}'s per-class-override branch and
 * {@link RetentionProperties#isBelowLegalMinimum(String, Duration)}'s non-audit-critical
 * short-circuit (both untouched by RetentionEnforcementJobTest's scenarios).
 */
class RetentionPropertiesTest {

    @Configuration
    @EnableConfigurationProperties(RetentionProperties.class)
    static class TestConfig {
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfig.class);

    @Test
    void noProperties_defaultsApply() {
        runner.run(context -> {
            RetentionProperties props = context.getBean(RetentionProperties.class);
            assertThat(props.getBulkDefaultDays()).isEqualTo(90);
            assertThat(props.getAuditCriticalDefaultWindow()).isEqualTo("P7Y");
            assertThat(props.getPerClassOverrides()).isEmpty();
        });
    }

    @Test
    void kebabCaseProperties_bindOntoEveryField() {
        runner.withPropertyValues(
                "history.retention.bulk-default-days=30",
                "history.retention.audit-critical-default-window=P10Y",
                "history.retention.per-class-overrides.DETAIL=P60D"
        ).run(context -> {
            RetentionProperties props = context.getBean(RetentionProperties.class);
            assertThat(props.getBulkDefaultDays()).isEqualTo(30);
            assertThat(props.getAuditCriticalDefaultWindow()).isEqualTo("P10Y");
            assertThat(props.getPerClassOverrides()).containsEntry("DETAIL", "P60D");
        });
    }

    @Test
    void windowFor_perClassOverridePresent_usesOverrideNotAuditCriticalOrBulkDefault() {
        RetentionProperties props = new RetentionProperties();
        props.setPerClassOverrides(Map.of("INCIDENT", "P60D")); // INCIDENT is audit-critical,
        // but an explicit override still takes precedence over the audit-critical default window.

        Duration window = props.windowFor("INCIDENT");

        assertThat(window).isEqualTo(Duration.ofDays(60));
    }

    @Test
    void windowFor_perClassOverrideWithYearsAndMonths_convertsToDaysApproximation() {
        RetentionProperties props = new RetentionProperties();
        props.setPerClassOverrides(Map.of("DETAIL", "P1Y2M3D"));

        Duration window = props.windowFor("DETAIL");

        assertThat(window).isEqualTo(Duration.ofDays(365 + 2 * 30 + 3));
    }

    @Test
    void windowFor_auditCriticalNoOverride_usesAuditCriticalDefaultWindow() {
        RetentionProperties props = new RetentionProperties();

        Duration window = props.windowFor("INCIDENT");

        assertThat(window).isEqualTo(Duration.ofDays(7L * 365));
    }

    @Test
    void windowFor_nonAuditCriticalNoOverride_usesBulkDefaultDays() {
        RetentionProperties props = new RetentionProperties();
        props.setBulkDefaultDays(45);

        Duration window = props.windowFor("TASKINST");

        assertThat(window).isEqualTo(Duration.ofDays(45));
    }

    @Test
    void isBelowLegalMinimum_nonAuditCriticalClass_alwaysFalse() {
        RetentionProperties props = new RetentionProperties();

        boolean result = props.isBelowLegalMinimum("TASKINST", Duration.ofDays(1));

        assertThat(result).isFalse();
    }

    @Test
    void isBelowLegalMinimum_auditCriticalBelowLegalWindow_true() {
        RetentionProperties props = new RetentionProperties(); // P7Y default

        boolean result = props.isBelowLegalMinimum("INCIDENT", Duration.ofDays(30));

        assertThat(result).isTrue();
    }

    @Test
    void isBelowLegalMinimum_auditCriticalAtOrAboveLegalWindow_false() {
        RetentionProperties props = new RetentionProperties(); // P7Y default

        boolean result = props.isBelowLegalMinimum("INCIDENT", Duration.ofDays(8L * 365));

        assertThat(result).isFalse();
    }
}
