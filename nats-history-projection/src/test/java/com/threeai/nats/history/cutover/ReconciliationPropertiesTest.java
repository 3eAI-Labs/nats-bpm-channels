package com.threeai.nats.history.cutover;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/**
 * Real Spring Boot binding for {@code history.reconciliation.*} (08_config.md §5), plus direct
 * unit coverage of {@link ReconciliationProperties#setCleanStreakTargetDefault(int)}'s defensive
 * VAL_RECONCILIATION_WINDOW_N_INVALID guard (rejects non-positive values, keeps the prior value).
 */
class ReconciliationPropertiesTest {

    @Configuration
    @EnableConfigurationProperties(ReconciliationProperties.class)
    static class TestConfig {
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfig.class);

    @Test
    void noProperties_defaultsApply() {
        runner.run(context -> {
            ReconciliationProperties props = context.getBean(ReconciliationProperties.class);
            assertThat(props.getCron()).isEqualTo("0 0 3 * * *");
            assertThat(props.getCleanStreakTargetOverrides()).isEmpty();
            assertThat(props.getBulkEpsilonOverrides()).isEmpty();
            assertThat(props.getAuditCriticalClasses()).isNotEmpty();
            assertThat(props.cleanStreakTargetFor("ACT_HI_PROCINST")).isEqualTo(7);
            assertThat(props.bulkEpsilonFor("ACT_HI_DETAIL")).isZero();
        });
    }

    @Test
    void kebabCaseProperties_bindOntoEveryField() {
        runner.withPropertyValues(
                "history.reconciliation.cron=0 30 4 * * *",
                "history.reconciliation.clean-streak-target-default=10",
                "history.reconciliation.clean-streak-target-overrides.ACT_HI_PROCINST=3",
                "history.reconciliation.bulk-epsilon-overrides.ACT_HI_DETAIL=50",
                "history.reconciliation.audit-critical-classes=ACT_HI_PROCINST,ACT_HI_TASKINST"
        ).run(context -> {
            ReconciliationProperties props = context.getBean(ReconciliationProperties.class);
            assertThat(props.getCron()).isEqualTo("0 30 4 * * *");
            assertThat(props.getCleanStreakTargetDefault()).isEqualTo(10);
            assertThat(props.getCleanStreakTargetOverrides()).containsEntry("ACT_HI_PROCINST", 3);
            assertThat(props.getBulkEpsilonOverrides()).containsEntry("ACT_HI_DETAIL", 50L);
            assertThat(props.getAuditCriticalClasses()).containsExactlyInAnyOrder("ACT_HI_PROCINST", "ACT_HI_TASKINST");
            assertThat(props.cleanStreakTargetFor("ACT_HI_PROCINST")).isEqualTo(3);
            assertThat(props.cleanStreakTargetFor("ACT_HI_OTHER")).isEqualTo(10);
            assertThat(props.bulkEpsilonFor("ACT_HI_DETAIL")).isEqualTo(50L);
        });
    }

    @Test
    void setCleanStreakTargetDefault_zeroOrNegative_rejectedKeepsPriorValue() {
        ReconciliationProperties props = new ReconciliationProperties();
        assertThat(props.getCleanStreakTargetDefault()).isEqualTo(7); // built-in default

        props.setCleanStreakTargetDefault(0);
        assertThat(props.getCleanStreakTargetDefault()).isEqualTo(7); // rejected, unchanged

        props.setCleanStreakTargetDefault(-5);
        assertThat(props.getCleanStreakTargetDefault()).isEqualTo(7); // rejected, unchanged

        props.setCleanStreakTargetDefault(3);
        assertThat(props.getCleanStreakTargetDefault()).isEqualTo(3); // valid, applied

        props.setCleanStreakTargetDefault(0); // now try to reject after a valid value was set
        assertThat(props.getCleanStreakTargetDefault()).isEqualTo(3); // still rejected, prior value kept
    }

    @Test
    void settersDirectly_roundTripCorrectly() {
        ReconciliationProperties props = new ReconciliationProperties();

        props.setCron("0 0 5 * * *");
        props.setCleanStreakTargetOverrides(Map.of("ACT_HI_PROCINST", 2));
        props.setAuditCriticalClasses(Set.of("ACT_HI_PROCINST"));

        assertThat(props.getCron()).isEqualTo("0 0 5 * * *");
        assertThat(props.getCleanStreakTargetOverrides()).containsEntry("ACT_HI_PROCINST", 2);
        assertThat(props.getAuditCriticalClasses()).containsExactly("ACT_HI_PROCINST");
    }
}
