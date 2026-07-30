package com.threeai.nats.camunda.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import com.threeai.nats.core.history.HistoryClassNames;
import org.junit.jupiter.api.Test;

class HistoryClassificationPropertiesTest {

    @Test
    void defaults_matchPoQ5() {
        HistoryClassificationProperties props = new HistoryClassificationProperties();

        assertThat(props.getAuditCriticalClasses()).containsExactlyInAnyOrder(
                HistoryClassNames.OP_LOG, HistoryClassNames.INCIDENT, HistoryClassNames.EXT_TASK_LOG);
        assertThat(props.isPseudonymizationOptIn()).isFalse();
        assertThat(props.getTenantKeyVersion()).isEqualTo(1);
    }

    @Test
    void isAuditCritical_reflectsConfiguredSet() {
        HistoryClassificationProperties props = new HistoryClassificationProperties();
        props.setAuditCriticalClasses(Set.of(HistoryClassNames.OP_LOG));

        assertThat(props.isAuditCritical(HistoryClassNames.OP_LOG)).isTrue();
        assertThat(props.isAuditCritical(HistoryClassNames.ACTINST)).isFalse();
    }

    @Test
    void settersAndGetters_roundTrip() {
        HistoryClassificationProperties props = new HistoryClassificationProperties();
        props.setPseudonymizationOptIn(true);
        props.setTenantKeyId("tenant-key-1");
        props.setTenantKeyVersion(2);

        assertThat(props.isPseudonymizationOptIn()).isTrue();
        assertThat(props.getTenantKeyId()).isEqualTo("tenant-key-1");
        assertThat(props.getTenantKeyVersion()).isEqualTo(2);
    }
}
