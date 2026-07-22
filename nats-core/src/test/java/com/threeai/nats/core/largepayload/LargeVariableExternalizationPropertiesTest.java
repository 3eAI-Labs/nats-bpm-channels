package com.threeai.nats.core.largepayload;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LargeVariableExternalizationPropertiesTest {

    @Test
    void defaults_enabledTrue_thresholdFourKb() {
        LargeVariableExternalizationProperties props = new LargeVariableExternalizationProperties();

        assertThat(props.isEnabled()).isTrue();
        assertThat(props.getThresholdBytes()).isEqualTo(4096);
        assertThat(props.getSweepCyclePeriodSeconds()).isEqualTo(60);
    }

    @Test
    void exceedsThreshold_atThreshold_false() {
        LargeVariableExternalizationProperties props = new LargeVariableExternalizationProperties();
        props.setThresholdBytes(100);

        assertThat(props.exceedsThreshold(100)).isFalse();
    }

    @Test
    void exceedsThreshold_overThreshold_true() {
        LargeVariableExternalizationProperties props = new LargeVariableExternalizationProperties();
        props.setThresholdBytes(100);

        assertThat(props.exceedsThreshold(101)).isTrue();
    }

    @Test
    void exceedsThreshold_underThreshold_false() {
        LargeVariableExternalizationProperties props = new LargeVariableExternalizationProperties();
        props.setThresholdBytes(100);

        assertThat(props.exceedsThreshold(1)).isFalse();
    }
}
