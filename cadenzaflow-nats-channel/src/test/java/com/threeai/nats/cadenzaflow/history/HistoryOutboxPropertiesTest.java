package com.threeai.nats.cadenzaflow.history;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HistoryOutboxPropertiesTest {

    @Test
    void defaults_matchLldQ5() {
        HistoryOutboxProperties props = new HistoryOutboxProperties();

        assertThat(props.getRelayCyclePeriodSeconds()).isEqualTo(30);
        assertThat(props.getStuckThresholdMultiplier()).isEqualTo(5);
        assertThat(props.stuckThresholdSeconds()).isEqualTo(150);
    }

    @Test
    void stuckThresholdSeconds_reflectsOverrides() {
        HistoryOutboxProperties props = new HistoryOutboxProperties();
        props.setRelayCyclePeriodSeconds(10);
        props.setStuckThresholdMultiplier(3);

        assertThat(props.stuckThresholdSeconds()).isEqualTo(30);
    }
}
