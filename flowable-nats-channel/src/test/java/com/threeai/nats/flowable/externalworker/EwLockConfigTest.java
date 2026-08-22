package com.threeai.nats.flowable.externalworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** D-B'v3: ADR-0001 umbrella floor is a HARD constraint; the recovery bound is only an SLO. */
class EwLockConfigTest {

    @Test
    void derivedDefault_isAboveFloor_andSafe() {
        EwLockConfig config = new EwLockConfig(new EwProperties());
        assertThat(config.isUnsafe()).isFalse();
        assertThat(config.lockDurationMillis()).isGreaterThan(0);
    }

    @Test
    void explicitL_belowFloor_throwsAtConstruction() {
        EwProperties p = new EwProperties();
        p.setLockDurationSeconds(1L);
        assertThatThrownBy(() -> new EwLockConfig(p))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("VAL_UMBRELLA_LOCK_TOO_SHORT");
    }

    @Test
    void explicitL_belowFloor_withEscapeHatch_isUnsafeNotFatal() {
        EwProperties p = new EwProperties();
        p.setLockDurationSeconds(1L);
        p.setAllowUnsafeLockDuration(true);
        EwLockConfig config = new EwLockConfig(p);
        assertThat(config.isUnsafe()).isTrue();
        assertThat(config.lockDurationMillis()).isEqualTo(1000L);
    }

    @Test
    void explicitL_atFloor_isSafe() {
        EwProperties p = new EwProperties();
        long floor = com.threeai.nats.core.config.UmbrellaLockCalculator.floorSeconds(
                p.getAckWaitSeconds(), p.getMaxDeliver(), p.getSweepPeriodSeconds(), p.getEpsilonSeconds());
        p.setLockDurationSeconds(floor);
        assertThatCode(() -> new EwLockConfig(p)).doesNotThrowAnyException();
        assertThat(new EwLockConfig(p).isUnsafe()).isFalse();
    }
}
