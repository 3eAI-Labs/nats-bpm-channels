package com.threeai.nats.core.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class UmbrellaLockCalculatorTest {

    @Test
    void backoffSumSeconds_capsEachTermAt30Seconds() {
        // n=1..3 (maxDeliver=4): 2^0 + 2^1 + 2^2 = 1+2+4 = 7
        assertThat(UmbrellaLockCalculator.backoffSumSeconds(4)).isEqualTo(7);
    }

    @Test
    void backoffSumSeconds_largeMaxDeliver_capsAt30PerTerm() {
        // n=1..7: 1+2+4+8+16+30(cap, would be 32)+30(cap, would be 64) = 91
        assertThat(UmbrellaLockCalculator.backoffSumSeconds(8)).isEqualTo(91);
    }

    @Test
    void floorSeconds_matchesAdrFormula() {
        // ADR-0001 canonical example: W=30, M=4, S=120, eps=60 -> floor=307
        long floor = UmbrellaLockCalculator.floorSeconds(30, 4, 120, 60);
        assertThat(floor).isEqualTo(307);
    }

    @Test
    void deriveDefaultLSeconds_addsFixed13SecondMargin() {
        // ADR-0001 canonical example: 307 + 13 = 320
        long l = UmbrellaLockCalculator.deriveDefaultLSeconds(30, 4, 120, 60);
        assertThat(l).isEqualTo(320);
    }

    @Test
    void deriveDefaultLSeconds_marginIsAbsoluteNotProportional() {
        // Large W override: margin must stay 13s, not scale with the (much larger) floor.
        long floorLargeW = UmbrellaLockCalculator.floorSeconds(90, 4, 120, 60);
        long lLargeW = UmbrellaLockCalculator.deriveDefaultLSeconds(90, 4, 120, 60);

        assertThat(lLargeW - floorLargeW).isEqualTo(13);
    }
}
