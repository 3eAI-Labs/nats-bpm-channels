package com.threeai.nats.history.query;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Direct unit coverage of {@link PageRequest}'s compact-constructor defensive clamps. */
class PageRequestTest {

    @Test
    void validPageAndSize_unchanged() {
        PageRequest req = new PageRequest(3, 50);

        assertThat(req.page()).isEqualTo(3);
        assertThat(req.size()).isEqualTo(50);
    }

    @Test
    void negativePage_clampedToZero() {
        PageRequest req = new PageRequest(-1, 50);

        assertThat(req.page()).isZero();
    }

    @Test
    void sizeBelowOne_clampedToDefaultTwenty() {
        PageRequest req = new PageRequest(0, 0);

        assertThat(req.size()).isEqualTo(20);
    }

    @Test
    void negativeSize_clampedToDefaultTwenty() {
        PageRequest req = new PageRequest(0, -5);

        assertThat(req.size()).isEqualTo(20);
    }

    @Test
    void sizeAboveTwoHundred_clampedToTwoHundred() {
        PageRequest req = new PageRequest(0, 500);

        assertThat(req.size()).isEqualTo(200);
    }

    @Test
    void sizeExactlyTwoHundred_unchanged() {
        PageRequest req = new PageRequest(0, 200);

        assertThat(req.size()).isEqualTo(200);
    }

    @Test
    void ofFactory_delegatesToCanonicalConstructor() {
        PageRequest req = PageRequest.of(-1, 1000);

        assertThat(req.page()).isZero();
        assertThat(req.size()).isEqualTo(200);
    }

    @Test
    void offset_computedAsPageTimesSize() {
        PageRequest req = new PageRequest(3, 20);

        assertThat(req.offset()).isEqualTo(60);
    }
}
