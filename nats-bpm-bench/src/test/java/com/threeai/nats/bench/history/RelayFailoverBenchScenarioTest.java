package com.threeai.nats.bench.history;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** Verifies the deliberate Phase 5.5 deferral contract (see class-level CODER-NOTE). */
class RelayFailoverBenchScenarioTest {

    @Test
    void run_isDesignOnly_throwsUnsupportedOperationException() {
        RelayFailoverBenchScenario scenario = new RelayFailoverBenchScenario();

        assertThatThrownBy(() -> scenario.run(null, 3))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Phase 5.5");
    }
}
