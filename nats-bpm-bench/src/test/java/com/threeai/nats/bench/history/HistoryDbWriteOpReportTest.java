package com.threeai.nats.bench.history;

import static org.assertj.core.api.Assertions.assertThat;

import com.threeai.nats.bench.BenchMode;
import org.junit.jupiter.api.Test;

class HistoryDbWriteOpReportTest {

    @Test
    void passesHardGate_historyOffload_requiresZeroActHiWrites() {
        HistoryDbWriteOpReport clean = new HistoryDbWriteOpReport(0, 5, 0, BenchMode.HISTORY_OFFLOAD);
        HistoryDbWriteOpReport dirty = new HistoryDbWriteOpReport(1, 5, 0, BenchMode.HISTORY_OFFLOAD);

        assertThat(clean.passesHardGate()).isTrue();
        assertThat(dirty.passesHardGate()).isFalse(); // BUS_BENCH_HISTORY_METRIC_REGRESSION
    }

    @Test
    void passesHardGate_dbHistoryBaseline_alwaysPasses() {
        HistoryDbWriteOpReport baseline = new HistoryDbWriteOpReport(42, 0, 0, BenchMode.DB_HISTORY_BASELINE);

        assertThat(baseline.passesHardGate()).isTrue();
    }
}
