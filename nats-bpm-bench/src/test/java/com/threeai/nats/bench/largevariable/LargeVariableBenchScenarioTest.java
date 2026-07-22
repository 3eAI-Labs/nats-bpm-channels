package com.threeai.nats.bench.largevariable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

import com.threeai.nats.bench.BenchEnvironment;
import com.threeai.nats.bench.BenchMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ContainerLaunchException;

/**
 * D-C' threshold-calibration signal (`docs/08-large-variable-externalization.md` §3): externalizing
 * an over-threshold BYTES variable must shrink its {@code ACT_GE_BYTEARRAY} footprint dramatically
 * relative to the naive (never-externalized) baseline. {@code @Tag("bench")} — nightly/manual only.
 */
@Tag("bench")
class LargeVariableBenchScenarioTest {

    private static final int INSTANCE_COUNT = 5;
    private static final int PAYLOAD_BYTES = 10_000; // well over any realistic threshold

    @Test
    void externalizedMode_overThreshold_dramaticallyReducesStoredBytes() {
        try (BenchEnvironment env = BenchEnvironment.start();
             LargeVariableBenchScenario scenario = new LargeVariableBenchScenario(env)) {
            int thresholdBytes = 4096;

            LargeVariableDbWriteSizeReport baseline =
                    scenario.run(BenchMode.LARGE_VARIABLE_BASELINE, INSTANCE_COUNT, PAYLOAD_BYTES, thresholdBytes);
            LargeVariableDbWriteSizeReport externalized =
                    scenario.run(BenchMode.LARGE_VARIABLE_EXTERNALIZED, INSTANCE_COUNT, PAYLOAD_BYTES, thresholdBytes);

            // Baseline mode is the comparison reference -- every byte really is stored inline.
            assertThat(baseline.totalBytesStored()).isEqualTo(baseline.naiveTotalBytes());
            assertThat(baseline.showsExpectedReduction()).isTrue(); // baseline mode never gates

            // Externalized mode: D-C' calibration signal.
            assertThat(externalized.showsExpectedReduction()).isTrue();
            assertThat(externalized.totalBytesStored()).isLessThan(baseline.totalBytesStored());
        } catch (ContainerLaunchException dockerUnavailable) {
            abort("SYS_BENCH_LARGE_VARIABLE_ENVIRONMENT_UNAVAILABLE — Docker/Testcontainers unavailable, main CI is not blocked");
        } catch (Exception e) {
            throw new RuntimeException("Large-variable bench scenario failed", e);
        }
    }

    /** D-C' calibration sweep: as the threshold rises past the fixed payload size, externalization
     *  stops triggering at all -- stored bytes converge back to the naive baseline. */
    @Test
    void calibrationSweep_thresholdAbovePayload_noLongerExternalizes() {
        try (BenchEnvironment env = BenchEnvironment.start();
             LargeVariableBenchScenario scenario = new LargeVariableBenchScenario(env)) {
            LargeVariableDbWriteSizeReport belowThreshold = scenario.run(
                    BenchMode.LARGE_VARIABLE_EXTERNALIZED, INSTANCE_COUNT, PAYLOAD_BYTES, PAYLOAD_BYTES / 2);
            LargeVariableDbWriteSizeReport aboveThreshold = scenario.run(
                    BenchMode.LARGE_VARIABLE_EXTERNALIZED, INSTANCE_COUNT, PAYLOAD_BYTES, PAYLOAD_BYTES * 2);

            assertThat(belowThreshold.totalBytesStored()).isLessThan(belowThreshold.naiveTotalBytes());
            assertThat(aboveThreshold.totalBytesStored()).isEqualTo(aboveThreshold.naiveTotalBytes());
        } catch (ContainerLaunchException dockerUnavailable) {
            abort("SYS_BENCH_LARGE_VARIABLE_ENVIRONMENT_UNAVAILABLE — Docker/Testcontainers unavailable, main CI is not blocked");
        } catch (Exception e) {
            throw new RuntimeException("Large-variable bench calibration sweep failed", e);
        }
    }
}
