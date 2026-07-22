package com.threeai.nats.bench.largevariable;

import com.threeai.nats.bench.BenchMode;

/**
 * D-C' threshold-calibration fingerprint (`docs/08-large-variable-externalization.md` §3 bench
 * item). Unlike basamak-2's {@code HistoryDbWriteOpReport} (op COUNT — every offloaded write
 * disappears from {@code ACT_HI_*} entirely), basamak-3's externalized value is NOT eliminated
 * from {@code ACT_GE_BYTEARRAY} — the row shrinks to a small marker. The meaningful signal here is
 * therefore total STORED BYTES for the benchmarked variables, not op count.
 *
 * @param totalBytesStored  {@code SUM(octet_length(BYTES_))} across the benchmarked variables'
 *                          {@code ACT_GE_BYTEARRAY} rows, measured AFTER any deferred
 *                          externalization has settled.
 * @param instanceCount     number of process instances driven (each sets ONE candidate variable).
 * @param payloadBytes      the (uniform) byte length of the value written per instance.
 * @param thresholdBytes    the {@code history.large-variable.threshold-bytes} value active for
 *                          this run (only meaningful for {@link BenchMode#LARGE_VARIABLE_EXTERNALIZED}).
 */
public record LargeVariableDbWriteSizeReport(
        long totalBytesStored, int instanceCount, int payloadBytes, int thresholdBytes, BenchMode mode) {

    /** @return bytes that would have been stored had NOTHING been externalized (the naive baseline). */
    public long naiveTotalBytes() {
        return (long) instanceCount * payloadBytes;
    }

    /**
     * D-C' calibration signal: for {@link BenchMode#LARGE_VARIABLE_EXTERNALIZED} with
     * {@code payloadBytes > thresholdBytes}, stored bytes must be DRAMATICALLY smaller than the
     * naive total (the externalization marker is a few dozen bytes vs a payload that can be KB+).
     * Not a fixed percentage — deliberately permissive (&lt; 50%) so this calibration signal stays
     * meaningful across a wide range of {@code (payloadBytes, thresholdBytes)} pairs a calibration
     * sweep might explore, rather than over-fitting to one specific ratio.
     */
    public boolean showsExpectedReduction() {
        if (mode != BenchMode.LARGE_VARIABLE_EXTERNALIZED || payloadBytes <= thresholdBytes) {
            return true; // nothing should have been externalized -- no reduction expected
        }
        return totalBytesStored < naiveTotalBytes() / 2;
    }
}
