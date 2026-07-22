package com.threeai.nats.core.largepayload;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code history.large-variable.*} — basamak-3 D-C' size-threshold + sweep cadence
 * (`docs/08-large-variable-externalization.md`). Engine-neutral (nats-core, shared by
 * {@code camunda-nats-channel}/{@code cadenzaflow-nats-channel} — same prefix, same defaults for
 * both fork mirrors, matching how {@code history.retention.*}/{@code history.vault.*} are already
 * engine-neutral rather than {@code spring.nats.camunda}-scoped).
 */
@ConfigurationProperties(prefix = "history.large-variable")
public class LargeVariableExternalizationProperties {

    /** D-C' kill-switch — {@code false} makes the custom serializer behave exactly like the
     *  built-in one it wraps (no staging/externalization at all), for a safe rollback path. */
    private boolean enabled = true;

    /** D-C' default ~4-8KB (bench-calibrated, PO-Q4 pattern) — BYTES/OBJECT/FILE values whose
     *  serialized byte length is STRICTLY GREATER than this become externalization candidates;
     *  everything at or below stays inline exactly like the built-in serializer today. */
    private int thresholdBytes = 4096;

    /** Catch-all sweep cadence (leader-elected, `LargeVariableExternalizationSweep`) — picks up
     *  any staged-but-not-yet-externalized variable the post-commit fast path missed (crash, WARN
     *  publish failure). Mirrors {@code HistoryOutboxProperties.relayCyclePeriodSeconds}. */
    private long sweepCyclePeriodSeconds = 60;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getThresholdBytes() {
        return thresholdBytes;
    }

    public void setThresholdBytes(int thresholdBytes) {
        this.thresholdBytes = thresholdBytes;
    }

    public long getSweepCyclePeriodSeconds() {
        return sweepCyclePeriodSeconds;
    }

    public void setSweepCyclePeriodSeconds(long sweepCyclePeriodSeconds) {
        this.sweepCyclePeriodSeconds = sweepCyclePeriodSeconds;
    }

    /** @return {@code true} if {@code byteLength} exceeds the threshold (D-C' "yalnız eşik üstü"). */
    public boolean exceedsThreshold(int byteLength) {
        return byteLength > thresholdBytes;
    }
}
