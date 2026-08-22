package com.threeai.nats.flowable.externalworker;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for Flowable external-worker dispatch over JetStream (docs/11, 0.10.0 slice).
 *
 * <p><b>Opt-in is dual-gated (decision D-F'):</b> {@link #enabled} must be {@code true} AND
 * {@link #topics} must be non-empty before any consumer, stream or interceptor is registered.
 * The explicit flag exists because A2's empty-topic-list-only opt-in proved too implicit
 * (0.8.1 lesson); the asymmetry with A2 is documented in the USER_GUIDE.
 *
 * <p>The W/M/S/epsilon knobs feed the same umbrella-lock inequality as A2 (ADR-0001 floor,
 * {@code L >= M*W + sum(backoff) + S + epsilon}) — see docs/11 D-B'v3. {@code maxDeliver}
 * deliberately feeds BOTH the floor formula and the JetStream consumer, never independently
 * overridable (A2Properties precedent).
 */
@ConfigurationProperties("spring.nats.flowable.external-worker")
public class EwProperties {

    /** Master switch — default OFF (docs/11 D-F'; independent, opt-in capability). */
    private boolean enabled = false;

    /**
     * External-worker topics dispatched over JetStream. Each topic must match
     * {@code ^[A-Za-z0-9_-]+$} (docs/11 D-H'): it becomes a subject token
     * ({@code ewjobs.<topic>}), a durable/deliver-group name
     * ({@code flw-ew-completion-<topic>}) and a DLQ subject ({@code dlq.ewjobs.<topic>}).
     */
    private List<String> topics = new ArrayList<>();

    /**
     * Sentinel worker id — the FIXED half of the lock-generation token
     * {@code <sentinelWorkerId>#<nonce>} (docs/11 D-B'v3). The full token is constructed
     * bridge-side; only the nonce travels on the wire ({@code X-Cadenzaflow-Lock-Nonce}).
     */
    private String sentinelWorkerId = "flw-ew-bridge";

    /** JetStream consumer ackWait (W), seconds. */
    private long ackWaitSeconds = 30;

    /** Delivery budget (M) — consumer maxDeliver is provisioned as M+1 (A2 parity). */
    private int maxDeliver = 4;

    /** Orphan-sweep period (S), seconds. */
    private long sweepPeriodSeconds = 120;

    /** Per-cycle sweep acquire cap (2026-08-20 hardening lesson: bound memory AND cycle time). */
    private int sweepBatchSize = 5000;

    /** Safety margin (epsilon) in the umbrella floor, seconds. */
    private long epsilonSeconds = 60;

    /** Lock duration (L), seconds; {@code null} derives the ADR-0001 floor value. */
    private Long lockDurationSeconds;

    /** retryTimeout passed to the engine failure builder on TRANSIENT replies, millis. */
    private long retryTimeoutMillis = 5000;

    /**
     * Escape hatch mirroring A2: downgrade the umbrella-floor violation from boot failure to a
     * per-cycle WARN. "Warn once and forget" was explicitly rejected (ADR-0001).
     */
    private boolean allowUnsafeLockDuration = false;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public String getSentinelWorkerId() {
        return sentinelWorkerId;
    }

    public void setSentinelWorkerId(String sentinelWorkerId) {
        this.sentinelWorkerId = sentinelWorkerId;
    }

    public long getAckWaitSeconds() {
        return ackWaitSeconds;
    }

    public void setAckWaitSeconds(long ackWaitSeconds) {
        this.ackWaitSeconds = ackWaitSeconds;
    }

    public int getMaxDeliver() {
        return maxDeliver;
    }

    public void setMaxDeliver(int maxDeliver) {
        this.maxDeliver = maxDeliver;
    }

    public long getSweepPeriodSeconds() {
        return sweepPeriodSeconds;
    }

    public void setSweepPeriodSeconds(long sweepPeriodSeconds) {
        this.sweepPeriodSeconds = sweepPeriodSeconds;
    }

    public int getSweepBatchSize() {
        return sweepBatchSize;
    }

    public void setSweepBatchSize(int sweepBatchSize) {
        this.sweepBatchSize = sweepBatchSize;
    }

    public long getEpsilonSeconds() {
        return epsilonSeconds;
    }

    public void setEpsilonSeconds(long epsilonSeconds) {
        this.epsilonSeconds = epsilonSeconds;
    }

    public Long getLockDurationSeconds() {
        return lockDurationSeconds;
    }

    public void setLockDurationSeconds(Long lockDurationSeconds) {
        this.lockDurationSeconds = lockDurationSeconds;
    }

    public long getRetryTimeoutMillis() {
        return retryTimeoutMillis;
    }

    public void setRetryTimeoutMillis(long retryTimeoutMillis) {
        this.retryTimeoutMillis = retryTimeoutMillis;
    }

    public boolean isAllowUnsafeLockDuration() {
        return allowUnsafeLockDuration;
    }

    public void setAllowUnsafeLockDuration(boolean allowUnsafeLockDuration) {
        this.allowUnsafeLockDuration = allowUnsafeLockDuration;
    }
}
