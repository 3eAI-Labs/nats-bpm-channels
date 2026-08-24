package com.threeai.nats.cadenzaflow.a2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code spring.nats.cadenzaflow.a2.*} — umbrella-lock parameters (ADR-0001, BR-A2-006/007,
 * FR-A8/A9, US-A5). Mirrored verbatim in {@code cadenzaflow-nats-channel} under
 * {@code spring.nats.cadenzaflow.a2}.
 */
@ConfigurationProperties(prefix = "spring.nats.cadenzaflow.a2")
public class A2Properties {

    /**
     * G4-P optimizasyonu (2026-08-25): post-commit publish varsayilan olarak ASYNC —
     * motor thread'i broker ACK'ini beklemez; kontrat degismez (kayip publish'i her iki
     * modda da sweep toplar). {@code false} = eski senkron yol (kacis kapisi).
     */
    private boolean asyncPublish = true;

    /** Async modda ayni anda ACK bekleyen publish tavani; asilirsa caller BLOKLANIR. */
    private int asyncPublishMaxInFlight = 256;

    public boolean isAsyncPublish() {
        return asyncPublish;
    }

    public void setAsyncPublish(boolean asyncPublish) {
        this.asyncPublish = asyncPublish;
    }

    public int getAsyncPublishMaxInFlight() {
        return asyncPublishMaxInFlight;
    }

    public void setAsyncPublishMaxInFlight(int asyncPublishMaxInFlight) {
        this.asyncPublishMaxInFlight = asyncPublishMaxInFlight;
    }

    /** BR-A2-003 — single cluster-wide constant lock owner. */
    private String sentinelWorkerId = "a2-jetstream-bridge";
    /** Only LITERAL {@code camunda:topic} values. */
    private List<String> topics = new ArrayList<>();
    private UmbrellaLockDefaults defaults = new UmbrellaLockDefaults();
    private Map<String, TopicLockOverride> topicOverrides = new HashMap<>();
    /** BAQ-3 escape-flag. */
    private boolean allowUnsafeLockDuration = false;
    /**
     * QA review fix (item 5, decision 2026-07-15) — per-topic allowlist of
     * process-variable names captured IN-TX at {@code A2ExternalTaskBehavior.execute()} time
     * (DB read is allowed there; the post-commit publish path itself stays DB-query-free per
     * BR-A2-004) and added to the job payload's {@code variables} object. Default is EMPTY for
     * every topic — the existing identity-only envelope (externalTaskId/topic/businessKey) is
     * preserved unless a topic explicitly opts in (PII minimization by default).
     */
    private Map<String, List<String>> variableAllowlist = new HashMap<>();
    /**
     * When the completion bridge writes the reply body to the {@code natsPayload} process
     * variable. See {@link ReplyPayloadVariable}.
     */
    private ReplyPayloadVariable replyPayloadVariable = ReplyPayloadVariable.WHEN_PRESENT;

    /**
     * The reply body reaches the process as a single {@code natsPayload} variable because its
     * shape is tenant-defined and this layer does not interpret it (asyncapi
     * {@code JobSuccessPayload}: {@code type} required, everything else opaque). That is a real
     * need — a worker's result has no other way in — but a reply carrying ONLY the {@code type}
     * discriminator has no result to deliver, and writing it still costs a variable row plus its
     * history row on every completion.
     */
    public enum ReplyPayloadVariable {
        /** Write only when the body has a field other than {@code type}. Default. */
        WHEN_PRESENT,
        /** Always write, even for a bare {@code {"type":"SUCCESS"}}. Behaviour before 0.8.1. */
        ALWAYS,
        /** Never write. The process cannot read worker results at all — opt in knowingly. */
        NEVER
    }

    public ReplyPayloadVariable getReplyPayloadVariable() {
        return replyPayloadVariable;
    }

    public void setReplyPayloadVariable(ReplyPayloadVariable replyPayloadVariable) {
        this.replyPayloadVariable = replyPayloadVariable;
    }

    public String getSentinelWorkerId() {
        return sentinelWorkerId;
    }

    public void setSentinelWorkerId(String sentinelWorkerId) {
        this.sentinelWorkerId = sentinelWorkerId;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public UmbrellaLockDefaults getDefaults() {
        return defaults;
    }

    public void setDefaults(UmbrellaLockDefaults defaults) {
        this.defaults = defaults;
    }

    public Map<String, TopicLockOverride> getTopicOverrides() {
        return topicOverrides;
    }

    public void setTopicOverrides(Map<String, TopicLockOverride> topicOverrides) {
        this.topicOverrides = topicOverrides;
    }

    public boolean isAllowUnsafeLockDuration() {
        return allowUnsafeLockDuration;
    }

    public void setAllowUnsafeLockDuration(boolean allowUnsafeLockDuration) {
        this.allowUnsafeLockDuration = allowUnsafeLockDuration;
    }

    public Map<String, List<String>> getVariableAllowlist() {
        return variableAllowlist;
    }

    public void setVariableAllowlist(Map<String, List<String>> variableAllowlist) {
        this.variableAllowlist = variableAllowlist;
    }

    public static class UmbrellaLockDefaults {

        /** W. */
        private long ackWaitSeconds = 30;
        /**
         * M — input to the L-derivation formula. Bootstrap wiring copies this value (or the
         * matching {@link TopicLockOverride#getMaxDeliver()}) verbatim into
         * {@link A2ConsumerConfig#getMaxDeliver()} for every A2 topic — the two config objects
         * always carry the same M for the two consumption points (L-formula / JetStream
         * consumer), never independently overridden.
         */
        private int maxDeliver = 4;
        /** S. */
        private long sweepPeriodSeconds = 120;
        /**
         * Per-cycle cap on how many fetchable orphans one sweep cycle materializes and
         * republishes; the remainder is picked up by the following cycles ({@code 0} =
         * unbounded, the pre-0.8.2 behaviour). Bounded by default since the 2026-08-20
         * sustained-load incident, where an unbounded cycle materialized 93,605 rows in one
         * list and ground for longer than the lease TTL (2*S), churning leadership mid-recovery.
         */
        private int sweepBatchSize = 5000;
        /** eps. */
        private long epsilonSeconds = 60;
        /** L — {@code null} means derive it (see {@link com.threeai.nats.core.config.UmbrellaLockCalculator}). */
        private Long lockDurationSeconds;
        /**
         * Residual retry-delay (ms) for TRANSIENT replies ({@code
         * ExternalTaskService.handleFailure}). QA review fix (item 7a, decision
         * 2026-07-15) — was hardcoded 5000ms in {@code A2ReplyPayloadDecoder}; default unchanged.
         */
        private long retryTimeoutMillis = 5000;

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
    }

    public static class TopicLockOverride {

        /** Topic-specific W override ({@code null} = use default). */
        private Long ackWaitSeconds;
        private Integer maxDeliver;
        /** Manual L override ({@code null} = derive). */
        private Long lockDurationSeconds;
        /** Topic-specific TRANSIENT retry-delay override ({@code null} = use default). */
        private Long retryTimeoutMillis;

        public Long getAckWaitSeconds() {
            return ackWaitSeconds;
        }

        public void setAckWaitSeconds(Long ackWaitSeconds) {
            this.ackWaitSeconds = ackWaitSeconds;
        }

        public Integer getMaxDeliver() {
            return maxDeliver;
        }

        public void setMaxDeliver(Integer maxDeliver) {
            this.maxDeliver = maxDeliver;
        }

        public Long getLockDurationSeconds() {
            return lockDurationSeconds;
        }

        public void setLockDurationSeconds(Long lockDurationSeconds) {
            this.lockDurationSeconds = lockDurationSeconds;
        }

        public Long getRetryTimeoutMillis() {
            return retryTimeoutMillis;
        }

        public void setRetryTimeoutMillis(Long retryTimeoutMillis) {
            this.retryTimeoutMillis = retryTimeoutMillis;
        }
    }
}
