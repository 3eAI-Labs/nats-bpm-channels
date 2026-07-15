package com.threeai.nats.camunda.a2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code spring.nats.camunda.a2.*} — umbrella-lock parameters (ADR-0001, BR-A2-006/007,
 * FR-A8/A9, US-A5). LLD 08_config.md §1.1 (mirrored verbatim in
 * {@code cadenzaflow-nats-channel} under {@code spring.nats.cadenzaflow.a2}).
 */
@ConfigurationProperties(prefix = "spring.nats.camunda.a2")
public class A2Properties {

    /** BR-A2-003 — single cluster-wide constant lock owner. */
    private String sentinelWorkerId = "a2-jetstream-bridge";
    /** Only LITERAL {@code camunda:topic} values (03_classes/2_camunda_a2.md §1.3). */
    private List<String> topics = new ArrayList<>();
    private UmbrellaLockDefaults defaults = new UmbrellaLockDefaults();
    private Map<String, TopicLockOverride> topicOverrides = new HashMap<>();
    /** BAQ-3 escape-flag. */
    private boolean allowUnsafeLockDuration = false;

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
        /** eps. */
        private long epsilonSeconds = 60;
        /** L — {@code null} means derive it (see {@link com.threeai.nats.core.config.UmbrellaLockCalculator}). */
        private Long lockDurationSeconds;
        /**
         * Residual retry-delay (ms) for TRANSIENT replies ({@code
         * ExternalTaskService.handleFailure}). Sentinel Phase 5.5 QA fix (item 7a, Levent karari
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
