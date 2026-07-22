package com.threeai.nats.core.metrics;

import java.util.function.Supplier;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class NatsChannelMetrics {

    private final MeterRegistry registry;

    public NatsChannelMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public Counter consumeCount(String subject, String channel) {
        return Counter.builder("nats.inbound.consumed")
                .tag("subject", subject).tag("channel", channel).register(registry);
    }

    public Counter consumeErrorCount(String subject, String channel) {
        return Counter.builder("nats.inbound.errors")
                .tag("subject", subject).tag("channel", channel).register(registry);
    }

    public Counter ackCount(String subject, String channel) {
        return Counter.builder("nats.jetstream.inbound.ack")
                .tag("subject", subject).tag("channel", channel).register(registry);
    }

    public Counter nakCount(String subject, String channel) {
        return Counter.builder("nats.jetstream.inbound.nak")
                .tag("subject", subject).tag("channel", channel).register(registry);
    }

    public Counter dlqCount(String subject, String channel) {
        return Counter.builder("nats.jetstream.inbound.dlq")
                .tag("subject", subject).tag("channel", channel).register(registry);
    }

    public Counter publishCount(String subject, String channel) {
        return Counter.builder("nats.outbound.published")
                .tag("subject", subject).tag("channel", channel).register(registry);
    }

    public Counter publishErrorCount(String subject, String channel) {
        return Counter.builder("nats.outbound.errors")
                .tag("subject", subject).tag("channel", channel).register(registry);
    }

    public Counter jsPublishCount(String subject, String channel) {
        return Counter.builder("nats.jetstream.outbound.published")
                .tag("subject", subject).tag("channel", channel).register(registry);
    }

    public Counter jsPublishErrorCount(String subject, String channel) {
        return Counter.builder("nats.jetstream.outbound.errors")
                .tag("subject", subject).tag("channel", channel).register(registry);
    }

    public Timer processingTimer(String subject, String channel) {
        return Timer.builder("nats.inbound.processing.duration")
                .tag("subject", subject).tag("channel", channel).register(registry);
    }

    public Counter reconnectCount() {
        return Counter.builder("nats.connection.reconnects").register(registry);
    }

    public Counter slowConsumerCount() {
        return Counter.builder("nats.connection.slow.consumers").register(registry);
    }

    // Request-Reply metrics (single subject tag — no channel concept)
    public Counter requestReplyCount(String subject) {
        return Counter.builder("nats.requestreply.requests")
                .tag("subject", subject).register(registry);
    }

    public Counter requestReplyErrorCount(String subject) {
        return Counter.builder("nats.requestreply.errors")
                .tag("subject", subject).register(registry);
    }

    // A2 / DLQ-bridge metrics (10_metrics.md §1)

    public Counter sweepRepublishCount(String topic) {
        return Counter.builder("nats.a2.sweep.republish")
                .tag("topic", topic).register(registry);
    }

    public Counter dlqPublishFailureCount(String subject, String channel) {
        return Counter.builder("nats.jetstream.dlq.publish.failures")
                .tag("subject", subject).tag("channel", channel).register(registry);
    }

    public Counter failureEventCorrelationMissCount(String channel) {
        return Counter.builder("nats.flowable.failure_event.correlation_miss")
                .tag("channel", channel).register(registry);
    }

    /** SYS_SENTINEL_WORKER_CONFLICT — metric side of the CRITICAL+page channel (ERROR_REGISTRY.md §4.1). */
    public Counter sentinelWorkerConflictCount(String topic) {
        return Counter.builder("nats.a2.sentinel_worker_conflict")
                .tag("topic", topic).register(registry);
    }

    public Timer dispatchLatencyTimer(String topic) {
        return Timer.builder("nats.a2.dispatch.latency")
                .tag("topic", topic).register(registry);
    }

    /** Oldest-orphan age gauge, updated by A2OrphanSweep every cycle. */
    public void registerOldestOrphanAgeGauge(String topic, Supplier<Number> ageSecondsSupplier) {
        Gauge.builder("nats.a2.sweep.oldest_orphan_age_seconds", ageSecondsSupplier)
                .tag("topic", topic).register(registry);
    }

    // --- History Offload (basamak-2, 10_metrics.md §1) ---

    public Counter historyOutboxWrittenCount(String historyClass, String engineId) {
        return Counter.builder("nats.history.outbox.written")
                .tag("history_class", historyClass).tag("engine_id", engineId).register(registry);
    }

    public Counter historyOutboxRelayedCount(String historyClass, String outcome) {
        return Counter.builder("nats.history.outbox.relayed")
                .tag("history_class", historyClass).tag("outcome", outcome).register(registry);
    }

    /** {@code SYS_OUTBOX_ROW_STUCK} signal source — updated by {@code HistoryOutboxRelay.checkStuckRows()}. */
    public void registerHistoryOutboxOldestRowAgeGauge(String engineId, Supplier<Number> ageSecondsSupplier) {
        Gauge.builder("nats.history.outbox.oldest_row_age_seconds", ageSecondsSupplier)
                .tag("engine_id", engineId).register(registry);
    }

    public Counter historyPostCommitPublishedCount(String historyClass) {
        return Counter.builder("nats.history.postcommit.published")
                .tag("history_class", historyClass).register(registry);
    }

    public Counter historyProjectionConsumedCount(String historyClass, String partition) {
        return Counter.builder("nats.history.projection.consumed")
                .tag("history_class", historyClass).tag("partition", partition).register(registry);
    }

    public Counter historyProjectionStaleDiscardedCount(String historyClass) {
        return Counter.builder("nats.history.projection.stale_discarded")
                .tag("history_class", historyClass).register(registry);
    }

    /** NFR-P3 SLI — event-to-query-store lag, p95. */
    public void registerHistoryProjectionLagGauge(String historyClass, String partition, Supplier<Number> lagSecondsSupplier) {
        Gauge.builder("nats.history.projection.lag_seconds", lagSecondsSupplier)
                .tag("history_class", historyClass).tag("partition", partition).register(registry);
    }

    public Counter historyDlqRoutedCount(String historyClass, String reason) {
        return Counter.builder("nats.history.dlq.routed")
                .tag("history_class", historyClass).tag("reason", reason).register(registry);
    }

    public void registerHistoryReconciliationDiffCountGauge(String historyClass, Supplier<Number> diffCountSupplier) {
        Gauge.builder("nats.history.reconciliation.diff_count", diffCountSupplier)
                .tag("history_class", historyClass).register(registry);
    }

    public void registerHistoryReconciliationCleanStreakGauge(String historyClass, Supplier<Number> streakDaysSupplier) {
        Gauge.builder("nats.history.reconciliation.clean_streak_days", streakDaysSupplier)
                .tag("history_class", historyClass).register(registry);
    }

    /** Enum-encoded cutover state (ordinal of {@code class_cutover_state.state}). */
    public void registerHistoryCutoverStateGauge(String historyClass, Supplier<Number> stateSupplier) {
        Gauge.builder("nats.history.cutover.state", stateSupplier)
                .tag("history_class", historyClass).register(registry);
    }

    public Counter historyRetentionDeletedRowsCount(String historyClass, String action) {
        return Counter.builder("nats.history.retention.deleted_rows")
                .tag("history_class", historyClass).tag("action", action).register(registry);
    }

    public Counter historyErasureProcessedCount(String historyClass, String action) {
        return Counter.builder("nats.history.erasure.processed")
                .tag("history_class", historyClass).tag("action", action).register(registry);
    }

    /** DP-16 — {@code pseudonym_token}/real value NEVER appear as tag values. */
    public Counter historyVaultAccessCount(String operation, boolean granted) {
        return Counter.builder("nats.history.vault.access")
                .tag("operation", operation).tag("granted", String.valueOf(granted)).register(registry);
    }

    // --- Large Variable Externalization (basamak-3, docs/08) ---

    public Counter largeVariableExternalizedCount(String engineId) {
        return Counter.builder("nats.large_variable.externalized")
                .tag("engine_id", engineId).register(registry);
    }
}
