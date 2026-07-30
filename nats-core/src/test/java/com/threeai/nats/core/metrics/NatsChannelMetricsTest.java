package com.threeai.nats.core.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NatsChannelMetricsTest {

    private SimpleMeterRegistry registry;
    private NatsChannelMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new NatsChannelMetrics(registry);
    }

    @Test
    void counters_registeredAndIncrementCorrectly() {
        Counter consume = metrics.consumeCount("order.new", "orderChannel");
        Counter consumeError = metrics.consumeErrorCount("order.new", "orderChannel");
        Counter ack = metrics.ackCount("order.new", "orderChannel");
        Counter nak = metrics.nakCount("order.new", "orderChannel");
        Counter dlq = metrics.dlqCount("order.new", "orderChannel");
        Counter publish = metrics.publishCount("order.out", "outChannel");
        Counter publishError = metrics.publishErrorCount("order.out", "outChannel");
        Counter jsPublish = metrics.jsPublishCount("order.out", "outChannel");
        Counter jsPublishError = metrics.jsPublishErrorCount("order.out", "outChannel");
        Counter reconnect = metrics.reconnectCount();
        Counter slowConsumer = metrics.slowConsumerCount();

        consume.increment();
        consumeError.increment();
        ack.increment();
        nak.increment();
        dlq.increment();
        reconnect.increment();

        assertThat(consume.count()).isEqualTo(1.0);
        assertThat(consumeError.count()).isEqualTo(1.0);
        assertThat(consumeError.getId().getName()).isEqualTo("nats.inbound.errors");
        assertThat(consumeError.getId().getTag("subject")).isEqualTo("order.new");
        assertThat(ack.count()).isEqualTo(1.0);
        assertThat(nak.count()).isEqualTo(1.0);
        assertThat(dlq.count()).isEqualTo(1.0);
        assertThat(reconnect.count()).isEqualTo(1.0);
    }

    @Test
    void requestReplyCounters_registeredAndIncrementCorrectly() {
        Counter requests = metrics.requestReplyCount("task.send-sms");
        Counter errors = metrics.requestReplyErrorCount("task.send-sms");
        requests.increment();
        errors.increment();
        assertThat(requests.count()).isEqualTo(1.0);
        assertThat(errors.count()).isEqualTo(1.0);
        assertThat(requests.getId().getName()).isEqualTo("nats.requestreply.requests");
        assertThat(requests.getId().getTag("subject")).isEqualTo("task.send-sms");
    }

    @Test
    void processingTimer_registeredCorrectly() {
        Timer timer = metrics.processingTimer("order.new", "orderChannel");

        assertThat(timer).isNotNull();
        assertThat(timer.getId().getName()).isEqualTo("nats.inbound.processing.duration");
        assertThat(timer.getId().getTag("subject")).isEqualTo("order.new");
        assertThat(timer.getId().getTag("channel")).isEqualTo("orderChannel");
    }

    @Test
    void a2Counters_registeredAndIncrementCorrectly() {
        Counter sweepRepublish = metrics.sweepRepublishCount("order-fulfillment");
        Counter dlqPublishFailure = metrics.dlqPublishFailureCount("jobs.order-fulfillment.reply", "order-fulfillment");
        Counter correlationMiss = metrics.failureEventCorrelationMissCount("orderChannel");
        Counter conflict = metrics.sentinelWorkerConflictCount("order-fulfillment");

        sweepRepublish.increment();
        dlqPublishFailure.increment();
        correlationMiss.increment();
        conflict.increment();

        assertThat(sweepRepublish.count()).isEqualTo(1.0);
        assertThat(sweepRepublish.getId().getName()).isEqualTo("nats.a2.sweep.republish");
        assertThat(dlqPublishFailure.count()).isEqualTo(1.0);
        assertThat(dlqPublishFailure.getId().getName()).isEqualTo("nats.jetstream.dlq.publish.failures");
        assertThat(correlationMiss.count()).isEqualTo(1.0);
        assertThat(correlationMiss.getId().getName()).isEqualTo("nats.flowable.failure_event.correlation_miss");
        assertThat(conflict.count()).isEqualTo(1.0);
        assertThat(conflict.getId().getName()).isEqualTo("nats.a2.sentinel_worker_conflict");
    }

    @Test
    void dispatchLatencyTimer_registeredCorrectly() {
        Timer timer = metrics.dispatchLatencyTimer("order-fulfillment");

        assertThat(timer.getId().getName()).isEqualTo("nats.a2.dispatch.latency");
        assertThat(timer.getId().getTag("topic")).isEqualTo("order-fulfillment");
    }

    @Test
    void oldestOrphanAgeGauge_reflectsSupplierValue() {
        java.util.concurrent.atomic.AtomicLong ageSeconds = new java.util.concurrent.atomic.AtomicLong(42);

        metrics.registerOldestOrphanAgeGauge("order-fulfillment", ageSeconds::get);

        io.micrometer.core.instrument.Gauge gauge = registry.find("nats.a2.sweep.oldest_orphan_age_seconds")
                .tag("topic", "order-fulfillment").gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(42.0);

        ageSeconds.set(99);
        assertThat(gauge.value()).isEqualTo(99.0);
    }

    @Test
    void historyCounters_registeredAndIncrementCorrectly() {
        Counter written = metrics.historyOutboxWrittenCount("OP_LOG", "camunda");
        Counter relayed = metrics.historyOutboxRelayedCount("OP_LOG", "published");
        Counter postCommit = metrics.historyPostCommitPublishedCount("ACTINST");
        Counter consumed = metrics.historyProjectionConsumedCount("ACTINST", "3");
        Counter staleDiscarded = metrics.historyProjectionStaleDiscardedCount("ACTINST");
        Counter dlqRouted = metrics.historyDlqRoutedCount("ACTINST", "schema_drift");
        Counter retentionDeleted = metrics.historyRetentionDeletedRowsCount("DETAIL", "drop");
        Counter erasureProcessed = metrics.historyErasureProcessedCount("PROCINST", "anonymize");
        Counter vaultAccess = metrics.historyVaultAccessCount("WRITE", true);

        written.increment();
        relayed.increment();
        postCommit.increment();
        consumed.increment();
        staleDiscarded.increment();
        dlqRouted.increment();
        retentionDeleted.increment();
        erasureProcessed.increment();
        vaultAccess.increment();

        assertThat(written.count()).isEqualTo(1.0);
        assertThat(written.getId().getName()).isEqualTo("nats.history.outbox.written");
        assertThat(written.getId().getTag("history_class")).isEqualTo("OP_LOG");
        assertThat(written.getId().getTag("engine_id")).isEqualTo("camunda");
        assertThat(relayed.getId().getName()).isEqualTo("nats.history.outbox.relayed");
        assertThat(relayed.getId().getTag("outcome")).isEqualTo("published");
        assertThat(postCommit.getId().getName()).isEqualTo("nats.history.postcommit.published");
        assertThat(consumed.getId().getName()).isEqualTo("nats.history.projection.consumed");
        assertThat(consumed.getId().getTag("partition")).isEqualTo("3");
        assertThat(staleDiscarded.getId().getName()).isEqualTo("nats.history.projection.stale_discarded");
        assertThat(dlqRouted.getId().getName()).isEqualTo("nats.history.dlq.routed");
        assertThat(dlqRouted.getId().getTag("reason")).isEqualTo("schema_drift");
        assertThat(retentionDeleted.getId().getName()).isEqualTo("nats.history.retention.deleted_rows");
        assertThat(retentionDeleted.getId().getTag("action")).isEqualTo("drop");
        assertThat(erasureProcessed.getId().getName()).isEqualTo("nats.history.erasure.processed");
        assertThat(vaultAccess.getId().getName()).isEqualTo("nats.history.vault.access");
        assertThat(vaultAccess.getId().getTag("operation")).isEqualTo("WRITE");
        assertThat(vaultAccess.getId().getTag("granted")).isEqualTo("true");
    }

    @Test
    void outboundCounters_registeredAndIncrementCorrectly() {
        Counter written = metrics.outboundOutboxWrittenCount("order_created", "camunda");
        Counter relayed = metrics.outboundOutboxRelayedCount("order_created", "published");
        Counter postCommit = metrics.outboundPostCommitPublishedCount("order_created");
        Counter flowablePublished = metrics.flowableOutboundPublishedCount("order.completed", "orderChannel");
        Counter flowableDlqRouted = metrics.flowableOutboundDlqRoutedCount("order.completed", "orderChannel");

        written.increment();
        relayed.increment();
        postCommit.increment();
        flowablePublished.increment();
        flowableDlqRouted.increment();

        assertThat(written.count()).isEqualTo(1.0);
        assertThat(written.getId().getName()).isEqualTo("nats.outbound.outbox.written");
        assertThat(written.getId().getTag("message_type")).isEqualTo("order_created");
        assertThat(written.getId().getTag("engine_id")).isEqualTo("camunda");
        assertThat(relayed.getId().getName()).isEqualTo("nats.outbound.outbox.relayed");
        assertThat(relayed.getId().getTag("outcome")).isEqualTo("published");
        assertThat(postCommit.getId().getName()).isEqualTo("nats.outbound.postcommit.published");
        assertThat(flowablePublished.count()).isEqualTo(1.0);
        assertThat(flowablePublished.getId().getName()).isEqualTo("nats.flowable.outbound.published");
        assertThat(flowablePublished.getId().getTag("subject")).isEqualTo("order.completed");
        assertThat(flowablePublished.getId().getTag("channel")).isEqualTo("orderChannel");
        assertThat(flowableDlqRouted.count()).isEqualTo(1.0);
        assertThat(flowableDlqRouted.getId().getName()).isEqualTo("nats.flowable.outbound.dlq_routed");
    }

    @Test
    void largeVariableExternalizedCount_registeredAndIncrementCorrectly() {
        Counter externalized = metrics.largeVariableExternalizedCount("camunda");

        externalized.increment();

        assertThat(externalized.count()).isEqualTo(1.0);
        assertThat(externalized.getId().getName()).isEqualTo("nats.large_variable.externalized");
        assertThat(externalized.getId().getTag("engine_id")).isEqualTo("camunda");
    }

    @Test
    void outboundOutboxOldestRowAgeGauge_reflectsSupplierValue() {
        java.util.concurrent.atomic.AtomicLong ageSeconds = new java.util.concurrent.atomic.AtomicLong(7);

        metrics.registerOutboundOutboxOldestRowAgeGauge("camunda", ageSeconds::get);

        io.micrometer.core.instrument.Gauge gauge = registry.find("nats.outbound.outbox.oldest_row_age_seconds")
                .tag("engine_id", "camunda").gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(7.0);
    }

    @Test
    void historyGauges_reflectSupplierValues() {
        java.util.concurrent.atomic.AtomicLong oldestRowAge = new java.util.concurrent.atomic.AtomicLong(10);
        java.util.concurrent.atomic.AtomicLong diffCount = new java.util.concurrent.atomic.AtomicLong(2);
        java.util.concurrent.atomic.AtomicLong cleanStreak = new java.util.concurrent.atomic.AtomicLong(3);
        java.util.concurrent.atomic.AtomicLong cutoverState = new java.util.concurrent.atomic.AtomicLong(0);
        java.util.concurrent.atomic.AtomicLong lagSeconds = new java.util.concurrent.atomic.AtomicLong(5);

        metrics.registerHistoryOutboxOldestRowAgeGauge("camunda", oldestRowAge::get);
        metrics.registerHistoryReconciliationDiffCountGauge("OP_LOG", diffCount::get);
        metrics.registerHistoryReconciliationCleanStreakGauge("OP_LOG", cleanStreak::get);
        metrics.registerHistoryCutoverStateGauge("OP_LOG", cutoverState::get);
        metrics.registerHistoryProjectionLagGauge("ACTINST", "3", lagSeconds::get);

        assertThat(registry.find("nats.history.outbox.oldest_row_age_seconds").tag("engine_id", "camunda").gauge().value())
                .isEqualTo(10.0);
        assertThat(registry.find("nats.history.reconciliation.diff_count").tag("history_class", "OP_LOG").gauge().value())
                .isEqualTo(2.0);
        assertThat(registry.find("nats.history.reconciliation.clean_streak_days").tag("history_class", "OP_LOG").gauge().value())
                .isEqualTo(3.0);
        assertThat(registry.find("nats.history.cutover.state").tag("history_class", "OP_LOG").gauge().value())
                .isEqualTo(0.0);
        assertThat(registry.find("nats.history.projection.lag_seconds").tag("history_class", "ACTINST")
                .tag("partition", "3").gauge().value()).isEqualTo(5.0);
    }
}
