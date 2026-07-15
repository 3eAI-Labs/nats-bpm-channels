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
        ack.increment();
        nak.increment();
        dlq.increment();
        reconnect.increment();

        assertThat(consume.count()).isEqualTo(1.0);
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
}
