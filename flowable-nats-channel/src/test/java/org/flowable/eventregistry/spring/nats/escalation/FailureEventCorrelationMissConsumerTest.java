package org.flowable.eventregistry.spring.nats.escalation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.flowable.eventregistry.api.EventRegistryEvent;
import org.flowable.eventregistry.api.EventRegistryProcessingInfo;
import org.junit.jupiter.api.Test;

/**
 * Unit-level coverage for {@link FailureEventCorrelationMissConsumer} in isolation — the
 * end-to-end proof that a REAL Flowable engine actually invokes this SPI on a genuine
 * "no waiting subscription" event is {@code FailureEventCorrelationMissConsumerIntegrationTest}
 * (embedded engine, based on {@code EventReceivedNoMatchBehaviorTest}'s infrastructure).
 */
class FailureEventCorrelationMissConsumerTest {

    @Test
    void handleNonMatchingEvent_channelKeyBound_incrementsMetricForThatChannel() {
        NatsChannelMetrics metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        FailureEventCorrelationMissConsumer consumer = new FailureEventCorrelationMissConsumer(metrics);

        consumer.bindChannelKeyForCurrentThread("orderChannel");
        try {
            consumer.handleNonMatchingEvent(mock(EventRegistryEvent.class), mock(EventRegistryProcessingInfo.class));
        } finally {
            consumer.clearChannelKeyForCurrentThread();
        }

        assertThat(metrics.failureEventCorrelationMissCount("orderChannel").count()).isEqualTo(1.0);
    }

    /**
     * Should not happen via {@link FailureEventBridge} (it always binds first), but Micrometer
     * {@code Tag} values must be non-null — this consumer must never NPE regardless of caller
     * discipline; falls back to an "unknown" channel tag instead.
     */
    @Test
    void handleNonMatchingEvent_noChannelKeyBound_fallsBackToUnknownChannelTag() {
        NatsChannelMetrics metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        FailureEventCorrelationMissConsumer consumer = new FailureEventCorrelationMissConsumer(metrics);

        assertThatCode(() -> consumer.handleNonMatchingEvent(
                mock(EventRegistryEvent.class), mock(EventRegistryProcessingInfo.class)))
                .doesNotThrowAnyException();
        assertThat(metrics.failureEventCorrelationMissCount("unknown").count()).isEqualTo(1.0);
    }

    @Test
    void handleNonMatchingEvent_nullMetrics_doesNotThrow() {
        FailureEventCorrelationMissConsumer consumer = new FailureEventCorrelationMissConsumer(null);

        consumer.bindChannelKeyForCurrentThread("orderChannel");
        try {
            assertThatCode(() -> consumer.handleNonMatchingEvent(
                    mock(EventRegistryEvent.class), mock(EventRegistryProcessingInfo.class)))
                    .doesNotThrowAnyException();
        } finally {
            consumer.clearChannelKeyForCurrentThread();
        }
    }

    @Test
    void clearChannelKeyForCurrentThread_afterClear_fallsBackToUnknownChannelTag() {
        NatsChannelMetrics metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        FailureEventCorrelationMissConsumer consumer = new FailureEventCorrelationMissConsumer(metrics);

        consumer.bindChannelKeyForCurrentThread("orderChannel");
        consumer.clearChannelKeyForCurrentThread();

        assertThatCode(() -> consumer.handleNonMatchingEvent(
                mock(EventRegistryEvent.class), mock(EventRegistryProcessingInfo.class)))
                .doesNotThrowAnyException();
        assertThat(metrics.failureEventCorrelationMissCount("orderChannel").count()).isEqualTo(0.0);
        assertThat(metrics.failureEventCorrelationMissCount("unknown").count()).isEqualTo(1.0);
    }
}
