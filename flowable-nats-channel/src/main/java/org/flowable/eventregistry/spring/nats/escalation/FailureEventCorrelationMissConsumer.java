package org.flowable.eventregistry.spring.nats.escalation;

import static net.logstash.logback.argument.StructuredArguments.kv;

import com.threeai.nats.core.metrics.NatsChannelMetrics;
import org.flowable.eventregistry.api.EventRegistryEvent;
import org.flowable.eventregistry.api.EventRegistryNonMatchingEventConsumer;
import org.flowable.eventregistry.api.EventRegistryProcessingInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sentinel Phase 5.5 QA fix (HIGH, Levent karari 2026-07-15) — {@link
 * EventReceivedNoMatchBehaviorTest} proved {@code EventRegistry.eventReceived(...)} does NOT
 * throw on "no waiting subscription"; it returns silently. That made {@link
 * FailureEventBridge}'s {@code catch (FlowableException noMatch)} branch dead code and meant
 * {@code RES_FAILURE_EVENT_CORRELATION_MISS} (ERROR_REGISTRY.md #10/§4.2, BR-FLW-003/005) was
 * never actually incremented by a real correlation-miss.
 *
 * <p>Bytecode evidence ({@code DefaultEventRegistry.sendEventToConsumers}, flowable-event-registry
 * 7.1.0): after dispatching to every registered {@code EventRegistryEventConsumer} and
 * aggregating an {@link EventRegistryProcessingInfo}, if {@code eventHandled()==false} the method
 * looks up {@code EventRegistryEngineConfiguration.getNonMatchingEventConsumer()} and — if
 * non-null — invokes {@link #handleNonMatchingEvent(EventRegistryEvent,
 * EventRegistryProcessingInfo)} on it, synchronously, in the SAME call. This is the only SPI hook
 * Flowable exposes for the no-match case; this class implements it and IS the actual
 * {@code RES_FAILURE_EVENT_CORRELATION_MISS} trigger point going forward. It must be registered
 * onto the host application's {@code EventRegistryEngineConfiguration} (see {@link
 * FailureEventBridge#subscribe()}).
 *
 * <p><b>Threading note:</b> {@link #bindChannelKeyForCurrentThread(String)} /
 * {@link #clearChannelKeyForCurrentThread()} are a per-thread hand-off from {@link
 * FailureEventBridge#handleDlqMessage} — {@code eventReceived(...)} dispatches to this SPI
 * synchronously on the SAME thread (per the bytecode evidence above, no executor hand-off), so a
 * {@link ThreadLocal} correctly threads the DLQ-bridge's channel-model key into a callback whose
 * own signature ({@link EventRegistryEvent}, {@link EventRegistryProcessingInfo}) carries no
 * channel context at all.
 */
public class FailureEventCorrelationMissConsumer implements EventRegistryNonMatchingEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(FailureEventCorrelationMissConsumer.class);

    /**
     * Micrometer {@code Tag} values must be non-null (DP-2 / low-cardinality, bounded label) —
     * used only if {@link #bindChannelKeyForCurrentThread(String)} was never called for the
     * current dispatch (should not happen via {@link FailureEventBridge}, but this consumer must
     * never NPE regardless of caller discipline).
     */
    private static final String UNKNOWN_CHANNEL = "unknown";

    private final ThreadLocal<String> currentChannelKey = new ThreadLocal<>();

    private final NatsChannelMetrics metrics;

    public FailureEventCorrelationMissConsumer(NatsChannelMetrics metrics) {
        this.metrics = metrics;
    }

    /** Called by {@link FailureEventBridge} immediately before {@code eventReceived(...)}. */
    void bindChannelKeyForCurrentThread(String channelKey) {
        currentChannelKey.set(channelKey);
    }

    /** Called by {@link FailureEventBridge} in a {@code finally} block, right after {@code eventReceived(...)}. */
    void clearChannelKeyForCurrentThread() {
        currentChannelKey.remove();
    }

    @Override
    public void handleNonMatchingEvent(EventRegistryEvent event, EventRegistryProcessingInfo processingInfo) {
        String channelKey = currentChannelKey.get();
        // ERROR_REGISTRY.md §4.2 / BAQ-8: a single miss is a benign race (instance likely already
        // resolved via another path) — WARN + metric, not ERROR; the sustained-repeat alert
        // thresholds (§4.2) are what actually signal a systemic problem.
        log.warn("No waiting event-subscription for failure-event — instance likely already "
                        + "resolved via another path",
                kv("channel", channelKey)); // RES_FAILURE_EVENT_CORRELATION_MISS
        if (metrics != null) {
            metrics.failureEventCorrelationMissCount(channelKey != null ? channelKey : UNKNOWN_CHANNEL).increment();
        }
    }
}
