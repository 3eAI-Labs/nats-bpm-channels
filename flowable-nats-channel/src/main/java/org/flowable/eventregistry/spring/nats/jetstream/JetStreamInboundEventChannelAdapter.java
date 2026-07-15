package org.flowable.eventregistry.spring.nats.jetstream;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.threeai.nats.core.dlq.DlqPublishOutcome;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.dlq.DlqReason;
import com.threeai.nats.core.headers.BpmHeaders;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.impl.NatsJetStreamMetaData;
import io.micrometer.core.instrument.Timer;
import org.flowable.eventregistry.api.EventRegistry;
import org.flowable.eventregistry.api.InboundEventChannelAdapter;
import org.flowable.eventregistry.model.InboundChannelModel;
import org.flowable.eventregistry.spring.nats.NatsInboundEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class JetStreamInboundEventChannelAdapter implements InboundEventChannelAdapter {

    private static final Logger log = LoggerFactory.getLogger(JetStreamInboundEventChannelAdapter.class);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    private final Connection connection;
    private final JetStream jetStream;
    private final String subject;
    private final int maxDeliver;
    private final String dlqSubject;
    private final NatsChannelMetrics metrics;
    private final String channelKey;
    private final DlqPublisher dlqPublisher;

    private EventRegistry eventRegistry;
    private InboundChannelModel inboundChannelModel;
    private Dispatcher dispatcher;
    private ExecutorService executor;

    public JetStreamInboundEventChannelAdapter(Connection connection, JetStream jetStream,
            String subject, int maxDeliver, String dlqSubject,
            NatsChannelMetrics metrics, String channelKey, DlqPublisher dlqPublisher) {
        this.connection = connection;
        this.jetStream = jetStream;
        this.subject = subject;
        this.maxDeliver = maxDeliver;
        this.dlqSubject = dlqSubject;
        this.metrics = metrics;
        this.channelKey = channelKey;
        this.dlqPublisher = dlqPublisher;
    }

    @Override
    public void setInboundChannelModel(InboundChannelModel inboundChannelModel) {
        this.inboundChannelModel = inboundChannelModel;
    }

    @Override
    public void setEventRegistry(EventRegistry eventRegistry) {
        this.eventRegistry = eventRegistry;
    }

    public void subscribe() {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.dispatcher = connection.createDispatcher();
        try {
            // Allow one extra delivery beyond the DLQ threshold so the adapter
            // can detect maxDeliver exceeded and route to the dead-letter subject.
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .ackWait(Duration.ofSeconds(30))
                    .maxDeliver(maxDeliver + 1)
                    .build();
            PushSubscribeOptions opts = PushSubscribeOptions.builder()
                    .configuration(cc)
                    .build();
            JetStreamSubscription sub = jetStream.subscribe(subject, dispatcher,
                    msg -> executor.submit(() -> handleMessage(msg)), false, opts);
            log.info("Subscribed to JetStream",
                    kv("channel", channelKey),
                    kv("subject", subject));
        } catch (Exception e) {
            log.error("Failed to subscribe to JetStream",
                    kv("channel", channelKey),
                    kv("subject", subject), e);
            throw new org.flowable.common.engine.api.FlowableException(
                    "Failed to subscribe to JetStream subject '" + subject + "'", e);
        }
    }

    public void unsubscribe() {
        if (dispatcher != null) {
            try {
                dispatcher.drain(Duration.ofSeconds(10));
            } catch (Exception e) {
                log.warn("Error draining JetStream dispatcher",
                        kv("channel", channelKey), e);
            }
        }
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
            }
        }
    }

    void handleMessage(Message msg) {
        String traceId = BpmHeaders.extractTraceIdWithFallback(msg);
        if (traceId != null) {
            MDC.put("trace_id", traceId);
        }
        try {
            byte[] data = msg.getData();
            if (data == null || data.length == 0) {
                log.warn("Empty message body — routing to DLQ",
                        kv("channel", channelKey),
                        kv("subject", msg.getSubject()));
                routeToDlqAndDecide(msg, DlqReason.EMPTY_MESSAGE_BODY);
                return;
            }

            // Check if max deliveries exceeded -> DLQ path
            long deliveryCount = getDeliveryCount(msg);
            if (deliveryCount > maxDeliver) {
                log.warn("Max deliveries exceeded, routing to DLQ",
                        kv("channel", channelKey),
                        kv("subject", msg.getSubject()),
                        kv("delivery_count", deliveryCount),
                        kv("max_deliver", maxDeliver));
                routeToDlqAndDecide(msg, DlqReason.DELIVERY_BUDGET_EXCEEDED);
                return;
            }

            // Normal processing
            Timer.Sample sample = metrics != null ? Timer.start() : null;
            NatsInboundEvent event = new NatsInboundEvent(msg);
            eventRegistry.eventReceived(inboundChannelModel, event);

            if (sample != null) {
                sample.stop(metrics.processingTimer(subject, channelKey));
            }
            if (metrics != null) {
                metrics.ackCount(subject, channelKey).increment();
            }
            msg.ack();
            log.debug("Message processed and acked",
                    kv("channel", channelKey),
                    kv("subject", msg.getSubject()));

        } catch (Exception e) {
            log.error("Error processing JetStream message",
                    kv("channel", channelKey),
                    kv("subject", msg.getSubject()), e);
            if (metrics != null) {
                metrics.nakCount(subject, channelKey).increment();
            }
            nakWithBackoff(msg);
        } finally {
            MDC.remove("trace_id");
        }
    }

    /**
     * Custody-transfer decision (contract-fix #2, BR-SUB-002): ack only on a successful DLQ
     * publish (JetStream or core-NATS fallback); nak — never a silent ack-drop — when the DLQ
     * subject is unconfigured or both publish paths fail.
     */
    private void routeToDlqAndDecide(Message msg, DlqReason reason) {
        DlqPublishOutcome outcome = dlqPublisher.publish(msg, dlqSubject, reason, subject, channelKey);
        switch (outcome) {
            case PUBLISHED_JETSTREAM, PUBLISHED_CORE_FALLBACK -> msg.ack();
            case FAILED_NO_DLQ_SUBJECT, FAILED_BOTH_PUBLISH -> {
                if (metrics != null) {
                    metrics.nakCount(subject, channelKey).increment();
                }
                msg.nakWithDelay(calculateBackoff(getDeliveryCount(msg)));
            }
        }
    }

    private long getDeliveryCount(Message msg) {
        try {
            NatsJetStreamMetaData metaData = msg.metaData();
            return metaData.deliveredCount();
        } catch (Exception e) {
            log.debug("Could not retrieve message metadata",
                    kv("channel", channelKey), e);
            return 1;
        }
    }

    private void nakWithBackoff(Message msg) {
        try {
            long deliveryCount = msg.metaData().deliveredCount();
            Duration backoff = calculateBackoff(deliveryCount);
            msg.nakWithDelay(backoff);
            log.debug("Message nacked with delay",
                    kv("channel", channelKey),
                    kv("delay", backoff));
        } catch (Exception e) {
            log.warn("Failed to get metadata for backoff, falling back to plain nak",
                    kv("channel", channelKey), e);
            msg.nak();
        }
    }

    Duration calculateBackoff(long deliveryCount) {
        long seconds = (long) Math.pow(2, deliveryCount - 1);
        Duration backoff = Duration.ofSeconds(seconds);
        return backoff.compareTo(MAX_BACKOFF) > 0 ? MAX_BACKOFF : backoff;
    }
}
