package com.threeai.nats.cadenzaflow.inbound;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.threeai.nats.cadenzaflow.eventing.EventDefinition;
import com.threeai.nats.cadenzaflow.eventing.EventingPayloadMapper;
import com.threeai.nats.core.NatsHeaderUtils;
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
import org.cadenzaflow.bpm.engine.RuntimeService;
import org.cadenzaflow.bpm.engine.runtime.MessageCorrelationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class JetStreamMessageCorrelationSubscriber {

    private static final Logger log = LoggerFactory.getLogger(JetStreamMessageCorrelationSubscriber.class);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    private final Connection connection;
    private final JetStream jetStream;
    private final RuntimeService runtimeService;
    private final SubscriptionConfig config;
    private final NatsChannelMetrics metrics;
    private final DlqPublisher dlqPublisher;
    /** Non-null on the definition path (docs/12 D-C v2); null keeps legacy semantics. */
    private final EventDefinition definition;

    private Dispatcher dispatcher;
    private ExecutorService executor;

    public JetStreamMessageCorrelationSubscriber(Connection connection, JetStream jetStream,
            RuntimeService runtimeService, SubscriptionConfig config, NatsChannelMetrics metrics,
            DlqPublisher dlqPublisher) {
        this.connection = connection;
        this.jetStream = jetStream;
        this.runtimeService = runtimeService;
        this.config = config;
        this.metrics = metrics;
        this.dlqPublisher = dlqPublisher;
        this.definition = null;
    }

    /** Definition-path constructor (docs/12): typed payload + correlation parameters. */
    public JetStreamMessageCorrelationSubscriber(Connection connection, JetStream jetStream,
            RuntimeService runtimeService, SubscriptionConfig config, NatsChannelMetrics metrics,
            DlqPublisher dlqPublisher, EventDefinition definition) {
        this.connection = connection;
        this.jetStream = jetStream;
        this.runtimeService = runtimeService;
        this.config = config;
        this.metrics = metrics;
        this.dlqPublisher = dlqPublisher;
        this.definition = definition;
    }

    public void subscribe() {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.dispatcher = connection.createDispatcher();
        try {
            ConsumerConfiguration.Builder ccBuilder = ConsumerConfiguration.builder()
                    .ackWait(Duration.ofSeconds(30))
                    .maxDeliver(config.getMaxDeliver() + 1);
            if (config.getDurableName() != null) {
                ccBuilder.durable(config.getDurableName());
            }
            String deliverGroup = config.resolveDeliverGroup();
            if (deliverGroup != null) {
                ccBuilder.deliverGroup(deliverGroup);
            }
            PushSubscribeOptions opts = PushSubscribeOptions.builder()
                    .configuration(ccBuilder.build())
                    .build();
            JetStreamSubscription sub = deliverGroup != null
                    ? jetStream.subscribe(config.getSubject(), deliverGroup, dispatcher,
                            msg -> executor.submit(() -> handleMessage(msg)), false, opts)
                    : jetStream.subscribe(config.getSubject(), dispatcher,
                            msg -> executor.submit(() -> handleMessage(msg)), false, opts);
            log.info("Subscribed to JetStream for CadenzaFlow correlation",
                    kv("subject", config.getSubject()),
                    kv("message_name", config.getMessageName()));
        } catch (Exception e) {
            log.error("Failed to subscribe to JetStream",
                    kv("subject", config.getSubject()),
                    kv("message_name", config.getMessageName()), e);
            throw new IllegalStateException(
                    "Failed to subscribe to JetStream subject '" + config.getSubject() + "'", e);
        }
    }

    public void unsubscribe() {
        if (dispatcher != null) {
            try {
                dispatcher.drain(Duration.ofSeconds(10));
            } catch (Exception e) {
                log.warn("Error draining JetStream dispatcher",
                        kv("subject", config.getSubject()), e);
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
                        kv("subject", msg.getSubject()),
                        kv("message_name", config.getMessageName()));
                routeToDlqAndDecide(msg, DlqReason.EMPTY_MESSAGE_BODY);
                return;
            }

            // Check if max deliveries exceeded -> DLQ path
            long deliveryCount = getDeliveryCount(msg);
            if (deliveryCount > config.getMaxDeliver()) {
                log.warn("Max deliveries exceeded, routing to DLQ",
                        kv("subject", msg.getSubject()),
                        kv("message_name", config.getMessageName()),
                        kv("delivery_count", deliveryCount),
                        kv("max_deliver", config.getMaxDeliver()));
                routeToDlqAndDecide(msg, DlqReason.DELIVERY_BUDGET_EXCEEDED);
                return;
            }

            // Normal processing: correlate with CadenzaFlow
            String payload = new String(data, StandardCharsets.UTF_8);

            Map<String, Object> variables;
            Map<String, Object> correlationValues = Map.of();
            if (definition != null) {
                // Definition path (docs/12 D-C v2): typed payload variables, no natsPayload blob.
                com.fasterxml.jackson.databind.JsonNode root = EventingPayloadMapper.parse(payload);
                variables = EventingPayloadMapper.typedVariables(root, definition.payloadTypes());
                try {
                    correlationValues = EventingPayloadMapper.correlationValues(root,
                            definition.correlationParameterTypes());
                } catch (EventingPayloadMapper.MissingCorrelationValueException e) {
                    // Permanent: no redelivery can supply the missing field -> DLQ, not nak.
                    log.warn("Payload lacks a declared correlation parameter, routing to DLQ",
                            kv("subject", msg.getSubject()),
                            kv("message_name", config.getMessageName()),
                            kv("detail", e.getMessage()));
                    routeToDlqAndDecide(msg, DlqReason.MISSING_CORRELATION_VALUE);
                    return;
                }
            } else {
                variables = new HashMap<>();
                variables.put("natsPayload", payload);
                variables.put("natsSubject", msg.getSubject());
            }

            String businessKey = resolveBusinessKey(msg, payload);

            MessageCorrelationBuilder builder = runtimeService
                    .createMessageCorrelation(config.getMessageName())
                    .setVariables(variables);
            for (Map.Entry<String, Object> correlation : correlationValues.entrySet()) {
                builder.processInstanceVariableEquals(correlation.getKey(), correlation.getValue());
            }

            if (businessKey != null) {
                builder.processInstanceBusinessKey(businessKey);
            }

            builder.correlateWithResult();

            if (metrics != null) {
                metrics.ackCount(config.getSubject(), config.getMessageName()).increment();
            }
            msg.ack();
            log.debug("Message correlated and acked",
                    kv("subject", msg.getSubject()),
                    kv("message_name", config.getMessageName()));

        } catch (Exception e) {
            log.error("Error correlating JetStream message",
                    kv("subject", msg.getSubject()),
                    kv("message_name", config.getMessageName()), e);
            if (metrics != null) {
                metrics.nakCount(config.getSubject(), config.getMessageName()).increment();
            }
            nakWithBackoff(msg);
        } finally {
            MDC.remove("trace_id");
        }
    }

    /**
     * Custody-transfer decision (contract-fix #2, BR-SUB-002): ack only on a successful DLQ
     * publish; nak — never a silent ack-drop — when the DLQ subject is unconfigured or both
     * publish paths fail.
     */
    private void routeToDlqAndDecide(Message msg, DlqReason reason) {
        DlqPublishOutcome outcome = dlqPublisher.publish(msg, config.getDlqSubject(), reason,
                config.getSubject(), config.getMessageName());
        switch (outcome) {
            case PUBLISHED_JETSTREAM, PUBLISHED_CORE_FALLBACK -> msg.ack();
            case FAILED_NO_DLQ_SUBJECT, FAILED_BOTH_PUBLISH -> {
                if (metrics != null) {
                    metrics.nakCount(config.getSubject(), config.getMessageName()).increment();
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
                    kv("subject", config.getSubject()), e);
            return 1;
        }
    }

    private void nakWithBackoff(Message msg) {
        try {
            long deliveryCount = msg.metaData().deliveredCount();
            Duration backoff = calculateBackoff(deliveryCount);
            msg.nakWithDelay(backoff);
            log.debug("Message nacked with delay",
                    kv("subject", config.getSubject()),
                    kv("delay", backoff));
        } catch (Exception e) {
            log.warn("Failed to get metadata for backoff, falling back to plain nak",
                    kv("subject", config.getSubject()), e);
            msg.nak();
        }
    }

    Duration calculateBackoff(long deliveryCount) {
        long seconds = (long) Math.pow(2, deliveryCount - 1);
        Duration backoff = Duration.ofSeconds(seconds);
        return backoff.compareTo(MAX_BACKOFF) > 0 ? MAX_BACKOFF : backoff;
    }

    private String resolveBusinessKey(Message msg, String payload) {
        if (config.getBusinessKeyHeader() != null) {
            return NatsHeaderUtils.extractHeader(msg, config.getBusinessKeyHeader());
        }
        if (config.getBusinessKeyVariable() != null) {
            // D-F v2 (behavior change, CHANGELOG'd): Jackson top-level-only scalar read —
            // nested fields are no longer found; numeric scalars now return the correct value.
            return EventingPayloadMapper.topLevelScalarAsText(payload, config.getBusinessKeyVariable());
        }
        return null;
    }

}
