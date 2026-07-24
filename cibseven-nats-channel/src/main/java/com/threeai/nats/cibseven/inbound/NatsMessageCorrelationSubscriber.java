package com.threeai.nats.cibseven.inbound;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.threeai.nats.core.NatsHeaderUtils;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import org.cibseven.bpm.engine.RuntimeService;
import org.cibseven.bpm.engine.runtime.MessageCorrelationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class NatsMessageCorrelationSubscriber {

    private static final Logger log = LoggerFactory.getLogger(NatsMessageCorrelationSubscriber.class);

    private final Connection connection;
    private final RuntimeService runtimeService;
    private final SubscriptionConfig config;
    private final NatsChannelMetrics metrics;

    private Dispatcher dispatcher;
    private ExecutorService executor;

    public NatsMessageCorrelationSubscriber(Connection connection, RuntimeService runtimeService,
            SubscriptionConfig config, NatsChannelMetrics metrics) {
        this.connection = connection;
        this.runtimeService = runtimeService;
        this.config = config;
        this.metrics = metrics;
    }

    public void subscribe() {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.dispatcher = connection.createDispatcher();
        dispatcher.subscribe(config.getSubject(), msg -> executor.submit(() -> handleMessage(msg)));
        log.info("Subscribed to NATS subject for CibSeven correlation",
                kv("subject", config.getSubject()),
                kv("message_name", config.getMessageName()));
    }

    public void unsubscribe() {
        if (dispatcher != null) {
            try {
                connection.closeDispatcher(dispatcher);
            } catch (Exception e) {
                log.warn("Error closing dispatcher",
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
        String traceId = NatsHeaderUtils.extractHeader(msg, "X-Trace-Id");
        if (traceId != null) {
            MDC.put("trace_id", traceId);
        }
        try {
            byte[] data = msg.getData();
            if (data == null || data.length == 0) {
                log.debug("Empty message body, skipping",
                        kv("subject", msg.getSubject()),
                        kv("message_name", config.getMessageName()));
                return;
            }

            String payload = new String(data, StandardCharsets.UTF_8);

            Map<String, Object> variables = new HashMap<>();
            variables.put("natsPayload", payload);
            variables.put("natsSubject", msg.getSubject());

            String businessKey = resolveBusinessKey(msg, payload);

            MessageCorrelationBuilder builder = runtimeService
                    .createMessageCorrelation(config.getMessageName())
                    .setVariables(variables);

            if (businessKey != null) {
                builder.processInstanceBusinessKey(businessKey);
            }

            builder.correlateWithResult();

            if (metrics != null) {
                metrics.consumeCount(config.getSubject(), config.getMessageName()).increment();
            }
            // DP-1 (NFR-S1, DATA_CLASSIFICATION.md §4): businessKey may carry PII (telco MSISDN
            // etc.) — never log its VALUE, only whether one was present (Sentinel Phase 5.5 QA
            // finding, HIGH).
            log.debug("Message correlated successfully",
                    kv("subject", msg.getSubject()),
                    kv("message_name", config.getMessageName()),
                    kv("has_business_key", businessKey != null));

        } catch (Exception e) {
            log.error("Error correlating message",
                    kv("subject", msg.getSubject()),
                    kv("message_name", config.getMessageName()), e);
            if (metrics != null) {
                metrics.consumeErrorCount(config.getSubject(), config.getMessageName()).increment();
            }
        } finally {
            MDC.remove("trace_id");
        }
    }

    private String resolveBusinessKey(Message msg, String payload) {
        if (config.getBusinessKeyHeader() != null) {
            return NatsHeaderUtils.extractHeader(msg, config.getBusinessKeyHeader());
        }
        if (config.getBusinessKeyVariable() != null) {
            // Simple JSON field extraction: parse "fieldName":"value" from payload
            return extractJsonField(payload, config.getBusinessKeyVariable());
        }
        return null;
    }

    private String extractJsonField(String json, String fieldName) {
        // Simple extraction for top-level string fields
        String pattern = "\"" + fieldName + "\"";
        int idx = json.indexOf(pattern);
        if (idx < 0) {
            return null;
        }
        int colonIdx = json.indexOf(':', idx + pattern.length());
        if (colonIdx < 0) {
            return null;
        }
        int quoteStart = json.indexOf('"', colonIdx + 1);
        if (quoteStart < 0) {
            return null;
        }
        int quoteEnd = json.indexOf('"', quoteStart + 1);
        if (quoteEnd < 0) {
            return null;
        }
        return json.substring(quoteStart + 1, quoteEnd);
    }
}
