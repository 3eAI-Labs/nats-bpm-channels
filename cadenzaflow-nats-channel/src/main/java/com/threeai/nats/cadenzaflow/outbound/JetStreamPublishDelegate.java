package com.threeai.nats.cadenzaflow.outbound;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.nio.charset.StandardCharsets;

import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.JetStream;
import io.nats.client.impl.NatsMessage;
import org.cadenzaflow.bpm.engine.ProcessEngineException;
import org.cadenzaflow.bpm.engine.delegate.DelegateExecution;
import org.cadenzaflow.bpm.engine.delegate.Expression;
import org.cadenzaflow.bpm.engine.delegate.JavaDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JetStreamPublishDelegate implements JavaDelegate {

    private static final Logger log = LoggerFactory.getLogger(JetStreamPublishDelegate.class);

    private final JetStream jetStream;
    private final NatsChannelMetrics metrics;

    private Expression subject;
    private Expression payloadVariable;
    private Expression idempotencyKey;

    public JetStreamPublishDelegate(JetStream jetStream, NatsChannelMetrics metrics) {
        this.jetStream = jetStream;
        this.metrics = metrics;
    }

    @Override
    public void execute(DelegateExecution execution) {
        String subjectVal = getRequiredString(subject, execution, "subject");
        String payloadVar = getString(payloadVariable, execution, "natsPayload");
        String idempotencyVal = getString(idempotencyKey, execution, null);
        byte[] data = serializePayload(execution.getVariable(payloadVar));

        try {
            NatsMessage msg = NatsMessage.builder()
                    .subject(subjectVal)
                    .data(data)
                    .headers(CadenzaflowHeaderBinder.from(execution, idempotencyVal))
                    .build();
            jetStream.publish(msg);

            if (metrics != null) {
                metrics.jsPublishCount(subjectVal, "cadenzaflow").increment();
            }
            log.debug("Published message to JetStream",
                    kv("subject", subjectVal),
                    kv("process_instance", execution.getProcessInstanceId()));
        } catch (Exception e) {
            if (metrics != null) {
                metrics.jsPublishErrorCount(subjectVal, "cadenzaflow").increment();
            }
            throw new ProcessEngineException(
                    "Failed to publish to JetStream subject '" + subjectVal + "'", e);
        }
    }

    private String getRequiredString(Expression expr, DelegateExecution execution, String fieldName) {
        if (expr == null) {
            throw new ProcessEngineException("JetStream publish: '" + fieldName + "' is required");
        }
        String value = (String) expr.getValue(execution);
        if (value == null || value.isBlank()) {
            throw new ProcessEngineException("JetStream publish: '" + fieldName + "' resolved to blank");
        }
        return value;
    }

    private String getString(Expression expr, DelegateExecution execution, String defaultValue) {
        if (expr == null) return defaultValue;
        Object value = expr.getValue(execution);
        return value != null ? value.toString() : defaultValue;
    }

    private byte[] serializePayload(Object payload) {
        if (payload == null) return new byte[0];
        if (payload instanceof byte[] bytes) return bytes;
        if (payload instanceof String str) return str.getBytes(StandardCharsets.UTF_8);
        return payload.toString().getBytes(StandardCharsets.UTF_8);
    }

    public void setSubject(Expression subject) { this.subject = subject; }
    public void setPayloadVariable(Expression payloadVariable) { this.payloadVariable = payloadVariable; }
    public void setIdempotencyKey(Expression idempotencyKey) { this.idempotencyKey = idempotencyKey; }
}
