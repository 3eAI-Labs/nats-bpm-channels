package com.threeai.nats.cadenzaflow.outbound;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.impl.NatsMessage;
import org.cadenzaflow.bpm.engine.ProcessEngineException;
import org.cadenzaflow.bpm.engine.delegate.DelegateExecution;
import org.cadenzaflow.bpm.engine.delegate.Expression;
import org.cadenzaflow.bpm.engine.delegate.JavaDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class NatsRequestReplyDelegate implements JavaDelegate {

    private static final Logger log = LoggerFactory.getLogger(NatsRequestReplyDelegate.class);

    private final Connection connection;
    private final NatsChannelMetrics metrics;

    private Expression subject;
    private Expression timeout;
    private Expression resultVariable;
    private Expression payloadVariable;
    private Expression idempotencyKey;

    public NatsRequestReplyDelegate(Connection connection, NatsChannelMetrics metrics) {
        this.connection = connection;
        this.metrics = metrics;
    }

    @Override
    public void execute(DelegateExecution execution) {
        String subjectVal = getRequiredString(subject, execution, "subject");
        Duration timeoutVal = parseDuration(timeout, execution, Duration.ofSeconds(30));
        String resultVar = getString(resultVariable, execution, "natsReplyPayload");
        String payloadVar = getString(payloadVariable, execution, "natsRequestPayload");
        String idempotencyVal = getString(idempotencyKey, execution, null);

        byte[] data = serializePayload(execution.getVariable(payloadVar));

        String traceId = (String) execution.getVariable("traceId");
        try {
            if (traceId != null) {
                MDC.put("trace_id", traceId);
            }

            log.debug("Sending NATS request",
                    kv("subject", subjectVal),
                    kv("timeout", timeoutVal),
                    kv("process_instance", execution.getProcessInstanceId()));

            NatsMessage request = NatsMessage.builder()
                    .subject(subjectVal)
                    .data(data)
                    .headers(CadenzaflowHeaderBinder.from(execution, idempotencyVal))
                    .build();
            Message reply = connection.request(request, timeoutVal);

            if (reply == null) {
                if (metrics != null) metrics.requestReplyErrorCount(subjectVal).increment();
                throw new ProcessEngineException(
                        "NATS request-reply timeout for subject '" + subjectVal
                        + "' after " + timeoutVal);
            }

            String replyBody = new String(reply.getData(), StandardCharsets.UTF_8);
            execution.setVariable(resultVar, replyBody);

            if (metrics != null) metrics.requestReplyCount(subjectVal).increment();

            log.debug("NATS reply received",
                    kv("subject", subjectVal),
                    kv("result_variable", resultVar));

        } catch (ProcessEngineException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (metrics != null) metrics.requestReplyErrorCount(subjectVal).increment();
            throw new ProcessEngineException(
                    "NATS request-reply interrupted for subject '" + subjectVal + "'", e);
        } catch (Exception e) {
            if (metrics != null) metrics.requestReplyErrorCount(subjectVal).increment();
            throw new ProcessEngineException(
                    "NATS request-reply failed for subject '" + subjectVal + "'", e);
        } finally {
            MDC.remove("trace_id");
        }
    }

    private String getRequiredString(Expression expr, DelegateExecution execution, String fieldName) {
        if (expr == null) {
            throw new ProcessEngineException("NATS request-reply: '" + fieldName + "' is required");
        }
        String value = (String) expr.getValue(execution);
        if (value == null || value.isBlank()) {
            throw new ProcessEngineException("NATS request-reply: '" + fieldName + "' resolved to blank");
        }
        return value;
    }

    private String getString(Expression expr, DelegateExecution execution, String defaultValue) {
        if (expr == null) return defaultValue;
        Object value = expr.getValue(execution);
        return value != null ? value.toString() : defaultValue;
    }

    private Duration parseDuration(Expression expr, DelegateExecution execution, Duration defaultValue) {
        if (expr == null) return defaultValue;
        Object value = expr.getValue(execution);
        if (value == null) return defaultValue;
        String str = value.toString().trim();
        if (str.matches("\\d+s")) return Duration.ofSeconds(Long.parseLong(str.replace("s", "")));
        if (str.matches("\\d+m")) return Duration.ofMinutes(Long.parseLong(str.replace("m", "")));
        if (str.matches("\\d+h")) return Duration.ofHours(Long.parseLong(str.replace("h", "")));
        return Duration.parse(str);
    }

    private byte[] serializePayload(Object payload) {
        if (payload == null) return new byte[0];
        if (payload instanceof byte[] bytes) return bytes;
        if (payload instanceof String str) return str.getBytes(StandardCharsets.UTF_8);
        return payload.toString().getBytes(StandardCharsets.UTF_8);
    }

    public void setSubject(Expression subject) { this.subject = subject; }
    public void setTimeout(Expression timeout) { this.timeout = timeout; }
    public void setResultVariable(Expression resultVariable) { this.resultVariable = resultVariable; }
    public void setPayloadVariable(Expression payloadVariable) { this.payloadVariable = payloadVariable; }
    public void setIdempotencyKey(Expression idempotencyKey) { this.idempotencyKey = idempotencyKey; }
}
