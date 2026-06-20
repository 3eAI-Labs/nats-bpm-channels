package com.threeai.nats.cadenzaflow.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import com.threeai.nats.core.headers.BpmHeaders;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.impl.NatsMessage;
import org.cadenzaflow.bpm.engine.ProcessEngineException;
import org.cadenzaflow.bpm.engine.delegate.DelegateExecution;
import org.cadenzaflow.bpm.engine.delegate.Expression;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.MDC;

class NatsRequestReplyDelegateTest {

    private Connection connection;
    private DelegateExecution execution;
    private NatsRequestReplyDelegate delegate;

    @BeforeEach
    void setUp() throws Exception {
        connection = mock(Connection.class);
        execution = mock(DelegateExecution.class);
        when(execution.getProcessInstanceId()).thenReturn("pi-1");
        when(execution.getActivityInstanceId()).thenReturn("ai-1");
        delegate = new NatsRequestReplyDelegate(connection, null);
    }

    @Test
    void execute_sendsRequestAndStoresReply() throws Exception {
        delegate.setSubject(mockExpression("task.process"));
        delegate.setTimeout(mockExpression("30s"));
        delegate.setResultVariable(mockExpression("result"));
        delegate.setPayloadVariable(mockExpression("requestPayload"));

        when(execution.getVariable("requestPayload")).thenReturn("{\"task\":\"do-it\"}");
        Message reply = mock(Message.class);
        when(reply.getData()).thenReturn("{\"status\":\"done\"}".getBytes(StandardCharsets.UTF_8));
        when(connection.request(any(NatsMessage.class), any(Duration.class))).thenReturn(reply);

        delegate.execute(execution);

        ArgumentCaptor<NatsMessage> captor = ArgumentCaptor.forClass(NatsMessage.class);
        verify(connection).request(captor.capture(), any(Duration.class));
        assertThat(captor.getValue().getSubject()).isEqualTo("task.process");
        assertThat(new String(captor.getValue().getData())).isEqualTo("{\"task\":\"do-it\"}");
        verify(execution).setVariable("result", "{\"status\":\"done\"}");
    }

    @Test
    void execute_timeout_throwsException() throws Exception {
        delegate.setSubject(mockExpression("task.process"));
        delegate.setPayloadVariable(mockExpression("requestPayload"));

        when(execution.getVariable("requestPayload")).thenReturn("data");
        when(connection.request(any(NatsMessage.class), any(Duration.class))).thenReturn(null);

        assertThatThrownBy(() -> delegate.execute(execution))
                .isInstanceOf(ProcessEngineException.class)
                .hasMessageContaining("timeout");
    }

    @Test
    void execute_nullPayload_sendsEmptyBody() throws Exception {
        delegate.setSubject(mockExpression("task.process"));
        delegate.setPayloadVariable(mockExpression("requestPayload"));

        when(execution.getVariable("requestPayload")).thenReturn(null);
        Message reply = mock(Message.class);
        when(reply.getData()).thenReturn("ok".getBytes(StandardCharsets.UTF_8));
        when(connection.request(any(NatsMessage.class), any(Duration.class))).thenReturn(reply);

        delegate.execute(execution);

        ArgumentCaptor<NatsMessage> captor = ArgumentCaptor.forClass(NatsMessage.class);
        verify(connection).request(captor.capture(), any(Duration.class));
        assertThat(captor.getValue().getData()).isEmpty();
    }

    @Test
    void execute_propagatesTraceId() throws Exception {
        delegate.setSubject(mockExpression("task.process"));
        delegate.setPayloadVariable(mockExpression("requestPayload"));

        when(execution.getVariable("requestPayload")).thenReturn("data");
        when(execution.getVariable("traceId")).thenReturn("trace-rr-999");

        final String[] capturedTraceId = {null};
        Message reply = mock(Message.class);
        when(reply.getData()).thenReturn("ok".getBytes(StandardCharsets.UTF_8));
        when(connection.request(any(NatsMessage.class), any(Duration.class)))
                .thenAnswer(invocation -> {
                    capturedTraceId[0] = MDC.get("trace_id");
                    return reply;
                });

        delegate.execute(execution);

        assertThat(capturedTraceId[0]).isEqualTo("trace-rr-999");
        assertThat(MDC.get("trace_id")).isNull();
    }

    @Test
    void execute_addsMandatoryHeaders() throws Exception {
        delegate.setSubject(mockExpression("task.process"));
        delegate.setPayloadVariable(mockExpression("requestPayload"));

        when(execution.getVariable("requestPayload")).thenReturn("data");
        when(execution.getVariable("traceId")).thenReturn("trace-x");
        when(execution.getProcessBusinessKey()).thenReturn("biz-1");

        Message reply = mock(Message.class);
        when(reply.getData()).thenReturn("ok".getBytes(StandardCharsets.UTF_8));
        when(connection.request(any(NatsMessage.class), any(Duration.class))).thenReturn(reply);

        delegate.execute(execution);

        ArgumentCaptor<NatsMessage> captor = ArgumentCaptor.forClass(NatsMessage.class);
        verify(connection).request(captor.capture(), any(Duration.class));
        assertThat(captor.getValue().getHeaders().getLast(BpmHeaders.TRACE_ID)).isEqualTo("trace-x");
        assertThat(captor.getValue().getHeaders().getLast(BpmHeaders.BUSINESS_KEY)).isEqualTo("biz-1");
        assertThat(captor.getValue().getHeaders().getLast(BpmHeaders.IDEMPOTENCY_KEY)).isEqualTo("pi-1:ai-1");
    }

    @Test
    void execute_explicitIdempotencyKey_overridesDefault() throws Exception {
        delegate.setSubject(mockExpression("task.process"));
        delegate.setPayloadVariable(mockExpression("requestPayload"));
        delegate.setIdempotencyKey(mockExpression("my-explicit-key"));

        when(execution.getVariable("requestPayload")).thenReturn("data");
        Message reply = mock(Message.class);
        when(reply.getData()).thenReturn("ok".getBytes(StandardCharsets.UTF_8));
        when(connection.request(any(NatsMessage.class), any(Duration.class))).thenReturn(reply);

        delegate.execute(execution);

        ArgumentCaptor<NatsMessage> captor = ArgumentCaptor.forClass(NatsMessage.class);
        verify(connection).request(captor.capture(), any(Duration.class));
        assertThat(captor.getValue().getHeaders().getLast(BpmHeaders.IDEMPOTENCY_KEY))
                .isEqualTo("my-explicit-key");
    }

    @Test
    void execute_businessKeyNull_skipsHeader() throws Exception {
        delegate.setSubject(mockExpression("task.process"));
        delegate.setPayloadVariable(mockExpression("requestPayload"));

        when(execution.getVariable("requestPayload")).thenReturn("data");
        when(execution.getProcessBusinessKey()).thenReturn(null);

        Message reply = mock(Message.class);
        when(reply.getData()).thenReturn("ok".getBytes(StandardCharsets.UTF_8));
        when(connection.request(any(NatsMessage.class), any(Duration.class))).thenReturn(reply);

        delegate.execute(execution);

        ArgumentCaptor<NatsMessage> captor = ArgumentCaptor.forClass(NatsMessage.class);
        verify(connection).request(captor.capture(), any(Duration.class));
        assertThat(captor.getValue().getHeaders().containsKey(BpmHeaders.BUSINESS_KEY)).isFalse();
    }

    private Expression mockExpression(String value) {
        Expression expr = mock(Expression.class);
        when(expr.getValue(any(DelegateExecution.class))).thenReturn(value);
        return expr;
    }
}
