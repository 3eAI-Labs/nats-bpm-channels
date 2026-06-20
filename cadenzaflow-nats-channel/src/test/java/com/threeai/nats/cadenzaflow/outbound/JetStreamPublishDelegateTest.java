package com.threeai.nats.cadenzaflow.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.threeai.nats.core.headers.BpmHeaders;
import io.nats.client.JetStream;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.NatsMessage;
import org.cadenzaflow.bpm.engine.ProcessEngineException;
import org.cadenzaflow.bpm.engine.delegate.DelegateExecution;
import org.cadenzaflow.bpm.engine.delegate.Expression;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class JetStreamPublishDelegateTest {

    private JetStream jetStream;
    private DelegateExecution execution;
    private JetStreamPublishDelegate delegate;

    @BeforeEach
    void setUp() throws Exception {
        jetStream = mock(JetStream.class);
        execution = mock(DelegateExecution.class);
        when(execution.getProcessInstanceId()).thenReturn("pi-1");
        when(execution.getActivityInstanceId()).thenReturn("ai-1");
        delegate = new JetStreamPublishDelegate(jetStream, null);
        when(jetStream.publish(any(NatsMessage.class))).thenReturn(mock(PublishAck.class));
    }

    @Test
    void execute_publishesToJetStream() throws Exception {
        delegate.setSubject(mockExpression("order.created"));
        delegate.setPayloadVariable(mockExpression("orderPayload"));

        when(execution.getVariable("orderPayload")).thenReturn("{\"orderId\":1}");

        delegate.execute(execution);

        verify(jetStream).publish(any(NatsMessage.class));
    }

    @Test
    void execute_addsMandatoryHeaders() throws Exception {
        delegate.setSubject(mockExpression("order.created"));
        delegate.setPayloadVariable(mockExpression("orderPayload"));

        when(execution.getVariable("orderPayload")).thenReturn("data");
        when(execution.getVariable("traceId")).thenReturn("trace-js");
        when(execution.getProcessBusinessKey()).thenReturn("order-77");

        delegate.execute(execution);

        ArgumentCaptor<NatsMessage> captor = ArgumentCaptor.forClass(NatsMessage.class);
        verify(jetStream).publish(captor.capture());
        assertThat(captor.getValue().getHeaders().getLast(BpmHeaders.TRACE_ID)).isEqualTo("trace-js");
        assertThat(captor.getValue().getHeaders().getLast(BpmHeaders.BUSINESS_KEY)).isEqualTo("order-77");
        assertThat(captor.getValue().getHeaders().getLast(BpmHeaders.IDEMPOTENCY_KEY)).isEqualTo("pi-1:ai-1");
    }

    @Test
    void execute_explicitIdempotencyKey_overridesDefault() throws Exception {
        delegate.setSubject(mockExpression("order.created"));
        delegate.setPayloadVariable(mockExpression("orderPayload"));
        delegate.setIdempotencyKey(mockExpression("js-explicit"));

        when(execution.getVariable("orderPayload")).thenReturn("data");

        delegate.execute(execution);

        ArgumentCaptor<NatsMessage> captor = ArgumentCaptor.forClass(NatsMessage.class);
        verify(jetStream).publish(captor.capture());
        assertThat(captor.getValue().getHeaders().getLast(BpmHeaders.IDEMPOTENCY_KEY))
                .isEqualTo("js-explicit");
    }

    @Test
    void execute_publishFails_throwsException() throws Exception {
        delegate.setSubject(mockExpression("order.created"));
        delegate.setPayloadVariable(mockExpression("orderPayload"));

        when(execution.getVariable("orderPayload")).thenReturn("data");
        when(jetStream.publish(any(NatsMessage.class)))
                .thenThrow(new RuntimeException("JetStream unavailable"));

        assertThatThrownBy(() -> delegate.execute(execution))
                .isInstanceOf(ProcessEngineException.class)
                .hasMessageContaining("JetStream");
    }

    private Expression mockExpression(String value) {
        Expression expr = mock(Expression.class);
        when(expr.getValue(any(DelegateExecution.class))).thenReturn(value);
        return expr;
    }
}
