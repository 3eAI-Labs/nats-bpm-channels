package com.threeai.nats.cadenzaflow.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.threeai.nats.core.headers.BpmHeaders;
import io.nats.client.Connection;
import io.nats.client.impl.NatsMessage;
import org.cadenzaflow.bpm.engine.ProcessEngineException;
import org.cadenzaflow.bpm.engine.delegate.DelegateExecution;
import org.cadenzaflow.bpm.engine.delegate.Expression;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class NatsPublishDelegateTest {

    private Connection connection;
    private DelegateExecution execution;
    private NatsPublishDelegate delegate;

    @BeforeEach
    void setUp() {
        connection = mock(Connection.class);
        execution = mock(DelegateExecution.class);
        when(execution.getProcessInstanceId()).thenReturn("pi-1");
        when(execution.getActivityInstanceId()).thenReturn("ai-1");
        delegate = new NatsPublishDelegate(connection, null);
    }

    @Test
    void execute_publishesMessage() {
        delegate.setSubject(mockExpression("order.created"));
        delegate.setPayloadVariable(mockExpression("orderPayload"));

        when(execution.getVariable("orderPayload")).thenReturn("{\"orderId\":1}");

        delegate.execute(execution);

        ArgumentCaptor<NatsMessage> captor = ArgumentCaptor.forClass(NatsMessage.class);
        verify(connection).publish(captor.capture());
        NatsMessage published = captor.getValue();
        assertThat(published.getSubject()).isEqualTo("order.created");
        assertThat(new String(published.getData())).isEqualTo("{\"orderId\":1}");
    }

    @Test
    void execute_addsMandatoryHeaders() {
        delegate.setSubject(mockExpression("order.created"));
        delegate.setPayloadVariable(mockExpression("orderPayload"));

        when(execution.getVariable("orderPayload")).thenReturn("data");
        when(execution.getVariable("traceId")).thenReturn("trace-pub");
        when(execution.getProcessBusinessKey()).thenReturn("order-99");

        delegate.execute(execution);

        ArgumentCaptor<NatsMessage> captor = ArgumentCaptor.forClass(NatsMessage.class);
        verify(connection).publish(captor.capture());
        assertThat(captor.getValue().getHeaders().getLast(BpmHeaders.TRACE_ID)).isEqualTo("trace-pub");
        assertThat(captor.getValue().getHeaders().getLast(BpmHeaders.BUSINESS_KEY)).isEqualTo("order-99");
        assertThat(captor.getValue().getHeaders().getLast(BpmHeaders.IDEMPOTENCY_KEY)).isEqualTo("pi-1:ai-1");
    }

    @Test
    void execute_explicitIdempotencyKey_overridesDefault() {
        delegate.setSubject(mockExpression("order.created"));
        delegate.setPayloadVariable(mockExpression("orderPayload"));
        delegate.setIdempotencyKey(mockExpression("explicit-pub-key"));

        when(execution.getVariable("orderPayload")).thenReturn("data");

        delegate.execute(execution);

        ArgumentCaptor<NatsMessage> captor = ArgumentCaptor.forClass(NatsMessage.class);
        verify(connection).publish(captor.capture());
        assertThat(captor.getValue().getHeaders().getLast(BpmHeaders.IDEMPOTENCY_KEY))
                .isEqualTo("explicit-pub-key");
    }

    @Test
    void execute_businessKeyNull_skipsHeader() {
        delegate.setSubject(mockExpression("order.created"));
        delegate.setPayloadVariable(mockExpression("orderPayload"));

        when(execution.getVariable("orderPayload")).thenReturn("data");
        when(execution.getProcessBusinessKey()).thenReturn(null);

        delegate.execute(execution);

        ArgumentCaptor<NatsMessage> captor = ArgumentCaptor.forClass(NatsMessage.class);
        verify(connection).publish(captor.capture());
        assertThat(captor.getValue().getHeaders().containsKey(BpmHeaders.BUSINESS_KEY)).isFalse();
    }

    @Test
    void execute_missingSubject_throwsException() {
        assertThatThrownBy(() -> delegate.execute(execution))
                .isInstanceOf(ProcessEngineException.class)
                .hasMessageContaining("subject");
    }

    private Expression mockExpression(String value) {
        Expression expr = mock(Expression.class);
        when(expr.getValue(any(DelegateExecution.class))).thenReturn(value);
        return expr;
    }
}
