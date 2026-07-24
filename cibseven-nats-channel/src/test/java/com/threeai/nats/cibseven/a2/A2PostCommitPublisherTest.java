package com.threeai.nats.cibseven.a2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.JetStream;
import io.nats.client.impl.NatsMessage;
import org.cibseven.bpm.engine.impl.persistence.entity.ExternalTaskEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class A2PostCommitPublisherTest {

    private JetStream jetStream;
    private NatsChannelMetrics metrics;
    private UmbrellaLockValidator lockValidator;
    private A2PostCommitPublisher publisher;

    @BeforeEach
    void setUp() {
        jetStream = mock(JetStream.class);
        metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        lockValidator = mock(UmbrellaLockValidator.class);
        publisher = new A2PostCommitPublisher(jetStream, metrics, lockValidator);
    }

    @Test
    void publish_success_publishesToJobsSubject() throws Exception {
        ExternalTaskEntity task = mockTask("task-1", "order-fulfillment", "biz-key-1");

        publisher.publish(task);

        verify(jetStream).publish(any(NatsMessage.class));
    }

    @Test
    void publish_jetStreamPublishFails_doesNotThrow() throws Exception {
        ExternalTaskEntity task = mockTask("task-2", "order-fulfillment", null);
        when(jetStream.publish(any(NatsMessage.class))).thenThrow(new IOException("JS down"));

        assertThatCode(() -> publisher.publish(task)).doesNotThrowAnyException();
    }

    @Test
    void publish_unsafeTopic_stillPublishes() throws Exception {
        ExternalTaskEntity task = mockTask("task-3", "unsafe-topic", null);
        when(lockValidator.isUnsafe("unsafe-topic")).thenReturn(true);

        publisher.publish(task);

        verify(jetStream).publish(any(NatsMessage.class));
    }

    /** Sentinel Phase 5.5 QA fix (item 5) — captured variables flow through to the wire payload. */
    @Test
    void publish_withCapturedVariables_appendsVariablesToPayload() throws Exception {
        ExternalTaskEntity task = mockTask("task-4", "payment-capture", "biz-key-4");

        publisher.publish(task, Map.of("amount", 42));

        ArgumentCaptor<NatsMessage> captor = ArgumentCaptor.forClass(NatsMessage.class);
        verify(jetStream).publish(captor.capture());
        assertThat(new String(captor.getValue().getData(), StandardCharsets.UTF_8))
                .contains("\"variables\":{\"amount\":42}");
    }

    /** Sentinel Phase 5.5 QA fix (item 5) — the no-arg overload preserves the identity-only envelope. */
    @Test
    void publish_singleArgOverload_noVariablesInPayload() throws Exception {
        ExternalTaskEntity task = mockTask("task-5", "order-fulfillment", "biz-key-5");

        publisher.publish(task);

        ArgumentCaptor<NatsMessage> captor = ArgumentCaptor.forClass(NatsMessage.class);
        verify(jetStream).publish(captor.capture());
        assertThat(new String(captor.getValue().getData(), StandardCharsets.UTF_8)).doesNotContain("variables");
    }

    private ExternalTaskEntity mockTask(String id, String topic, String businessKey) {
        ExternalTaskEntity task = mock(ExternalTaskEntity.class);
        when(task.getId()).thenReturn(id);
        when(task.getTopicName()).thenReturn(topic);
        when(task.getBusinessKey()).thenReturn(businessKey);
        return task;
    }
}
