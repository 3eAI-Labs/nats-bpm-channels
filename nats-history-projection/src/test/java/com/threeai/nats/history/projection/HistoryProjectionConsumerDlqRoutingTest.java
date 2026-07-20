package com.threeai.nats.history.projection;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.threeai.nats.core.dlq.DlqPublishOutcome;
import com.threeai.nats.core.dlq.DlqReason;
import io.nats.client.JetStream;
import io.nats.client.Message;
import org.junit.jupiter.api.Test;

/**
 * FINDING-004 (faz-5 review): {@code routeToDlqThenAck}'s custody-transfer decision — narrow,
 * mocked fault-injection unit test (a real-dependency Testcontainers test cannot easily force
 * "both JetStream and core-NATS DLQ publish fail"). Complements the real-NATS
 * {@code HistoryProjectionConsumerTest} which only exercises the SUCCESS path.
 */
class HistoryProjectionConsumerDlqRoutingTest {

    @Test
    void onMessage_dlqPublishSucceeds_acksOriginalMessage() {
        Message msg = mock(Message.class);
        when(msg.getHeaders()).thenReturn(null); // forces schema-drift -> DLQ routing
        HistoryDlqConsumer dlqConsumer = mock(HistoryDlqConsumer.class);
        when(dlqConsumer.routeToDlq(any(), any())).thenReturn(DlqPublishOutcome.PUBLISHED_JETSTREAM);
        HistoryProjectionConsumer consumer = new HistoryProjectionConsumer(
                0, mock(JetStream.class), mock(ProjectionStore.class), dlqConsumer, null, 4);

        consumer.onMessage(msg);

        verify(dlqConsumer).routeToDlq(msg, DlqReason.HISTORY_SCHEMA_DRIFT);
        verify(msg).ack();
        verify(msg, never()).nak();
    }

    /** SYS_HISTORY_DLQ_PUBLISH_FAILED: a failed DLQ publish must NOT silently ack (custody lost) --
     *  it naks instead, so the message is redelivered and DLQ-routing retried. */
    @Test
    void onMessage_dlqPublishFails_naksOriginalMessage_doesNotAck() {
        Message msg = mock(Message.class);
        when(msg.getHeaders()).thenReturn(null); // forces schema-drift -> DLQ routing
        HistoryDlqConsumer dlqConsumer = mock(HistoryDlqConsumer.class);
        when(dlqConsumer.routeToDlq(any(), any())).thenReturn(DlqPublishOutcome.FAILED_BOTH_PUBLISH);
        HistoryProjectionConsumer consumer = new HistoryProjectionConsumer(
                0, mock(JetStream.class), mock(ProjectionStore.class), dlqConsumer, null, 4);

        consumer.onMessage(msg);

        verify(dlqConsumer).routeToDlq(msg, DlqReason.HISTORY_SCHEMA_DRIFT);
        verify(msg).nak();
        verify(msg, never()).ack();
    }
}
