package com.threeai.nats.history.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.threeai.nats.core.dlq.DlqPublishOutcome;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.dlq.DlqReason;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Message;
import org.junit.jupiter.api.Test;

class HistoryDlqConsumerTest {

    @Test
    void routeToDlq_delegatesToPublisherWithDlqPrefixedSubjectAndClassTag() {
        DlqPublisher publisher = mock(DlqPublisher.class);
        when(publisher.publish(any(Message.class), anyString(), any(DlqReason.class), anyString(), anyString()))
                .thenReturn(DlqPublishOutcome.PUBLISHED_JETSTREAM);
        HistoryDlqConsumer consumer = new HistoryDlqConsumer(publisher, null);
        Message msg = mock(Message.class);
        when(msg.getSubject()).thenReturn("history.camunda.OP_LOG.proc-1");

        DlqPublishOutcome outcome = consumer.routeToDlq(msg, DlqReason.HISTORY_SCHEMA_DRIFT);

        assertThat(outcome).isEqualTo(DlqPublishOutcome.PUBLISHED_JETSTREAM);
        verify(publisher).publish(msg, "dlq.history.camunda.OP_LOG.proc-1", DlqReason.HISTORY_SCHEMA_DRIFT,
                "history.camunda.OP_LOG.proc-1", "OP_LOG");
    }

    @Test
    void routeToDlq_subjectWithFewerThanThreeSegments_tagsAsUnknown() {
        DlqPublisher publisher = mock(DlqPublisher.class);
        when(publisher.publish(any(Message.class), anyString(), any(DlqReason.class), anyString(), anyString()))
                .thenReturn(DlqPublishOutcome.PUBLISHED_JETSTREAM);
        HistoryDlqConsumer consumer = new HistoryDlqConsumer(publisher, null);
        Message msg = mock(Message.class);
        when(msg.getSubject()).thenReturn("history.camunda"); // only 2 segments

        consumer.routeToDlq(msg, DlqReason.HISTORY_SCHEMA_DRIFT);

        verify(publisher).publish(msg, "dlq.history.camunda", DlqReason.HISTORY_SCHEMA_DRIFT,
                "history.camunda", "UNKNOWN");
    }

    @Test
    void routeToDlq_metricsPresent_incrementsHistoryDlqRoutedCount() {
        DlqPublisher publisher = mock(DlqPublisher.class);
        when(publisher.publish(any(Message.class), anyString(), any(DlqReason.class), anyString(), anyString()))
                .thenReturn(DlqPublishOutcome.PUBLISHED_JETSTREAM);
        NatsChannelMetrics metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        HistoryDlqConsumer consumer = new HistoryDlqConsumer(publisher, metrics);
        Message msg = mock(Message.class);
        when(msg.getSubject()).thenReturn("history.camunda.OP_LOG.proc-1");

        consumer.routeToDlq(msg, DlqReason.HISTORY_SCHEMA_DRIFT);

        assertThat(metrics.historyDlqRoutedCount("OP_LOG", DlqReason.HISTORY_SCHEMA_DRIFT.headerValue()).count())
                .isEqualTo(1.0);
    }
}
