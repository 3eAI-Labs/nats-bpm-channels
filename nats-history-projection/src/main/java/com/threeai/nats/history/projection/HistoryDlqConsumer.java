package com.threeai.nats.history.projection;

import com.threeai.nats.core.dlq.DlqPublishOutcome;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.dlq.DlqReason;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.Message;

/**
 * {@code deliveryCount>maxDeliver} escalation target from {@link HistoryProjectionConsumer} AND
 * schema-drift escalation (BR-REL-005, ADR-0013/0019+0004, `03_classes/2_relay_projection.md` §4).
 * Reuses {@code nats-core DlqPublisher.publish(...)} (basamak-1 [07§4] asset, byte-mirror +
 * {@code Nats-Msg-Id=<original>.dlq} + custody-transfer).
 */
public class HistoryDlqConsumer {

    private static final String DLQ_SUBJECT_PREFIX = "dlq.";

    private final DlqPublisher dlqPublisher;
    private final NatsChannelMetrics metrics;

    public HistoryDlqConsumer(DlqPublisher dlqPublisher, NatsChannelMetrics metrics) {
        this.dlqPublisher = dlqPublisher;
        this.metrics = metrics;
    }

    public DlqPublishOutcome routeToDlq(Message originalMsg, DlqReason reason) {
        String originalSubject = originalMsg.getSubject();
        String dlqSubject = DLQ_SUBJECT_PREFIX + originalSubject; // dlq.history.<engineId>.<class>.<processInstanceId>
        String historyClassTag = extractHistoryClassTag(originalSubject);

        DlqPublishOutcome outcome = dlqPublisher.publish(originalMsg, dlqSubject, reason, originalSubject, historyClassTag);
        if (metrics != null) {
            metrics.historyDlqRoutedCount(historyClassTag, reason.headerValue()).increment();
        }
        return outcome;
    }

    /** Best-effort routing/metric tag extraction from {@code history.<engineId>.<class>.<processInstanceId>}. */
    private static String extractHistoryClassTag(String subject) {
        String[] parts = subject.split("\\.");
        return parts.length >= 3 ? parts[2] : "UNKNOWN";
    }
}
