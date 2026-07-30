package com.threeai.nats.cadenzaflow.history;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.util.Map;

import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.JetStream;
import io.nats.client.impl.NatsMessage;
import org.cadenzaflow.bpm.engine.impl.history.event.HistoryEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bulk path, zero DB writes (BR-HDL-004, ADR-0010). Registered
 * via {@code TransactionContext.addTransactionListener(TransactionState.COMMITTED, ...)} from
 * {@link NatsHistoryEventHandler#handleEvent} — a verbatim reuse of the increment 1 post-commit
 * {@code TransactionListener} pattern (increment 2 design, {@code A2PostCommitPublisher}). Zero DB
 * reads/writes. Publish failure is caught and logged (WARN, the history counterpart of the
 * increment 1 {@code EXT_JETSTREAM_PUBLISH_UNAVAILABLE} pattern) — CANNOT roll back the
 * already-committed runtime transaction (D-A's conscious bulk-loss acceptance, validation
 * deferred by design review, item #3).
 */
public class HistoryPostCommitPublisher {

    private static final Logger log = LoggerFactory.getLogger(HistoryPostCommitPublisher.class);

    private final JetStream jetStream;
    private final NatsChannelMetrics metrics;

    public HistoryPostCommitPublisher(JetStream jetStream, NatsChannelMetrics metrics) {
        this.jetStream = jetStream;
        this.metrics = metrics;
    }

    public void publish(HistoryEvent historyEvent, String historyClass, String engineId) {
        String subject = "history." + engineId + "." + historyClass + "." + historyEvent.getProcessInstanceId();
        try {
            Map<String, Object> fields = HistoryEventFieldExtractor.extractFields(historyEvent);
            String businessKey = HistoryEventFieldExtractor.businessKeyOf(historyEvent);
            byte[] largePayload = HistoryEventFieldExtractor.extractLargePayload(historyEvent, historyClass).orElse(null);
            java.time.Instant eventTime = HistoryEventFieldExtractor.eventTimeOf(historyEvent);

            NatsMessage msg = HistoryWireMessageFactory.build(engineId, historyClass, historyEvent.getId(),
                    historyEvent.getEventType(), historyEvent.getProcessInstanceId(), businessKey, fields,
                    largePayload, eventTime);
            jetStream.publish(msg); // Nats-Msg-Id dedup, same subject/dedup schema as the relay path (NFR-M3)

            if (metrics != null) {
                metrics.historyPostCommitPublishedCount(historyClass).increment();
            }
        } catch (Exception e) {
            // EXT_JETSTREAM_PUBLISH_UNAVAILABLE-equivalent -- WARN only, cannot roll back the
            // already-committed transaction (D-A at-most-once bulk-loss, conscious acceptance,
            // detected later by ReconciliationJob's diff, not restored).
            log.warn("Post-commit history publish failed — bulk event is lost by design (at-most-once, D-A); "
                    + "reconciliation will surface the gap",
                    kv("history_class", historyClass), kv("subject", subject), kv("engine_id", engineId), e);
        }
    }
}
