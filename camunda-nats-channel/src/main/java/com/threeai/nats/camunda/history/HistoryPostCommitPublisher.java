package com.threeai.nats.camunda.history;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.util.Map;

import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.JetStream;
import io.nats.client.impl.NatsMessage;
import org.camunda.bpm.engine.impl.history.event.HistoryEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bulk yol, sıfır DB yazımı (BR-HDL-004, ADR-0010, `03_classes/1_handler_outbox.md` §3). Registered
 * via {@code TransactionContext.addTransactionListener(TransactionState.COMMITTED, ...)} from
 * {@link NatsHistoryEventHandler#handleEvent} — basamak-1 post-commit {@code TransactionListener}
 * deseni ([07§4], {@code A2PostCommitPublisher}) birebir yeniden kullanım. Zero DB reads/writes.
 * Publish failure is caught and logged (WARN, basamak-1 {@code EXT_JETSTREAM_PUBLISH_UNAVAILABLE}
 * pattern's history izdüşümü) — CANNOT roll back the already-committed runtime transaction
 * (D-A's conscious bulk-loss acceptance, `01_overview.md` "Phase3'ün devrettiği doğrulamalar #3").
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

            NatsMessage msg = HistoryWireMessageFactory.build(engineId, historyClass, historyEvent.getId(),
                    historyEvent.getEventType(), historyEvent.getProcessInstanceId(), businessKey, fields, largePayload);
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
