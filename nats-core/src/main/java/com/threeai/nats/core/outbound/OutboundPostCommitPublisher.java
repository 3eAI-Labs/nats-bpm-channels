package com.threeai.nats.core.outbound;

import static net.logstash.logback.argument.StructuredArguments.kv;

import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.JetStream;
import io.nats.client.impl.NatsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Best-effort/at-most-once outbound publish path (docs/09-outbound-handoff.md D-A'/D-C') —
 * registered via {@code TransactionContext.addTransactionListener(TransactionState.COMMITTED,
 * ...)} from the per-engine {@code NatsOutboundPublisher} ExecutionListener. This is the THIRD use
 * of the basamak-1/2 post-commit {@code TransactionListener} pattern ({@code
 * A2ExternalTaskBehavior:71-74} / {@code HistoryPostCommitPublisher}) — publish failure is caught
 * and logged (WARN) — it CANNOT roll back the already-committed runtime transaction (same
 * conscious at-most-once loss acceptance as the two prior uses).
 *
 * <p>Engine-neutral (no camunda/cadenzaflow-fork types) — lives once in {@code nats-core}, reused
 * directly by every engine module's {@code NatsOutboundPublisher} (mirrors {@link
 * OutboundMessageOutboxWriter}'s placement rationale).
 */
public class OutboundPostCommitPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboundPostCommitPublisher.class);

    private final JetStream jetStream;
    private final NatsChannelMetrics metrics;

    public OutboundPostCommitPublisher(JetStream jetStream, NatsChannelMetrics metrics) {
        this.jetStream = jetStream;
        this.metrics = metrics;
    }

    public void publish(OutboundMessageDraft draft) {
        try {
            NatsMessage msg = OutboundWireMessageFactory.buildMessage(draft);
            jetStream.publish(msg); // Nats-Msg-Id dedup, freshly minted per attempt (no retry on this path)

            if (metrics != null) {
                metrics.outboundPostCommitPublishedCount(draft.messageType()).increment();
            }
        } catch (Exception e) {
            // Best-effort/at-most-once (D-C') -- WARN only, cannot roll back the already-committed
            // engine transaction (basamak-1/2 precedent's 3rd use of this exact pattern).
            log.warn("Post-commit outbound publish failed — best-effort message is lost by design (at-most-once, D-C')",
                    kv("message_type", draft.messageType()), kv("subject", draft.subject()),
                    kv("engine_id", draft.engineId()), e);
        }
    }
}
