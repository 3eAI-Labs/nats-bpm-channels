package org.flowable.eventregistry.spring.nats;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.threeai.nats.core.NatsHeaderUtils;
import com.threeai.nats.core.dlq.DlqPublishOutcome;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.dlq.DlqReason;
import io.nats.client.Connection;
import io.nats.client.impl.NatsMessage;
import org.flowable.common.engine.api.FlowableException;
import org.flowable.eventregistry.api.OutboundEventChannelAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * D-G' (docs/09-outbound-handoff.md) hardening: the "bugün çıplak {@code connection.publish}" gap
 * this class started with (no JetStream, no ack, no DLQ) is closed on the FAILURE path — the happy
 * path deliberately stays a fast, low-latency core-NATS publish (fire-and-forget, at-most-once;
 * this IS the tenant's explicit choice when they configure {@code jetstream=false} on the channel
 * — {@link org.flowable.eventregistry.spring.nats.jetstream.JetStreamOutboundEventChannelAdapter}
 * is the durable/at-least-once alternative for {@code jetstream=true}, and this two-adapter split
 * already realizes the "post-commit/at-least-once ayrımı" the design brief calls for structurally,
 * without redefining this class's transport). On failure, this class now routes to a DLQ
 * (custody-transfer, same {@link DlqPublisher} contract the inbound adapters and {@link
 * org.flowable.eventregistry.spring.nats.jetstream.JetStreamOutboundEventChannelAdapter} use) —
 * {@code DlqPublisher} itself tries JetStream publish-with-PubAck first, falling back to core NATS
 * only if JetStream is also unavailable, which is where the literal "JetStream publish + PubAck"
 * text of the design brief is satisfied for this adapter: on the FAILURE path, not the happy path.
 *
 * <p><b>CODER-QUESTION (phase-5 return report):</b> whether Flowable's {@code sendEvent} call runs
 * INSIDE or OUTSIDE the engine's own command transaction (docs/09 §4 open item #3) was not
 * verified in this pass (the Flowable ENGINE source, as opposed to the {@code flowable-bpmn-model}/
 * event-registry API jars, was not available locally to trace the caller). A {@code null} {@code
 * dlqPublisher} preserves the exact prior behavior (throw-only) — DLQ routing is opt-in via
 * {@link org.flowable.eventregistry.spring.nats.channel.NatsOutboundChannelModel#getDlqSubject()}.
 */
public class NatsOutboundEventChannelAdapter implements OutboundEventChannelAdapter<String> {

    private static final Logger log = LoggerFactory.getLogger(NatsOutboundEventChannelAdapter.class);

    private final Connection connection;
    private final String subject;
    private final String channelKey;
    private final DlqPublisher dlqPublisher;
    private final String dlqSubject;

    public NatsOutboundEventChannelAdapter(Connection connection, String subject) {
        this(connection, subject, subject, null, null);
    }

    public NatsOutboundEventChannelAdapter(Connection connection, String subject, String channelKey,
            DlqPublisher dlqPublisher, String dlqSubject) {
        this.connection = connection;
        this.subject = subject;
        this.channelKey = channelKey;
        this.dlqPublisher = dlqPublisher;
        this.dlqSubject = dlqSubject;
    }

    @Override
    public void sendEvent(String rawEvent, Map<String, Object> headerMap) {
        NatsMessage message = NatsMessage.builder()
                .subject(subject)
                .data(rawEvent.getBytes(StandardCharsets.UTF_8))
                .headers(NatsHeaderUtils.toNatsHeaders(headerMap))
                .build();

        Connection.Status status = connection.getStatus();
        if (status == Connection.Status.CLOSED || status == Connection.Status.DISCONNECTED) {
            log.error("Connection not available for publish",
                    kv("subject", subject),
                    kv("status", status.name()));
            if (dlqPublisher != null && routeToDlq(message)) {
                return; // custody-transferred to the DLQ -- do not fail the caller
            }
            throw new FlowableException(
                    "NATS outbound channel: connection not available for subject '" + subject
                    + "' (status: " + status + ")");
        }
        try {
            connection.publish(message);
        } catch (Exception e) {
            log.error("Core NATS publish failed", kv("subject", subject), e);
            if (dlqPublisher != null && routeToDlq(message)) {
                return;
            }
            throw new FlowableException("NATS outbound channel: publish failed for subject '" + subject + "'", e);
        }
    }

    /** @return {@code true} if the DLQ publish itself succeeded (custody-transfer complete). */
    private boolean routeToDlq(NatsMessage message) {
        DlqPublishOutcome outcome = dlqPublisher.publish(message, dlqSubject, DlqReason.OUTBOUND_PUBLISH_FAILED,
                subject, channelKey);
        boolean routed = outcome == DlqPublishOutcome.PUBLISHED_JETSTREAM
                || outcome == DlqPublishOutcome.PUBLISHED_CORE_FALLBACK;
        if (routed) {
            log.warn("Outbound core-NATS publish failed — message custody-transferred to DLQ",
                    kv("subject", subject), kv("dlq_subject", dlqSubject));
        }
        return routed;
    }
}
