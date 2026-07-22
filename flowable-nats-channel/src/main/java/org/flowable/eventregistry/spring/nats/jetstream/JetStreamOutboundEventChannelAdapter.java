package org.flowable.eventregistry.spring.nats.jetstream;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.threeai.nats.core.NatsHeaderUtils;
import com.threeai.nats.core.dlq.DlqPublishOutcome;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.dlq.DlqReason;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.JetStream;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.NatsMessage;
import org.flowable.common.engine.api.FlowableException;
import org.flowable.eventregistry.api.OutboundEventChannelAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * D-G' (docs/09-outbound-handoff.md) hardening: on publish failure, routes to a DLQ subject
 * (custody-transfer, same {@link DlqPublisher} contract the inbound adapters already use) instead
 * of only throwing. A {@code null} {@code dlqPublisher} preserves the exact prior behavior
 * (throw-only) — DLQ routing is opt-in via {@link NatsOutboundChannelModel#getDlqSubject()}
 * resolution in {@code NatsChannelDefinitionProcessor}, mirroring the inbound adapters' default
 * ({@code "dlq." + subject}) when unconfigured.
 */
public class JetStreamOutboundEventChannelAdapter implements OutboundEventChannelAdapter<String> {

    private static final Logger log = LoggerFactory.getLogger(JetStreamOutboundEventChannelAdapter.class);

    private final JetStream jetStream;
    private final String subject;
    private final NatsChannelMetrics metrics;
    private final String channelKey;
    private final DlqPublisher dlqPublisher;
    private final String dlqSubject;

    public JetStreamOutboundEventChannelAdapter(JetStream jetStream, String subject,
            NatsChannelMetrics metrics, String channelKey) {
        this(jetStream, subject, metrics, channelKey, null, null);
    }

    public JetStreamOutboundEventChannelAdapter(JetStream jetStream, String subject,
            NatsChannelMetrics metrics, String channelKey, DlqPublisher dlqPublisher, String dlqSubject) {
        this.jetStream = jetStream;
        this.subject = subject;
        this.metrics = metrics;
        this.channelKey = channelKey;
        this.dlqPublisher = dlqPublisher;
        this.dlqSubject = dlqSubject;
    }

    @Override
    public void sendEvent(String rawEvent, Map<String, Object> headerMap) {
        byte[] data = rawEvent.getBytes(StandardCharsets.UTF_8);
        NatsMessage message = NatsMessage.builder()
                .subject(subject)
                .data(data)
                .headers(NatsHeaderUtils.toNatsHeaders(headerMap))
                .build();
        try {
            PublishAck ack = jetStream.publish(message);
            if (metrics != null) {
                metrics.jsPublishCount(subject, channelKey).increment();
            }
            log.debug("Published to JetStream",
                    kv("channel", channelKey),
                    kv("subject", subject),
                    kv("stream_seq", ack.getSeqno()));
        } catch (Exception e) {
            if (metrics != null) {
                metrics.jsPublishErrorCount(subject, channelKey).increment();
            }
            log.error("JetStream publish failed",
                    kv("channel", channelKey),
                    kv("subject", subject), e);
            if (dlqPublisher != null && routeToDlq(message)) {
                return; // custody-transferred to the DLQ -- do not fail the caller
            }
            throw new FlowableException(
                    "JetStream publish failed for channel '" + channelKey
                    + "' on subject '" + subject + "'", e);
        }
    }

    /** @return {@code true} if the DLQ publish itself succeeded (custody-transfer complete). */
    private boolean routeToDlq(NatsMessage message) {
        DlqPublishOutcome outcome = dlqPublisher.publish(message, dlqSubject, DlqReason.OUTBOUND_PUBLISH_FAILED,
                subject, channelKey);
        boolean routed = outcome == DlqPublishOutcome.PUBLISHED_JETSTREAM
                || outcome == DlqPublishOutcome.PUBLISHED_CORE_FALLBACK;
        if (routed) {
            log.warn("Outbound JetStream publish failed — message custody-transferred to DLQ",
                    kv("channel", channelKey), kv("subject", subject), kv("dlq_subject", dlqSubject));
        }
        return routed;
    }
}
