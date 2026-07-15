package com.threeai.nats.core.dlq;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

import com.threeai.nats.core.headers.DlqHeaders;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.NatsJetStreamConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single common {@code publishToDlq} implementation, replacing the three near-identical private
 * methods previously duplicated in the flowable/camunda/cadenzaflow inbound adapters (contract-fix
 * #1/#2/#3, LLD 03_classes/1_nats_core_common.md §2). Preserves the original message's headers
 * verbatim, appends DLQ meta-headers, and derives a dedup-safe {@code Nats-Msg-Id} for the DLQ
 * copy — but never acks/naks the original message itself; that custody-transfer decision is
 * returned as a {@link DlqPublishOutcome} for the caller to apply.
 */
public class DlqPublisher {

    private static final Logger log = LoggerFactory.getLogger(DlqPublisher.class);

    private final JetStream jetStream;
    private final Connection coreConnection;
    private final NatsChannelMetrics metrics;

    public DlqPublisher(JetStream jetStream, Connection coreConnection, NatsChannelMetrics metrics) {
        this.jetStream = jetStream;
        this.coreConnection = coreConnection;
        this.metrics = metrics;
    }

    /**
     * BR-SUB-001 (header preservation) + BR-SUB-003 (dedup id) + BR-SUB-002 (custody-transfer
     * decision returned, no ack/nak performed here).
     *
     * @param originalMsg the original message being routed to the DLQ (job/reply/event)
     * @param dlqSubject  destination DLQ subject ({@code null} means missing configuration, row 4)
     * @param reason      DLQ reason (class/code only — DP-6)
     * @param subjectTag  metric tag (original subject)
     * @param channelTag  metric tag (channel/messageName)
     */
    public DlqPublishOutcome publish(Message originalMsg, String dlqSubject,
            DlqReason reason, String subjectTag, String channelTag) {

        if (dlqSubject == null) {
            log.warn("DLQ subject not configured — message will be NAKed, not discarded",
                    kv("subject", subjectTag), kv("channel", channelTag));
            if (metrics != null) {
                metrics.dlqPublishFailureCount(subjectTag, channelTag).increment();
            }
            return DlqPublishOutcome.FAILED_NO_DLQ_SUBJECT;
        }

        Headers dlqHeaders = copyOriginalHeadersVerbatim(originalMsg.getHeaders());
        appendMetaHeaders(dlqHeaders, originalMsg.getSubject(), deliveryCountOf(originalMsg), reason, Instant.now());
        String originalMsgId = extractOriginalMsgId(originalMsg, subjectTag);
        dlqHeaders.put(NatsJetStreamConstants.MSG_ID_HDR, originalMsgId + ".dlq");

        NatsMessage dlqMsg = NatsMessage.builder()
                .subject(dlqSubject)
                .data(originalMsg.getData())
                .headers(dlqHeaders)
                .build();

        try {
            jetStream.publish(dlqMsg);
            if (metrics != null) {
                metrics.dlqCount(subjectTag, channelTag).increment();
            }
            return DlqPublishOutcome.PUBLISHED_JETSTREAM;
        } catch (Exception jsEx) {
            log.warn("JetStream DLQ publish failed, falling back to core NATS",
                    kv("subject", subjectTag), kv("dlq_subject", dlqSubject), jsEx);
            try {
                coreConnection.publish(dlqMsg.getSubject(), dlqMsg.getHeaders(), dlqMsg.getData());
                if (metrics != null) {
                    metrics.dlqCount(subjectTag, channelTag).increment();
                }
                return DlqPublishOutcome.PUBLISHED_CORE_FALLBACK;
            } catch (Exception fallbackEx) {
                log.error("DLQ publish failed on both JetStream and core NATS",
                        kv("subject", subjectTag), kv("dlq_subject", dlqSubject), fallbackEx);
                if (metrics != null) {
                    metrics.dlqPublishFailureCount(subjectTag, channelTag).increment();
                }
                return DlqPublishOutcome.FAILED_BOTH_PUBLISH;
            }
        }
    }

    private Headers copyOriginalHeadersVerbatim(Headers original) {
        return original != null ? new Headers(original) : new Headers();
    }

    private void appendMetaHeaders(Headers h, String originalSubject, long deliveryCount,
            DlqReason reason, Instant now) {
        h.add(DlqHeaders.ORIGINAL_SUBJECT, originalSubject);
        h.add(DlqHeaders.DELIVERY_COUNT, String.valueOf(deliveryCount));
        h.add(DlqHeaders.REASON, reason.headerValue());
        h.add(DlqHeaders.TIMESTAMP, DateTimeFormatter.ISO_INSTANT.format(now));
    }

    private long deliveryCountOf(Message msg) {
        try {
            return msg.metaData().deliveredCount();
        } catch (Exception e) {
            return 1;
        }
    }

    private String extractOriginalMsgId(Message msg, String subjectTag) {
        Headers headers = msg.getHeaders();
        String existing = headers != null ? lastValue(headers.get(NatsJetStreamConstants.MSG_ID_HDR)) : null;
        if (existing != null) {
            return existing;
        }
        String fallback = "unknown-" + UUID.randomUUID();
        log.warn("Original message has no Nats-Msg-Id header — generating a synthetic id for the DLQ copy",
                kv("subject", subjectTag), kv("synthetic_id", fallback));
        return fallback;
    }

    private String lastValue(List<String> values) {
        return values == null || values.isEmpty() ? null : values.get(values.size() - 1);
    }
}
