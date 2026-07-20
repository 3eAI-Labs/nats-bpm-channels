package com.threeai.nats.history.projection;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.time.Duration;

import com.threeai.nats.core.headers.DlqHeaders;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.nats.client.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ops-only inspection consumer for {@code dlq.history.>} (`03_classes/2_relay_projection.md` §4).
 * {@code RES_HISTORY_DLQ_ACCESS_DENIED} is enforced at the subject-ACL layer
 * (`09_security/1_transport_authz.md` §2, primary defense) — this class does not implement authz
 * itself, only metrics/visibility (US-E2). CB-protected (basamak-1
 * {@code DlqBridgeCircuitBreakerFactory} reused, {@code cb-history-dlq-inspection}).
 */
public class HistoryDlqInspectionConsumer {

    private static final Logger log = LoggerFactory.getLogger(HistoryDlqInspectionConsumer.class);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    private final CircuitBreaker circuitBreaker;

    public HistoryDlqInspectionConsumer(CircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
    }

    public void onMessage(Message dlqMsg) {
        try {
            circuitBreaker.executeRunnable(() -> logDlqVisibility(dlqMsg));
            dlqMsg.ack();
        } catch (CallNotPermittedException cbOpen) {
            dlqMsg.nakWithDelay(calculateBackoff(deliveryCountOf(dlqMsg)));
        } catch (Exception e) {
            log.error("History-DLQ inspection processing failed", e); // SYS_DLQ_BRIDGE_PROCESSING_FAILED-equivalent
            dlqMsg.nakWithDelay(calculateBackoff(deliveryCountOf(dlqMsg)));
        }
    }

    /** DP-1/DP-6: only routing/reason metadata is logged — never payload or business-key values. */
    private void logDlqVisibility(Message dlqMsg) {
        String reason = dlqMsg.getHeaders() != null ? dlqMsg.getHeaders().getLast(DlqHeaders.REASON) : "UNKNOWN";
        String originalSubject = dlqMsg.getHeaders() != null ? dlqMsg.getHeaders().getLast(DlqHeaders.ORIGINAL_SUBJECT) : null;
        log.warn("History DLQ message observed", kv("original_subject", originalSubject), kv("reason", reason));
    }

    private long deliveryCountOf(Message msg) {
        try {
            return msg.metaData().deliveredCount();
        } catch (Exception e) {
            return 1;
        }
    }

    private Duration calculateBackoff(long deliveryCount) {
        long seconds = (long) Math.pow(2, Math.max(0, deliveryCount - 1));
        Duration backoff = Duration.ofSeconds(seconds);
        return backoff.compareTo(MAX_BACKOFF) > 0 ? MAX_BACKOFF : backoff;
    }
}
