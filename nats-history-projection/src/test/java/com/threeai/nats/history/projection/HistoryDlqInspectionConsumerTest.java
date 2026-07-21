package com.threeai.nats.history.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;

import com.threeai.nats.core.headers.DlqHeaders;
import com.threeai.nats.core.resilience.DlqBridgeCircuitBreakerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsJetStreamMetaData;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

/**
 * Phase 5.5 (QA) — {@link HistoryDlqInspectionConsumer} had ZERO test coverage before this class
 * (a 20.8% line-coverage gap, JaCoCo-measured) despite guarding the {@code
 * RES_HISTORY_DLQ_ACCESS_DENIED} class's operational visibility surface (`dlq.history.>`
 * inspection, `09_security/1_transport_authz.md` §2 — subject-ACL is the primary defense, this
 * class is the CB-protected/DP-1-compliant fallback visibility path). A REAL {@link
 * CircuitBreaker} (basamak-1 {@link DlqBridgeCircuitBreakerFactory}, same style as {@code
 * A2IncidentBridgeTest}) is used rather than mocking {@code executeRunnable} — only the NATS
 * {@link Message} I/O boundary is mocked.
 */
class HistoryDlqInspectionConsumerTest {

    @Test
    void onMessage_success_acksAndDoesNotNak() {
        CircuitBreaker cb = DlqBridgeCircuitBreakerFactory.create("cb-history-dlq-inspection-test-1", null);
        HistoryDlqInspectionConsumer consumer = new HistoryDlqInspectionConsumer(cb);
        Message msg = dlqMessage("BUS_HISTORY_DELIVERY_BUDGET_EXCEEDED", 1);

        consumer.onMessage(msg);

        verify(msg).ack();
        verify(msg, never()).nak();
        verify(msg, never()).nakWithDelay(any(Duration.class));
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    /** DP-1/DP-6 (class Javadoc): only routing/reason metadata is logged, payload is never read. */
    @Test
    void onMessage_success_logsOnlyRoutingMetadata_neverReadsPayload() {
        CircuitBreaker cb = DlqBridgeCircuitBreakerFactory.create("cb-history-dlq-inspection-test-2", null);
        HistoryDlqInspectionConsumer consumer = new HistoryDlqInspectionConsumer(cb);
        Message msg = dlqMessage("SYS_PROJECTION_SCHEMA_DRIFT", 1);

        Logger consumerLogger = (Logger) LoggerFactory.getLogger(HistoryDlqInspectionConsumer.class);
        ListAppender<ILoggingEvent> appender = captureLogsAt(consumerLogger, Level.WARN);
        try {
            consumer.onMessage(msg);
        } finally {
            consumerLogger.detachAppender(appender);
        }

        assertThat(appender.list).anyMatch(event -> event.getFormattedMessage().contains("History DLQ message observed"));
        verify(msg, never()).getData(); // never touches the raw payload -- header-derived fields only
    }

    @Test
    void onMessage_processingThrows_naksWithBackoff_doesNotAck() {
        CircuitBreaker cb = DlqBridgeCircuitBreakerFactory.create("cb-history-dlq-inspection-test-3", null);
        HistoryDlqInspectionConsumer consumer = new HistoryDlqInspectionConsumer(cb);
        Message msg = mock(Message.class);
        when(msg.getHeaders()).thenThrow(new RuntimeException("boom")); // forces the generic-Exception catch branch
        NatsJetStreamMetaData metaData = mock(NatsJetStreamMetaData.class);
        when(metaData.deliveredCount()).thenReturn(1L);
        when(msg.metaData()).thenReturn(metaData);

        consumer.onMessage(msg);

        verify(msg).nakWithDelay(any(Duration.class));
        verify(msg, never()).ack();
    }

    @Test
    void onMessage_circuitBreakerOpen_naksFastWithoutProcessing() {
        CircuitBreaker cb = DlqBridgeCircuitBreakerFactory.create("cb-history-dlq-inspection-test-4", null);
        HistoryDlqInspectionConsumer consumer = new HistoryDlqInspectionConsumer(cb);
        // Trip the circuit breaker (5 consecutive failures -> OPEN, basamak-1 ADR-0004 threshold).
        for (int i = 0; i < 5; i++) {
            consumer.onMessage(failingMessage());
        }
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        Message msg = dlqMessage("BUS_HISTORY_DELIVERY_BUDGET_EXCEEDED", 1);
        consumer.onMessage(msg);

        verify(msg).nakWithDelay(any(Duration.class));
        verify(msg, never()).ack();
        verify(msg, never()).getHeaders(); // CB-open short-circuits before the runnable ever executes
    }

    @Test
    void onMessage_deliveryCountFetchFails_defaultsToFirstAttemptBackoff() {
        CircuitBreaker cb = DlqBridgeCircuitBreakerFactory.create("cb-history-dlq-inspection-test-5", null);
        HistoryDlqInspectionConsumer consumer = new HistoryDlqInspectionConsumer(cb);
        Message msg = mock(Message.class);
        when(msg.getHeaders()).thenThrow(new RuntimeException("boom"));
        when(msg.metaData()).thenThrow(new RuntimeException("no jetstream metadata"));

        consumer.onMessage(msg);

        ArgumentCaptor<Duration> backoff = ArgumentCaptor.forClass(Duration.class);
        verify(msg).nakWithDelay(backoff.capture());
        assertThat(backoff.getValue()).isEqualTo(Duration.ofSeconds(1)); // deliveryCount defaults to 1 -> 2^0=1s
    }

    /** Exponential backoff (2^(deliveryCount-1)), capped at 30s -- mirrors A2IncidentBridge's scheme. */
    @Test
    void onMessage_processingFails_backoffGrowsExponentiallyThenCapsAt30Seconds() {
        assertBackoffFor(1, Duration.ofSeconds(1));
        assertBackoffFor(2, Duration.ofSeconds(2));
        assertBackoffFor(3, Duration.ofSeconds(4));
        assertBackoffFor(5, Duration.ofSeconds(16));
        assertBackoffFor(6, Duration.ofSeconds(30)); // 2^5=32s, capped
        assertBackoffFor(20, Duration.ofSeconds(30)); // deep redelivery -- still capped
    }

    private void assertBackoffFor(long deliveryCount, Duration expected) {
        CircuitBreaker cb = DlqBridgeCircuitBreakerFactory.create(
                "cb-history-dlq-inspection-backoff-" + deliveryCount, null);
        HistoryDlqInspectionConsumer consumer = new HistoryDlqInspectionConsumer(cb);
        Message msg = mock(Message.class);
        when(msg.getHeaders()).thenThrow(new RuntimeException("boom"));
        NatsJetStreamMetaData metaData = mock(NatsJetStreamMetaData.class);
        when(metaData.deliveredCount()).thenReturn(deliveryCount);
        when(msg.metaData()).thenReturn(metaData);

        consumer.onMessage(msg);

        ArgumentCaptor<Duration> backoff = ArgumentCaptor.forClass(Duration.class);
        verify(msg).nakWithDelay(backoff.capture());
        assertThat(backoff.getValue()).isEqualTo(expected);
    }

    private Message failingMessage() {
        Message msg = mock(Message.class);
        when(msg.getHeaders()).thenThrow(new RuntimeException("boom"));
        NatsJetStreamMetaData metaData = mock(NatsJetStreamMetaData.class);
        when(metaData.deliveredCount()).thenReturn(1L);
        when(msg.metaData()).thenReturn(metaData);
        return msg;
    }

    private Message dlqMessage(String reason, long deliveryCount) {
        Headers headers = new Headers();
        headers.add(DlqHeaders.REASON, reason);
        headers.add(DlqHeaders.ORIGINAL_SUBJECT, "history.camunda.OP_LOG.proc-1");
        Message msg = mock(Message.class);
        when(msg.getHeaders()).thenReturn(headers);
        NatsJetStreamMetaData metaData = mock(NatsJetStreamMetaData.class);
        when(metaData.deliveredCount()).thenReturn(deliveryCount);
        when(msg.metaData()).thenReturn(metaData);
        return msg;
    }

    private ListAppender<ILoggingEvent> captureLogsAt(Logger logger, Level level) {
        logger.setLevel(level);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        return appender;
    }
}
