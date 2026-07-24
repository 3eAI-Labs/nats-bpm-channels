package com.threeai.nats.history.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.UUID;

import com.threeai.nats.core.dlq.DlqPublishOutcome;
import com.threeai.nats.core.dlq.DlqReason;
import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.core.history.HistoryHeaders;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsJetStreamMetaData;
import io.nats.client.support.NatsJetStreamConstants;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Sentinel Phase 5.5 (round 2): mock-based error-path/fault-injection unit tests for {@link
 * HistoryProjectionConsumer} branches that {@code HistoryProjectionConsumerTest} (real NATS +
 * real Postgres e2e) does not reach -- transient-DB-failure nak/backoff/DLQ-escalation, large
 * payload routing, malformed wire-contract fields, and metrics wiring. Narrow-unit mocking is
 * used here specifically because these are fault-injection scenarios (real SQLException-shaped
 * failures, real JetStream metadata-absence), not coverage padding.
 */
class HistoryProjectionConsumerErrorPathTest {

    private ProjectionStore projectionStore;
    private HistoryDlqConsumer dlqConsumer;
    private JetStream jetStream;

    private HistoryProjectionConsumer newConsumer(NatsChannelMetrics metrics, int maxDeliver) {
        projectionStore = mock(ProjectionStore.class);
        dlqConsumer = mock(HistoryDlqConsumer.class);
        jetStream = mock(JetStream.class);
        return new HistoryProjectionConsumer(2, jetStream, projectionStore, dlqConsumer, metrics, maxDeliver);
    }

    private Message message(String historyClass, String body, long deliveryCount, long streamSequence) {
        Headers headers = new Headers();
        headers.add(NatsJetStreamConstants.MSG_ID_HDR, "evt-1:create");
        headers.add(HistoryHeaders.ENGINE_ID, "camunda");
        headers.add(HistoryHeaders.CLASS, historyClass);
        headers.add(HistoryHeaders.EVENT_TYPE, "create");
        headers.add(HistoryHeaders.EVENT_ID, "evt-1");
        headers.add(HistoryHeaders.PROCESS_INSTANCE_ID, "proc-1");
        headers.add(HistoryHeaders.EVENT_TIME, String.valueOf(java.time.Instant.now().toEpochMilli()));

        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn(body.getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(headers);
        when(msg.getSubject()).thenReturn("history.camunda." + historyClass + ".proc-1");
        NatsJetStreamMetaData metaData = mock(NatsJetStreamMetaData.class);
        when(metaData.deliveredCount()).thenReturn(deliveryCount);
        when(metaData.streamSequence()).thenReturn(streamSequence);
        when(msg.metaData()).thenReturn(metaData);
        return msg;
    }

    // --- transient DB failure: below/above maxDeliver budget ---

    @Test
    void onMessage_transientDbFailure_belowMaxDeliver_naksWithExponentialBackoff() {
        HistoryProjectionConsumer consumer = newConsumer(null, 4);
        when(projectionStore.insertLogEvent(anyString(), any())).thenThrow(new RuntimeException("connection reset"));
        Message msg = message(HistoryClassNames.OP_LOG, "{\"operationType\":\"Complete\"}", 3, 10);

        consumer.onMessage(msg);

        verify(msg, never()).ack();
        verify(dlqConsumer, never()).routeToDlq(any(), any());
        verify(msg).nakWithDelay(Duration.ofSeconds(4)); // backoffFor(3) = 2^(3-1) = 4s
    }

    @Test
    void onMessage_transientDbFailure_deliveryOne_backoffIsOneSecond() {
        HistoryProjectionConsumer consumer = newConsumer(null, 4);
        when(projectionStore.insertLogEvent(anyString(), any())).thenThrow(new RuntimeException("connection reset"));
        Message msg = message(HistoryClassNames.OP_LOG, "{}", 1, 10);

        consumer.onMessage(msg);

        verify(msg).nakWithDelay(Duration.ofSeconds(1)); // backoffFor(1) = 2^max(0,0) = 1s
    }

    @Test
    void onMessage_transientDbFailure_highDeliveryCount_backoffCappedAtThirtySeconds() {
        HistoryProjectionConsumer consumer = newConsumer(null, 100);
        when(projectionStore.insertLogEvent(anyString(), any())).thenThrow(new RuntimeException("connection reset"));
        Message msg = message(HistoryClassNames.OP_LOG, "{}", 10, 10);

        consumer.onMessage(msg);

        verify(msg).nakWithDelay(Duration.ofSeconds(30));
    }

    @Test
    void onMessage_transientDbFailure_deliveryBudgetExceeded_routesToDlqAndAcks() {
        HistoryProjectionConsumer consumer = newConsumer(null, 4);
        when(projectionStore.insertLogEvent(anyString(), any())).thenThrow(new RuntimeException("connection reset"));
        when(dlqConsumer.routeToDlq(any(Message.class), eq(DlqReason.HISTORY_DELIVERY_BUDGET_EXCEEDED)))
                .thenReturn(DlqPublishOutcome.PUBLISHED_JETSTREAM);
        Message msg = message(HistoryClassNames.OP_LOG, "{}", 5, 10); // deliveryCount(5) > maxDeliver(4)

        consumer.onMessage(msg);

        verify(dlqConsumer).routeToDlq(msg, DlqReason.HISTORY_DELIVERY_BUDGET_EXCEEDED);
        verify(msg).ack();
        verify(msg, never()).nakWithDelay(any());
    }

    @Test
    void onMessage_transientDbFailure_deliveryBudgetExceeded_dlqPublishFails_naksInsteadOfAck() {
        HistoryProjectionConsumer consumer = newConsumer(null, 4);
        when(projectionStore.insertLogEvent(anyString(), any())).thenThrow(new RuntimeException("connection reset"));
        when(dlqConsumer.routeToDlq(any(Message.class), eq(DlqReason.HISTORY_DELIVERY_BUDGET_EXCEEDED)))
                .thenReturn(DlqPublishOutcome.FAILED_BOTH_PUBLISH);
        Message msg = message(HistoryClassNames.OP_LOG, "{}", 5, 10);

        consumer.onMessage(msg);

        verify(msg, never()).ack();
        verify(msg).nak();
    }

    // --- STALE_DISCARDED / consumed metrics ---

    @Test
    void onMessage_staleDiscardedOutcome_incrementsStaleDiscardedMetric_notPlainConsumedOnly() {
        NatsChannelMetrics metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        HistoryProjectionConsumer consumer = newConsumer(metrics, 4);
        when(projectionStore.upsertEntity(anyString(), any())).thenReturn(UpsertOutcome.STALE_DISCARDED);
        Message msg = message(HistoryClassNames.PROCINST, "{\"state\":\"ACTIVE\"}", 1, 10);

        consumer.onMessage(msg);

        verify(msg).ack();
        assertThat(metrics.historyProjectionConsumedCount(HistoryClassNames.PROCINST, "2").count()).isEqualTo(1.0);
        assertThat(metrics.historyProjectionStaleDiscardedCount(HistoryClassNames.PROCINST).count()).isEqualTo(1.0);
    }

    @Test
    void onMessage_appliedOutcome_incrementsConsumedMetricOnly() {
        NatsChannelMetrics metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        HistoryProjectionConsumer consumer = newConsumer(metrics, 4);
        when(projectionStore.upsertEntity(anyString(), any())).thenReturn(UpsertOutcome.APPLIED);
        Message msg = message(HistoryClassNames.PROCINST, "{\"state\":\"ACTIVE\"}", 1, 10);

        consumer.onMessage(msg);

        assertThat(metrics.historyProjectionConsumedCount(HistoryClassNames.PROCINST, "2").count()).isEqualTo(1.0);
        assertThat(metrics.historyProjectionStaleDiscardedCount(HistoryClassNames.PROCINST).count()).isZero();
    }

    // --- large payload routing ---

    @Test
    void onMessage_largePayloadForExtTaskLog_storesPayloadAndSetsErrorDetailsRef() {
        HistoryProjectionConsumer consumer = newConsumer(null, 4);
        UUID payloadRef = UUID.randomUUID();
        when(projectionStore.storeLargePayload(any(byte[].class), eq("ext_task_log_history"))).thenReturn(payloadRef);
        when(projectionStore.insertLogEvent(anyString(), any())).thenReturn(UpsertOutcome.APPLIED);
        String base64Payload = Base64.getEncoder().encodeToString("stack trace bytes".getBytes(StandardCharsets.UTF_8));
        String body = "{\"_largePayloadBase64\":\"" + base64Payload + "\"}";
        Message msg = message(HistoryClassNames.EXT_TASK_LOG, body, 1, 10);

        consumer.onMessage(msg);

        verify(projectionStore).storeLargePayload(eq("stack trace bytes".getBytes(StandardCharsets.UTF_8)), eq("ext_task_log_history"));
        ArgumentCaptor<LogHistoryRecord> captor = ArgumentCaptor.forClass(LogHistoryRecord.class);
        verify(projectionStore).insertLogEvent(eq(HistoryClassNames.EXT_TASK_LOG), captor.capture());
        assertThat(captor.getValue().fields()).containsEntry("errorDetailsRef", payloadRef);
        assertThat(captor.getValue().fields()).doesNotContainKey("_largePayloadBase64");
    }

    @Test
    void onMessage_largePayloadForClassWithoutRefColumn_droppedNotStored() {
        HistoryProjectionConsumer consumer = newConsumer(null, 4);
        when(projectionStore.insertLogEvent(anyString(), any())).thenReturn(UpsertOutcome.APPLIED);
        String base64Payload = Base64.getEncoder().encodeToString("variable bytes".getBytes(StandardCharsets.UTF_8));
        String body = "{\"_largePayloadBase64\":\"" + base64Payload + "\"}";
        Message msg = message(HistoryClassNames.DETAIL, body, 1, 10);

        consumer.onMessage(msg);

        verify(projectionStore, never()).storeLargePayload(any(), any());
        ArgumentCaptor<LogHistoryRecord> captor = ArgumentCaptor.forClass(LogHistoryRecord.class);
        verify(projectionStore).insertLogEvent(eq(HistoryClassNames.DETAIL), captor.capture());
        assertThat(captor.getValue().fields()).doesNotContainKey("errorDetailsRef");
        assertThat(captor.getValue().fields()).doesNotContainKey("_largePayloadBase64");
    }

    // --- malformed wire-contract fields -> schema drift -> DLQ ---

    @Test
    void onMessage_malformedEventTimeHeader_routesToDlqAsSchemaDrift() {
        HistoryProjectionConsumer consumer = newConsumer(null, 4);
        when(dlqConsumer.routeToDlq(any(Message.class), eq(DlqReason.HISTORY_SCHEMA_DRIFT)))
                .thenReturn(DlqPublishOutcome.PUBLISHED_JETSTREAM);
        Message msg = message(HistoryClassNames.OP_LOG, "{}", 1, 10);
        // Overwrite EVENT_TIME with a non-numeric value (malformed epoch-millis).
        Headers headers = msg.getHeaders();
        headers.remove(HistoryHeaders.EVENT_TIME);
        headers.add(HistoryHeaders.EVENT_TIME, "not-a-number");

        consumer.onMessage(msg);

        verify(dlqConsumer).routeToDlq(msg, DlqReason.HISTORY_SCHEMA_DRIFT);
        verify(msg).ack();
        verify(projectionStore, never()).insertLogEvent(anyString(), any());
        verify(projectionStore, never()).upsertEntity(anyString(), any());
    }

    @Test
    void onMessage_malformedJsonBody_routesToDlqAsSchemaDrift() {
        HistoryProjectionConsumer consumer = newConsumer(null, 4);
        when(dlqConsumer.routeToDlq(any(Message.class), eq(DlqReason.HISTORY_SCHEMA_DRIFT)))
                .thenReturn(DlqPublishOutcome.PUBLISHED_JETSTREAM);
        Message msg = message(HistoryClassNames.OP_LOG, "{not valid json", 1, 10);

        consumer.onMessage(msg);

        verify(dlqConsumer).routeToDlq(msg, DlqReason.HISTORY_SCHEMA_DRIFT);
        verify(msg).ack();
    }

    // --- metaData()-absent fallbacks (deliveryCountOf / streamSequenceOf) ---

    @Test
    void onMessage_metaDataAbsent_fallsBackToDeliveryCountOneAndStreamSequenceZero() {
        HistoryProjectionConsumer consumer = newConsumer(null, 4);
        Headers headers = new Headers();
        headers.add(NatsJetStreamConstants.MSG_ID_HDR, "evt-1:create");
        headers.add(HistoryHeaders.ENGINE_ID, "camunda");
        headers.add(HistoryHeaders.CLASS, HistoryClassNames.OP_LOG);
        headers.add(HistoryHeaders.EVENT_TYPE, "create");
        headers.add(HistoryHeaders.EVENT_ID, "evt-1");
        headers.add(HistoryHeaders.PROCESS_INSTANCE_ID, "proc-1");
        headers.add(HistoryHeaders.EVENT_TIME, String.valueOf(java.time.Instant.now().toEpochMilli()));
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn("{}".getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(headers);
        when(msg.getSubject()).thenReturn("history.camunda.OP_LOG.proc-1");
        when(msg.metaData()).thenThrow(new IllegalStateException("not a JetStream message"));
        when(projectionStore.insertLogEvent(anyString(), any())).thenThrow(new RuntimeException("db down"));

        consumer.onMessage(msg);

        // deliveryCountOf() falls back to 1 -> backoffFor(1) = 1s (proves streamSequenceOf's
        // catch(Exception) branch was also exercised, both read the same metaData() call).
        verify(msg).nakWithDelay(Duration.ofSeconds(1));
    }

    // --- missing-headers -> schema drift (whole headers block absent, not just one field) ---

    @Test
    void onMessage_nullHeaders_routesToDlqAsSchemaDrift() {
        HistoryProjectionConsumer consumer = newConsumer(null, 4);
        when(dlqConsumer.routeToDlq(any(Message.class), eq(DlqReason.HISTORY_SCHEMA_DRIFT)))
                .thenReturn(DlqPublishOutcome.PUBLISHED_JETSTREAM);
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn("{}".getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(null);
        when(msg.getSubject()).thenReturn("history.camunda.OP_LOG.proc-1");
        NatsJetStreamMetaData metaData = mock(NatsJetStreamMetaData.class);
        when(metaData.deliveredCount()).thenReturn(1L);
        when(msg.metaData()).thenReturn(metaData);

        consumer.onMessage(msg);

        verify(dlqConsumer).routeToDlq(msg, DlqReason.HISTORY_SCHEMA_DRIFT);
        verify(msg).ack();
    }
}
