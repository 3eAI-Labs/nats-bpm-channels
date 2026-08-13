package com.threeai.nats.core.dlq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsJetStreamMetaData;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.NatsJetStreamConstants;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

class DlqPublisherTest {

    private JetStream jetStream;
    private Connection connection;
    private NatsChannelMetrics metrics;
    private DlqPublisher publisher;

    @BeforeEach
    void setUp() {
        jetStream = mock(JetStream.class);
        connection = mock(Connection.class);
        metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        publisher = new DlqPublisher(jetStream, connection, metrics);
    }

    @Test
    void publish_success_publishesToJetStreamWithPreservedHeadersAndMetaHeaders() throws Exception {
        Headers original = new Headers();
        original.add("X-Cadenzaflow-Trace-Id", "trace-1");
        original.add(NatsJetStreamConstants.MSG_ID_HDR, "task-42");
        Message msg = createMockMessage("payload".getBytes(StandardCharsets.UTF_8), original, "jobs.foo.reply", 5);

        DlqPublishOutcome outcome = publisher.publish(msg, "dlq.jobs.foo", DlqReason.DELIVERY_BUDGET_EXCEEDED,
                "jobs.foo.reply", "foo");

        assertThat(outcome).isEqualTo(DlqPublishOutcome.PUBLISHED_JETSTREAM);
        ArgumentCaptor<NatsMessage> captor = ArgumentCaptor.forClass(NatsMessage.class);
        verify(jetStream).publish(captor.capture());
        NatsMessage published = captor.getValue();
        assertThat(published.getSubject()).isEqualTo("dlq.jobs.foo");
        assertThat(published.getHeaders().getLast("X-Cadenzaflow-Trace-Id")).isEqualTo("trace-1");
        assertThat(published.getHeaders().getLast(NatsJetStreamConstants.MSG_ID_HDR)).isEqualTo("task-42.dlq");
        assertThat(published.getHeaders().getLast("X-Cadenzaflow-Dlq-Original-Subject")).isEqualTo("jobs.foo.reply");
        assertThat(published.getHeaders().getLast("X-Cadenzaflow-Dlq-Delivery-Count")).isEqualTo("5");
        assertThat(published.getHeaders().getLast("X-Cadenzaflow-Dlq-Reason")).isEqualTo("BUS_REPLY_DELIVERY_BUDGET_EXCEEDED");
        assertThat(published.getHeaders().getLast("X-Cadenzaflow-Dlq-Timestamp")).isNotNull();
    }

    @Test
    void publish_missingNatsMsgId_generatesSyntheticIdWithDlqSuffix() throws Exception {
        Message msg = createMockMessage("payload".getBytes(StandardCharsets.UTF_8), new Headers(), "jobs.foo.reply", 1);

        publisher.publish(msg, "dlq.jobs.foo", DlqReason.EMPTY_MESSAGE_BODY, "jobs.foo.reply", "foo");

        ArgumentCaptor<NatsMessage> captor = ArgumentCaptor.forClass(NatsMessage.class);
        verify(jetStream).publish(captor.capture());
        String msgId = captor.getValue().getHeaders().getLast(NatsJetStreamConstants.MSG_ID_HDR);
        assertThat(msgId).startsWith("unknown-").endsWith(".dlq");
    }

    @Test
    void publish_dlqSubjectNull_returnsFailedNoDlqSubject_doesNotPublish() throws Exception {
        Message msg = createMockMessage("payload".getBytes(StandardCharsets.UTF_8), new Headers(), "jobs.foo.reply", 1);

        DlqPublishOutcome outcome = publisher.publish(msg, null, DlqReason.EMPTY_MESSAGE_BODY, "jobs.foo.reply", "foo");

        assertThat(outcome).isEqualTo(DlqPublishOutcome.FAILED_NO_DLQ_SUBJECT);
        verify(jetStream, never()).publish(any(NatsMessage.class));
    }

    @Test
    void publish_jetStreamFails_fallsBackToCoreNats() throws Exception {
        Message msg = createMockMessage("payload".getBytes(StandardCharsets.UTF_8), new Headers(), "jobs.foo.reply", 1);
        when(jetStream.publish(any(NatsMessage.class))).thenThrow(new IOException("JS down"));

        DlqPublishOutcome outcome = publisher.publish(msg, "dlq.jobs.foo", DlqReason.EMPTY_MESSAGE_BODY, "jobs.foo.reply", "foo");

        assertThat(outcome).isEqualTo(DlqPublishOutcome.PUBLISHED_CORE_FALLBACK);
        verify(connection).publish(org.mockito.ArgumentMatchers.eq("dlq.jobs.foo"), any(Headers.class), any(byte[].class));
    }

    @Test
    void publish_bothPublishPathsFail_returnsFailedBothPublish() throws Exception {
        Message msg = createMockMessage("payload".getBytes(StandardCharsets.UTF_8), new Headers(), "jobs.foo.reply", 1);
        when(jetStream.publish(any(NatsMessage.class))).thenThrow(new IOException("JS down"));
        org.mockito.Mockito.doThrow(new RuntimeException("core down"))
                .when(connection).publish(any(String.class), any(Headers.class), any(byte[].class));

        DlqPublishOutcome outcome = publisher.publish(msg, "dlq.jobs.foo", DlqReason.EMPTY_MESSAGE_BODY, "jobs.foo.reply", "foo");

        assertThat(outcome).isEqualTo(DlqPublishOutcome.FAILED_BOTH_PUBLISH);
    }

    /**
     * A core-NATS (non-JetStream) message has no delivery metadata -- {@code msg.metaData()}
     * throws (real jnats {@code NatsMessage} behavior) rather than returning null. {@code
     * deliveryCountOf} must swallow this and fall back to {@code 1}, not propagate.
     */
    @Test
    void publish_originalMessageHasNoJetStreamMetadata_deliveryCountDefaultsToOne() throws Exception {
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn("payload".getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(new Headers());
        when(msg.getSubject()).thenReturn("jobs.foo.reply");
        when(msg.metaData()).thenThrow(new IllegalStateException("Message is not a JetStream message"));

        publisher.publish(msg, "dlq.jobs.foo", DlqReason.EMPTY_MESSAGE_BODY, "jobs.foo.reply", "foo");

        ArgumentCaptor<NatsMessage> captor = ArgumentCaptor.forClass(NatsMessage.class);
        verify(jetStream).publish(captor.capture());
        assertThat(captor.getValue().getHeaders().getLast("X-Cadenzaflow-Dlq-Delivery-Count")).isEqualTo("1");
    }

    private Message createMockMessage(byte[] data, Headers headers, String subject, long deliveryCount) {
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn(data);
        when(msg.getHeaders()).thenReturn(headers);
        when(msg.getSubject()).thenReturn(subject);
        NatsJetStreamMetaData metaData = mock(NatsJetStreamMetaData.class);
        when(metaData.deliveredCount()).thenReturn(deliveryCount);
        when(msg.metaData()).thenReturn(metaData);
        return msg;
    }

    /**
     * A message reaching the DLQ is a message the system refused to process — most often because a
     * worker broke the wire contract. That decision used to produce no log line at all: only
     * failures of the DLQ publish itself were logged, so from the engine's side a stream of
     * contract violations looked exactly like healthy operation.
     *
     * <p>It cost real time. The bench fixture's simulated worker replies with {@code {}}, which has
     * no {@code type} discriminator, so every reply was routed here and no external task ever
     * completed — and the only symptom was a benchmark timing out after sixty seconds of silence.
     *
     * <p>ERROR_REGISTRY rows 4, 5 and 24 all prescribe WARN for these reasons. This asserts the
     * line is emitted and carries enough to identify what was dropped and why.
     */
    @Test
    void publish_success_logsWarnWithReasonAndIdentity() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(DlqPublisher.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            publisher.publish(message("task-7", "{}"), "dlq.jobs.orders",
                    DlqReason.INVALID_REPLY_TYPE, "jobs.orders.reply", "orders");

            assertThat(appender.list)
                    .as("a refused message must not pass silently")
                    .anySatisfy(event -> {
                        assertThat(event.getLevel()).isEqualTo(Level.WARN);
                        assertThat(event.getFormattedMessage()).contains("DLQ");
                        String args = String.valueOf(java.util.Arrays.toString(event.getArgumentArray()));
                        assertThat(args).contains("INVALID_REPLY_TYPE");
                        assertThat(args).contains("jobs.orders.reply");
                        assertThat(args).contains("task-7");
                    });
        } finally {
            logger.detachAppender(appender);
        }
    }

    private Message message(String msgId, String body) {
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn(body.getBytes(StandardCharsets.UTF_8));
        Headers headers = new Headers();
        headers.add(NatsJetStreamConstants.MSG_ID_HDR, msgId);
        when(msg.getHeaders()).thenReturn(headers);
        when(msg.getSubject()).thenReturn("jobs.orders.reply");
        NatsJetStreamMetaData metaData = mock(NatsJetStreamMetaData.class);
        when(metaData.deliveredCount()).thenReturn(1L);
        when(msg.metaData()).thenReturn(metaData);
        return msg;
    }
}
