package com.threeai.nats.core.jetstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.ServerInfo;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.api.SubjectTransform;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

class JetStreamStreamManagerTest {

    private Connection connection;
    private JetStreamManagement jsm;
    private JetStreamStreamManager manager;

    @BeforeEach
    void setUp() throws IOException, JetStreamApiException {
        connection = mock(Connection.class);
        jsm = mock(JetStreamManagement.class);
        when(connection.jetStreamManagement()).thenReturn(jsm);
        manager = new JetStreamStreamManager();
    }

    @Test
    void ensureStream_exists_noAction() throws Exception {
        when(jsm.getStreamInfo("ORDERS")).thenReturn(mock(StreamInfo.class));

        assertThatCode(() -> manager.ensureStream("ORDERS", "order.>", connection))
                .doesNotThrowAnyException();

        verify(jsm, never()).addStream(any(StreamConfiguration.class));
    }

    @Test
    void ensureStream_notFound_creates() throws Exception {
        JetStreamApiException notFound = mock(JetStreamApiException.class);
        when(notFound.getErrorCode()).thenReturn(404);
        when(jsm.getStreamInfo("ORDERS")).thenThrow(notFound);

        manager.ensureStream("ORDERS", "order.>", connection);

        verify(jsm).addStream(any(StreamConfiguration.class));
    }

    /**
     * The replica count reaches the stream configuration — without this the setting would be
     * accepted, stored and silently dropped, which is the failure mode that produced most of this
     * class's history.
     */
    @Test
    void ensureStream_notFound_appliesConfiguredReplicaCount() throws Exception {
        JetStreamApiException notFound = mock(JetStreamApiException.class);
        when(notFound.getErrorCode()).thenReturn(404);
        when(jsm.getStreamInfo("ORDERS")).thenThrow(notFound);

        new JetStreamStreamManager(3).ensureStream("ORDERS", "order.>", connection);

        ArgumentCaptor<StreamConfiguration> captor = ArgumentCaptor.forClass(StreamConfiguration.class);
        verify(jsm).addStream(captor.capture());
        assertThat(captor.getValue().getReplicas()).isEqualTo(3);
    }

    @Test
    void constructor_rejectsReplicaCountBelowOne() {
        assertThatThrownBy(() -> new JetStreamStreamManager(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be >= 1");
    }

    /**
     * A standalone server reports no cluster name, so the single-replica warning must stay silent
     * — every single-node dev and test environment creates streams this way, and a warning they
     * cannot act on is one they learn to scroll past. The clustered counterpart lives in
     * {@code NatsClusterNodeFailureReliabilityTest}, which needs three real nodes.
     */
    @Test
    void ensureStream_singleReplica_onStandaloneServer_doesNotWarn() throws Exception {
        JetStreamApiException notFound = mock(JetStreamApiException.class);
        when(notFound.getErrorCode()).thenReturn(404);
        when(jsm.getStreamInfo("ORDERS")).thenThrow(notFound);
        ServerInfo standalone = mock(ServerInfo.class);
        when(standalone.getCluster()).thenReturn("");
        when(connection.getServerInfo()).thenReturn(standalone);

        Logger logger = (Logger) LoggerFactory.getLogger(JetStreamStreamManager.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            new JetStreamStreamManager(1).ensureStream("ORDERS", "order.>", connection);
            assertThat(appender.list)
                    .filteredOn(e -> e.getLevel() == Level.WARN)
                    .as("no cluster means no exposure, so no warning")
                    .isEmpty();
        } finally {
            logger.detachAppender(appender);
        }
    }

    @Test
    void ensureStream_apiFails_throwsIllegalStateException() throws Exception {
        JetStreamApiException serverError = mock(JetStreamApiException.class);
        when(serverError.getErrorCode()).thenReturn(500);
        when(jsm.getStreamInfo("ORDERS")).thenThrow(serverError);

        assertThatThrownBy(() -> manager.ensureStream("ORDERS", "order.>", connection))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ORDERS");
    }

    /**
     * QA review fix (item 6, decision 2026-07-15) — non-DLQ subjects keep the
     * pre-existing no-age-limit behavior via the 3-arg convenience overload.
     */
    @Test
    void ensureStream_nonDlqSubject_notFound_createsWithNoMaxAge() throws Exception {
        JetStreamApiException notFound = mock(JetStreamApiException.class);
        when(notFound.getErrorCode()).thenReturn(404);
        when(jsm.getStreamInfo("ORDERS")).thenThrow(notFound);

        manager.ensureStream("ORDERS", "order.>", connection);

        ArgumentCaptor<StreamConfiguration> captor = ArgumentCaptor.forClass(StreamConfiguration.class);
        verify(jsm).addStream(captor.capture());
        assertThat(captor.getValue().getMaxAge()).isEqualTo(Duration.ZERO);
    }

    /**
     * QA review fix (item 6, decision 2026-07-15) — {@code dlq.}-prefixed
     * subjects auto-create with a 14-day retention default (data-classification decision Q3).
     */
    @Test
    void ensureStream_dlqPrefixedSubject_notFound_createsWith14DayMaxAge() throws Exception {
        JetStreamApiException notFound = mock(JetStreamApiException.class);
        when(notFound.getErrorCode()).thenReturn(404);
        when(jsm.getStreamInfo("DLQ")).thenThrow(notFound);

        manager.ensureStream("DLQ", "dlq.>", connection);

        ArgumentCaptor<StreamConfiguration> captor = ArgumentCaptor.forClass(StreamConfiguration.class);
        verify(jsm).addStream(captor.capture());
        assertThat(captor.getValue().getMaxAge()).isEqualTo(Duration.ofDays(14));
    }

    /** Explicit {@code maxAge} argument always wins over the dlq-prefix default. */
    @Test
    void ensureStream_explicitMaxAge_overridesDlqDefault() throws Exception {
        JetStreamApiException notFound = mock(JetStreamApiException.class);
        when(notFound.getErrorCode()).thenReturn(404);
        when(jsm.getStreamInfo("DLQ")).thenThrow(notFound);

        manager.ensureStream("DLQ", "dlq.>", connection, Duration.ofDays(30));

        ArgumentCaptor<StreamConfiguration> captor = ArgumentCaptor.forClass(StreamConfiguration.class);
        verify(jsm).addStream(captor.capture());
        assertThat(captor.getValue().getMaxAge()).isEqualTo(Duration.ofDays(30));
    }

    /** Explicit {@code maxAge} argument also applies to non-DLQ subjects when the caller opts in. */
    @Test
    void ensureStream_explicitMaxAge_appliesToNonDlqSubjectToo() throws Exception {
        JetStreamApiException notFound = mock(JetStreamApiException.class);
        when(notFound.getErrorCode()).thenReturn(404);
        when(jsm.getStreamInfo("ORDERS")).thenThrow(notFound);

        manager.ensureStream("ORDERS", "order.>", connection, Duration.ofDays(7));

        ArgumentCaptor<StreamConfiguration> captor = ArgumentCaptor.forClass(StreamConfiguration.class);
        verify(jsm).addStream(captor.capture());
        assertThat(captor.getValue().getMaxAge()).isEqualTo(Duration.ofDays(7));
    }

    @Test
    void ensureStream_fourArgOverload_alreadyExists_noAction() throws Exception {
        when(jsm.getStreamInfo("ORDERS")).thenReturn(mock(StreamInfo.class));

        assertThatCode(() -> manager.ensureStream("ORDERS", "order.>", connection, Duration.ofDays(7)))
                .doesNotThrowAnyException();

        verify(jsm, never()).addStream(any(StreamConfiguration.class));
    }

    /**
     * Post-review follow-up fix F-2 (decision 2026-07-15) — {@code jobs.}-prefixed
     * subjects (asyncapi {@code a2JobDispatch}/{@code a2JobReply}, {@code streamRetention:
     * WorkQueue}) auto-create with {@link RetentionPolicy#WorkQueue} via the 3-arg convenience
     * overload, closing the drift between the declared wire contract and this dev/test
     * auto-create path.
     */
    @Test
    void ensureStream_jobsPrefixedSubject_notFound_createsWithWorkQueueRetention() throws Exception {
        JetStreamApiException notFound = mock(JetStreamApiException.class);
        when(notFound.getErrorCode()).thenReturn(404);
        when(jsm.getStreamInfo("JOBS")).thenThrow(notFound);

        manager.ensureStream("JOBS", "jobs.order-fulfillment", connection);

        ArgumentCaptor<StreamConfiguration> captor = ArgumentCaptor.forClass(StreamConfiguration.class);
        verify(jsm).addStream(captor.capture());
        assertThat(captor.getValue().getRetentionPolicy()).isEqualTo(RetentionPolicy.WorkQueue);
    }

    /** {@code jobs.<topic>.reply} is also {@code jobs.}-prefixed and gets the same WorkQueue default. */
    @Test
    void ensureStream_jobsReplySubject_notFound_createsWithWorkQueueRetention() throws Exception {
        JetStreamApiException notFound = mock(JetStreamApiException.class);
        when(notFound.getErrorCode()).thenReturn(404);
        when(jsm.getStreamInfo("JOBS-REPLY")).thenThrow(notFound);

        manager.ensureStream("JOBS-REPLY", "jobs.order-fulfillment.reply", connection);

        ArgumentCaptor<StreamConfiguration> captor = ArgumentCaptor.forClass(StreamConfiguration.class);
        verify(jsm).addStream(captor.capture());
        assertThat(captor.getValue().getRetentionPolicy()).isEqualTo(RetentionPolicy.WorkQueue);
    }

    /** {@code dlq.}-prefixed subjects keep the pre-existing {@link RetentionPolicy#Limits} default. */
    @Test
    void ensureStream_dlqPrefixedSubject_notFound_createsWithLimitsRetention() throws Exception {
        JetStreamApiException notFound = mock(JetStreamApiException.class);
        when(notFound.getErrorCode()).thenReturn(404);
        when(jsm.getStreamInfo("DLQ")).thenThrow(notFound);

        manager.ensureStream("DLQ", "dlq.>", connection);

        ArgumentCaptor<StreamConfiguration> captor = ArgumentCaptor.forClass(StreamConfiguration.class);
        verify(jsm).addStream(captor.capture());
        assertThat(captor.getValue().getRetentionPolicy()).isEqualTo(RetentionPolicy.Limits);
    }

    /** docs/11 G4 — {@code ewjobs.}-prefixed subjects get the same WorkQueue default as {@code jobs.}. */
    @Test
    void ensureStream_ewjobsPrefixedSubject_notFound_createsWithWorkQueueRetention() throws Exception {
        JetStreamApiException notFound = mock(JetStreamApiException.class);
        when(notFound.getErrorCode()).thenReturn(404);
        when(jsm.getStreamInfo("FLW-EW-JOBS")).thenThrow(notFound);

        manager.ensureStream("FLW-EW-JOBS", "ewjobs.order-fulfillment", connection);

        ArgumentCaptor<StreamConfiguration> captor = ArgumentCaptor.forClass(StreamConfiguration.class);
        verify(jsm).addStream(captor.capture());
        assertThat(captor.getValue().getRetentionPolicy()).isEqualTo(RetentionPolicy.WorkQueue);
    }

    /** Subjects outside both the {@code jobs.} and {@code dlq.} namespaces default to {@link RetentionPolicy#Limits}. */
    @Test
    void ensureStream_nonJobsNonDlqSubject_notFound_createsWithLimitsRetention() throws Exception {
        JetStreamApiException notFound = mock(JetStreamApiException.class);
        when(notFound.getErrorCode()).thenReturn(404);
        when(jsm.getStreamInfo("ORDERS")).thenThrow(notFound);

        manager.ensureStream("ORDERS", "order.>", connection);

        ArgumentCaptor<StreamConfiguration> captor = ArgumentCaptor.forClass(StreamConfiguration.class);
        verify(jsm).addStream(captor.capture());
        assertThat(captor.getValue().getRetentionPolicy()).isEqualTo(RetentionPolicy.Limits);
    }

    /** Explicit {@code retentionPolicy} argument (5-arg overload) always wins over the subject-based default. */
    @Test
    void ensureStream_explicitRetentionPolicy_overridesJobsDefault() throws Exception {
        JetStreamApiException notFound = mock(JetStreamApiException.class);
        when(notFound.getErrorCode()).thenReturn(404);
        when(jsm.getStreamInfo("JOBS")).thenThrow(notFound);

        manager.ensureStream("JOBS", "jobs.order-fulfillment", connection, null, RetentionPolicy.Limits);

        ArgumentCaptor<StreamConfiguration> captor = ArgumentCaptor.forClass(StreamConfiguration.class);
        verify(jsm).addStream(captor.capture());
        assertThat(captor.getValue().getRetentionPolicy()).isEqualTo(RetentionPolicy.Limits);
    }

    @Test
    void ensureStream_fiveArgOverload_alreadyExists_noAction() throws Exception {
        when(jsm.getStreamInfo("JOBS")).thenReturn(mock(StreamInfo.class));

        assertThatCode(() -> manager.ensureStream("JOBS", "jobs.order-fulfillment", connection,
                Duration.ofDays(7), RetentionPolicy.WorkQueue))
                        .doesNotThrowAnyException();

        verify(jsm, never()).addStream(any(StreamConfiguration.class));
    }

    /** LLD-Q2/ARCH-Q3 increment 2 subject-mapped partitioning — the 6-arg overload's {@code
     *  subjectTransform} must be applied to the builder for a newly-created stream. */
    @Test
    void ensureStream_sixArgOverload_notFound_appliesSubjectTransform() throws Exception {
        JetStreamApiException notFound = mock(JetStreamApiException.class);
        when(notFound.getErrorCode()).thenReturn(404);
        when(jsm.getStreamInfo("ORDERS")).thenThrow(notFound);
        SubjectTransform transform = new SubjectTransform("order.>", "order.{{partition(4,1)}}.>");

        manager.ensureStream("ORDERS", "order.>", connection, null, RetentionPolicy.Limits, transform);

        ArgumentCaptor<StreamConfiguration> captor = ArgumentCaptor.forClass(StreamConfiguration.class);
        verify(jsm).addStream(captor.capture());
        assertThat(captor.getValue().getSubjectTransform()).isNotNull();
        assertThat(captor.getValue().getSubjectTransform().getSource()).isEqualTo("order.>");
    }

    @Test
    void ensureStream_sixArgOverload_nullSubjectTransform_noTransformApplied() throws Exception {
        JetStreamApiException notFound = mock(JetStreamApiException.class);
        when(notFound.getErrorCode()).thenReturn(404);
        when(jsm.getStreamInfo("ORDERS")).thenThrow(notFound);

        manager.ensureStream("ORDERS", "order.>", connection, null, RetentionPolicy.Limits, null);

        ArgumentCaptor<StreamConfiguration> captor = ArgumentCaptor.forClass(StreamConfiguration.class);
        verify(jsm).addStream(captor.capture());
        assertThat(captor.getValue().getSubjectTransform()).isNull();
    }

    @Test
    void ensureStream_connectionJetStreamManagementFailsWithIOException_wrapsAsIllegalStateException() throws Exception {
        Connection brokenConnection = mock(Connection.class);
        when(brokenConnection.jetStreamManagement()).thenThrow(new java.io.IOException("no connection"));

        assertThatThrownBy(() -> manager.ensureStream("ORDERS", "order.>", brokenConnection))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("I/O error")
                .hasCauseInstanceOf(java.io.IOException.class);
    }

    @Test
    void ensureStream_unexpectedRuntimeException_wrapsAsIllegalStateException() throws Exception {
        Connection brokenConnection = mock(Connection.class);
        when(brokenConnection.jetStreamManagement()).thenThrow(new RuntimeException("unexpected"));

        assertThatThrownBy(() -> manager.ensureStream("ORDERS", "order.>", brokenConnection))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unexpected error")
                .hasCauseInstanceOf(RuntimeException.class);
    }
}
