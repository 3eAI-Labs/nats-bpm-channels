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

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

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
     * Sentinel Phase 5.5 QA fix (item 6, Levent karari 2026-07-15) — non-DLQ subjects keep the
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
     * Sentinel Phase 5.5 QA fix (item 6, Levent karari 2026-07-15) — {@code dlq.}-prefixed
     * subjects auto-create with a 14-day retention default (DATA_CLASSIFICATION.md §5 Q3 karari).
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
     * Sentinel Phase 6 follow-up fix F-2 (Levent karari 2026-07-15) — {@code jobs.}-prefixed
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
}
