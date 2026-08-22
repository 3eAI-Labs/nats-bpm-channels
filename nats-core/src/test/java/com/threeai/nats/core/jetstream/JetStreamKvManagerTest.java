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
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValueManagement;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueStatus;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.LoggerFactory;
import org.junit.jupiter.api.Test;

class JetStreamKvManagerTest {

    private Connection connection;
    private KeyValueManagement kvm;
    private JetStreamKvManager manager;

    @BeforeEach
    void setUp() throws Exception {
        connection = mock(Connection.class);
        kvm = mock(KeyValueManagement.class);
        when(connection.keyValueManagement()).thenReturn(kvm);
        manager = new JetStreamKvManager();
    }

    @Test
    void ensureBucket_exists_noAction() throws Exception {
        when(kvm.getBucketInfo("a2-sweep-leader")).thenReturn(mock(KeyValueStatus.class));

        assertThatCode(() -> manager.ensureBucket("a2-sweep-leader", Duration.ofSeconds(240), 3, connection))
                .doesNotThrowAnyException();

        verify(kvm, never()).create(any(KeyValueConfiguration.class));
    }

    @Test
    void ensureBucket_existingBucketWithDriftedConfig_warnsAndKeepsExisting() throws Exception {
        KeyValueStatus existing = mock(KeyValueStatus.class);
        when(existing.getTtl()).thenReturn(Duration.ofSeconds(999));
        when(existing.getReplicas()).thenReturn(1);
        when(kvm.getBucketInfo("a2-sweep-leader")).thenReturn(existing);

        ListAppender<ILoggingEvent> appender = attachAppender();
        try {
            manager.ensureBucket("a2-sweep-leader", Duration.ofSeconds(240), 3, connection);
        } finally {
            detachAppender(appender);
        }

        verify(kvm, never()).create(any(KeyValueConfiguration.class));
        assertThat(appender.list).anySatisfy(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.WARN);
            assertThat(event.getFormattedMessage()).contains("different configuration");
        });
    }

    @Test
    void ensureBucket_existingBucketWithMatchingConfig_staysQuiet() throws Exception {
        KeyValueStatus existing = mock(KeyValueStatus.class);
        when(existing.getTtl()).thenReturn(Duration.ofSeconds(240));
        when(existing.getReplicas()).thenReturn(3);
        when(kvm.getBucketInfo("a2-sweep-leader")).thenReturn(existing);

        ListAppender<ILoggingEvent> appender = attachAppender();
        try {
            manager.ensureBucket("a2-sweep-leader", Duration.ofSeconds(240), 3, connection);
        } finally {
            detachAppender(appender);
        }

        assertThat(appender.list).noneMatch(event -> event.getLevel() == Level.WARN);
    }

    private ListAppender<ILoggingEvent> attachAppender() {
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(JetStreamKvManager.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        return appender;
    }

    private void detachAppender(ListAppender<ILoggingEvent> appender) {
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(JetStreamKvManager.class);
        logger.detachAppender(appender);
        appender.stop();
    }

    @Test
    void ensureBucket_notFound_creates() throws Exception {
        JetStreamApiException notFound = mock(JetStreamApiException.class);
        when(notFound.getErrorCode()).thenReturn(404);
        when(kvm.getBucketInfo("a2-sweep-leader")).thenThrow(notFound);

        manager.ensureBucket("a2-sweep-leader", Duration.ofSeconds(240), 3, connection);

        verify(kvm).create(any(KeyValueConfiguration.class));
    }

    @Test
    void ensureBucket_apiFails_throwsIllegalStateException() throws Exception {
        JetStreamApiException serverError = mock(JetStreamApiException.class);
        when(serverError.getErrorCode()).thenReturn(500);
        when(kvm.getBucketInfo("a2-sweep-leader")).thenThrow(serverError);

        assertThatThrownBy(() -> manager.ensureBucket("a2-sweep-leader", Duration.ofSeconds(240), 3, connection))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("a2-sweep-leader");
    }

    @Test
    void ensureBucket_connectionKeyValueManagementFailsWithIOException_wrapsAsIllegalStateException() throws Exception {
        Connection brokenConnection = mock(Connection.class);
        when(brokenConnection.keyValueManagement()).thenThrow(new IOException("no connection"));

        assertThatThrownBy(() -> manager.ensureBucket("a2-sweep-leader", Duration.ofSeconds(240), 3, brokenConnection))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("I/O error")
                .hasCauseInstanceOf(IOException.class);
    }

    @Test
    void ensureBucket_unexpectedRuntimeException_wrapsAsIllegalStateException() throws Exception {
        Connection brokenConnection = mock(Connection.class);
        when(brokenConnection.keyValueManagement()).thenThrow(new RuntimeException("unexpected"));

        assertThatThrownBy(() -> manager.ensureBucket("a2-sweep-leader", Duration.ofSeconds(240), 3, brokenConnection))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unexpected error")
                .hasCauseInstanceOf(RuntimeException.class);
    }
}
