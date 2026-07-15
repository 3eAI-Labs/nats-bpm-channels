package com.threeai.nats.core.jetstream;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValueManagement;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueStatus;
import org.junit.jupiter.api.BeforeEach;
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
}
