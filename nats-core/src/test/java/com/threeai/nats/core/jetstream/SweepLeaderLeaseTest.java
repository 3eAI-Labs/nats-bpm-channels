package com.threeai.nats.core.jetstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SweepLeaderLeaseTest {

    private Connection connection;
    private JetStream jetStream;
    private JetStreamKvManager kvManager;
    private KeyValue keyValue;

    @BeforeEach
    void setUp() throws Exception {
        connection = mock(Connection.class);
        jetStream = mock(JetStream.class);
        kvManager = mock(JetStreamKvManager.class);
        keyValue = mock(KeyValue.class);
        when(connection.keyValue("a2-sweep-leader")).thenReturn(keyValue);
    }

    private SweepLeaderLease newLease(String engineId, String nodeId) {
        return new SweepLeaderLease(jetStream, kvManager, connection, engineId, nodeId, Duration.ofSeconds(240));
    }

    @Test
    void key_isNamespacedPerEngine() {
        SweepLeaderLease camunda = newLease("camunda", "pod-1");
        SweepLeaderLease cadenzaflow = newLease("cadenzaflow", "pod-1");

        assertThat(camunda.getKey()).isEqualTo("sweep-leader.camunda");
        assertThat(cadenzaflow.getKey()).isEqualTo("sweep-leader.cadenzaflow");
    }

    @Test
    void getTtl_returnsConfiguredValue() {
        SweepLeaderLease lease = newLease("camunda", "pod-1");

        assertThat(lease.getTtl()).isEqualTo(Duration.ofSeconds(240));
    }

    @Test
    void basamak2Overload_usesCustomBucketAndKeyPrefix_notDefault() throws Exception {
        // history-relay-leader reuse (08_config.md §3) -- separate bucket/key namespace from
        // a2-sweep-leader, verifying the two lease families never collide.
        when(connection.keyValue("history-relay-leader")).thenReturn(keyValue);
        when(keyValue.create(eq("relay-leader.camunda"), any(byte[].class))).thenReturn(1L);

        SweepLeaderLease historyRelayLease = new SweepLeaderLease(jetStream, kvManager, connection,
                "history-relay-leader", "relay-leader.", "camunda", "pod-1", Duration.ofSeconds(60));

        assertThat(historyRelayLease.getKey()).isEqualTo("relay-leader.camunda");
        assertThat(historyRelayLease.tryAcquireOrRenew()).isTrue();
        assertThat(historyRelayLease.isLeader()).isTrue();
        // a2-sweep-leader's KV handle (stubbed in setUp) must NOT have been touched by this lease.
        org.mockito.Mockito.verify(connection, org.mockito.Mockito.never()).keyValue("a2-sweep-leader");
    }

    @Test
    void tryAcquireOrRenew_keyDoesNotExist_createsAndBecomesLeader() throws Exception {
        when(keyValue.create(eq("sweep-leader.camunda"), any(byte[].class))).thenReturn(1L);
        SweepLeaderLease lease = newLease("camunda", "pod-1");

        boolean acquired = lease.tryAcquireOrRenew();

        assertThat(acquired).isTrue();
        assertThat(lease.isLeader()).isTrue();
    }

    @Test
    void tryAcquireOrRenew_alreadyLeader_renewsSuccessfully() throws Exception {
        when(keyValue.create(eq("sweep-leader.camunda"), any(byte[].class)))
                .thenThrow(mock(JetStreamApiException.class));
        KeyValueEntry entry = mock(KeyValueEntry.class);
        when(entry.getValue()).thenReturn("pod-1".getBytes(StandardCharsets.UTF_8));
        when(entry.getRevision()).thenReturn(5L);
        when(keyValue.get("sweep-leader.camunda")).thenReturn(entry);
        when(keyValue.update(eq("sweep-leader.camunda"), any(byte[].class), eq(5L))).thenReturn(6L);

        SweepLeaderLease lease = newLease("camunda", "pod-1");
        boolean acquired = lease.tryAcquireOrRenew();

        assertThat(acquired).isTrue();
        assertThat(lease.isLeader()).isTrue();
    }

    @Test
    void tryAcquireOrRenew_anotherNodeHoldsLease_returnsFalse() throws Exception {
        when(keyValue.create(eq("sweep-leader.camunda"), any(byte[].class)))
                .thenThrow(mock(JetStreamApiException.class));
        KeyValueEntry entry = mock(KeyValueEntry.class);
        when(entry.getValue()).thenReturn("pod-2".getBytes(StandardCharsets.UTF_8));
        when(keyValue.get("sweep-leader.camunda")).thenReturn(entry);

        SweepLeaderLease lease = newLease("camunda", "pod-1");
        boolean acquired = lease.tryAcquireOrRenew();

        assertThat(acquired).isFalse();
        assertThat(lease.isLeader()).isFalse();
    }

    @Test
    void tryAcquireOrRenew_renewRaceLost_returnsFalse() throws Exception {
        when(keyValue.create(eq("sweep-leader.camunda"), any(byte[].class)))
                .thenThrow(mock(JetStreamApiException.class));
        KeyValueEntry entry = mock(KeyValueEntry.class);
        when(entry.getValue()).thenReturn("pod-1".getBytes(StandardCharsets.UTF_8));
        when(entry.getRevision()).thenReturn(5L);
        when(keyValue.get("sweep-leader.camunda")).thenReturn(entry);
        when(keyValue.update(eq("sweep-leader.camunda"), any(byte[].class), anyLong()))
                .thenThrow(mock(JetStreamApiException.class));

        SweepLeaderLease lease = newLease("camunda", "pod-1");
        boolean acquired = lease.tryAcquireOrRenew();

        assertThat(acquired).isFalse();
    }

    @Test
    void tryAcquireOrRenew_connectionFailure_returnsFalse() throws Exception {
        when(connection.keyValue("a2-sweep-leader")).thenThrow(new IOException("no connection"));

        SweepLeaderLease lease = newLease("camunda", "pod-1");
        boolean acquired = lease.tryAcquireOrRenew();

        assertThat(acquired).isFalse();
        assertThat(lease.isLeader()).isFalse();
    }

    @Test
    void tryAcquireOrRenew_bucketMissingEntirely_returnsFalse() throws Exception {
        when(keyValue.create(eq("sweep-leader.camunda"), any(byte[].class)))
                .thenThrow(mock(JetStreamApiException.class));
        when(keyValue.get("sweep-leader.camunda")).thenReturn(null);

        SweepLeaderLease lease = newLease("camunda", "pod-1");
        boolean acquired = lease.tryAcquireOrRenew();

        assertThat(acquired).isFalse();
    }

    // NEW-001 (faz-5 re-run, Levent kararı 2026-07-21): heldRevision must be reset to null on
    // EVERY failure path that follows a previously-successful acquire/renew, not just left at its
    // last-held value -- otherwise isLeader() reports a stale "true" after leadership was
    // actually lost. Each test below first drives a REAL successful acquire (heldRevision set),
    // then a REAL failure of the same kind already covered above, and asserts isLeader() flips to
    // false as a DIRECT consequence -- the real renew-fail -> heldRevision-reset -> isLeader()
    // false chain, not independently-stubbed return values.

    @Test
    void tryAcquireOrRenew_anotherNodeTakesOverAfterHolding_isLeaderBecomesFalse() throws Exception {
        when(keyValue.create(eq("sweep-leader.camunda"), any(byte[].class))).thenReturn(1L);
        SweepLeaderLease lease = newLease("camunda", "pod-1");
        assertThat(lease.tryAcquireOrRenew()).isTrue();
        assertThat(lease.isLeader()).isTrue();

        when(keyValue.create(eq("sweep-leader.camunda"), any(byte[].class)))
                .thenThrow(mock(JetStreamApiException.class));
        KeyValueEntry entry = mock(KeyValueEntry.class);
        when(entry.getValue()).thenReturn("pod-2".getBytes(StandardCharsets.UTF_8));
        when(keyValue.get("sweep-leader.camunda")).thenReturn(entry);

        boolean acquired = lease.tryAcquireOrRenew();

        assertThat(acquired).isFalse();
        assertThat(lease.isLeader()).isFalse();
    }

    @Test
    void tryAcquireOrRenew_renewRaceLostAfterHolding_isLeaderBecomesFalse() throws Exception {
        when(keyValue.create(eq("sweep-leader.camunda"), any(byte[].class))).thenReturn(1L);
        SweepLeaderLease lease = newLease("camunda", "pod-1");
        assertThat(lease.tryAcquireOrRenew()).isTrue();
        assertThat(lease.isLeader()).isTrue();

        when(keyValue.create(eq("sweep-leader.camunda"), any(byte[].class)))
                .thenThrow(mock(JetStreamApiException.class));
        KeyValueEntry entry = mock(KeyValueEntry.class);
        when(entry.getValue()).thenReturn("pod-1".getBytes(StandardCharsets.UTF_8));
        when(entry.getRevision()).thenReturn(1L);
        when(keyValue.get("sweep-leader.camunda")).thenReturn(entry);
        when(keyValue.update(eq("sweep-leader.camunda"), any(byte[].class), anyLong()))
                .thenThrow(mock(JetStreamApiException.class));

        boolean acquired = lease.tryAcquireOrRenew();

        assertThat(acquired).isFalse();
        assertThat(lease.isLeader()).isFalse();
    }

    @Test
    void tryAcquireOrRenew_connectionFailsAfterHolding_isLeaderBecomesFalse() throws Exception {
        when(keyValue.create(eq("sweep-leader.camunda"), any(byte[].class))).thenReturn(1L);
        SweepLeaderLease lease = newLease("camunda", "pod-1");
        assertThat(lease.tryAcquireOrRenew()).isTrue();
        assertThat(lease.isLeader()).isTrue();

        when(connection.keyValue("a2-sweep-leader")).thenThrow(new IOException("no connection"));

        boolean acquired = lease.tryAcquireOrRenew();

        assertThat(acquired).isFalse();
        assertThat(lease.isLeader()).isFalse();
    }

    @Test
    void tryAcquireOrRenew_kvLookupFailsAfterHolding_isLeaderBecomesFalse() throws Exception {
        when(keyValue.create(eq("sweep-leader.camunda"), any(byte[].class))).thenReturn(1L);
        SweepLeaderLease lease = newLease("camunda", "pod-1");
        assertThat(lease.tryAcquireOrRenew()).isTrue();
        assertThat(lease.isLeader()).isTrue();

        when(keyValue.create(eq("sweep-leader.camunda"), any(byte[].class)))
                .thenThrow(mock(JetStreamApiException.class));
        when(keyValue.get("sweep-leader.camunda")).thenThrow(mock(JetStreamApiException.class));

        boolean acquired = lease.tryAcquireOrRenew();

        assertThat(acquired).isFalse();
        assertThat(lease.isLeader()).isFalse();
    }
}
