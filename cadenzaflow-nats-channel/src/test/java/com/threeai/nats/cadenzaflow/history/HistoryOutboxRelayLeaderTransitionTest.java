package com.threeai.nats.cadenzaflow.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import javax.sql.DataSource;

import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.jetstream.SweepLeaderLease;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * FINDING-004 (faz-5 review): {@code SYS_OUTBOX_RELAY_LEADER_LOST} transition detection.
 *
 * <p><b>NEW-001 (faz-5 re-run, Levent kararı 2026-07-21):</b> the original version of this test
 * fully mocked {@link SweepLeaderLease} and independently stubbed {@code isLeader()} and {@code
 * tryAcquireOrRenew()} — an inconsistent combination a real {@link SweepLeaderLease} instance
 * could never actually produce, which is exactly what masked the {@code heldRevision}
 * never-reset-on-failure bug this test was meant to guard. This version instead constructs a
 * REAL {@link SweepLeaderLease} (mocking only the NATS {@link Connection}/{@link KeyValue}
 * I/O boundary, same style as {@code SweepLeaderLeaseTest}) and drives it through a genuine
 * acquire-success cycle followed by a genuine renew-failure cycle, so {@code wasLeader} in
 * {@link HistoryOutboxRelay#relayCycle()} is computed from the real {@code heldRevision}
 * reset-to-null chain, not from hand-picked stub values. A real KV lookup/lease NEVER reaches
 * the {@link DataSource} on the failure path (short-circuits before {@code fetchOldestFirst()}),
 * so this remains a fast, no-Testcontainers unit test; {@link HistoryOutboxRelayTest} still
 * covers the real-dependency leader/non-leader DB-read paths this class does not re-test.
 */
class HistoryOutboxRelayLeaderTransitionTest {

    private static final String BUCKET = "a2-sweep-leader";
    private static final String KEY = "sweep-leader.cadenzaflow";

    @Test
    void relayCycle_wasLeaderNowIsnt_logsLeadershipLostWarning() throws Exception {
        Connection connection = mock(Connection.class);
        KeyValue keyValue = mock(KeyValue.class);
        when(connection.keyValue(BUCKET)).thenReturn(keyValue);
        SweepLeaderLease leaderLease = new SweepLeaderLease(mock(JetStream.class), mock(JetStreamKvManager.class),
                connection, "cadenzaflow", "pod-1", Duration.ofSeconds(240));

        // Cycle N: genuinely acquire the lease -- real success path, heldRevision set for real.
        when(keyValue.create(eq(KEY), any(byte[].class))).thenReturn(1L);
        assertThat(leaderLease.tryAcquireOrRenew()).isTrue();
        assertThat(leaderLease.isLeader()).isTrue();

        // Cycle N+1: another node has genuinely taken over the key -- real renew-fail path,
        // heldRevision reset to null for real (NEW-001 fix under test).
        when(keyValue.create(eq(KEY), any(byte[].class))).thenThrow(mock(io.nats.client.JetStreamApiException.class));
        KeyValueEntry entry = mock(KeyValueEntry.class);
        when(entry.getValue()).thenReturn("pod-2".getBytes(StandardCharsets.UTF_8));
        when(keyValue.get(KEY)).thenReturn(entry);

        HistoryOutboxRelay relay = new HistoryOutboxRelay(mock(DataSource.class), mock(JetStream.class),
                leaderLease, new HistoryOutboxProperties(), null, "cadenzaflow");

        Logger relayLogger = (Logger) LoggerFactory.getLogger(HistoryOutboxRelay.class);
        ListAppender<ILoggingEvent> appender = captureLogsAt(relayLogger, Level.WARN);
        try {
            relay.relayCycle();
        } finally {
            relayLogger.detachAppender(appender);
        }

        assertThat(leaderLease.isLeader()).isFalse();
        assertThat(appender.list).anyMatch(event -> event.getFormattedMessage().contains("leadership lost"));
    }

    @Test
    void relayCycle_neverWasLeader_noLeadershipLostWarning_routineSilence() throws Exception {
        Connection connection = mock(Connection.class);
        KeyValue keyValue = mock(KeyValue.class);
        when(connection.keyValue(BUCKET)).thenReturn(keyValue);
        SweepLeaderLease leaderLease = new SweepLeaderLease(mock(JetStream.class), mock(JetStreamKvManager.class),
                connection, "cadenzaflow", "pod-1", Duration.ofSeconds(240));

        // Never acquired: another node already holds the key -- real failure path from the start,
        // heldRevision is still its initial null (never leader), same as it would be on a
        // freshly-started replica.
        when(keyValue.create(eq(KEY), any(byte[].class))).thenThrow(mock(io.nats.client.JetStreamApiException.class));
        KeyValueEntry entry = mock(KeyValueEntry.class);
        when(entry.getValue()).thenReturn("pod-2".getBytes(StandardCharsets.UTF_8));
        when(keyValue.get(KEY)).thenReturn(entry);
        assertThat(leaderLease.isLeader()).isFalse();

        HistoryOutboxRelay relay = new HistoryOutboxRelay(mock(DataSource.class), mock(JetStream.class),
                leaderLease, new HistoryOutboxProperties(), null, "cadenzaflow");

        Logger relayLogger = (Logger) LoggerFactory.getLogger(HistoryOutboxRelay.class);
        ListAppender<ILoggingEvent> appender = captureLogsAt(relayLogger, Level.WARN);
        try {
            relay.relayCycle();
        } finally {
            relayLogger.detachAppender(appender);
        }

        assertThat(leaderLease.isLeader()).isFalse();
        assertThat(appender.list).noneMatch(event -> event.getFormattedMessage().contains("leadership lost"));
    }

    private ListAppender<ILoggingEvent> captureLogsAt(Logger logger, Level level) {
        logger.setLevel(level);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        return appender;
    }
}
