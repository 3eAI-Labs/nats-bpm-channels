package com.threeai.nats.camunda.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.sql.DataSource;

import com.threeai.nats.core.jetstream.SweepLeaderLease;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.nats.client.JetStream;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * FINDING-004 (faz-5 review): {@code SYS_OUTBOX_RELAY_LEADER_LOST} transition detection — narrow,
 * mocked unit test (a genuine KV-lease-expiry race is slow/flaky to orchestrate deterministically
 * against real NATS; {@link HistoryOutboxRelayTest} covers the real-dependency leader/non-leader
 * paths this class does NOT re-test). Log-capture pattern reused from {@code
 * NatsMessageCorrelationSubscriberTest} (basamak-1).
 */
class HistoryOutboxRelayLeaderTransitionTest {

    @Test
    void relayCycle_wasLeaderNowIsnt_logsLeadershipLostWarning() {
        SweepLeaderLease leaderLease = mock(SweepLeaderLease.class);
        when(leaderLease.isLeader()).thenReturn(true); // held leadership as of the PREVIOUS cycle
        when(leaderLease.tryAcquireOrRenew()).thenReturn(false); // lost it THIS cycle
        HistoryOutboxRelay relay = new HistoryOutboxRelay(mock(DataSource.class), mock(JetStream.class),
                leaderLease, new HistoryOutboxProperties(), null, "camunda");

        Logger relayLogger = (Logger) LoggerFactory.getLogger(HistoryOutboxRelay.class);
        ListAppender<ILoggingEvent> appender = captureLogsAt(relayLogger, Level.WARN);
        try {
            relay.relayCycle();
        } finally {
            relayLogger.detachAppender(appender);
        }

        assertThat(appender.list).anyMatch(event -> event.getFormattedMessage().contains("leadership lost"));
    }

    @Test
    void relayCycle_neverWasLeader_noLeadershipLostWarning_routineSilence() {
        SweepLeaderLease leaderLease = mock(SweepLeaderLease.class);
        when(leaderLease.isLeader()).thenReturn(false); // was never the leader
        when(leaderLease.tryAcquireOrRenew()).thenReturn(false); // still isn't
        HistoryOutboxRelay relay = new HistoryOutboxRelay(mock(DataSource.class), mock(JetStream.class),
                leaderLease, new HistoryOutboxProperties(), null, "camunda");

        Logger relayLogger = (Logger) LoggerFactory.getLogger(HistoryOutboxRelay.class);
        ListAppender<ILoggingEvent> appender = captureLogsAt(relayLogger, Level.WARN);
        try {
            relay.relayCycle();
        } finally {
            relayLogger.detachAppender(appender);
        }

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
