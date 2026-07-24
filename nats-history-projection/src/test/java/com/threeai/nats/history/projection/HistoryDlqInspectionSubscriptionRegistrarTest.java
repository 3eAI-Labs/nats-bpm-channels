package com.threeai.nats.history.projection;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.MessageHandler;
import io.nats.client.PushSubscribeOptions;
import org.junit.jupiter.api.Test;

/**
 * Sentinel Phase 5.5 (round 2): {@link HistoryDlqInspectionSubscriptionRegistrar} had no
 * dedicated test at all -- only exercised indirectly (with mocks that never throw) via
 * {@code NatsHistoryProjectionAutoConfigurationTest}. These tests cover its
 * InitializingBean/DisposableBean error paths directly.
 */
class HistoryDlqInspectionSubscriptionRegistrarTest {

    @Test
    void afterPropertiesSet_subscribeSucceeds_registersDispatcher() throws Exception {
        Connection connection = mock(Connection.class);
        Dispatcher dispatcher = mock(Dispatcher.class);
        when(connection.createDispatcher()).thenReturn(dispatcher);
        JetStream jetStream = mock(JetStream.class);
        when(jetStream.subscribe(anyString(), any(Dispatcher.class), any(MessageHandler.class),
                anyBoolean(), any(PushSubscribeOptions.class))).thenReturn(mock(JetStreamSubscription.class));
        HistoryDlqInspectionConsumer consumer = mock(HistoryDlqInspectionConsumer.class);
        HistoryDlqInspectionSubscriptionRegistrar registrar =
                new HistoryDlqInspectionSubscriptionRegistrar(connection, jetStream, consumer);

        assertThatCode(registrar::afterPropertiesSet).doesNotThrowAnyException();

        verify(connection).createDispatcher();
        verify(jetStream).subscribe(org.mockito.ArgumentMatchers.eq("dlq.history.>"), any(Dispatcher.class),
                any(MessageHandler.class), org.mockito.ArgumentMatchers.eq(false), any(PushSubscribeOptions.class));
    }

    @Test
    void afterPropertiesSet_jetStreamSubscribeThrows_wrapsInIllegalStateException() throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.createDispatcher()).thenReturn(mock(Dispatcher.class));
        JetStream jetStream = mock(JetStream.class);
        when(jetStream.subscribe(anyString(), any(Dispatcher.class), any(MessageHandler.class),
                anyBoolean(), any(PushSubscribeOptions.class))).thenThrow(new IOException("broker unreachable"));
        HistoryDlqInspectionConsumer consumer = mock(HistoryDlqInspectionConsumer.class);
        HistoryDlqInspectionSubscriptionRegistrar registrar =
                new HistoryDlqInspectionSubscriptionRegistrar(connection, jetStream, consumer);

        assertThatThrownBy(registrar::afterPropertiesSet)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to subscribe history DLQ inspection consumer")
                .hasCauseInstanceOf(IOException.class);
    }

    @Test
    void destroy_beforeSubscribe_doesNotThrow() {
        HistoryDlqInspectionSubscriptionRegistrar registrar = new HistoryDlqInspectionSubscriptionRegistrar(
                mock(Connection.class), mock(JetStream.class), mock(HistoryDlqInspectionConsumer.class));

        assertThatCode(registrar::destroy).doesNotThrowAnyException();
    }

    @Test
    void destroy_afterSubscribe_drainsDispatcher() throws Exception {
        Connection connection = mock(Connection.class);
        Dispatcher dispatcher = mock(Dispatcher.class);
        when(connection.createDispatcher()).thenReturn(dispatcher);
        JetStream jetStream = mock(JetStream.class);
        when(jetStream.subscribe(anyString(), any(Dispatcher.class), any(MessageHandler.class),
                anyBoolean(), any(PushSubscribeOptions.class))).thenReturn(mock(JetStreamSubscription.class));
        HistoryDlqInspectionSubscriptionRegistrar registrar = new HistoryDlqInspectionSubscriptionRegistrar(
                connection, jetStream, mock(HistoryDlqInspectionConsumer.class));
        registrar.afterPropertiesSet();

        registrar.destroy();

        verify(dispatcher).drain(Duration.ofSeconds(10));
    }

    @Test
    void destroy_dispatcherDrainThrows_logsWarnAndDoesNotPropagate() throws Exception {
        Connection connection = mock(Connection.class);
        Dispatcher dispatcher = mock(Dispatcher.class);
        when(connection.createDispatcher()).thenReturn(dispatcher);
        doThrow(new RuntimeException("drain failed")).when(dispatcher).drain(any(Duration.class));
        JetStream jetStream = mock(JetStream.class);
        when(jetStream.subscribe(anyString(), any(Dispatcher.class), any(MessageHandler.class),
                anyBoolean(), any(PushSubscribeOptions.class))).thenReturn(mock(JetStreamSubscription.class));
        HistoryDlqInspectionSubscriptionRegistrar registrar = new HistoryDlqInspectionSubscriptionRegistrar(
                connection, jetStream, mock(HistoryDlqInspectionConsumer.class));
        registrar.afterPropertiesSet();

        assertThatCode(registrar::destroy).doesNotThrowAnyException();

        verify(dispatcher).drain(Duration.ofSeconds(10));
    }
}
