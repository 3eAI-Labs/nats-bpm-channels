package com.threeai.nats.camunda.history;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;

import org.camunda.bpm.engine.impl.context.Context;
import org.camunda.bpm.engine.impl.history.event.HistoricProcessInstanceEventEntity;
import org.camunda.bpm.engine.impl.history.handler.DbHistoryEventHandler;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Unit test for the class-based routing/dual-run decision (BR-HDL-002/005) — never exercised
 * end-to-end before (`HistoryOffloadAuditCriticalFlowIntegrationTest` only covers the NOT-cut-over
 * dual-run path); this test isolates the {@code isCutOver} skip branch with mocks so it does not
 * need a real engine transaction/DataSource.
 */
class NatsHistoryEventHandlerTest {

    @Test
    void handleEvent_classCutOver_skipsInternalDbDelegate() {
        ClassCutoverStateRegistry cutoverRegistry = mock(ClassCutoverStateRegistry.class);
        when(cutoverRegistry.isCutOver("PROCINST")).thenReturn(true);
        HistoryClassificationProperties classification = new HistoryClassificationProperties();
        HistoryPostCommitPublisher postCommitPublisher = mock(HistoryPostCommitPublisher.class);
        DbHistoryEventHandler internalDbDelegate = mock(DbHistoryEventHandler.class);
        NatsHistoryEventHandler handler = new NatsHistoryEventHandler(
                cutoverRegistry, classification, null, postCommitPublisher, internalDbDelegate, "camunda");

        HistoricProcessInstanceEventEntity event = new HistoricProcessInstanceEventEntity();

        try (MockedStatic<Context> context = mockStatic(Context.class)) {
            CommandContext commandContext = mock(CommandContext.class, org.mockito.Answers.RETURNS_DEEP_STUBS);
            context.when(Context::getCommandContext).thenReturn(commandContext);

            handler.handleEvent(event);
        }

        verify(internalDbDelegate, never()).handleEvent(any());
    }

    @Test
    void handleEvent_classNotCutOver_dualRunCallsInternalDbDelegate() {
        ClassCutoverStateRegistry cutoverRegistry = mock(ClassCutoverStateRegistry.class);
        when(cutoverRegistry.isCutOver("PROCINST")).thenReturn(false);
        HistoryClassificationProperties classification = new HistoryClassificationProperties();
        HistoryPostCommitPublisher postCommitPublisher = mock(HistoryPostCommitPublisher.class);
        DbHistoryEventHandler internalDbDelegate = mock(DbHistoryEventHandler.class);
        NatsHistoryEventHandler handler = new NatsHistoryEventHandler(
                cutoverRegistry, classification, null, postCommitPublisher, internalDbDelegate, "camunda");

        HistoricProcessInstanceEventEntity event = new HistoricProcessInstanceEventEntity();

        try (MockedStatic<Context> context = mockStatic(Context.class)) {
            CommandContext commandContext = mock(CommandContext.class, org.mockito.Answers.RETURNS_DEEP_STUBS);
            context.when(Context::getCommandContext).thenReturn(commandContext);

            handler.handleEvent(event);
        }

        verify(internalDbDelegate, times(1)).handleEvent(event);
    }
}
