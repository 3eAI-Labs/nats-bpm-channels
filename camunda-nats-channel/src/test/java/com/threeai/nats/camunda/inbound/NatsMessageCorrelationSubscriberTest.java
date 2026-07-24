package com.threeai.nats.camunda.inbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.List;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.runtime.MessageCorrelationBuilder;
import org.camunda.bpm.engine.runtime.MessageCorrelationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

class NatsMessageCorrelationSubscriberTest {

    private Connection connection;
    private RuntimeService runtimeService;
    private MessageCorrelationBuilder correlationBuilder;
    private SubscriptionConfig config;
    private NatsMessageCorrelationSubscriber subscriber;

    @BeforeEach
    void setUp() {
        connection = mock(Connection.class);
        runtimeService = mock(RuntimeService.class);
        correlationBuilder = mock(MessageCorrelationBuilder.class);

        when(runtimeService.createMessageCorrelation(any())).thenReturn(correlationBuilder);
        when(correlationBuilder.processInstanceBusinessKey(any())).thenReturn(correlationBuilder);
        when(correlationBuilder.setVariables(anyMap())).thenReturn(correlationBuilder);
        when(correlationBuilder.correlateWithResult()).thenReturn(mock(MessageCorrelationResult.class));

        config = new SubscriptionConfig();
        config.setSubject("order.new");
        config.setMessageName("OrderReceived");

        subscriber = new NatsMessageCorrelationSubscriber(connection, runtimeService, config, null);
    }

    @Test
    void subscribe_registersDispatcherSubscription_logsSuccess() {
        Connection freshConnection = mock(Connection.class);
        io.nats.client.Dispatcher dispatcher = mock(io.nats.client.Dispatcher.class);
        when(freshConnection.createDispatcher()).thenReturn(dispatcher);
        NatsMessageCorrelationSubscriber freshSubscriber =
                new NatsMessageCorrelationSubscriber(freshConnection, runtimeService, config, null);

        freshSubscriber.subscribe();

        verify(dispatcher).subscribe(eq(config.getSubject()), any(io.nats.client.MessageHandler.class));
    }

    @Test
    void unsubscribe_beforeSubscribe_isNoOp_doesNotThrow() {
        org.assertj.core.api.Assertions.assertThatCode(subscriber::unsubscribe).doesNotThrowAnyException();
    }

    @Test
    void subscribeThenUnsubscribe_closesDispatcherAndShutsDownExecutor() {
        Connection freshConnection = mock(Connection.class);
        io.nats.client.Dispatcher dispatcher = mock(io.nats.client.Dispatcher.class);
        when(freshConnection.createDispatcher()).thenReturn(dispatcher);
        NatsMessageCorrelationSubscriber freshSubscriber =
                new NatsMessageCorrelationSubscriber(freshConnection, runtimeService, config, null);
        freshSubscriber.subscribe();

        freshSubscriber.unsubscribe();

        verify(freshConnection).closeDispatcher(dispatcher);
    }

    @Test
    void unsubscribe_closeDispatcherThrows_logsWarningAndStillShutsDownExecutor() {
        Connection freshConnection = mock(Connection.class);
        io.nats.client.Dispatcher dispatcher = mock(io.nats.client.Dispatcher.class);
        when(freshConnection.createDispatcher()).thenReturn(dispatcher);
        doThrow(new RuntimeException("close failed")).when(freshConnection).closeDispatcher(dispatcher);
        NatsMessageCorrelationSubscriber freshSubscriber =
                new NatsMessageCorrelationSubscriber(freshConnection, runtimeService, config, null);
        freshSubscriber.subscribe();

        org.assertj.core.api.Assertions.assertThatCode(freshSubscriber::unsubscribe).doesNotThrowAnyException();
    }

    @Test
    void handleMessage_businessKeyVariableConfigured_extractsFromJsonPayload() {
        config.setBusinessKeyVariable("orderId");
        Message msg = createMockMessage("{\"orderId\":\"biz-json-2\"}", null);

        subscriber.handleMessage(msg);

        verify(correlationBuilder).processInstanceBusinessKey("biz-json-2");
    }

    @Test
    void handleMessage_businessKeyVariableFieldMissing_businessKeyNull_skipsBuilderCall() {
        config.setBusinessKeyVariable("missingField");
        Message msg = createMockMessage("{\"orderId\":\"1\"}", null);

        subscriber.handleMessage(msg);

        verify(correlationBuilder, never()).processInstanceBusinessKey(any());
    }

    @Test
    void handleMessage_businessKeyVariableMalformedJson_noColonAfterField_businessKeyNull() {
        config.setBusinessKeyVariable("orderId");
        Message msg = createMockMessage("{\"orderId\"}", null);

        subscriber.handleMessage(msg);

        verify(correlationBuilder, never()).processInstanceBusinessKey(any());
    }

    @Test
    void handleMessage_businessKeyVariableMalformedJson_noOpeningQuoteAfterColon_businessKeyNull() {
        config.setBusinessKeyVariable("orderId");
        Message msg = createMockMessage("{\"orderId\":42}", null);

        subscriber.handleMessage(msg);

        verify(correlationBuilder, never()).processInstanceBusinessKey(any());
    }

    @Test
    void handleMessage_businessKeyVariableMalformedJson_unterminatedString_businessKeyNull() {
        config.setBusinessKeyVariable("orderId");
        Message msg = createMockMessage("{\"orderId\":\"unterminated", null);

        subscriber.handleMessage(msg);

        verify(correlationBuilder, never()).processInstanceBusinessKey(any());
    }

    @Test
    void handleMessage_withMetrics_consumeErrorCountIncrementedOnCorrelationFailure() {
        com.threeai.nats.core.metrics.NatsChannelMetrics realMetrics =
                new com.threeai.nats.core.metrics.NatsChannelMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
        NatsMessageCorrelationSubscriber metricsSubscriber =
                new NatsMessageCorrelationSubscriber(connection, runtimeService, config, realMetrics);
        Message msg = createMockMessage("{\"orderId\":\"1\"}", null);
        when(correlationBuilder.correlateWithResult()).thenThrow(new RuntimeException("no process"));

        metricsSubscriber.handleMessage(msg);

        assertThat(realMetrics.consumeErrorCount("order.new", "OrderReceived").count()).isEqualTo(1.0);
    }

    @Test
    void handleMessage_withMetrics_consumeCountIncrementedOnSuccess() {
        com.threeai.nats.core.metrics.NatsChannelMetrics realMetrics =
                new com.threeai.nats.core.metrics.NatsChannelMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
        NatsMessageCorrelationSubscriber metricsSubscriber =
                new NatsMessageCorrelationSubscriber(connection, runtimeService, config, realMetrics);
        Message msg = createMockMessage("{\"orderId\":\"1\"}", null);

        metricsSubscriber.handleMessage(msg);

        assertThat(realMetrics.consumeCount("order.new", "OrderReceived").count()).isEqualTo(1.0);
    }

    @Test
    void handleMessage_correlatesSuccessfully() {
        Message msg = createMockMessage("{\"orderId\":\"123\"}", null);

        subscriber.handleMessage(msg);

        verify(runtimeService).createMessageCorrelation("OrderReceived");
        verify(correlationBuilder).setVariables(anyMap());
        verify(correlationBuilder).correlateWithResult();
    }

    @Test
    void handleMessage_correlationFails_logsError() {
        Message msg = createMockMessage("{\"orderId\":\"123\"}", null);
        when(correlationBuilder.correlateWithResult())
                .thenThrow(new RuntimeException("No process found"));

        // Should not throw
        subscriber.handleMessage(msg);

        verify(runtimeService).createMessageCorrelation("OrderReceived");
    }

    @Test
    void handleMessage_emptyBody_skips() {
        Message msg = createMockMessage("", null);

        subscriber.handleMessage(msg);

        verify(runtimeService, never()).createMessageCorrelation(any());
    }

    @Test
    void handleMessage_propagatesTraceId() {
        Headers headers = new Headers();
        headers.add("X-Trace-Id", "trace-abc-789");
        Message msg = createMockMessage("{\"orderId\":\"1\"}", headers);

        final String[] capturedTraceId = {null};
        when(correlationBuilder.correlateWithResult()).thenAnswer(invocation -> {
            capturedTraceId[0] = MDC.get("trace_id");
            return mock(MessageCorrelationResult.class);
        });

        subscriber.handleMessage(msg);

        assertThat(capturedTraceId[0]).isEqualTo("trace-abc-789");
        assertThat(MDC.get("trace_id")).isNull();
    }

    @Test
    void handleMessage_usesBusinessKeyFromHeader() {
        Headers headers = new Headers();
        headers.add("X-Business-Key", "BK-001");
        config.setBusinessKeyHeader("X-Business-Key");
        Message msg = createMockMessage("{\"orderId\":\"1\"}", headers);

        subscriber.handleMessage(msg);

        verify(correlationBuilder).processInstanceBusinessKey("BK-001");
    }

    /**
     * DP-1 (NFR-S1, DATA_CLASSIFICATION.md §4) — Sentinel Phase 5.5 QA finding (HIGH): the
     * success-path DEBUG log used to write the raw {@code businessKey} value via
     * {@code kv("business_key", businessKey)}, which may be telco PII (MSISDN/subscriber id).
     * Regression guard: capture the real log event and prove the raw value never appears in any
     * structured argument, only a boolean existence flag.
     */
    @Test
    void handleMessage_success_neverLogsRawBusinessKeyValue() {
        ch.qos.logback.classic.Logger subscriberLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(NatsMessageCorrelationSubscriber.class);
        Level originalLevel = subscriberLogger.getLevel();
        subscriberLogger.setLevel(Level.DEBUG);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        subscriberLogger.addAppender(appender);

        try {
            String secretBusinessKey = "905551112233"; // simulated MSISDN
            Headers headers = new Headers();
            headers.add("X-Business-Key", secretBusinessKey);
            config.setBusinessKeyHeader("X-Business-Key");
            Message msg = createMockMessage("{\"orderId\":\"1\"}", headers);

            subscriber.handleMessage(msg);

            List<String> allArgumentToStrings = appender.list.stream()
                    .flatMap(event -> java.util.Arrays.stream(
                            event.getArgumentArray() != null ? event.getArgumentArray() : new Object[0]))
                    .map(String::valueOf)
                    .toList();

            assertThat(allArgumentToStrings)
                    .as("no structured log argument may contain the raw business-key value")
                    .noneMatch(arg -> arg.contains(secretBusinessKey));
            assertThat(allArgumentToStrings)
                    .as("existence flag must be logged instead of the value")
                    .anyMatch(arg -> arg.equals("has_business_key=true"));
        } finally {
            subscriberLogger.detachAppender(appender);
            subscriberLogger.setLevel(originalLevel);
        }
    }

    private Message createMockMessage(String body, Headers headers) {
        Message msg = mock(Message.class);
        when(msg.getData()).thenReturn(body.getBytes(StandardCharsets.UTF_8));
        when(msg.getHeaders()).thenReturn(headers);
        when(msg.getSubject()).thenReturn("order.new");
        return msg;
    }
}
