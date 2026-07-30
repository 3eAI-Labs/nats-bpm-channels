package com.threeai.nats.camunda.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import io.nats.client.Connection;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClassCutoverStateRegistryTest {

    private Connection connection;
    private KeyValue keyValue;
    private JetStreamKvManager kvManager;

    @BeforeEach
    void setUp() throws Exception {
        connection = mock(Connection.class);
        keyValue = mock(KeyValue.class);
        kvManager = mock(JetStreamKvManager.class);
        when(connection.keyValue("history-cutover-state")).thenReturn(keyValue);
    }

    @Test
    void loadAtBootstrap_noKvEntries_allClassesDefaultToNotCutOver() throws Exception {
        when(keyValue.get(org.mockito.ArgumentMatchers.anyString())).thenReturn(null);

        ClassCutoverStateRegistry registry = new ClassCutoverStateRegistry(kvManager, connection, "camunda");
        registry.loadAtBootstrap();

        for (String historyClass : HistoryClassNames.ALL_CLASSES) {
            assertThat(registry.isCutOver(historyClass)).isFalse();
        }
    }

    @Test
    void loadAtBootstrap_trueEntry_reflectedInIsCutOver() throws Exception {
        KeyValueEntry opLogEntry = mock(KeyValueEntry.class);
        when(opLogEntry.getValue()).thenReturn("true".getBytes(StandardCharsets.UTF_8));
        when(keyValue.get(eq("cutover.camunda.OP_LOG"))).thenReturn(opLogEntry);
        when(keyValue.get(org.mockito.ArgumentMatchers.argThat(
                key -> key != null && !key.equals("cutover.camunda.OP_LOG")))).thenReturn(null);

        ClassCutoverStateRegistry registry = new ClassCutoverStateRegistry(kvManager, connection, "camunda");
        registry.loadAtBootstrap();

        assertThat(registry.isCutOver(HistoryClassNames.OP_LOG)).isTrue();
        assertThat(registry.isCutOver(HistoryClassNames.INCIDENT)).isFalse();
    }

    @Test
    void loadAtBootstrap_falseEntry_notCutOver() throws Exception {
        KeyValueEntry falseEntry = mock(KeyValueEntry.class);
        when(falseEntry.getValue()).thenReturn("false".getBytes(StandardCharsets.UTF_8));
        when(keyValue.get(org.mockito.ArgumentMatchers.anyString())).thenReturn(falseEntry);

        ClassCutoverStateRegistry registry = new ClassCutoverStateRegistry(kvManager, connection, "camunda");
        registry.loadAtBootstrap();

        assertThat(registry.isCutOver(HistoryClassNames.OP_LOG)).isFalse();
    }

    @Test
    void loadAtBootstrap_bucketUnavailable_defaultsAllToNotCutOver() throws Exception {
        when(connection.keyValue("history-cutover-state")).thenThrow(new IOException("no connection"));

        ClassCutoverStateRegistry registry = new ClassCutoverStateRegistry(kvManager, connection, "camunda");
        registry.loadAtBootstrap();

        assertThat(registry.isCutOver(HistoryClassNames.OP_LOG)).isFalse();
    }

    @Test
    void loadAtBootstrap_perKeyReadFailure_defaultsThatClassOnly() throws Exception {
        when(keyValue.get(eq("cutover.camunda.OP_LOG"))).thenThrow(new RuntimeException("kv read failed"));
        when(keyValue.get(org.mockito.ArgumentMatchers.argThat(
                key -> key != null && !key.equals("cutover.camunda.OP_LOG")))).thenReturn(null);

        ClassCutoverStateRegistry registry = new ClassCutoverStateRegistry(kvManager, connection, "camunda");
        registry.loadAtBootstrap();

        assertThat(registry.isCutOver(HistoryClassNames.OP_LOG)).isFalse();
    }

    @Test
    void isCutOver_beforeBootstrapLoad_defaultsFalse() {
        ClassCutoverStateRegistry registry = new ClassCutoverStateRegistry(kvManager, connection, "camunda");

        assertThat(registry.isCutOver(HistoryClassNames.OP_LOG)).isFalse();
    }
}
