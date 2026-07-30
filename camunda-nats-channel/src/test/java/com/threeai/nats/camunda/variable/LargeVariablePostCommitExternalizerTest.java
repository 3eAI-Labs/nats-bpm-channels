package com.threeai.nats.camunda.variable;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;

import com.threeai.nats.core.largepayload.ContentAddressedLargePayloadStore;
import com.threeai.nats.core.largepayload.ContentHash;
import com.threeai.nats.core.largepayload.ExternalizationMarker;
import com.threeai.nats.core.largepayload.LargePayloadReference;
import com.threeai.nats.core.largepayload.LargeVariableExternalizationProperties;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.camunda.bpm.engine.OptimisticLockingException;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.impl.db.entitymanager.DbEntityManager;
import org.camunda.bpm.engine.impl.interceptor.Command;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.interceptor.CommandExecutor;
import org.camunda.bpm.engine.impl.persistence.entity.VariableInstanceEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@code externalizeNow} core logic — the command-executor/DbEntityManager
 * layer is mocked (a real engine is exercised separately by {@link LargeVariableExternalizationE2eTest});
 * this class focuses on branch coverage the happy-path E2E test does not reach (unbound config,
 * already-gone/no-longer-eligible re-checks, optimistic-locking races, generic failures).
 */
@SuppressWarnings("unchecked")
class LargeVariablePostCommitExternalizerTest {

    private ContentAddressedLargePayloadStore largePayloadStore;
    private LargeVariableExternalizationProperties properties;
    private NatsChannelMetrics metrics;
    private LargeVariablePostCommitExternalizer externalizer;
    private ProcessEngineConfigurationImpl configuration;
    private CommandExecutor commandExecutor;
    private CommandContext commandContext;
    private DbEntityManager dbEntityManager;

    @BeforeEach
    void setUp() {
        largePayloadStore = mock(ContentAddressedLargePayloadStore.class);
        properties = new LargeVariableExternalizationProperties();
        properties.setThresholdBytes(10);
        metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        externalizer = new LargeVariablePostCommitExternalizer(largePayloadStore, properties, metrics, "camunda");

        configuration = mock(ProcessEngineConfigurationImpl.class);
        commandExecutor = mock(CommandExecutor.class);
        commandContext = mock(CommandContext.class);
        dbEntityManager = mock(DbEntityManager.class);
        when(configuration.getCommandExecutorTxRequired()).thenReturn(commandExecutor);
        when(commandContext.getDbEntityManager()).thenReturn(dbEntityManager);
        // Drive the Command directly against our mocked CommandContext, like the real
        // CommandExecutor would (just without an actual transaction/DB round trip).
        when(commandExecutor.execute(any(Command.class)))
                .thenAnswer(invocation -> ((Command<?>) invocation.getArgument(0)).execute(commandContext));
    }

    @Test
    void externalizeNow_configurationNotBound_logsWarnDoesNotThrow() {
        assertThatCode(() -> externalizer.externalizeNow("var-1")).doesNotThrowAnyException();
        verify(largePayloadStore, never()).storeAndAcquireReference(any(), any());
    }

    @Test
    void externalizeNow_entityNotFound_noopDoesNotThrow() {
        externalizer.bindConfiguration(configuration);
        when(dbEntityManager.selectById(VariableInstanceEntity.class, "var-gone")).thenReturn(null);

        assertThatCode(() -> externalizer.externalizeNow("var-gone")).doesNotThrowAnyException();
        verify(largePayloadStore, never()).storeAndAcquireReference(any(), any());
    }

    @Test
    void externalizeNow_alreadyExternalized_noop() {
        externalizer.bindConfiguration(configuration);
        VariableInstanceEntity entity = mock(VariableInstanceEntity.class);
        String hash = ContentHash.sha256Hex("x".getBytes(StandardCharsets.UTF_8));
        when(entity.getByteArrayValue()).thenReturn(ExternalizationMarker.encode(hash));
        when(dbEntityManager.selectById(VariableInstanceEntity.class, "var-1")).thenReturn(entity);

        externalizer.externalizeNow("var-1");

        verify(largePayloadStore, never()).storeAndAcquireReference(any(), any());
        verify(entity, never()).setByteArrayValue(any());
    }

    @Test
    void externalizeNow_noLongerEligible_underThreshold_noop() {
        externalizer.bindConfiguration(configuration);
        VariableInstanceEntity entity = mock(VariableInstanceEntity.class);
        when(entity.getByteArrayValue()).thenReturn(new byte[5]); // < threshold(10) -- overwritten smaller since scheduling
        when(dbEntityManager.selectById(VariableInstanceEntity.class, "var-1")).thenReturn(entity);

        externalizer.externalizeNow("var-1");

        verify(largePayloadStore, never()).storeAndAcquireReference(any(), any());
    }

    @Test
    void externalizeNow_eligible_storesAndSetsMarker_incrementsMetric() {
        externalizer.bindConfiguration(configuration);
        VariableInstanceEntity entity = mock(VariableInstanceEntity.class);
        byte[] current = "large-enough-content".getBytes(StandardCharsets.UTF_8); // > threshold(10)
        when(entity.getByteArrayValue()).thenReturn(current);
        when(dbEntityManager.selectById(VariableInstanceEntity.class, "var-1")).thenReturn(entity);
        String hash = ContentHash.sha256Hex(current);
        when(largePayloadStore.storeAndAcquireReference(current, "runtime.camunda"))
                .thenReturn(new LargePayloadReference(java.util.UUID.randomUUID(), hash, 1, true));

        externalizer.externalizeNow("var-1");

        verify(entity).setByteArrayValue(ExternalizationMarker.encode(hash));
        assertThatCode(() -> metrics.largeVariableExternalizedCount("camunda")).doesNotThrowAnyException();
    }

    @Test
    void externalizeNow_optimisticLockingException_doesNotPropagate() {
        externalizer.bindConfiguration(configuration);
        when(commandExecutor.execute(any(Command.class))).thenThrow(new OptimisticLockingException("raced"));

        assertThatCode(() -> externalizer.externalizeNow("var-1")).doesNotThrowAnyException();
    }

    @Test
    void externalizeNow_genericFailure_doesNotPropagate() {
        externalizer.bindConfiguration(configuration);
        when(commandExecutor.execute(any(Command.class))).thenThrow(new RuntimeException("DB down"));

        assertThatCode(() -> externalizer.externalizeNow("var-1")).doesNotThrowAnyException();
    }
}
