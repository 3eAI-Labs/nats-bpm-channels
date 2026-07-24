package com.threeai.nats.cibseven.history;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import com.threeai.nats.core.history.HistoryClassNames;
import org.cibseven.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.cibseven.bpm.engine.impl.history.HistoryLevelFull;
import org.cibseven.bpm.engine.impl.history.HistoryLevelNone;
import org.junit.jupiter.api.Test;

class HistoryBootstrapValidatorTest {

    @Test
    void validate_historyLevelFull_producesAllConfiguredClasses_noWarnNoThrow() {
        ProcessEngineConfigurationImpl configuration = mock(ProcessEngineConfigurationImpl.class);
        when(configuration.getHistoryLevel()).thenReturn(new HistoryLevelFull());

        HistoryClassificationProperties classification = new HistoryClassificationProperties();
        classification.setAuditCriticalClasses(HistoryClassNames.DEFAULT_AUDIT_CRITICAL_CLASSES);

        // BA-Q4: WARN-only, never throws -- validate() completing without exception is the contract.
        assertThatCode(() -> HistoryBootstrapValidator.validate(configuration, classification, "cibseven"))
                .doesNotThrowAnyException();
    }

    @Test
    void validate_historyLevelNone_mismatchIsWarnOnly_neverThrows() {
        ProcessEngineConfigurationImpl configuration = mock(ProcessEngineConfigurationImpl.class);
        when(configuration.getHistoryLevel()).thenReturn(new HistoryLevelNone());

        HistoryClassificationProperties classification = new HistoryClassificationProperties();
        classification.setAuditCriticalClasses(HistoryClassNames.DEFAULT_AUDIT_CRITICAL_CLASSES);

        // BA-Q4 KARAR: mismatch is WARN, not hard-reject -- must not throw even though HistoryLevelNone
        // produces nothing.
        assertThatCode(() -> HistoryBootstrapValidator.validate(configuration, classification, "cibseven"))
                .doesNotThrowAnyException();
    }

    @Test
    void validate_unmappedClass_skippedWithoutError() {
        ProcessEngineConfigurationImpl configuration = mock(ProcessEngineConfigurationImpl.class);
        when(configuration.getHistoryLevel()).thenReturn(new HistoryLevelFull());

        HistoryClassificationProperties classification = new HistoryClassificationProperties();
        classification.setAuditCriticalClasses(Set.of(HistoryClassNames.COMMENT, HistoryClassNames.ATTACHMENT));

        assertThatCode(() -> HistoryBootstrapValidator.validate(configuration, classification, "cibseven"))
                .doesNotThrowAnyException();
    }
}
