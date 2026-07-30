package com.threeai.nats.core.history.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HistoryExceptionsTest {

    @Test
    void retentionAuditLogWriteFailedException_carriesMessageAndCause() {
        Throwable cause = new RuntimeException("db down");
        RetentionAuditLogWriteFailedException ex =
                new RetentionAuditLogWriteFailedException("audit write failed", cause);

        assertThat(ex.getMessage()).isEqualTo("audit write failed");
        assertThat(ex.getCause()).isSameAs(cause);
        assertThat(ex).isInstanceOf(RuntimeException.class);
    }

    @Test
    void erasureVerificationFailedException_messageOnlyConstructor() {
        ErasureVerificationFailedException ex = new ErasureVerificationFailedException("still visible");

        assertThat(ex.getMessage()).isEqualTo("still visible");
        assertThat(ex.getCause()).isNull();
    }

    @Test
    void erasureVerificationFailedException_messageAndCauseConstructor() {
        Throwable cause = new RuntimeException("query failed");
        ErasureVerificationFailedException ex = new ErasureVerificationFailedException("still visible", cause);

        assertThat(ex.getCause()).isSameAs(cause);
    }

    @Test
    void pseudonymVaultAccessDeniedException_isSecurityException() {
        PseudonymVaultAccessDeniedException ex = new PseudonymVaultAccessDeniedException("unauthorized reidentify");

        assertThat(ex).isInstanceOf(SecurityException.class);
        assertThat(ex.getMessage()).isEqualTo("unauthorized reidentify");
    }

    @Test
    void historyLevelAuditCriticalMismatchWarning_carriesFieldsAndToString() {
        HistoryLevelAuditCriticalMismatchWarning warning =
                new HistoryLevelAuditCriticalMismatchWarning("OP_LOG", "camunda");

        assertThat(warning.historyClass()).isEqualTo("OP_LOG");
        assertThat(warning.engineId()).isEqualTo("camunda");
        assertThat(warning.toString()).contains("OP_LOG").contains("camunda");
    }
}
