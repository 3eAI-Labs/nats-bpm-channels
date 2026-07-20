package com.threeai.nats.core.history.exception;

/**
 * {@code VAL_HISTORY_LEVEL_AUDIT_CRITICAL_MISMATCH} marker (`ERROR_REGISTRY.md` §3.1 row 2).
 * Bootstrap-time fail-fast is NOT used here (BA-Q4 decision — WARN, not hard-reject) — this is a
 * plain immutable value carrier, never thrown, used only so the bootstrap guard's WARN log
 * ("`08_config.md` §1") has a single, type-safe shape shared across engine modules
 * (`camunda-nats-channel`/`cadenzaflow-nats-channel`) instead of an ad hoc string.
 */
public final class HistoryLevelAuditCriticalMismatchWarning {

    private final String historyClass;
    private final String engineId;

    public HistoryLevelAuditCriticalMismatchWarning(String historyClass, String engineId) {
        this.historyClass = historyClass;
        this.engineId = engineId;
    }

    public String historyClass() {
        return historyClass;
    }

    public String engineId() {
        return engineId;
    }

    @Override
    public String toString() {
        return "HistoryLevelAuditCriticalMismatchWarning{historyClass=" + historyClass
                + ", engineId=" + engineId + "}";
    }
}
