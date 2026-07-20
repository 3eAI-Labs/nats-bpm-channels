package com.threeai.nats.core.history.exception;

/**
 * {@code SYS_RETENTION_AUDIT_LOG_WRITE_FAILED} — CRITICAL, on-call page (`ERROR_REGISTRY.md` §3.9
 * row 33). Thrown by {@code RetentionEnforcementJob.enforceRetention()} when a partition
 * DROP/DETACH succeeded but the mandatory {@code retention_audit_log} write failed — a
 * compliance-invariant violation (`DATA_GOVERNANCE v4.0 §4.4`: every deletion MUST have an audit
 * row).
 */
public class RetentionAuditLogWriteFailedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RetentionAuditLogWriteFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
