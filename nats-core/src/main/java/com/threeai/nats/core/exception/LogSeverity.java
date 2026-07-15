package com.threeai.nats.core.exception;

/**
 * Log-severity taxonomy used by the error registry (`docs/sentinel/phase4/ERROR_REGISTRY.md` §1).
 * {@code CRITICAL} is ERROR plus a paging side-effect (SYS_SENTINEL_WORKER_CONFLICT,
 * ERROR_REGISTRY.md §4.1) — it is not a distinct SLF4J level, only a documentation/log-marker
 * concept layered on top of ERROR.
 */
public enum LogSeverity {
    DEBUG, INFO, WARN, ERROR, CRITICAL
}
