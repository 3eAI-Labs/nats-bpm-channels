package com.threeai.nats.core.exception;

/**
 * Error-code category taxonomy, unchanged from {@code EXCEPTION_CODES.md} (phase2) and
 * {@code ERROR_HANDLING_GUIDELINE.md} §1.1 — this basamak only binds the taxonomy to Java
 * classes/log fields (`docs/sentinel/phase4/ERROR_REGISTRY.md` §1).
 */
public enum ErrorCategory {
    VAL, BUS, RES, SYS, EXT
}
