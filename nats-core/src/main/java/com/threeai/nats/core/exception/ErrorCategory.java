package com.threeai.nats.core.exception;

/**
 * Error-code category taxonomy, unchanged from the design's exception-code list and the
 * error-handling guideline — this increment only binds the taxonomy to Java
 * classes/log fields.
 */
public enum ErrorCategory {
    VAL, BUS, RES, SYS, EXT
}
