package com.threeai.nats.core.history.exception;

/**
 * {@code RES_ERASURE_VERIFICATION_FAILED} — CRITICAL, compliance-risk (`ERROR_REGISTRY.md` §3.10
 * row 38). Thrown by {@code ErasurePipeline.executeAnonymization(...)} when the post-erasure
 * verification query (via {@code HistoryQueryApi}) shows the subject's PII still surfaces — KVKK
 * 30-day SLA risk.
 */
public class ErasureVerificationFailedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ErasureVerificationFailedException(String message) {
        super(message);
    }

    public ErasureVerificationFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
