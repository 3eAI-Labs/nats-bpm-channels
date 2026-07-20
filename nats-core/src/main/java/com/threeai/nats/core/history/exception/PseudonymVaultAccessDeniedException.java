package com.threeai.nats.core.history.exception;

/**
 * {@code AUTH_PSEUDONYM_VAULT_ACCESS_DENIED} — CRITICAL, security-page (`ERROR_REGISTRY.md` §3.11
 * row 41). Thrown by {@code PseudonymizationVaultClient.reidentify(...)} when the caller invokes
 * re-identification without prior authorization (defense-in-depth, DP-16) — the access-control
 * decision itself is made by the caller's own authz layer BEFORE this method is invoked; this
 * exception is the vault's own second-layer refusal.
 */
public class PseudonymVaultAccessDeniedException extends SecurityException {

    private static final long serialVersionUID = 1L;

    public PseudonymVaultAccessDeniedException(String message) {
        super(message);
    }
}
