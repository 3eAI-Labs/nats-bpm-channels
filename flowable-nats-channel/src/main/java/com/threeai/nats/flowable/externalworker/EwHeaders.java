package com.threeai.nats.flowable.externalworker;

import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Wire-header contract of the external-worker dispatch slice (docs/11 §2, D-I'v2).
 *
 * <p>Only the NONCE travels on the wire. The full lock-generation token
 * {@code <sentinelWorkerId>#<nonce>} is constructed BRIDGE-side from the locally configured
 * sentinel id — the sentinel half never comes from the wire (docs/11 §2 md. 3, decision
 * N-red-1): trusting a wire-supplied worker id would let a malformed reply masquerade as any
 * lock generation.
 */
public final class EwHeaders {

    /** Nonce header the worker must echo back on the reply (worker-contract difference vs A2). */
    public static final String LOCK_NONCE = "X-Cadenzaflow-Lock-Nonce";

    /** Token rule for the nonce as received from the wire. */
    public static final Pattern NONCE_PATTERN = Pattern.compile("^[A-Za-z0-9_-]{8,}$");

    private EwHeaders() {
    }

    /** 32-char lowercase hex (UUID without dashes) — always satisfies {@link #NONCE_PATTERN}. */
    public static String mintNonce() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
