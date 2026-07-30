package com.threeai.nats.core.headers;

/**
 * Outbound-handoff-specific NATS header names (increment 4, decision D-E').
 * Mirrors the naming convention {@link com.threeai.nats.core.history.HistoryHeaders} established
 * for increment 2. The three increment 1 headers (trace id / business key / idempotency key) are NOT
 * redeclared here — callers reuse {@link BpmHeaders} verbatim, and the dedup header is the
 * NATS-JetStream-standard {@code Nats-Msg-Id} ({@code io.nats.client.support.NatsJetStreamConstants.MSG_ID_HDR}).
 */
public final class OutboundHeaders {

    /** Engine instance id (INTERNAL). Same value as the subject's second segment. */
    public static final String ENGINE_ID = "X-Cadenzaflow-Outbound-Engine-Id";

    /** Tenant-defined outbound message type (PUBLIC, D-C' classification key). */
    public static final String MESSAGE_TYPE = "X-Cadenzaflow-Outbound-Message-Type";

    /** Process-instance id (PSEUDONYMOUS; subject/partition key, D-E'). */
    public static final String PROCESS_INSTANCE_ID = "X-Cadenzaflow-Outbound-Process-Instance-Id";

    /** {@code CRITICAL}/{@code BEST_EFFORT} — D-C' classification result, informational (observability only). */
    public static final String CLASSIFICATION = "X-Cadenzaflow-Outbound-Classification";

    private OutboundHeaders() {
    }
}
