package com.threeai.nats.core.headers;

/**
 * Outbound-handoff-specific NATS header names (basamak-4, docs/09-outbound-handoff.md D-E').
 * Mirrors the naming convention {@link com.threeai.nats.core.history.HistoryHeaders} established
 * for basamak-2. The three basamak-1 headers (trace id / business key / idempotency key) are NOT
 * redeclared here — callers reuse {@link BpmHeaders} verbatim, and the dedup header is the
 * NATS-JetStream-standard {@code Nats-Msg-Id} ({@code io.nats.client.support.NatsJetStreamConstants.MSG_ID_HDR}).
 */
public final class OutboundHeaders {

    /** Motor örneği kimliği (INTERNAL). Subject'in ikinci segmenti ile aynı değer. */
    public static final String ENGINE_ID = "X-Cadenzaflow-Outbound-Engine-Id";

    /** Tenant-tanımlı çıkış mesaj tipi (PUBLIC, D-C' sınıflandırma anahtarı). */
    public static final String MESSAGE_TYPE = "X-Cadenzaflow-Outbound-Message-Type";

    /** Process-instance id (PSEUDONYMOUS; subject/partition anahtarı, D-E'). */
    public static final String PROCESS_INSTANCE_ID = "X-Cadenzaflow-Outbound-Process-Instance-Id";

    /** {@code CRITICAL}/{@code BEST_EFFORT} — D-C' sınıflandırma sonucu, bilgi amaçlı (yalnız gözlemlenebilirlik). */
    public static final String CLASSIFICATION = "X-Cadenzaflow-Outbound-Classification";

    private OutboundHeaders() {
    }
}
