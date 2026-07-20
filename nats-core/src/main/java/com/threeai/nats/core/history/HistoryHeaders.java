package com.threeai.nats.core.history;

/**
 * History-specific NATS header names (basamak-2, `asyncapi.yaml` {@code HistoryHeaders} schema —
 * {@code docs/sentinel/step2/phase3/api/asyncapi.yaml} components/schemas/HistoryHeaders, IR-2).
 * The three basamak-1 headers this schema also requires (trace id / business key / idempotency
 * key) are NOT redeclared here — callers reuse {@link com.threeai.nats.core.headers.BpmHeaders}
 * verbatim (same header names, same extraction helper), and the dedup header is the
 * NATS-JetStream-standard {@code Nats-Msg-Id} ({@code io.nats.client.support.NatsJetStreamConstants.MSG_ID_HDR}).
 */
public final class HistoryHeaders {

    /** Motor örneği kimliği (INTERNAL). Required (asyncapi HistoryHeaders.required). */
    public static final String ENGINE_ID = "X-Cadenzaflow-History-Engine-Id";

    /** ACT_HI event sınıfı (PUBLIC, düşük-kardinalite). Required. */
    public static final String CLASS = "X-Cadenzaflow-History-Class";

    /** Event tipi (create/update/delete/complete vb.; INTERNAL). Required. */
    public static final String EVENT_TYPE = "X-Cadenzaflow-History-Event-Type";

    /** Motor history event surrogate id (INTERNAL; dedup anahtarının parçası). Required. */
    public static final String EVENT_ID = "X-Cadenzaflow-History-Event-Id";

    /** Process-instance id (PSEUDONYMOUS; partition/sıra anahtarı). Required. */
    public static final String PROCESS_INSTANCE_ID = "X-Cadenzaflow-History-Process-Instance-Id";

    private HistoryHeaders() {
    }
}
