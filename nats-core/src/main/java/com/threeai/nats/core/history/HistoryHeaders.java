package com.threeai.nats.core.history;

/**
 * History-specific NATS header names (increment 2, AsyncAPI
 * {@code components/schemas/HistoryHeaders} schema, IR-2).
 * The three increment 1 headers this schema also requires (trace id / business key / idempotency
 * key) are NOT redeclared here — callers reuse {@link com.threeai.nats.core.headers.BpmHeaders}
 * verbatim (same header names, same extraction helper), and the dedup header is the
 * NATS-JetStream-standard {@code Nats-Msg-Id} ({@code io.nats.client.support.NatsJetStreamConstants.MSG_ID_HDR}).
 */
public final class HistoryHeaders {

    /** Engine instance id (INTERNAL). Required (asyncapi HistoryHeaders.required). */
    public static final String ENGINE_ID = "X-Cadenzaflow-History-Engine-Id";

    /** ACT_HI event class (PUBLIC, low-cardinality). Required. */
    public static final String CLASS = "X-Cadenzaflow-History-Class";

    /** Event type (create/update/delete/complete etc.; INTERNAL). Required. */
    public static final String EVENT_TYPE = "X-Cadenzaflow-History-Event-Type";

    /** Engine history event surrogate id (INTERNAL; part of the dedup key). Required. */
    public static final String EVENT_ID = "X-Cadenzaflow-History-Event-Id";

    /** Process-instance id (PSEUDONYMOUS; partition/ordering key). Required. */
    public static final String PROCESS_INSTANCE_ID = "X-Cadenzaflow-History-Process-Instance-Id";

    /**
     * The engine's ACTUAL history-event timestamp (INTERNAL; NOT the ingest/consume time) —
     * epoch-millis, UTC, string-encoded long. Required (FINDING-001, code review): it populates
     * the projection {@code event_time} column, which is a component of the append-log dedup
     * unique key, the range-partition ANCHOR (PARTITION BY RANGE(event_time)) and part of the PK —
     * it is NEVER filled from the consumer's own clock.
     */
    public static final String EVENT_TIME = "X-Cadenzaflow-History-Event-Time";

    private HistoryHeaders() {
    }
}
