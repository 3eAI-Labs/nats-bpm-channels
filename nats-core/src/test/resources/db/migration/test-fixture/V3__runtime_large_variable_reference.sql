-- Test-only fixture for ContentAddressedLargePayloadStoreTest -- a nats-core-local copy of the
-- FINAL (post-V5) runtime_large_variable_ref shape from nats-history-projection's real migration
-- chain (V5__runtime_large_variable_reference.sql). Duplicated here for the same reactor-cycle
-- reason as V2__large_payload_store.sql (see that file's header).
CREATE TABLE runtime_large_variable_ref (
    engine_id     VARCHAR(80)  NOT NULL,
    variable_id   VARCHAR(64)  NOT NULL,
    payload_id    UUID         NOT NULL REFERENCES projection_large_payload(id),
    content_hash  CHAR(64)     NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (engine_id, variable_id)
);

CREATE INDEX idx_runtime_large_variable_ref_payload ON runtime_large_variable_ref (payload_id);
