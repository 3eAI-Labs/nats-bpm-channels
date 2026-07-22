-- Test-only fixture for ContentAddressedLargePayloadStoreTest -- a nats-core-local copy of the
-- FINAL (post-V4) projection_large_payload shape from nats-history-projection's real migration
-- chain (V1__entity_lifecycle_tables.sql + V4__large_payload_content_addressing.sql). Duplicated
-- here (rather than depended on) because nats-history-projection depends ON nats-core, so a
-- reverse test-scope dependency would create a Maven reactor cycle. The REAL migration chain is
-- exercised end-to-end by nats-history-projection's own ProjectionStoreTest/ProjectionStore
-- integration tests -- this fixture only needs to be shape-compatible for THIS class's SQL.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE projection_large_payload (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_table  VARCHAR(64) NOT NULL,
    payload_bytes BYTEA NOT NULL,
    content_hash  CHAR(64) NOT NULL,
    ref_count     INTEGER NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT unq_projection_large_payload_content_hash UNIQUE (content_hash)
);
