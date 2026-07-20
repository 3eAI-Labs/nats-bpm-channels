-- Migration: 001_compact_history_outbox.sql
-- Target database: ENGINE DB (same Postgres instance/schema as ACT_RU_*/ACT_HI_*; tx-atomic with runtime writes)
-- Owner: NatsHistoryEventHandler (writer) / HistoryOutboxRelay (reader+deleter) -- see DB_ACCESS_MAP.md §1
-- ADR: 0010 (hybrid publish topology) + ARCH-Q1 (payload = reference, not inline)
-- BR: BR-HDL-003, BR-REL-001 | FR: FR-A4, FR-B1 | US: US-A3, US-B1
--
-- IMPORTANT: this is the ONLY table basamak-2 adds to the engine database. It does NOT touch
-- ACT_RU_*/ACT_HI_* (fork-owned, migrated by the engine's own Liquibase/DDL -- basamak-1 precedent,
-- docs/sentinel/phase4/DB_ACCESS_MAP.md §0). This migration is applied by the TENANT alongside their
-- engine schema migrations (Phase 5 concern: Flyway/Liquibase changelog integration).

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================================
-- compact_history_outbox -- audit-critical event, <=1 row/tx (NFR-P2).
-- Written by NatsHistoryEventHandler in the SAME transaction as the runtime
-- engine write (BR-HDL-003). Read + deleted by HistoryOutboxRelay after PubAck
-- (custody-transfer, BR-REL-001). Never updated -- write-once, delete-once.
-- ============================================================================
CREATE TABLE compact_history_outbox (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    history_event_id    VARCHAR(64)  NOT NULL,          -- dedup component 1 (Nats-Msg-Id)
    event_type          VARCHAR(64)  NOT NULL,          -- dedup component 2 (Nats-Msg-Id)
    history_class       VARCHAR(40)  NOT NULL,          -- OP_LOG | INCIDENT | EXT_TASK_LOG | tenant override
    engine_id           VARCHAR(80)  NOT NULL,
    process_instance_id VARCHAR(64)  NOT NULL,          -- subject/partition key (D-E)
    business_key        VARCHAR(255),                   -- CONFIDENTIAL/koşullu PII (DP-2/DP-8); never in subject
    -- ARCH-Q1 (2026-07-18): payload taşıma = referans. payload_scalar carries the small/bounded
    -- audit-critical fields inline (userId, operationType, entityType, property, orgValue, newValue,
    -- incidentMessage, configuration, activityId, workerId, errorMessage) as an opaque compact blob --
    -- NOT the full normalized ACT_HI multi-column shape. payload_large_ref is a soft pointer (no FK,
    -- see rationale in DB_SCHEMA.md §2.1) to compact_history_outbox_payload for the rare large-byte
    -- case (typically EXT_TASK_LOG.errorDetails stack traces).
    payload_scalar      JSONB        NOT NULL,
    payload_large_ref   UUID,
    event_time          TIMESTAMPTZ  NOT NULL,           -- source HistoryEvent timestamp (display only, ADR-0012)
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT unq_compact_history_outbox_event UNIQUE (history_event_id, event_type)
);

COMMENT ON TABLE compact_history_outbox IS
    'Audit-critical history event, durable in-tx handoff row (ADR-0010). Relay deletes after PubAck (custody-transfer).';
COMMENT ON COLUMN compact_history_outbox.payload_scalar IS
    'Compact scalar fields only -- RESTRICTED/PII (DP-12), short exposure window (relay deletes post-PubAck).';

-- Relay reads oldest-first (fairness + SYS_OUTBOX_ROW_STUCK age detection, BA-Q7).
CREATE INDEX idx_compact_history_outbox_created_at ON compact_history_outbox (created_at);
-- Relay's own leader-elected read filters by engine_id in multi-engine deployments.
CREATE INDEX idx_compact_history_outbox_engine_id ON compact_history_outbox (engine_id);

-- ============================================================================
-- compact_history_outbox_payload -- optional large-byte companion (ARCH-Q1 "referans").
-- Written in the SAME transaction as the parent outbox row, ONLY when the source
-- HistoryEvent carries a byte-array payload (mirrors ByteArrayEntity(...,HISTORY),
-- DbHistoryEventHandler.java:97-105 -- but captured independently by our own writer
-- so reconstruction does not depend on ACT_GE_BYTEARRAY still being written once the
-- class is eventually cut over, BR-HDL-005). See LLD-Q2 (01_overview.md) for the
-- write-count interpretation of NFR-P2 when this companion row is present.
-- ============================================================================
CREATE TABLE compact_history_outbox_payload (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    outbox_row_id  UUID NOT NULL REFERENCES compact_history_outbox(id) ON DELETE CASCADE,
    payload_bytes  BYTEA NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_compact_history_outbox_payload_outbox_row_id
    ON compact_history_outbox_payload (outbox_row_id);

COMMENT ON TABLE compact_history_outbox_payload IS
    'Large byte-array companion for compact_history_outbox (ARCH-Q1 reference target); ON DELETE CASCADE keeps custody-transfer atomic.';
