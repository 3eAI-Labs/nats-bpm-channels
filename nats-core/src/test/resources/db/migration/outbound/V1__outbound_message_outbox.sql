-- Migration: V1__outbound_message_outbox.sql
-- Target database: ENGINE DB (same Postgres instance/schema as ACT_RU_*/ACT_HI_*; tx-atomic with runtime writes)
-- Owner: NatsOutboundPublisher (writer, via OutboundMessageOutboxWriter) / OutboundMessageRelay (reader+deleter)
-- Basamak-4 (docs/09-outbound-handoff.md) -- kilitli kararlar D-A'/D-C'/D-E'/D-F'
--
-- CODER-NOTE (phase-5 return report): docs/09 Sec.3 module list places this table/migration under
-- "nats-core". This repo places it here instead (byte-mirrored into cadenzaflow-nats-channel),
-- following the SAME precedent compact_history_outbox already established in basamak-2: an
-- ENGINE-DB table must be migrated via the OWNING engine module's own Flyway/tenant migration
-- chain -- nats-core has no engine DataSource of its own to run a migration against (it is a
-- shared library, not a deployable app). This is applied by the TENANT alongside their engine
-- schema migrations (Phase 5+ concern: Flyway/Liquibase changelog integration), same as
-- compact_history_outbox.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================================
-- outbound_message_outbox -- critical outbound message (message-throw/send-task), <=1 row/tx.
-- Written by NatsOutboundPublisher (via OutboundMessageOutboxWriter) in the SAME transaction as
-- the runtime engine write (D-A'/D-C' critical classification). Read + deleted by
-- OutboundMessageRelay after PubAck (custody-transfer, D-F'). Never updated -- write-once, delete-once.
-- ============================================================================
CREATE TABLE outbound_message_outbox (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),  -- dedup key (Nats-Msg-Id) at relay time
    engine_id           VARCHAR(80)  NOT NULL,
    message_type        VARCHAR(255) NOT NULL,          -- tenant-defined outbound message type (D-C' classification key)
    process_instance_id VARCHAR(64)  NOT NULL,          -- subject/partition key (D-E')
    business_key        VARCHAR(255),                   -- CONFIDENTIAL/koşullu PII; never in subject
    trace_id            VARCHAR(64)  NOT NULL,
    subject             VARCHAR(600) NOT NULL,           -- events.<engineId>.<type>.<processInstanceId> (D-E')
    payload             BYTEA        NOT NULL,           -- pre-serialized wire envelope (OutboundWireMessageFactory)
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now()
);

COMMENT ON TABLE outbound_message_outbox IS
    'Critical outbound BPM message, durable in-tx handoff row (D-A''/D-F''). Relay deletes after PubAck (custody-transfer).';
COMMENT ON COLUMN outbound_message_outbox.payload IS
    'Pre-serialized wire envelope -- may carry tenant-opted-in process variables (PII opt-in, DP-1); short exposure window (relay deletes post-PubAck).';

-- Relay reads oldest-first (fairness + stuck-row age detection, basamak-2 BA-Q7 precedent).
CREATE INDEX idx_outbound_message_outbox_created_at ON outbound_message_outbox (created_at);
-- Relay's own leader-elected read filters by engine_id in multi-engine deployments.
CREATE INDEX idx_outbound_message_outbox_engine_id ON outbound_message_outbox (engine_id);
