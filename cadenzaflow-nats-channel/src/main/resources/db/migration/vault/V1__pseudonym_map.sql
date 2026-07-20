-- Migration: 001_pseudonym_map.sql
-- Target database: PSEUDONYM VAULT Postgres (ARCH-Q2 KARAR 2026-07-18 -- separate instance/schema,
-- isolated from BOTH engine DB and projection DB; L4-adjacent, DP-16, ADR-0016)
-- Owner: PseudonymizationVault -- see DB_ACCESS_MAP.md §3
--
-- Design: pseudonym VALUE is a deterministic keyed-hash computed IN-TX by NatsHistoryEventHandler
-- (BA-Q5, pure/no I/O) -- pseudonym_token is therefore already known before the vault is ever
-- contacted; the vault's job is to durably persist the reverse (token -> real identity) mapping,
-- downstream/async (BA-Q5), for rare authorized re-identification, and to make "delete = tersinmez"
-- (ADR-0016) a single-row hard DELETE.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================================
-- pseudonym_map -- kimlik<->takma-ad haritası. HARD DELETE on erasure (not soft-delete): the entire
-- point of "silme = harita-kaydı silme" (ADR-0016) is that no residual reversible value remains once
-- deleted -- a soft-deleted row with real_user_id_encrypted still present would not satisfy
-- "tersinmez" under key-holder access. Encrypted at rest twice over: disk-level (NFR-S8 infra) AND
-- column-level via pgcrypto (defense-in-depth for the single highest-sensitivity column in the system).
-- ============================================================================
CREATE TABLE pseudonym_map (
    pseudonym_token       VARCHAR(128) PRIMARY KEY,               -- deterministic keyed-hash (BA-Q5), computed by caller
    engine_id             VARCHAR(80)  NOT NULL,
    tenant_key_version    INTEGER      NOT NULL,                  -- which keyed-hash key produced this token (rotation tracking)
    real_user_id_encrypted BYTEA       NOT NULL,                  -- pgcrypto pgp_sym_encrypt(real_user_id, vault_column_key)
    source_class          VARCHAR(40)  NOT NULL,                  -- OP_LOG | INCIDENT | EXT_TASK_LOG (audit-kritik only)
    first_seen_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT now()
);

COMMENT ON TABLE pseudonym_map IS
    'ADR-0016 L4-adjacent re-identification key. DELETE FROM pseudonym_map = BUS_PSEUDONYM_MAP_ENTRY_DELETED (irreversible). Never soft-deleted.';
COMMENT ON COLUMN pseudonym_map.real_user_id_encrypted IS
    'pgcrypto-encrypted (defense-in-depth over disk-level AES-256, NFR-S8); decryption key held outside this database (OpenBao/KMS, deploy-time).';

-- ============================================================================
-- vault_access_audit -- every WRITE/READ/DELETE/REIDENTIFY_ATTEMPT (DP-16: en-az-yetki + audit).
-- Unauthorized attempts are logged too (granted=false) -- AUTH_PSEUDONYM_VAULT_ACCESS_DENIED trail.
-- ============================================================================
CREATE TABLE vault_access_audit (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pseudonym_token   VARCHAR(128) NOT NULL,
    operation         VARCHAR(20)  NOT NULL,                     -- WRITE | READ | DELETE | REIDENTIFY_ATTEMPT
    accessor_identity VARCHAR(255) NOT NULL,
    access_reason     VARCHAR(500),
    granted           BOOLEAN      NOT NULL,
    occurred_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT chk_vault_access_audit_operation
        CHECK (operation IN ('WRITE','READ','DELETE','REIDENTIFY_ATTEMPT'))
);
CREATE INDEX idx_vault_access_audit_token ON vault_access_audit (pseudonym_token);
CREATE INDEX idx_vault_access_audit_denied ON vault_access_audit (occurred_at) WHERE granted = FALSE;

COMMENT ON TABLE vault_access_audit IS
    'DP-16 en-az-yetki + audit. granted=false rows are the AUTH_PSEUDONYM_VAULT_ACCESS_DENIED (CRITICAL, security-page) evidence trail.';
