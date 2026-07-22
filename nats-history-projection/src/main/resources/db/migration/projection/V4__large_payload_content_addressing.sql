-- Migration: 004_large_payload_content_addressing.sql
-- Target database: PROJECTION Postgres (same instance as 001/002/003)
-- Owner (write): ProjectionStore.storeLargePayload/releaseLargePayloadReference (nats-history-projection),
--                 LargeVariableExternalizer (camunda-nats-channel/cadenzaflow-nats-channel, basamak-3)
-- Basamak-3 (large-variable externalization, docs/08-large-variable-externalization.md):
--   D-B'/D-G' unified content-addressed store; D-D' RUNTIME+HISTORY 3-copy -> 1 object; D-F' refcount/GC.
--
-- projection_large_payload becomes CONTENT-ADDRESSED: a payload's SHA-256 hash is now the natural
-- dedup key (UNIQUE), and every table that previously stored a private byte-array copy of the SAME
-- logical content (RUNTIME ACT_RU_VARIABLE via the new basamak-3 custom serializer, HISTORY
-- ACT_HI_DETAIL/ACT_HI_VARINST/EXT_TASK_LOG/ATTACHMENT via the existing basamak-2 projection write
-- path) converges on ONE row here. ref_count tracks how many *_ref/*_hash foreign keys currently
-- point at a row; a row is deleted only once ref_count reaches 0 (D-F' GC, never a synchronous
-- per-entity delete -- avoids dangling references under dedup).

ALTER TABLE projection_large_payload ADD COLUMN content_hash CHAR(64);
ALTER TABLE projection_large_payload ADD COLUMN ref_count INTEGER NOT NULL DEFAULT 0;

-- Backfill: every pre-existing row (basamak-2, pre-dedup) gets its content hash computed from the
-- bytes it already stores. digest(...,'sha256') is pgcrypto (already enabled by 001).
UPDATE projection_large_payload
   SET content_hash = encode(digest(payload_bytes, 'sha256'), 'hex')
 WHERE content_hash IS NULL;

-- Merge pre-existing duplicate-content rows onto ONE canonical row per hash (lowest id, deterministic)
-- BEFORE the uniqueness constraint below is added -- basamak-2 never deduplicated, so two rows with
-- byte-identical payload_bytes (e.g. the same repeated EXT_TASK_LOG stack trace) are legal today.
-- Postgres' uuid type has no built-in MIN()/MAX() aggregate (only comparison operators) -- MIN()
-- over the text representation is equivalent (uuid ordering is a bytewise/hex compare, which the
-- canonical hyphenated-hex string representation preserves) and any deterministic pick is fine
-- here anyway (which surviving row becomes canonical is arbitrary, never observed by callers).
CREATE TEMP TABLE bk3_canonical_payload AS
SELECT content_hash, MIN(id::text)::uuid AS canonical_id
FROM projection_large_payload
GROUP BY content_hash;

UPDATE variable_instance_history v
   SET variable_value_ref = c.canonical_id
  FROM projection_large_payload p
  JOIN bk3_canonical_payload c ON c.content_hash = p.content_hash
 WHERE v.variable_value_ref = p.id
   AND p.id <> c.canonical_id;

UPDATE ext_task_log_history e
   SET error_details_ref = c.canonical_id
  FROM projection_large_payload p
  JOIN bk3_canonical_payload c ON c.content_hash = p.content_hash
 WHERE e.error_details_ref = p.id
   AND p.id <> c.canonical_id;

UPDATE attachment_history a
   SET content_ref = c.canonical_id
  FROM projection_large_payload p
  JOIN bk3_canonical_payload c ON c.content_hash = p.content_hash
 WHERE a.content_ref = p.id
   AND p.id <> c.canonical_id;

DELETE FROM projection_large_payload p
 USING bk3_canonical_payload c
 WHERE c.content_hash = p.content_hash
   AND c.canonical_id <> p.id;

DROP TABLE bk3_canonical_payload;

-- Recompute ref_count from the actual surviving references across the three known referencing
-- tables (D-D' 3-copy scope: variable_value_ref, error_details_ref, content_ref).
UPDATE projection_large_payload p
   SET ref_count = (
        (SELECT count(*) FROM variable_instance_history v WHERE v.variable_value_ref = p.id) +
        (SELECT count(*) FROM ext_task_log_history e WHERE e.error_details_ref = p.id) +
        (SELECT count(*) FROM attachment_history a WHERE a.content_ref = p.id)
   );

-- A payload with zero surviving references is a pre-existing orphan (should not occur under the
-- basamak-2 invariants, but the old ErasurePipeline.deleteLargePayloads/hard-delete path predates
-- this migration -- defensive cleanup rather than carrying a zombie row forward under a NOT NULL/
-- CHECK ref_count >= 1 regime).
DELETE FROM projection_large_payload WHERE ref_count = 0;

ALTER TABLE projection_large_payload ALTER COLUMN content_hash SET NOT NULL;
ALTER TABLE projection_large_payload ADD CONSTRAINT chk_projection_large_payload_ref_count CHECK (ref_count >= 0);
ALTER TABLE projection_large_payload ADD CONSTRAINT unq_projection_large_payload_content_hash UNIQUE (content_hash);

COMMENT ON COLUMN projection_large_payload.content_hash IS
    'SHA-256 (hex) of payload_bytes -- basamak-3 D-B''/D-D'' dedup key. Content-addressed: the SAME bytes from RUNTIME (ACT_RU_VARIABLE) and HISTORY (ACT_HI_DETAIL/ACT_HI_VARINST/EXT_TASK_LOG/ATTACHMENT) converge on one row.';
COMMENT ON COLUMN projection_large_payload.ref_count IS
    'basamak-3 D-F'' refcount/GC -- number of live *_ref foreign keys pointing at this row. Row is deleted only when this reaches 0 (RetentionEnforcementJob/ErasurePipeline decrement it; never a direct per-caller DELETE).';
