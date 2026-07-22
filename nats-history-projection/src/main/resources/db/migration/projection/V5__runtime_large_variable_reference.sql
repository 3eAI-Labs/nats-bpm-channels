-- Migration: 005_runtime_large_variable_reference.sql
-- Target database: PROJECTION Postgres (same instance as 001/002/003/004)
-- Owner (write): ContentAddressedLargePayloadStore.recordRuntimeReference/deleteRuntimeReferenceRecord
--                 (camunda-nats-channel/cadenzaflow-nats-channel LargeVariablePostCommitExternalizer)
-- Owner (read/reconcile): LargeVariableExternalizationSweep.reconcileRuntimeReferences
--
-- FINDING-001 fix (Sentinel review, 2026-07): D-F' refcount/GC previously only released references
-- on the HISTORY side (ErasurePipeline/RetentionEnforcementJob). RUNTIME references (ACT_RU_VARIABLE
-- externalized to this content-addressed store) were NEVER released -- a hard-deleted variable/
-- process's externalized PII payload survived forever (refcount never reached 0), a KVKK/D-F'
-- regression. The fork's byte-array delete path (ByteArrayField.deleteByteArrayValue()) does not
-- re-enter the custom serializer, so a synchronous release-on-delete hook is structurally
-- impossible (docs/08 evidence) -- this ledger is what makes an ASYNCHRONOUS reconciliation
-- possible instead (basamak-2 ReconciliationJob idiom): it records "this RUNTIME variable is the
-- reason this payload's refcount includes +1"; a periodic sweep (leader-elected,
-- LargeVariableExternalizationSweep) compares ledger rows against ACT_RU_VARIABLE's CURRENT
-- externalization marker for that variable id and releases (ref_count--; 0 -> object deleted) any
-- row whose variable is gone or no longer carries that same content hash (deleted, overwritten with
-- a different/no-longer-externalized value).
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

COMMENT ON TABLE runtime_large_variable_ref IS
    'D-F'' RUNTIME-side refcount ledger (FINDING-001 fix) -- one row per (engine_id, variable_id) currently holding an externalization reference; reconciled against ACT_RU_VARIABLE by LargeVariableExternalizationSweep since the fork cannot synchronously notify this store of a variable/process delete.';
