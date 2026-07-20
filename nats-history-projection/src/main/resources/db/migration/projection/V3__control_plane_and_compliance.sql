-- Migration: 003_control_plane_and_compliance.sql
-- Target database: PROJECTION Postgres (same instance as 001/002)
-- Owner: CutoverControlPlane/ReconciliationJob (class_cutover_state), RetentionEnforcementJob
-- (retention_audit_log), ErasurePipeline (erasure_audit_log, erasure_scope_confirmation)
-- ADR: 0015 (cutover/reconciliation), 0017 (erasure), 0018 (retention audit-log invariant)
-- Per DATABASE_GUIDELINE §3.3: audit tables for L3/L4/compliance-critical entities.

-- ============================================================================
-- class_cutover_state -- persistent state for the class-based cutover lifecycle
-- (BUSINESS_LOGIC.md §2.1: DUAL_RUN -> RECONCILING -> N_GUN_TEMIZ -> CUTOVER_TALEP -> CUTOVERLANMIS).
-- One row per (engine_id, history_class). Survives ReconciliationJob/CutoverControlPlane restarts.
-- ============================================================================
CREATE TABLE class_cutover_state (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    engine_id             VARCHAR(80)  NOT NULL,
    history_class         VARCHAR(40)  NOT NULL,
    consistency_path      VARCHAR(20)  NOT NULL,                 -- AUDIT_CRITICAL | BULK (BR-HDL-002)
    state                 VARCHAR(20)  NOT NULL DEFAULT 'DUAL_RUN',
    -- DUAL_RUN | RECONCILING | N_GUN_TEMIZ | CUTOVER_TALEP | CUTOVERLANMIS
    clean_streak_days     INTEGER      NOT NULL DEFAULT 0,
    clean_streak_target   INTEGER      NOT NULL DEFAULT 7,       -- N (PO-Q4 default 7g, sınıf-başına konfig)
    last_reconciled_at    TIMESTAMPTZ,
    last_diff_count       BIGINT       NOT NULL DEFAULT 0,
    cutover_applied_at    TIMESTAMPTZ,
    rollback_count        INTEGER      NOT NULL DEFAULT 0,
    last_rollback_at      TIMESTAMPTZ,
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT chk_class_cutover_state_state
        CHECK (state IN ('DUAL_RUN','RECONCILING','N_GUN_TEMIZ','CUTOVER_TALEP','CUTOVERLANMIS')),
    CONSTRAINT chk_class_cutover_state_path
        CHECK (consistency_path IN ('AUDIT_CRITICAL','BULK')),
    CONSTRAINT unq_class_cutover_state_engine_class UNIQUE (engine_id, history_class)
);

COMMENT ON TABLE class_cutover_state IS
    'Persistent per-class cutover state machine (BUSINESS_LOGIC.md §2.1) -- ReconciliationJob/CutoverControlPlane operational metadata, not PII (DP-14).';

-- ============================================================================
-- retention_audit_log -- DATABASE_GUIDELINE §3.3 audit pattern. Compliance-critical: a FAILED
-- write here after a successful partition drop is SYS_RETENTION_AUDIT_LOG_WRITE_FAILED (CRITICAL).
-- ============================================================================
CREATE TABLE retention_audit_log (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    engine_id         VARCHAR(80)  NOT NULL,
    history_class     VARCHAR(40)  NOT NULL,
    target_table      VARCHAR(64)  NOT NULL,
    partition_name    VARCHAR(64)  NOT NULL,
    action            VARCHAR(20)  NOT NULL,                     -- DROP_PARTITION | DETACH_PARTITION
    row_count_estimate BIGINT,
    retention_window_days INTEGER  NOT NULL,
    legal_basis       VARCHAR(255),                              -- e.g. 'KVKK v1.0 §4.2 (7y)' -- audit-kritik sınıflar
    performed_by      VARCHAR(100) NOT NULL DEFAULT 'retention-enforcement-job',
    performed_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    reason            VARCHAR(500)
);
CREATE INDEX idx_retention_audit_log_class_time
    ON retention_audit_log (history_class, performed_at);

COMMENT ON TABLE retention_audit_log IS
    'DATA_GOVERNANCE v4.0 §4.4 audit-log-per-deletion invariant. Write failure after successful drop = SYS_RETENTION_AUDIT_LOG_WRITE_FAILED (CRITICAL page).';

-- ============================================================================
-- erasure_audit_log -- every erasure/anonymize operation (ADR-0017).
-- ============================================================================
CREATE TABLE erasure_audit_log (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id        UUID NOT NULL,
    subject_key       VARCHAR(255) NOT NULL,                     -- businessKey/userId (NOT logged elsewhere -- DP-1)
    target_table      VARCHAR(64)  NOT NULL,
    action            VARCHAR(20)  NOT NULL,                     -- SOFT_DELETE | ANONYMIZE | LEGAL_HOLD_BLOCKED
    affected_row_count BIGINT      NOT NULL DEFAULT 0,
    verification_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',  -- PENDING | PASSED | FAILED
    performed_by      VARCHAR(100) NOT NULL DEFAULT 'erasure-pipeline',
    performed_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    verified_at       TIMESTAMPTZ,
    reason            VARCHAR(500),
    CONSTRAINT chk_erasure_audit_log_action
        CHECK (action IN ('SOFT_DELETE','ANONYMIZE','LEGAL_HOLD_BLOCKED')),
    CONSTRAINT chk_erasure_audit_log_verification
        CHECK (verification_status IN ('PENDING','PASSED','FAILED'))
);
CREATE INDEX idx_erasure_audit_log_request_id ON erasure_audit_log (request_id);
CREATE INDEX idx_erasure_audit_log_subject_key ON erasure_audit_log (subject_key);

COMMENT ON TABLE erasure_audit_log IS
    'ADR-0017 erasure audit trail. verification_status=FAILED after a completed action = RES_ERASURE_VERIFICATION_FAILED (CRITICAL, KVKK 30g SLA).';

-- ============================================================================
-- erasure_scope_confirmation -- BA-Q6 kapsam-onayı akışı (telco MSISDN churn koruması).
-- ============================================================================
CREATE TABLE erasure_scope_confirmation (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id            UUID NOT NULL,
    subject_key           VARCHAR(255) NOT NULL,
    candidate_instances   JSONB NOT NULL,                        -- [{processInstanceId, timeRangeStart, timeRangeEnd}, ...]
    status                VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- PENDING | CONFIRMED | REJECTED
    confirmed_scope       JSONB,                                 -- subset of candidate_instances the requester confirmed
    requested_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    confirmed_by          VARCHAR(100),
    confirmed_at          TIMESTAMPTZ,
    CONSTRAINT chk_erasure_scope_confirmation_status
        CHECK (status IN ('PENDING','CONFIRMED','REJECTED'))
);
CREATE INDEX idx_erasure_scope_confirmation_request_id ON erasure_scope_confirmation (request_id);

COMMENT ON TABLE erasure_scope_confirmation IS
    'ADR-0017/BA-Q6: VAL_ERASURE_SUBJECT_KEY_AMBIGUOUS candidate list + explicit confirmation before pipeline trigger.';
