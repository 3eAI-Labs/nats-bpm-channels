-- Migration: 002_append_log_tables.sql
-- Target database: PROJECTION Postgres (same instance as 001)
-- Owner (write): HistoryProjectionConsumer -- see DB_ACCESS_MAP.md §2
--
-- Scope: ACT_HI classes whose projection row is an IMMUTABLE point-in-time fact (no lifecycle
-- update -- each incoming event IS a new row). Merge-upsert degenerates to a dedup'd insert:
--   INSERT ... ON CONFLICT (engine_id, history_event_id, event_type, event_time) DO NOTHING
-- stream_sequence is still stored (ordering/display + BUS_MERGE_UPSERT_CONFLICT_AMBIGUOUS audit),
-- but there is no "newer state overwrites older" concept for these classes (ADR-0012 applies to
-- entity-lifecycle tables in 001; here dedup alone satisfies NFR-R4/R6).
-- event_time IS the partition key directly (no partition-anchor indirection needed -- see 001 header
-- for why entity-lifecycle tables need the fixed-anchor pattern and these do not).
-- Classes: DETAIL, IDENTITYLINK, OP_LOG, EXT_TASK_LOG, JOB_LOG, COMMENT, ATTACHMENT, DECINST, BATCH.

-- ============================================================================
-- variable_detail_history (DETAIL) -- bulk, en büyük hacim (ilk cutover adayı, hacim-öncelikli).
-- ============================================================================
CREATE TABLE variable_detail_history (
    id                    UUID NOT NULL DEFAULT gen_random_uuid(),
    engine_id             VARCHAR(80)  NOT NULL,
    process_instance_id   VARCHAR(64)  NOT NULL,
    history_event_id      VARCHAR(64)  NOT NULL,
    event_type            VARCHAR(64)  NOT NULL,
    variable_instance_id  VARCHAR(64),
    variable_name         VARCHAR(255),
    variable_type         VARCHAR(100),
    revision              INTEGER,
    variable_value_text   TEXT,                                  -- RESTRICTED/PII
    stream_sequence       BIGINT      NOT NULL,
    event_time            TIMESTAMPTZ NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by             VARCHAR(100) NOT NULL DEFAULT 'history-projection-consumer',
    deleted_at             TIMESTAMPTZ,
    anonymized_at          TIMESTAMPTZ,
    legal_hold              BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, event_time)
) PARTITION BY RANGE (event_time);

CREATE UNIQUE INDEX unq_variable_detail_history_dedup
    ON variable_detail_history (engine_id, history_event_id, event_type, event_time);
CREATE INDEX idx_variable_detail_history_process_instance
    ON variable_detail_history (process_instance_id) WHERE deleted_at IS NULL;

CREATE TABLE variable_detail_history_default PARTITION OF variable_detail_history DEFAULT;

-- ============================================================================
-- identity_link_history (IDENTITYLINK) -- bulk (PO-Q5); ADD/DELETE link events.
-- ============================================================================
CREATE TABLE identity_link_history (
    id                    UUID NOT NULL DEFAULT gen_random_uuid(),
    engine_id             VARCHAR(80)  NOT NULL,
    process_instance_id   VARCHAR(64)  NOT NULL,
    history_event_id      VARCHAR(64)  NOT NULL,
    event_type            VARCHAR(64)  NOT NULL,
    task_id               VARCHAR(64),
    link_type             VARCHAR(100),                          -- assignee | candidate | owner
    user_id               VARCHAR(255),                          -- RESTRICTED/PII
    group_id              VARCHAR(255),
    operation_type        VARCHAR(20),                           -- ADD | DELETE
    stream_sequence       BIGINT      NOT NULL,
    event_time            TIMESTAMPTZ NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by             VARCHAR(100) NOT NULL DEFAULT 'history-projection-consumer',
    deleted_at             TIMESTAMPTZ,
    anonymized_at          TIMESTAMPTZ,
    legal_hold              BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, event_time)
) PARTITION BY RANGE (event_time);

CREATE UNIQUE INDEX unq_identity_link_history_dedup
    ON identity_link_history (engine_id, history_event_id, event_type, event_time);
CREATE INDEX idx_identity_link_history_process_instance
    ON identity_link_history (process_instance_id) WHERE deleted_at IS NULL;

CREATE TABLE identity_link_history_default PARTITION OF identity_link_history DEFAULT;

-- ============================================================================
-- operation_log_history (OP_LOG) -- AUDIT-KRİTİK. Denetim izinin kendisi (kim-ne-yaptı).
-- legal_hold defaults TRUE. Pseudonymization opt-in (BR-PII-003/004, ADR-0016).
-- ============================================================================
CREATE TABLE operation_log_history (
    id                     UUID NOT NULL DEFAULT gen_random_uuid(),
    engine_id              VARCHAR(80)  NOT NULL,
    process_instance_id    VARCHAR(64)  NOT NULL,
    history_event_id       VARCHAR(64)  NOT NULL,
    event_type             VARCHAR(64)  NOT NULL,
    operation_id           VARCHAR(64),
    user_id                VARCHAR(255),                         -- RESTRICTED/PII (raw, only when NOT pseudonymized)
    user_id_pseudonymized  BOOLEAN NOT NULL DEFAULT FALSE,
    pseudonym_token        VARCHAR(128),                         -- deterministic keyed-hash (BA-Q5); real value lives ONLY in the vault DB
    operation_type         VARCHAR(100),
    entity_type            VARCHAR(100),
    property               VARCHAR(255),
    org_value              TEXT,
    new_value              TEXT,
    stream_sequence        BIGINT      NOT NULL,
    event_time             TIMESTAMPTZ NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by              VARCHAR(100) NOT NULL DEFAULT 'history-projection-consumer',
    deleted_at              TIMESTAMPTZ,                          -- erasure REJECTED (legal-hold); reserved for schema symmetry
    anonymized_at           TIMESTAMPTZ,
    legal_hold               BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT chk_operation_log_history_pseudonym_consistency
        CHECK (NOT user_id_pseudonymized OR pseudonym_token IS NOT NULL),
    PRIMARY KEY (id, event_time)
) PARTITION BY RANGE (event_time);

CREATE UNIQUE INDEX unq_operation_log_history_dedup
    ON operation_log_history (engine_id, history_event_id, event_type, event_time);
CREATE INDEX idx_operation_log_history_process_instance
    ON operation_log_history (process_instance_id);
CREATE INDEX idx_operation_log_history_pseudonym_token
    ON operation_log_history (pseudonym_token) WHERE pseudonym_token IS NOT NULL;

CREATE TABLE operation_log_history_default PARTITION OF operation_log_history DEFAULT;

-- ============================================================================
-- ext_task_log_history (EXT_TASK_LOG) -- AUDIT-KRİTİK. legal_hold defaults TRUE.
-- ============================================================================
CREATE TABLE ext_task_log_history (
    id                    UUID NOT NULL DEFAULT gen_random_uuid(),
    engine_id             VARCHAR(80)  NOT NULL,
    process_instance_id   VARCHAR(64)  NOT NULL,
    history_event_id      VARCHAR(64)  NOT NULL,
    event_type            VARCHAR(64)  NOT NULL,
    external_task_id      VARCHAR(64),
    worker_id             VARCHAR(255),                          -- RESTRICTED/PII
    topic_name            VARCHAR(255),
    activity_id           VARCHAR(255),
    error_message         TEXT,                                  -- RESTRICTED/PII
    error_details_ref     UUID REFERENCES projection_large_payload(id),   -- ARCH-Q1 referans (stack trace)
    state                 VARCHAR(20),                           -- CREATED | FAILED | SUCCESSFUL | DELETED
    stream_sequence       BIGINT      NOT NULL,
    event_time            TIMESTAMPTZ NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by             VARCHAR(100) NOT NULL DEFAULT 'history-projection-consumer',
    deleted_at             TIMESTAMPTZ,
    anonymized_at          TIMESTAMPTZ,
    legal_hold              BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (id, event_time)
) PARTITION BY RANGE (event_time);

CREATE UNIQUE INDEX unq_ext_task_log_history_dedup
    ON ext_task_log_history (engine_id, history_event_id, event_type, event_time);
CREATE INDEX idx_ext_task_log_history_process_instance
    ON ext_task_log_history (process_instance_id);

CREATE TABLE ext_task_log_history_default PARTITION OF ext_task_log_history DEFAULT;

-- ============================================================================
-- job_log_history (JOB_LOG) -- bulk.
-- ============================================================================
CREATE TABLE job_log_history (
    id                    UUID NOT NULL DEFAULT gen_random_uuid(),
    engine_id             VARCHAR(80)  NOT NULL,
    process_instance_id   VARCHAR(64)  NOT NULL,
    history_event_id      VARCHAR(64)  NOT NULL,
    event_type            VARCHAR(64)  NOT NULL,
    job_id                VARCHAR(64),
    job_def_type          VARCHAR(100),
    exception_message     TEXT,                                  -- INTERNAL -> PII riski (serbest metin)
    retries               INTEGER,
    state                 VARCHAR(20),
    stream_sequence       BIGINT      NOT NULL,
    event_time            TIMESTAMPTZ NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by             VARCHAR(100) NOT NULL DEFAULT 'history-projection-consumer',
    deleted_at             TIMESTAMPTZ,
    anonymized_at          TIMESTAMPTZ,
    legal_hold              BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, event_time)
) PARTITION BY RANGE (event_time);

CREATE UNIQUE INDEX unq_job_log_history_dedup
    ON job_log_history (engine_id, history_event_id, event_type, event_time);
CREATE INDEX idx_job_log_history_process_instance
    ON job_log_history (process_instance_id) WHERE deleted_at IS NULL;

CREATE TABLE job_log_history_default PARTITION OF job_log_history DEFAULT;

-- ============================================================================
-- comment_history (COMMENT) -- bulk (PO-Q5); serbest metin.
-- ============================================================================
CREATE TABLE comment_history (
    id                    UUID NOT NULL DEFAULT gen_random_uuid(),
    engine_id             VARCHAR(80)  NOT NULL,
    process_instance_id   VARCHAR(64)  NOT NULL,
    history_event_id      VARCHAR(64)  NOT NULL,
    event_type            VARCHAR(64)  NOT NULL,
    task_id               VARCHAR(64),
    user_id               VARCHAR(255),                          -- RESTRICTED/PII
    message                TEXT,                                  -- RESTRICTED/PII (serbest metin)
    stream_sequence       BIGINT      NOT NULL,
    event_time            TIMESTAMPTZ NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by             VARCHAR(100) NOT NULL DEFAULT 'history-projection-consumer',
    deleted_at             TIMESTAMPTZ,
    anonymized_at          TIMESTAMPTZ,
    legal_hold              BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, event_time)
) PARTITION BY RANGE (event_time);

CREATE UNIQUE INDEX unq_comment_history_dedup
    ON comment_history (engine_id, history_event_id, event_type, event_time);
CREATE INDEX idx_comment_history_process_instance
    ON comment_history (process_instance_id) WHERE deleted_at IS NULL;

CREATE TABLE comment_history_default PARTITION OF comment_history DEFAULT;

-- ============================================================================
-- attachment_history (ATTACHMENT) -- bulk (PO-Q5).
-- ============================================================================
CREATE TABLE attachment_history (
    id                    UUID NOT NULL DEFAULT gen_random_uuid(),
    engine_id             VARCHAR(80)  NOT NULL,
    process_instance_id   VARCHAR(64)  NOT NULL,
    history_event_id      VARCHAR(64)  NOT NULL,
    event_type            VARCHAR(64)  NOT NULL,
    task_id               VARCHAR(64),
    user_id               VARCHAR(255),                          -- RESTRICTED/PII
    attachment_name       VARCHAR(255),
    attachment_type       VARCHAR(100),
    url                   VARCHAR(4000),
    content_ref           UUID REFERENCES projection_large_payload(id),
    stream_sequence       BIGINT      NOT NULL,
    event_time            TIMESTAMPTZ NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by             VARCHAR(100) NOT NULL DEFAULT 'history-projection-consumer',
    deleted_at             TIMESTAMPTZ,
    anonymized_at          TIMESTAMPTZ,
    legal_hold              BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, event_time)
) PARTITION BY RANGE (event_time);

CREATE UNIQUE INDEX unq_attachment_history_dedup
    ON attachment_history (engine_id, history_event_id, event_type, event_time);
CREATE INDEX idx_attachment_history_process_instance
    ON attachment_history (process_instance_id) WHERE deleted_at IS NULL;

CREATE TABLE attachment_history_default PARTITION OF attachment_history DEFAULT;

-- ============================================================================
-- decision_evaluation_history (DECINST) -- bulk; karar girdi/çıktıları PII riski taşıyabilir.
-- ============================================================================
CREATE TABLE decision_evaluation_history (
    id                          UUID NOT NULL DEFAULT gen_random_uuid(),
    engine_id                   VARCHAR(80)  NOT NULL,
    process_instance_id         VARCHAR(64)  NOT NULL,
    history_event_id            VARCHAR(64)  NOT NULL,
    event_type                  VARCHAR(64)  NOT NULL,
    decision_definition_id      VARCHAR(64),
    decision_definition_key     VARCHAR(255),
    root_decision_instance_id   VARCHAR(64),
    inputs                      JSONB,                            -- CONFIDENTIAL -> içerik PII riski
    outputs                     JSONB,
    stream_sequence             BIGINT      NOT NULL,
    event_time                  TIMESTAMPTZ NOT NULL,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by                   VARCHAR(100) NOT NULL DEFAULT 'history-projection-consumer',
    deleted_at                   TIMESTAMPTZ,
    anonymized_at                TIMESTAMPTZ,
    legal_hold                    BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, event_time)
) PARTITION BY RANGE (event_time);

CREATE UNIQUE INDEX unq_decision_evaluation_history_dedup
    ON decision_evaluation_history (engine_id, history_event_id, event_type, event_time);
CREATE INDEX idx_decision_evaluation_history_process_instance
    ON decision_evaluation_history (process_instance_id) WHERE deleted_at IS NULL;

CREATE TABLE decision_evaluation_history_default PARTITION OF decision_evaluation_history DEFAULT;

-- ============================================================================
-- batch_history (BATCH) -- bulk, batch meta (PII değeri taşımaz -- yalnız job sayaçları).
-- ============================================================================
CREATE TABLE batch_history (
    id                    UUID NOT NULL DEFAULT gen_random_uuid(),
    engine_id             VARCHAR(80)  NOT NULL,
    process_instance_id   VARCHAR(64)  NOT NULL DEFAULT '',      -- BATCH is not process-instance-scoped; kept for schema symmetry/partition routing
    history_event_id      VARCHAR(64)  NOT NULL,
    event_type            VARCHAR(64)  NOT NULL,
    batch_id              VARCHAR(64),
    batch_type            VARCHAR(100),
    total_jobs            INTEGER,
    jobs_per_seed         INTEGER,
    start_time            TIMESTAMPTZ,
    end_time              TIMESTAMPTZ,
    stream_sequence       BIGINT      NOT NULL,
    event_time            TIMESTAMPTZ NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by             VARCHAR(100) NOT NULL DEFAULT 'history-projection-consumer',
    deleted_at             TIMESTAMPTZ,
    anonymized_at          TIMESTAMPTZ,
    legal_hold              BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, event_time)
) PARTITION BY RANGE (event_time);

CREATE UNIQUE INDEX unq_batch_history_dedup
    ON batch_history (engine_id, history_event_id, event_type, event_time);

CREATE TABLE batch_history_default PARTITION OF batch_history DEFAULT;
