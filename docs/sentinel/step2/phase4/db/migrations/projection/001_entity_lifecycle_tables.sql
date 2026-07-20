-- Migration: 001_entity_lifecycle_tables.sql
-- Target database: PROJECTION Postgres (separate instance/schema from engine DB -- ADR-0011, D-B)
-- Owner (write): HistoryProjectionConsumer (merge-upsert) -- see DB_ACCESS_MAP.md §2
-- Owner (read): HistoryQueryApi, ReconciliationJob, RetentionEnforcementJob, ErasurePipeline
--
-- Scope: ACT_HI classes whose projection row represents ONE EVOLVING ENTITY (create -> update -> end),
-- merge-upsert target = the entity's natural id (ADR-0011/0012). Classes: PROCINST, ACTINST, VARINST,
-- TASKINST, INCIDENT, CASEINST.
--
-- Partitioning note (ADR-0011/0018 -- range-partition for DROP/DETACH retention):
--   partition_anchor_at is FIXED at first-insert time (the entity's "born" timestamp) and NEVER
--   changes on subsequent upserts -- this keeps every lifecycle update of the SAME entity routed to
--   the SAME partition (Postgres declarative partitioning requires the partition key inside every
--   UNIQUE/PK constraint; if we partitioned by "latest event time" instead, an entity's row would
--   need to physically MOVE partitions on every update, which Postgres does not do transparently).
--   event_time is a SEPARATE, non-partition-key column carrying the latest event's own timestamp
--   (ADR-0012: display-only, never the merge-upsert tie-break authority -- stream_sequence is).
--   Merge-upsert protocol (LLD, no native single-statement ON CONFLICT possible across partitions):
--     1. SELECT partition_anchor_at, stream_sequence FROM <table> WHERE engine_id=? AND <entity_id>=?
--        (uses the non-unique global index below -- point lookup, partition-pruning not required).
--     2. Found + incoming.stream_sequence > existing.stream_sequence -> UPDATE ... WHERE engine_id=?
--        AND <entity_id>=? AND partition_anchor_at=<found value> (single-partition, index-backed).
--     3. Found + incoming.stream_sequence <= existing.stream_sequence -> no-op (BUS_PROJECTION_STALE_EVENT_DISCARDED).
--     4. Not found -> INSERT with partition_anchor_at = event_time of this (first) event.
--   See docs/sentinel/step2/phase4/lld/history-offload/03_classes/2_relay_projection.md §2.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Shared large-object companion (VARINST byte-array values, EXT_TASK_LOG.errorDetails, ATTACHMENT
-- content) -- ARCH-Q1 "referans" pattern reused inside the projection store; NOT partitioned (small
-- table relative to class tables; retention cascades via source row's ON DELETE, see 002/003).
CREATE TABLE projection_large_payload (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_table  VARCHAR(64) NOT NULL,
    payload_bytes BYTEA NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================================
-- process_instance_history (PROCINST) -- çekirdek-4 pattern 1/2/3 anchor table.
-- ============================================================================
CREATE TABLE process_instance_history (
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    engine_id                VARCHAR(80)  NOT NULL,
    process_instance_id      VARCHAR(64)  NOT NULL,
    process_definition_key   VARCHAR(255),
    process_definition_id    VARCHAR(64),
    business_key             VARCHAR(255),                       -- CONFIDENTIAL/koşullu PII (DP-2/DP-8)
    start_user_id             VARCHAR(255),                      -- RESTRICTED/PII (DP-11)
    super_process_instance_id VARCHAR(64),
    start_time               TIMESTAMPTZ,
    end_time                 TIMESTAMPTZ,
    duration_millis          BIGINT,
    delete_reason            VARCHAR(4000),
    state                    VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    stream_sequence          BIGINT      NOT NULL,
    event_time               TIMESTAMPTZ NOT NULL,
    partition_anchor_at      TIMESTAMPTZ NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by                VARCHAR(100) NOT NULL DEFAULT 'history-projection-consumer',
    updated_by                VARCHAR(100),
    deleted_at                TIMESTAMPTZ,
    anonymized_at             TIMESTAMPTZ,
    legal_hold                 BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, partition_anchor_at)
) PARTITION BY RANGE (partition_anchor_at);

CREATE UNIQUE INDEX unq_process_instance_history_entity
    ON process_instance_history (engine_id, process_instance_id, partition_anchor_at);
CREATE INDEX idx_process_instance_history_entity_lookup
    ON process_instance_history (engine_id, process_instance_id);
CREATE INDEX idx_process_instance_history_business_key
    ON process_instance_history (business_key) WHERE deleted_at IS NULL;
CREATE INDEX idx_process_instance_history_def_time
    ON process_instance_history (process_definition_key, start_time) WHERE deleted_at IS NULL;

CREATE TABLE process_instance_history_default PARTITION OF process_instance_history DEFAULT;

-- ============================================================================
-- activity_instance_history (ACTINST) -- bulk, ~%90 hacim payı (DETAIL/VARINST/ACTINST).
-- ============================================================================
CREATE TABLE activity_instance_history (
    id                    UUID NOT NULL DEFAULT gen_random_uuid(),
    engine_id             VARCHAR(80)  NOT NULL,
    activity_instance_id  VARCHAR(64)  NOT NULL,
    process_instance_id   VARCHAR(64)  NOT NULL,
    activity_id           VARCHAR(255),
    activity_type         VARCHAR(255),
    activity_name         VARCHAR(255),
    task_assignee         VARCHAR(255),                          -- RESTRICTED/PII when user-task
    start_time            TIMESTAMPTZ,
    end_time              TIMESTAMPTZ,
    duration_millis       BIGINT,
    canceled              BOOLEAN NOT NULL DEFAULT FALSE,
    stream_sequence       BIGINT      NOT NULL,
    event_time            TIMESTAMPTZ NOT NULL,
    partition_anchor_at   TIMESTAMPTZ NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by             VARCHAR(100) NOT NULL DEFAULT 'history-projection-consumer',
    updated_by             VARCHAR(100),
    deleted_at             TIMESTAMPTZ,
    anonymized_at          TIMESTAMPTZ,
    legal_hold              BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, partition_anchor_at)
) PARTITION BY RANGE (partition_anchor_at);

CREATE UNIQUE INDEX unq_activity_instance_history_entity
    ON activity_instance_history (engine_id, activity_instance_id, partition_anchor_at);
CREATE INDEX idx_activity_instance_history_entity_lookup
    ON activity_instance_history (engine_id, activity_instance_id);
CREATE INDEX idx_activity_instance_history_process_instance
    ON activity_instance_history (process_instance_id) WHERE deleted_at IS NULL;

CREATE TABLE activity_instance_history_default PARTITION OF activity_instance_history DEFAULT;

-- ============================================================================
-- variable_instance_history (VARINST) -- current value of a variable; bulk, highest volume.
-- ============================================================================
CREATE TABLE variable_instance_history (
    id                    UUID NOT NULL DEFAULT gen_random_uuid(),
    engine_id             VARCHAR(80)  NOT NULL,
    variable_instance_id  VARCHAR(64)  NOT NULL,
    process_instance_id   VARCHAR(64)  NOT NULL,
    variable_name         VARCHAR(255),
    variable_type         VARCHAR(100),
    variable_value_text   TEXT,                                  -- RESTRICTED/PII (DP-9/DP-10)
    variable_value_ref    UUID REFERENCES projection_large_payload(id),
    is_byte_array         BOOLEAN NOT NULL DEFAULT FALSE,
    stream_sequence       BIGINT      NOT NULL,
    event_time            TIMESTAMPTZ NOT NULL,
    partition_anchor_at   TIMESTAMPTZ NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by             VARCHAR(100) NOT NULL DEFAULT 'history-projection-consumer',
    updated_by             VARCHAR(100),
    deleted_at             TIMESTAMPTZ,
    anonymized_at          TIMESTAMPTZ,
    legal_hold              BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, partition_anchor_at)
) PARTITION BY RANGE (partition_anchor_at);

CREATE UNIQUE INDEX unq_variable_instance_history_entity
    ON variable_instance_history (engine_id, variable_instance_id, partition_anchor_at);
CREATE INDEX idx_variable_instance_history_entity_lookup
    ON variable_instance_history (engine_id, variable_instance_id);
CREATE INDEX idx_variable_instance_history_process_instance
    ON variable_instance_history (process_instance_id) WHERE deleted_at IS NULL;

CREATE TABLE variable_instance_history_default PARTITION OF variable_instance_history DEFAULT;

-- ============================================================================
-- task_instance_history (TASKINST) -- bulk.
-- ============================================================================
CREATE TABLE task_instance_history (
    id                    UUID NOT NULL DEFAULT gen_random_uuid(),
    engine_id             VARCHAR(80)  NOT NULL,
    task_id               VARCHAR(64)  NOT NULL,
    process_instance_id   VARCHAR(64)  NOT NULL,
    task_name             VARCHAR(255),
    task_description      TEXT,
    assignee              VARCHAR(255),                          -- RESTRICTED/PII (DP-11)
    owner                 VARCHAR(255),                          -- RESTRICTED/PII (DP-11)
    start_time            TIMESTAMPTZ,
    end_time              TIMESTAMPTZ,
    duration_millis       BIGINT,
    delete_reason         VARCHAR(4000),
    priority              INTEGER,
    stream_sequence       BIGINT      NOT NULL,
    event_time            TIMESTAMPTZ NOT NULL,
    partition_anchor_at   TIMESTAMPTZ NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by             VARCHAR(100) NOT NULL DEFAULT 'history-projection-consumer',
    updated_by             VARCHAR(100),
    deleted_at             TIMESTAMPTZ,
    anonymized_at          TIMESTAMPTZ,
    legal_hold              BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, partition_anchor_at)
) PARTITION BY RANGE (partition_anchor_at);

CREATE UNIQUE INDEX unq_task_instance_history_entity
    ON task_instance_history (engine_id, task_id, partition_anchor_at);
CREATE INDEX idx_task_instance_history_entity_lookup
    ON task_instance_history (engine_id, task_id);
CREATE INDEX idx_task_instance_history_process_instance
    ON task_instance_history (process_instance_id) WHERE deleted_at IS NULL;

CREATE TABLE task_instance_history_default PARTITION OF task_instance_history DEFAULT;

-- ============================================================================
-- incident_history (INCIDENT) -- AUDIT-KRİTİK (PO-Q5 default set). legal_hold defaults TRUE.
-- ============================================================================
CREATE TABLE incident_history (
    id                    UUID NOT NULL DEFAULT gen_random_uuid(),
    engine_id             VARCHAR(80)  NOT NULL,
    incident_id           VARCHAR(64)  NOT NULL,
    process_instance_id   VARCHAR(64)  NOT NULL,
    incident_type         VARCHAR(100),
    incident_message      TEXT,                                  -- RESTRICTED/PII, audit-kritik
    configuration         VARCHAR(4000),
    activity_id           VARCHAR(255),
    create_time           TIMESTAMPTZ,
    end_time              TIMESTAMPTZ,
    resolved              BOOLEAN NOT NULL DEFAULT FALSE,
    stream_sequence       BIGINT      NOT NULL,
    event_time            TIMESTAMPTZ NOT NULL,
    partition_anchor_at   TIMESTAMPTZ NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by             VARCHAR(100) NOT NULL DEFAULT 'history-projection-consumer',
    updated_by             VARCHAR(100),
    deleted_at             TIMESTAMPTZ,                           -- erasure REJECTED for this class (ADR-0017); reserved for schema symmetry only
    anonymized_at          TIMESTAMPTZ,
    legal_hold              BOOLEAN NOT NULL DEFAULT TRUE,          -- audit-kritik => yasal-saklama istisnası (ADR-0017/0018)
    PRIMARY KEY (id, partition_anchor_at)
) PARTITION BY RANGE (partition_anchor_at);

CREATE UNIQUE INDEX unq_incident_history_entity
    ON incident_history (engine_id, incident_id, partition_anchor_at);
CREATE INDEX idx_incident_history_entity_lookup
    ON incident_history (engine_id, incident_id);
CREATE INDEX idx_incident_history_process_instance
    ON incident_history (process_instance_id);

CREATE TABLE incident_history_default PARTITION OF incident_history DEFAULT;

-- ============================================================================
-- case_instance_history (CASEINST) -- CMMN, düşük hacim.
-- ============================================================================
CREATE TABLE case_instance_history (
    id                    UUID NOT NULL DEFAULT gen_random_uuid(),
    engine_id             VARCHAR(80)  NOT NULL,
    case_instance_id      VARCHAR(64)  NOT NULL,
    case_definition_id    VARCHAR(64),
    business_key          VARCHAR(255),                          -- CONFIDENTIAL/koşullu PII
    create_time           TIMESTAMPTZ,
    close_time            TIMESTAMPTZ,
    state                 VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    stream_sequence       BIGINT      NOT NULL,
    event_time            TIMESTAMPTZ NOT NULL,
    partition_anchor_at   TIMESTAMPTZ NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by             VARCHAR(100) NOT NULL DEFAULT 'history-projection-consumer',
    updated_by             VARCHAR(100),
    deleted_at             TIMESTAMPTZ,
    anonymized_at          TIMESTAMPTZ,
    legal_hold              BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, partition_anchor_at)
) PARTITION BY RANGE (partition_anchor_at);

CREATE UNIQUE INDEX unq_case_instance_history_entity
    ON case_instance_history (engine_id, case_instance_id, partition_anchor_at);
CREATE INDEX idx_case_instance_history_entity_lookup
    ON case_instance_history (engine_id, case_instance_id);

CREATE TABLE case_instance_history_default PARTITION OF case_instance_history DEFAULT;
