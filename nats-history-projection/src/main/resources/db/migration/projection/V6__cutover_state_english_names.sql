-- V6: rename the cutover state machine's value names to English.
--
-- V3 created class_cutover_state with three Turkish-derived values —
-- N_GUN_TEMIZ ("N days clean"), CUTOVER_TALEP ("cutover requested") and
-- CUTOVERLANMIS ("cutover applied"). They are renamed to CLEAN_STREAK,
-- CUTOVER_REQUESTED and CUTOVER_APPLIED. CLEAN_STREAK matches the vocabulary
-- the table already uses in clean_streak_days / clean_streak_target.
--
-- V3 is deliberately left untouched. These scripts are published DDL that a
-- tenant adopts into their own Flyway/Liquibase chain, so editing an already
-- released script in place would break its checksum for anyone who has applied
-- it. This is a forward migration instead: a fresh install runs V3 then V6, an
-- existing install runs only V6.
--
-- Order matters. The CHECK constraint from V3 only admits the old values, so it
-- has to be dropped before the UPDATE, not after. Dropping first also makes the
-- script re-runnable: a second run drops the new constraint, updates nothing,
-- and recreates it.

ALTER TABLE class_cutover_state
    DROP CONSTRAINT IF EXISTS chk_class_cutover_state_state;

UPDATE class_cutover_state
SET state = CASE state
        WHEN 'N_GUN_TEMIZ'   THEN 'CLEAN_STREAK'
        WHEN 'CUTOVER_TALEP' THEN 'CUTOVER_REQUESTED'
        WHEN 'CUTOVERLANMIS' THEN 'CUTOVER_APPLIED'
        ELSE state
    END
WHERE state IN ('N_GUN_TEMIZ', 'CUTOVER_TALEP', 'CUTOVERLANMIS');

ALTER TABLE class_cutover_state
    ADD CONSTRAINT chk_class_cutover_state_state
        CHECK (state IN ('DUAL_RUN', 'RECONCILING', 'CLEAN_STREAK', 'CUTOVER_REQUESTED', 'CUTOVER_APPLIED'));

COMMENT ON COLUMN class_cutover_state.state IS
    'Cutover state machine: DUAL_RUN -> RECONCILING -> CLEAN_STREAK -> CUTOVER_REQUESTED -> CUTOVER_APPLIED. Rollback returns CUTOVER_APPLIED to DUAL_RUN.';
