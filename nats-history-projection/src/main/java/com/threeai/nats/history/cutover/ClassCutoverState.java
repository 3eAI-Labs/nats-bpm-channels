package com.threeai.nats.history.cutover;

import java.time.Instant;

/**
 * Persistent per-class cutover state (`BUSINESS_LOGIC.md §2.1`, `DB_SCHEMA.md §2.7`
 * {@code class_cutover_state} table). DTO carrier for {@link ClassCutoverStateStore}.
 */
public record ClassCutoverState(
        String engineId,
        String historyClass,
        ConsistencyPath consistencyPath,
        CutoverState state,
        int cleanStreakDays,
        int cleanStreakTarget,
        Instant lastReconciledAt,
        long lastDiffCount,
        Instant cutoverAppliedAt,
        int rollbackCount,
        Instant lastRollbackAt) {

    public enum ConsistencyPath { AUDIT_CRITICAL, BULK }

    public enum CutoverState { DUAL_RUN, RECONCILING, N_GUN_TEMIZ, CUTOVER_TALEP, CUTOVERLANMIS }
}
