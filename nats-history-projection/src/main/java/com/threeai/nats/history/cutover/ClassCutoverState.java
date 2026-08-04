package com.threeai.nats.history.cutover;

import java.time.Instant;

/**
 * Persistent per-class cutover state ({@code class_cutover_state} table). DTO carrier for
 * {@link ClassCutoverStateStore}.
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

    public enum CutoverState { DUAL_RUN, RECONCILING, CLEAN_STREAK, CUTOVER_REQUESTED, CUTOVER_APPLIED }
}
