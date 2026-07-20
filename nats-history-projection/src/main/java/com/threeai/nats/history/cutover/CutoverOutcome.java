package com.threeai.nats.history.cutover;

/** Result of {@link CutoverControlPlane#requestCutover}. */
public enum CutoverOutcome {
    /** BUS_CUTOVER_GATE_NOT_MET — state != N_GUN_TEMIZ. */
    GATE_NOT_MET,
    /** KV written + state=CUTOVER_TALEP; rolling-restart confirmation is a separate, async step. */
    REQUESTED,
    /** SYS_CUTOVER_CONFIG_APPLY_FAILED — KV write failed, dual-run continues (fail-safe). */
    APPLY_FAILED
}
