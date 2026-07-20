package com.threeai.nats.history.projection;

/** `04_interfaces/2_projection_dtos.md` §2. */
public enum UpsertOutcome {
    APPLIED,
    STALE_DISCARDED,
    DEDUP_SKIPPED
}
