package com.threeai.nats.history.projection;

/** Outcome of a {@link ProjectionStore} write. */
public enum UpsertOutcome {
    APPLIED,
    STALE_DISCARDED,
    DEDUP_SKIPPED
}
