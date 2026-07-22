package com.threeai.nats.core.largepayload;

import java.util.UUID;

/**
 * The result of {@link ContentAddressedLargePayloadStore#storeAndAcquireReference} — the canonical
 * row identity for a piece of content, plus whether THIS call was the one that physically inserted
 * it (vs. a dedup hit that only bumped {@code ref_count}, `docs/08-large-variable-externalization.md`
 * D-B'/D-D').
 *
 * @param id            {@code projection_large_payload.id} — the surrogate key existing HISTORY
 *                       {@code *_ref} foreign-key columns (variable_value_ref/error_details_ref/
 *                       content_ref) already use (basamak-2 schema, unchanged by basamak-3).
 * @param contentHash    SHA-256 hex — the self-describing key the RUNTIME-side reference marker
 *                       carries (no dependency on knowing the internal surrogate {@code id}).
 * @param refCountAfter  {@code ref_count} immediately after this call (>= 1).
 * @param newlyStored    {@code true} only when this call physically inserted the row (first writer
 *                       for this content); {@code false} on a dedup hit (ref_count incremented on
 *                       an already-existing row).
 */
public record LargePayloadReference(UUID id, String contentHash, int refCountAfter, boolean newlyStored) {
}
