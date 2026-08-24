package com.threeai.nats.core.shard;

/**
 * Reads a TOP-LEVEL scalar field from a JSON payload — the router's payload fallback for
 * the shard key (docs/13 D-C: header first, then top-level field). nats-core deliberately
 * carries no JSON parser; adapters inject a real (Jackson-backed) implementation. The
 * hand-rolled indexOf scan this project once shipped is the documented bug class this seam
 * exists to avoid (ER-parity slice 5 breaking change).
 */
@FunctionalInterface
public interface PayloadFieldReader {

    /** @return the field's scalar value as text, or null (absent/nested/non-scalar/unparseable). */
    String topLevelScalar(String payload, String field);
}
