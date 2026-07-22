package com.threeai.nats.core.largepayload;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Self-describing RUNTIME-side reference marker (`docs/08-large-variable-externalization.md`
 * D-A'/D-E', §3). Written into the engine's {@code byteArrayValue} field ({@code ACT_GE_BYTEARRAY})
 * IN PLACE OF the real payload once the deferred externalization worker has moved that payload
 * into the content-addressed {@link ContentAddressedLargePayloadStore}.
 *
 * <p><b>CODER-NOTE (why {@code byteArrayValue}, not {@code TEXT_}/{@code TEXT2_} as docs/08 §3
 * originally sketched):</b> fork-verified against the compiled 7.24.0 engine source
 * ({@code FileValueSerializer}/{@code AbstractObjectValueSerializer}): {@code TEXT_} carries the
 * FILE type's filename and {@code TEXT2_} carries FILE's mimetype/encoding AND (separately)
 * OBJECT's Java class name — BOTH fields are genuinely occupied by data the built-in deserializers
 * still need after externalization (so a Cockpit/REST caller can keep showing a file's name without
 * dereferencing its content, and {@code readValue()} can keep deserializing an OBJECT without first
 * fetching bytes). Only {@code byteArrayValue} is free of a second purpose across ALL THREE target
 * types (BYTES/OBJECT/FILE) uniformly — reusing it here still satisfies D-E's "zero fork schema
 * change, existing {@code ValueFields} column reuse" intent (it is the EXACT column the un-externalized
 * value already occupies), it is just not literally the specific column named in the original
 * sketch. A single marker mechanism for all three types (vs. a TEXT_-based one for two types and a
 * different one for FILE) is also simpler to implement/test/reason about.
 */
public final class ExternalizationMarker {

    /** Versioned so a future encoding change can coexist with rows written under v1. */
    private static final String PREFIX = "NATSEXT1:";
    private static final byte[] PREFIX_BYTES = PREFIX.getBytes(StandardCharsets.US_ASCII);

    private ExternalizationMarker() {
    }

    /** @return the small marker byte array to stage into {@code byteArrayValue} once externalized. */
    public static byte[] encode(String contentHash) {
        if (!ContentHash.isWellFormedHex(contentHash)) {
            throw new IllegalArgumentException("Not a well-formed content hash: " + contentHash);
        }
        return (PREFIX + contentHash).getBytes(StandardCharsets.US_ASCII);
    }

    /**
     * @return the content hash if {@code byteArrayValue} is an externalization marker (this
     *         serializer's own prior write), or empty if it is ordinary (pending/never-externalized)
     *         content — the two are never ambiguous: a marker is always exactly
     *         {@code PREFIX.length() + 64} bytes of 7-bit ASCII, which real BYTES/serialized-OBJECT/
     *         FILE content essentially never collides with by chance, and even in the astronomically
     *         unlikely case it did, {@link ContentHash#isWellFormedHex} additionally constrains the
     *         suffix to lower-case hex.
     */
    public static Optional<String> decode(byte[] byteArrayValue) {
        if (byteArrayValue == null || byteArrayValue.length != PREFIX_BYTES.length + ContentHash.HEX_LENGTH) {
            return Optional.empty();
        }
        for (int i = 0; i < PREFIX_BYTES.length; i++) {
            if (byteArrayValue[i] != PREFIX_BYTES[i]) {
                return Optional.empty();
            }
        }
        String candidateHash = new String(byteArrayValue, PREFIX_BYTES.length, ContentHash.HEX_LENGTH, StandardCharsets.US_ASCII);
        return ContentHash.isWellFormedHex(candidateHash) ? Optional.of(candidateHash) : Optional.empty();
    }
}
