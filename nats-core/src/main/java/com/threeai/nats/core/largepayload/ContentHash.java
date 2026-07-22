package com.threeai.nats.core.largepayload;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * SHA-256 (lower-case hex, 64 chars) — the basamak-3 content-addressing key
 * (`docs/08-large-variable-externalization.md` D-B'/D-D'). Shared by {@link
 * ContentAddressedLargePayloadStore} (the projection-side write/read/release path) and the
 * engine-side custom variable serializer, which computes the SAME hash locally (pure, no I/O)
 * before ever calling the store, so the reference marker it stages in {@code ACT_RU_VARIABLE} is
 * self-describing and independently re-derivable for verification.
 */
public final class ContentHash {

    private static final String ALGORITHM = "SHA-256";
    public static final int HEX_LENGTH = 64;

    private ContentHash() {
    }

    /** @return the lower-case hex SHA-256 digest of {@code payload} (64 chars). */
    public static String sha256Hex(byte[] payload) {
        if (payload == null) {
            throw new IllegalArgumentException("Cannot hash a null payload");
        }
        try {
            MessageDigest digest = MessageDigest.getInstance(ALGORITHM);
            byte[] hash = digest.digest(payload);
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by every JDK security provider (JLS/JCA baseline) -- unreachable
            // in practice; wrapped so callers never need to declare a checked exception for it.
            throw new IllegalStateException("JVM does not provide the mandatory SHA-256 algorithm", e);
        }
    }

    /** @return {@code true} if {@code candidate} is shaped like a value this class could have produced. */
    public static boolean isWellFormedHex(String candidate) {
        if (candidate == null || candidate.length() != HEX_LENGTH) {
            return false;
        }
        for (int i = 0; i < candidate.length(); i++) {
            char c = candidate.charAt(i);
            boolean hex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
            if (!hex) {
                return false;
            }
        }
        return true;
    }

    /** Convenience overload for callers holding a UTF-8 string payload (e.g. reference markers). */
    public static String sha256Hex(String payload) {
        return sha256Hex(payload.getBytes(StandardCharsets.UTF_8));
    }
}
