package com.threeai.nats.core.largepayload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class ExternalizationMarkerTest {

    @Test
    void encodeThenDecode_roundTrips() {
        String hash = ContentHash.sha256Hex("payload".getBytes(StandardCharsets.UTF_8));

        byte[] marker = ExternalizationMarker.encode(hash);

        assertThat(ExternalizationMarker.decode(marker)).contains(hash);
    }

    @Test
    void decode_ordinaryContent_empty() {
        byte[] ordinary = "just a regular small byte-array value".getBytes(StandardCharsets.UTF_8);

        assertThat(ExternalizationMarker.decode(ordinary)).isEmpty();
    }

    @Test
    void decode_null_empty() {
        assertThat(ExternalizationMarker.decode(null)).isEmpty();
    }

    @Test
    void decode_wrongLength_empty() {
        assertThat(ExternalizationMarker.decode("too short".getBytes(StandardCharsets.UTF_8))).isEmpty();
    }

    @Test
    void decode_correctLengthButNotWellFormedHexSuffix_empty() {
        // Same total length as a real marker, but the "hash" portion is not hex.
        String hash = ContentHash.sha256Hex("x".getBytes(StandardCharsets.UTF_8));
        byte[] tampered = ("NATSEXT1:" + "z".repeat(hash.length())).getBytes(StandardCharsets.US_ASCII);

        assertThat(ExternalizationMarker.decode(tampered)).isEmpty();
    }

    @Test
    void encode_malformedHash_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> ExternalizationMarker.encode("not-a-hash"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void encode_differentHashes_produceDifferentMarkers() {
        String hashA = ContentHash.sha256Hex("a".getBytes(StandardCharsets.UTF_8));
        String hashB = ContentHash.sha256Hex("b".getBytes(StandardCharsets.UTF_8));

        assertThat(ExternalizationMarker.encode(hashA)).isNotEqualTo(ExternalizationMarker.encode(hashB));
    }
}
