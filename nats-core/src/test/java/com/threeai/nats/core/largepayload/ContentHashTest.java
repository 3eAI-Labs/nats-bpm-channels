package com.threeai.nats.core.largepayload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class ContentHashTest {

    @Test
    void sha256Hex_isDeterministic_sameInputSameHash() {
        byte[] payload = "hello world".getBytes(StandardCharsets.UTF_8);

        assertThat(ContentHash.sha256Hex(payload)).isEqualTo(ContentHash.sha256Hex(payload));
    }

    @Test
    void sha256Hex_knownVector_matchesPublishedDigest() {
        // NIST/RFC test vector: SHA-256("abc")
        String hash = ContentHash.sha256Hex("abc".getBytes(StandardCharsets.UTF_8));

        assertThat(hash).isEqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }

    @Test
    void sha256Hex_differentInput_differentHash() {
        byte[] a = "content-a".getBytes(StandardCharsets.UTF_8);
        byte[] b = "content-b".getBytes(StandardCharsets.UTF_8);

        assertThat(ContentHash.sha256Hex(a)).isNotEqualTo(ContentHash.sha256Hex(b));
    }

    @Test
    void sha256Hex_producesLowercaseHex64Chars() {
        String hash = ContentHash.sha256Hex(new byte[] {1, 2, 3});

        assertThat(hash).hasSize(ContentHash.HEX_LENGTH).matches("^[0-9a-f]{64}$");
    }

    @Test
    void sha256Hex_nullPayload_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> ContentHash.sha256Hex((byte[]) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void isWellFormedHex_validHash_true() {
        String hash = ContentHash.sha256Hex("x".getBytes(StandardCharsets.UTF_8));

        assertThat(ContentHash.isWellFormedHex(hash)).isTrue();
    }

    @Test
    void isWellFormedHex_wrongLength_false() {
        assertThat(ContentHash.isWellFormedHex("abc")).isFalse();
    }

    @Test
    void isWellFormedHex_nonHexChars_false() {
        assertThat(ContentHash.isWellFormedHex("z".repeat(64))).isFalse();
    }

    @Test
    void isWellFormedHex_null_false() {
        assertThat(ContentHash.isWellFormedHex(null)).isFalse();
    }

    @Test
    void sha256Hex_stringOverload_matchesByteArrayOverloadOnItsUtf8Bytes() {
        String stringOverloadResult = ContentHash.sha256Hex("hello world");
        String byteArrayOverloadResult = ContentHash.sha256Hex("hello world".getBytes(StandardCharsets.UTF_8));

        assertThat(stringOverloadResult).isEqualTo(byteArrayOverloadResult);
    }
}
