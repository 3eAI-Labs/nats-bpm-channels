package com.threeai.nats.core.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class PseudonymTokenGeneratorTest {

    private final PseudonymTokenGenerator generator = new PseudonymTokenGenerator();

    @Test
    void generate_sameInputs_sameToken_deterministic() {
        String token1 = generator.generate("user-42", "tenant-key-a", 1);
        String token2 = generator.generate("user-42", "tenant-key-a", 1);

        assertThat(token1).isEqualTo(token2);
        assertThat(token1).hasSize(64); // hex-encoded SHA-256 HMAC
    }

    @Test
    void generate_differentRealValue_differentToken() {
        String token1 = generator.generate("user-42", "tenant-key-a", 1);
        String token2 = generator.generate("user-43", "tenant-key-a", 1);

        assertThat(token1).isNotEqualTo(token2);
    }

    @Test
    void generate_differentTenantKey_differentToken() {
        String token1 = generator.generate("user-42", "tenant-key-a", 1);
        String token2 = generator.generate("user-42", "tenant-key-b", 1);

        assertThat(token1).isNotEqualTo(token2);
    }

    @Test
    void generate_keyRotation_differentVersion_differentToken() {
        String v1 = generator.generate("user-42", "tenant-key-a", 1);
        String v2 = generator.generate("user-42", "tenant-key-a", 2);

        assertThat(v1).isNotEqualTo(v2);
    }

    @Test
    void generate_neverLeaksRealValueInOutput() {
        String token = generator.generate("very-secret-user-id", "tenant-key-a", 1);

        assertThat(token).doesNotContain("very-secret-user-id");
    }

    @Test
    void generate_nullOrEmptyRealValue_rejected() {
        assertThatThrownBy(() -> generator.generate(null, "tenant-key-a", 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> generator.generate("", "tenant-key-a", 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void generate_nullOrEmptyTenantKeyId_rejected() {
        assertThatThrownBy(() -> generator.generate("user-42", null, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> generator.generate("user-42", "", 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void generate_isLowercaseHex() {
        String token = generator.generate("user-42", "tenant-key-a", 1);

        assertThat(token).matches("[0-9a-f]{64}");
    }
}
