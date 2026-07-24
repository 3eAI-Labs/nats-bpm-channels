package com.threeai.nats.core.largepayload;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * The class Javadoc CODER-NOTE explains the hazard these three names must avoid: reusing a
 * BUILT-IN serializer name would let the engine's own serializer (added to the name-keyed map
 * SECOND, same {@code put} key) silently overwrite this module's custom one. {@link
 * LargeVariableSerializerNames#ALL} is the set every target-type write path consults — this test
 * pins it to exactly the three distinct, non-built-in names, guarding that invariant directly
 * (not merely "the class loads without error").
 */
class LargeVariableSerializerNamesTest {

    @Test
    void all_containsExactlyTheThreeDistinctSerializerNames() {
        assertThat(LargeVariableSerializerNames.ALL)
                .containsExactlyInAnyOrder(
                        LargeVariableSerializerNames.BYTES,
                        LargeVariableSerializerNames.OBJECT,
                        LargeVariableSerializerNames.FILE);
    }

    @Test
    void names_areDistinctFromEachOtherAndFromKnownBuiltInSerializerNames() {
        assertThat(LargeVariableSerializerNames.BYTES)
                .isNotEqualTo(LargeVariableSerializerNames.OBJECT)
                .isNotEqualTo(LargeVariableSerializerNames.FILE);
        assertThat(LargeVariableSerializerNames.OBJECT).isNotEqualTo(LargeVariableSerializerNames.FILE);

        // The exact built-in names the class Javadoc CODER-NOTE says must never be reused.
        assertThat(LargeVariableSerializerNames.ALL).noneMatch(
                name -> name.equals("Bytes") || name.equals("serializable") || name.equals("file"));
    }
}
