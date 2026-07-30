package com.threeai.nats.core.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * {@link ErrorCategory} is a fixed taxonomy contract (unchanged from the design's exception-code
 * list and the error-handling guideline, bound to the error
 * registry) — every error code in the registry is tagged with
 * exactly one of these five values. This test pins the exact value SET (not just "some enum
 * exists") so an accidental addition/removal/rename silently drifting from the documented
 * taxonomy fails the build rather than the next error-registry cross-check.
 */
class ErrorCategoryTest {

    @Test
    void values_matchTheDocumentedTaxonomyExactly() {
        assertThat(ErrorCategory.values())
                .extracting(Enum::name)
                .containsExactly("VAL", "BUS", "RES", "SYS", "EXT");
    }

    @Test
    void valueOf_roundTripsEveryName() {
        for (ErrorCategory category : ErrorCategory.values()) {
            assertThat(ErrorCategory.valueOf(category.name())).isSameAs(category);
        }
    }
}
