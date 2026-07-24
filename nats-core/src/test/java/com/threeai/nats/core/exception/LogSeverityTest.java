package com.threeai.nats.core.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * {@link LogSeverity} is the error-registry's severity taxonomy contract (`docs/sentinel/phase4/
 * ERROR_REGISTRY.md` §1) — {@code CRITICAL} specifically is a documentation/log-marker layered on
 * top of {@code ERROR} (paging side-effect), NOT a distinct SLF4J level. Pinning the exact value
 * set (order matters for {@code CRITICAL} being ranked above plain {@code ERROR}) guards the
 * registry cross-check against silent drift.
 */
class LogSeverityTest {

    @Test
    void values_matchTheDocumentedTaxonomyExactly_inSeverityOrder() {
        assertThat(LogSeverity.values())
                .extracting(Enum::name)
                .containsExactly("DEBUG", "INFO", "WARN", "ERROR", "CRITICAL");
    }

    @Test
    void valueOf_roundTripsEveryName() {
        for (LogSeverity severity : LogSeverity.values()) {
            assertThat(LogSeverity.valueOf(severity.name())).isSameAs(severity);
        }
    }

    @Test
    void critical_isRankedAboveError_viaOrdinal() {
        assertThat(LogSeverity.CRITICAL.ordinal()).isGreaterThan(LogSeverity.ERROR.ordinal());
    }
}
