package com.threeai.nats.history.cutover;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.threeai.nats.core.history.HistoryClassNames;
import org.junit.jupiter.api.Test;

/** Package-private {@code ReconciliationJob} helper -- direct unit coverage of the unknown-class guard. */
class ActHiTableNamesTest {

    @Test
    void of_knownClass_returnsActHiTableName() {
        assertThat(ActHiTableNames.of(HistoryClassNames.PROCINST)).isEqualTo("ACT_HI_PROCINST");
        assertThat(ActHiTableNames.of(HistoryClassNames.OP_LOG)).isEqualTo("ACT_HI_OP_LOG");
    }

    @Test
    void of_unknownClass_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> ActHiTableNames.of("NOT_A_REAL_CLASS"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown ACT_HI class: NOT_A_REAL_CLASS");
    }
}
