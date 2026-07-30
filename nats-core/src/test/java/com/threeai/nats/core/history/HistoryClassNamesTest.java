package com.threeai.nats.core.history;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HistoryClassNamesTest {

    @Test
    void allClasses_has15Entries_noOverlapBetweenTheTwoBehavioralSets() {
        assertThat(HistoryClassNames.ALL_CLASSES).hasSize(15);
        assertThat(HistoryClassNames.ENTITY_LIFECYCLE_CLASSES).hasSize(6);
        assertThat(HistoryClassNames.APPEND_ONLY_LOG_CLASSES).hasSize(9);
        assertThat(HistoryClassNames.ENTITY_LIFECYCLE_CLASSES)
                .doesNotContainAnyElementsOf(HistoryClassNames.APPEND_ONLY_LOG_CLASSES);
    }

    @Test
    void defaultAuditCriticalClasses_matchesPoQ5() {
        assertThat(HistoryClassNames.DEFAULT_AUDIT_CRITICAL_CLASSES)
                .containsExactlyInAnyOrder(HistoryClassNames.OP_LOG, HistoryClassNames.INCIDENT,
                        HistoryClassNames.EXT_TASK_LOG);
    }

    @Test
    void unclassified_isNotInAllClasses() {
        assertThat(HistoryClassNames.ALL_CLASSES).doesNotContain(HistoryClassNames.UNCLASSIFIED);
    }
}
