package com.threeai.nats.history.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.threeai.nats.core.history.HistoryClassNames;
import org.junit.jupiter.api.Test;

class HistoryClassColumnMappingTest {

    @Test
    void tableFor_allKnownClasses_resolve() {
        for (String historyClass : HistoryClassNames.ALL_CLASSES) {
            assertThat(HistoryClassColumnMapping.tableFor(historyClass)).isNotNull();
        }
    }

    @Test
    void tableFor_unknownClass_throws() {
        assertThatThrownBy(() -> HistoryClassColumnMapping.tableFor("NOT_A_CLASS"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void entityLifecycleClasses_haveEntityIdColumn() {
        for (String cls : HistoryClassNames.ENTITY_LIFECYCLE_CLASSES) {
            assertThat(HistoryClassColumnMapping.tableFor(cls).isEntityLifecycle()).isTrue();
        }
    }

    @Test
    void appendOnlyClasses_haveNoEntityIdColumn() {
        for (String cls : HistoryClassNames.APPEND_ONLY_LOG_CLASSES) {
            assertThat(HistoryClassColumnMapping.tableFor(cls).isEntityLifecycle()).isFalse();
        }
    }

    @Test
    void columnFor_defaultCamelToSnakeConversion() {
        assertThat(HistoryClassColumnMapping.columnFor(HistoryClassNames.PROCINST, "businessKey"))
                .isEqualTo("business_key");
        assertThat(HistoryClassColumnMapping.columnFor(HistoryClassNames.OP_LOG, "userIdPseudonymized"))
                .isEqualTo("user_id_pseudonymized");
    }

    @Test
    void columnFor_overrides_takesPrecedenceOverDefault() {
        assertThat(HistoryClassColumnMapping.columnFor(HistoryClassNames.TASKINST, "name")).isEqualTo("task_name");
        assertThat(HistoryClassColumnMapping.columnFor(HistoryClassNames.DETAIL, "textValue"))
                .isEqualTo("variable_value_text");
        assertThat(HistoryClassColumnMapping.columnFor(HistoryClassNames.IDENTITYLINK, "type")).isEqualTo("link_type");
        assertThat(HistoryClassColumnMapping.columnFor(HistoryClassNames.JOB_LOG, "jobDefinitionType"))
                .isEqualTo("job_def_type");
        assertThat(HistoryClassColumnMapping.columnFor(HistoryClassNames.JOB_LOG, "jobExceptionMessage"))
                .isEqualTo("exception_message");
        assertThat(HistoryClassColumnMapping.columnFor(HistoryClassNames.BATCH, "type")).isEqualTo("batch_type");
    }

    @Test
    void camelToSnake_handlesConsecutiveWordsAndSingleWord() {
        assertThat(HistoryClassColumnMapping.camelToSnake("activityId")).isEqualTo("activity_id");
        assertThat(HistoryClassColumnMapping.camelToSnake("state")).isEqualTo("state");
        assertThat(HistoryClassColumnMapping.camelToSnake("processDefinitionKey")).isEqualTo("process_definition_key");
    }
}
