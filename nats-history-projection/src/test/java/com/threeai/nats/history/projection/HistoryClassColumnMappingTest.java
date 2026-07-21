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

    // --- [BLOCKING] SQL-injection allowlist regression tests (security review, commit 03439e1) ---

    @Test
    void columnFor_sqlInjectionAttempt_viaFieldKey_rejected() {
        // A malicious/unexpected wire-payload field key must NEVER resolve to a usable column --
        // camelToSnake() alone would pass punctuation through verbatim into a dynamic column list.
        assertThatThrownBy(() -> HistoryClassColumnMapping.columnFor(HistoryClassNames.PROCINST,
                "x = 1; DROP TABLE process_instance_history; --"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> HistoryClassColumnMapping.columnFor(HistoryClassNames.PROCINST,
                "state) VALUES ('x'); DROP TABLE process_instance_history; --"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> HistoryClassColumnMapping.columnFor(HistoryClassNames.PROCINST, "1=1 OR '1'='1"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void columnFor_unknownButSyntacticallySafeFieldKey_stillRejected_notOnAllowlist() {
        // A syntactically clean camelCase key with NO corresponding real column must also be
        // rejected -- the allowlist is authoritative, not just the identifier-shape regex.
        assertThatThrownBy(() -> HistoryClassColumnMapping.columnFor(HistoryClassNames.PROCINST, "notARealField"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void columnFor_everyKnownEngineExtractedField_isOnItsClassAllowlist() {
        // Regression guard: every field key the engine-side HistoryEventFieldExtractor actually
        // emits (04_interfaces/1_wire_contract_refs.md) must resolve successfully -- the allowlist
        // fix must not silently break legitimate, currently-populated fields.
        assertThat(HistoryClassColumnMapping.columnFor(HistoryClassNames.PROCINST, "processDefinitionKey"))
                .isEqualTo("process_definition_key");
        assertThat(HistoryClassColumnMapping.columnFor(HistoryClassNames.ACTINST, "taskAssignee"))
                .isEqualTo("task_assignee");
        assertThat(HistoryClassColumnMapping.columnFor(HistoryClassNames.INCIDENT, "incidentMessage"))
                .isEqualTo("incident_message");
        assertThat(HistoryClassColumnMapping.columnFor(HistoryClassNames.CASEINST, "businessKey"))
                .isEqualTo("business_key");
        assertThat(HistoryClassColumnMapping.columnFor(HistoryClassNames.EXT_TASK_LOG, "errorDetailsRef"))
                .isEqualTo("error_details_ref");
        assertThat(HistoryClassColumnMapping.columnFor(HistoryClassNames.DECINST, "decisionDefinitionId"))
                .isEqualTo("decision_definition_id");
        assertThat(HistoryClassColumnMapping.columnFor(HistoryClassNames.BATCH, "batchId")).isEqualTo("batch_id");
    }

    /**
     * CQ-3: {@code ProjectionStore.allowedColumnsFor} is the cross-package accessor {@code
     * ErasurePipeline} validates its {@code ANONYMIZATION_COLUMNS} entries against — verifies it
     * delegates to the SAME allowlist this class enforces for the write path, not a copy.
     */
    @Test
    void allowedColumnsFor_delegatesToSameAllowlist_asColumnFor() {
        assertThat(ProjectionStore.allowedColumnsFor(HistoryClassNames.VARINST))
                .contains("variable_value_text", "variable_value_ref");
        assertThat(ProjectionStore.allowedColumnsFor(HistoryClassNames.TASKINST))
                .contains("task_name", "task_description", "assignee", "owner");
        assertThat(ProjectionStore.allowedColumnsFor(HistoryClassNames.COMMENT)).contains("message");
        assertThat(ProjectionStore.allowedColumnsFor(HistoryClassNames.DETAIL)).contains("variable_value_text");
    }
}
