package com.threeai.nats.history.projection;

import java.util.Map;

import com.threeai.nats.core.history.HistoryClassNames;

/**
 * ACT_HI class → target table + entity-id column + field-map key → DB column translation
 * (`04_interfaces/2_projection_dtos.md` §2 "HistoryClassColumnMapping (config-benzeri sabit
 * harita, Phase 5 detayı)"). Package-private — internal {@link ProjectionStore} plumbing, not an
 * LLD-listed public class.
 *
 * <p>Field keys are the exact camelCase keys the engine-side {@code HistoryEventFieldExtractor}
 * (`camunda-nats-channel`/`cadenzaflow-nats-channel`) emits (`04_interfaces/1_wire_contract_refs.md`
 * — the wire {@code payload} is opaque to this module; its JSON keys are the contract). Most
 * translate to their snake_case DB column automatically ({@link #columnFor}); a small override
 * table covers the columns whose name diverges from the field key (e.g. {@code TASKINST.name} →
 * {@code task_name}, not {@code name}). An unmapped field key is silently dropped (DEBUG log, not
 * an error) — the wire payload may carry more fields than this basamak's DB columns model.
 */
final class HistoryClassColumnMapping {

    /**
     * @param hasProcessInstanceIdColumn whether the table carries a separate
     *                                   {@code process_instance_id} column referencing the
     *                                   OWNING process instance (true for ACTINST/VARINST/
     *                                   TASKINST/INCIDENT — false for PROCINST itself, whose
     *                                   entity id already IS the process instance id, and for
     *                                   CASEINST, which has no such column, per
     *                                   {@code V1__entity_lifecycle_tables.sql})
     */
    record TableMeta(String tableName, String entityIdColumn, boolean hasProcessInstanceIdColumn) {
        boolean isEntityLifecycle() {
            return entityIdColumn != null;
        }
    }

    private static TableMeta appendOnly(String tableName) {
        return new TableMeta(tableName, null, false);
    }

    private static final Map<String, TableMeta> TABLES = Map.ofEntries(
            // Entity-lifecycle (DB_SCHEMA.md §2.2)
            Map.entry(HistoryClassNames.PROCINST, new TableMeta("process_instance_history", "process_instance_id", false)),
            Map.entry(HistoryClassNames.ACTINST, new TableMeta("activity_instance_history", "activity_instance_id", true)),
            Map.entry(HistoryClassNames.VARINST, new TableMeta("variable_instance_history", "variable_instance_id", true)),
            Map.entry(HistoryClassNames.TASKINST, new TableMeta("task_instance_history", "task_id", true)),
            Map.entry(HistoryClassNames.INCIDENT, new TableMeta("incident_history", "incident_id", true)),
            Map.entry(HistoryClassNames.CASEINST, new TableMeta("case_instance_history", "case_instance_id", false)),
            // Append-only log (DB_SCHEMA.md §2.4) -- all 9 carry process_instance_id directly.
            Map.entry(HistoryClassNames.DETAIL, appendOnly("variable_detail_history")),
            Map.entry(HistoryClassNames.IDENTITYLINK, appendOnly("identity_link_history")),
            Map.entry(HistoryClassNames.OP_LOG, appendOnly("operation_log_history")),
            Map.entry(HistoryClassNames.EXT_TASK_LOG, appendOnly("ext_task_log_history")),
            Map.entry(HistoryClassNames.JOB_LOG, appendOnly("job_log_history")),
            Map.entry(HistoryClassNames.COMMENT, appendOnly("comment_history")),
            Map.entry(HistoryClassNames.ATTACHMENT, appendOnly("attachment_history")),
            Map.entry(HistoryClassNames.DECINST, appendOnly("decision_evaluation_history")),
            Map.entry(HistoryClassNames.BATCH, appendOnly("batch_history")));

    /** Field-key → column-name overrides where automatic camelCase→snake_case would be wrong. */
    private static final Map<String, Map<String, String>> COLUMN_OVERRIDES = Map.of(
            HistoryClassNames.TASKINST, Map.of("name", "task_name"),
            HistoryClassNames.DETAIL, Map.of("textValue", "variable_value_text"),
            HistoryClassNames.IDENTITYLINK, Map.of("type", "link_type"),
            HistoryClassNames.JOB_LOG, Map.of("jobDefinitionType", "job_def_type", "jobExceptionMessage", "exception_message"),
            HistoryClassNames.BATCH, Map.of("type", "batch_type"));

    private HistoryClassColumnMapping() {
    }

    static TableMeta tableFor(String historyClass) {
        TableMeta meta = TABLES.get(historyClass);
        if (meta == null) {
            throw new IllegalArgumentException("Unknown ACT_HI class for projection store: " + historyClass);
        }
        return meta;
    }

    static String columnFor(String historyClass, String fieldKey) {
        Map<String, String> overrides = COLUMN_OVERRIDES.get(historyClass);
        if (overrides != null && overrides.containsKey(fieldKey)) {
            return overrides.get(fieldKey);
        }
        return camelToSnake(fieldKey);
    }

    static String camelToSnake(String camel) {
        StringBuilder sb = new StringBuilder(camel.length() + 8);
        for (int i = 0; i < camel.length(); i++) {
            char c = camel.charAt(i);
            if (Character.isUpperCase(c)) {
                sb.append('_').append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
