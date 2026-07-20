package com.threeai.nats.history.projection;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

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
 *
 * <p><b>[BLOCKING] SQL-injection defense-in-depth (security review, commit 03439e1 follow-up):</b>
 * {@link #columnFor} output is interpolated directly into DDL/DML column lists in
 * {@link ProjectionStore} (JDBC cannot bind identifiers via {@code ?} — only values). A
 * wire-payload field key is attacker-influenceable input (it flows from the JetStream message
 * body, ultimately originating upstream of this module's trust boundary), so {@link #camelToSnake}
 * alone — which only case-converts and passes every other character through verbatim — is NOT
 * safe to use as a column name. Every resolved column name (override-table hit OR automatic
 * conversion) is now validated against an explicit per-class ALLOWLIST derived from the actual
 * migration DDL ({@code V1__entity_lifecycle_tables.sql}/{@code V2__append_log_tables.sql}) before
 * being accepted; anything not on the allowlist is treated exactly like an unmapped field
 * (rejected here, silently skipped by {@code ProjectionStore.appendMappedFields}'s existing
 * catch-and-skip path — no behavior change for legitimate fields, only for attacker-shaped ones).
 * A secondary regex check ({@code ^[a-z][a-z0-9_]*$}) guards the allowlist entries themselves.
 * This is NOT a schema/contract change — the LLD's own intent ("write to known columns") is what
 * this enforces; the DB schema's real column set is unchanged.
 */
final class HistoryClassColumnMapping {

    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("^[a-z][a-z0-9_]*$");

    /**
     * @param hasProcessInstanceIdColumn whether the table carries a separate
     *                                   {@code process_instance_id} column referencing the
     *                                   OWNING process instance (true for ACTINST/VARINST/
     *                                   TASKINST/INCIDENT — false for PROCINST itself, whose
     *                                   entity id already IS the process instance id, and for
     *                                   CASEINST, which has no such column, per
     *                                   {@code V1__entity_lifecycle_tables.sql})
     * @param allowedColumns             the exact set of non-structural DB columns this class's
     *                                   table exposes for field-map-driven writes (SQL-injection
     *                                   allowlist — see class Javadoc [BLOCKING] note)
     */
    record TableMeta(String tableName, String entityIdColumn, boolean hasProcessInstanceIdColumn,
            Set<String> allowedColumns) {
        boolean isEntityLifecycle() {
            return entityIdColumn != null;
        }
    }

    private static TableMeta entityLifecycle(String tableName, String entityIdColumn,
            boolean hasProcessInstanceIdColumn, String... columns) {
        return new TableMeta(tableName, entityIdColumn, hasProcessInstanceIdColumn, Set.of(columns));
    }

    private static TableMeta appendOnly(String tableName, String... columns) {
        return new TableMeta(tableName, null, false, Set.of(columns));
    }

    private static final Map<String, TableMeta> TABLES = Map.ofEntries(
            // Entity-lifecycle (DB_SCHEMA.md §2.2, V1__entity_lifecycle_tables.sql)
            Map.entry(HistoryClassNames.PROCINST, entityLifecycle("process_instance_history", "process_instance_id", false,
                    "process_definition_key", "process_definition_id", "business_key", "start_user_id",
                    "super_process_instance_id", "start_time", "end_time", "duration_millis", "delete_reason", "state")),
            Map.entry(HistoryClassNames.ACTINST, entityLifecycle("activity_instance_history", "activity_instance_id", true,
                    "activity_id", "activity_type", "activity_name", "task_assignee", "start_time", "end_time",
                    "duration_millis", "canceled")),
            Map.entry(HistoryClassNames.VARINST, entityLifecycle("variable_instance_history", "variable_instance_id", true,
                    "variable_name", "variable_type", "variable_value_text", "variable_value_ref", "is_byte_array")),
            Map.entry(HistoryClassNames.TASKINST, entityLifecycle("task_instance_history", "task_id", true,
                    "task_name", "task_description", "assignee", "owner", "start_time", "end_time",
                    "duration_millis", "delete_reason", "priority")),
            Map.entry(HistoryClassNames.INCIDENT, entityLifecycle("incident_history", "incident_id", true,
                    "incident_type", "incident_message", "configuration", "activity_id", "create_time", "end_time", "resolved")),
            Map.entry(HistoryClassNames.CASEINST, entityLifecycle("case_instance_history", "case_instance_id", false,
                    "case_definition_id", "business_key", "create_time", "close_time", "state")),
            // Append-only log (DB_SCHEMA.md §2.4, V2__append_log_tables.sql) -- all 9 carry process_instance_id directly.
            Map.entry(HistoryClassNames.DETAIL, appendOnly("variable_detail_history",
                    "variable_instance_id", "variable_name", "variable_type", "revision", "variable_value_text")),
            Map.entry(HistoryClassNames.IDENTITYLINK, appendOnly("identity_link_history",
                    "task_id", "link_type", "user_id", "group_id", "operation_type")),
            Map.entry(HistoryClassNames.OP_LOG, appendOnly("operation_log_history",
                    "operation_id", "user_id", "user_id_pseudonymized", "pseudonym_token", "operation_type",
                    "entity_type", "property", "org_value", "new_value")),
            Map.entry(HistoryClassNames.EXT_TASK_LOG, appendOnly("ext_task_log_history",
                    "external_task_id", "worker_id", "topic_name", "activity_id", "error_message",
                    "error_details_ref", "state")),
            Map.entry(HistoryClassNames.JOB_LOG, appendOnly("job_log_history",
                    "job_id", "job_def_type", "exception_message", "retries", "state")),
            Map.entry(HistoryClassNames.COMMENT, appendOnly("comment_history", "task_id", "user_id", "message")),
            Map.entry(HistoryClassNames.ATTACHMENT, appendOnly("attachment_history",
                    "task_id", "user_id", "attachment_name", "attachment_type", "url", "content_ref")),
            Map.entry(HistoryClassNames.DECINST, appendOnly("decision_evaluation_history",
                    "decision_definition_id", "decision_definition_key", "root_decision_instance_id", "inputs", "outputs")),
            Map.entry(HistoryClassNames.BATCH, appendOnly("batch_history",
                    "batch_id", "batch_type", "total_jobs", "jobs_per_seed", "start_time", "end_time")));

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

    /**
     * @throws IllegalArgumentException if {@code fieldKey} does not resolve to a column on this
     *                                   class's SQL-injection allowlist (caller — {@code
     *                                   ProjectionStore.appendMappedFields} — treats this
     *                                   identically to an unmapped field: skip, DEBUG log).
     */
    static String columnFor(String historyClass, String fieldKey) {
        Map<String, String> overrides = COLUMN_OVERRIDES.get(historyClass);
        String candidate = (overrides != null && overrides.containsKey(fieldKey))
                ? overrides.get(fieldKey)
                : camelToSnake(fieldKey);

        if (!SAFE_IDENTIFIER.matcher(candidate).matches()) {
            throw new IllegalArgumentException("Resolved column '" + candidate + "' for field '" + fieldKey
                    + "' is not a safe SQL identifier");
        }
        Set<String> allowed = tableFor(historyClass).allowedColumns();
        if (!allowed.contains(candidate)) {
            throw new IllegalArgumentException("Column '" + candidate + "' is not on the allowlist for class " + historyClass);
        }
        return candidate;
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
