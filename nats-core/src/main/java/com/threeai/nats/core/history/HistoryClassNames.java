package com.threeai.nats.core.history;

import java.util.Set;

/**
 * The 15 {@code ACT_HI_*} class names this basamak offloads (`DB_SCHEMA.md` §2.1 — 6
 * entity-lifecycle + 9 append-only log classes). Shared by the engine-side classifier
 * (`camunda-nats-channel`/`cadenzaflow-nats-channel`), the projection consumer/store
 * (`nats-history-projection`), and the bench module (`nats-bpm-bench`) so the class-name string
 * literals live in exactly one place.
 */
public final class HistoryClassNames {

    // Entity-lifecycle (merge-upsert, DB_SCHEMA.md §2.2)
    public static final String PROCINST = "PROCINST";
    public static final String ACTINST = "ACTINST";
    public static final String VARINST = "VARINST";
    public static final String TASKINST = "TASKINST";
    public static final String INCIDENT = "INCIDENT";
    public static final String CASEINST = "CASEINST";

    // Append-only log (dedup insert, DB_SCHEMA.md §2.4)
    public static final String DETAIL = "DETAIL";
    public static final String IDENTITYLINK = "IDENTITYLINK";
    public static final String OP_LOG = "OP_LOG";
    public static final String EXT_TASK_LOG = "EXT_TASK_LOG";
    public static final String JOB_LOG = "JOB_LOG";
    public static final String COMMENT = "COMMENT";
    public static final String ATTACHMENT = "ATTACHMENT";
    public static final String DECINST = "DECINST";
    public static final String BATCH = "BATCH";

    /** Fail-safe-bulk fallback when the engine produces an event this basamak does not classify. */
    public static final String UNCLASSIFIED = "UNCLASSIFIED";

    public static final Set<String> ENTITY_LIFECYCLE_CLASSES =
            Set.of(PROCINST, ACTINST, VARINST, TASKINST, INCIDENT, CASEINST);

    public static final Set<String> APPEND_ONLY_LOG_CLASSES =
            Set.of(DETAIL, IDENTITYLINK, OP_LOG, EXT_TASK_LOG, JOB_LOG, COMMENT, ATTACHMENT, DECINST, BATCH);

    /** PO-Q5 default audit-critical set (`08_config.md` §1) — callers may override via config. */
    public static final Set<String> DEFAULT_AUDIT_CRITICAL_CLASSES = Set.of(OP_LOG, INCIDENT, EXT_TASK_LOG);

    public static final Set<String> ALL_CLASSES;

    static {
        var all = new java.util.HashSet<String>();
        all.addAll(ENTITY_LIFECYCLE_CLASSES);
        all.addAll(APPEND_ONLY_LOG_CLASSES);
        ALL_CLASSES = Set.copyOf(all);
    }

    private HistoryClassNames() {
    }
}
