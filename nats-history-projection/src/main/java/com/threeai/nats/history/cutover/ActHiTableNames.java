package com.threeai.nats.history.cutover;

import java.util.Map;

import com.threeai.nats.core.history.HistoryClassNames;

/**
 * ACT_HI class → native fork table name (fork-verified,
 * {@code org/camunda/bpm/engine/db/create/activiti.postgres.create.{history,case.history,decision.history}.sql}).
 * Package-private — {@link ReconciliationJob}'s own read-only comparison target, not an LLD DTO.
 */
final class ActHiTableNames {

    private static final Map<String, String> TABLES = Map.ofEntries(
            Map.entry(HistoryClassNames.PROCINST, "ACT_HI_PROCINST"),
            Map.entry(HistoryClassNames.ACTINST, "ACT_HI_ACTINST"),
            Map.entry(HistoryClassNames.VARINST, "ACT_HI_VARINST"),
            Map.entry(HistoryClassNames.TASKINST, "ACT_HI_TASKINST"),
            Map.entry(HistoryClassNames.INCIDENT, "ACT_HI_INCIDENT"),
            Map.entry(HistoryClassNames.CASEINST, "ACT_HI_CASEINST"),
            Map.entry(HistoryClassNames.DETAIL, "ACT_HI_DETAIL"),
            Map.entry(HistoryClassNames.IDENTITYLINK, "ACT_HI_IDENTITYLINK"),
            Map.entry(HistoryClassNames.OP_LOG, "ACT_HI_OP_LOG"),
            Map.entry(HistoryClassNames.EXT_TASK_LOG, "ACT_HI_EXT_TASK_LOG"),
            Map.entry(HistoryClassNames.JOB_LOG, "ACT_HI_JOB_LOG"),
            Map.entry(HistoryClassNames.COMMENT, "ACT_HI_COMMENT"),
            Map.entry(HistoryClassNames.ATTACHMENT, "ACT_HI_ATTACHMENT"),
            Map.entry(HistoryClassNames.DECINST, "ACT_HI_DECINST"),
            Map.entry(HistoryClassNames.BATCH, "ACT_HI_BATCH"));

    private ActHiTableNames() {
    }

    static String of(String historyClass) {
        String table = TABLES.get(historyClass);
        if (table == null) {
            throw new IllegalArgumentException("Unknown ACT_HI class: " + historyClass);
        }
        return table;
    }
}
