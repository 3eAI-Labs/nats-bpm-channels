package com.threeai.nats.cadenzaflow.history;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.util.Map;

import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.core.history.exception.HistoryLevelAuditCriticalMismatchWarning;
import org.cadenzaflow.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.cadenzaflow.bpm.engine.impl.history.event.HistoryEventType;
import org.cadenzaflow.bpm.engine.impl.history.event.HistoryEventTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bootstrap-time guard: {@code VAL_HISTORY_LEVEL_AUDIT_CRITICAL_MISMATCH} (`08_config.md` §1,
 * BA-Q4 KARAR). For every configured {@code auditCriticalClasses} member, checks whether the
 * engine's current {@code HistoryLevel} would actually PRODUCE that class's events — if not,
 * WARNs (does NOT fail-fast; basamak-1's {@code UmbrellaLockValidator}/{@code VAL_UMBRELLA_LOCK_TOO_SHORT}
 * hard-reject pattern is a DELIBERATE non-match here, per BA-Q4).
 *
 * <p>Basamak-1 {@code UmbrellaLockValidator}-style bootstrap-time-check pattern (not an
 * {@code InitializingBean} — invoked explicitly by the module's {@code ProcessEnginePlugin}
 * {@code preInit}/{@code postInit} hook, where the engine configuration and history level are
 * already resolved).
 */
public class HistoryBootstrapValidator {

    private static final Logger log = LoggerFactory.getLogger(HistoryBootstrapValidator.class);

    /**
     * Representative {@link HistoryEventType} per ACT_HI class, used only to ask
     * {@code HistoryLevel.isHistoryEventProduced(type, null)} whether the level would produce
     * this class's events at all (fork-verified: built-in {@code HistoryLevel} implementations
     * compare {@code eventType} only — the {@code entity} argument is unused by
     * {@code HistoryLevelAudit}/{@code HistoryLevelFull}/etc., so passing {@code null} is safe).
     * {@code COMMENT}/{@code ATTACHMENT} are intentionally absent — see
     * {@link HistoryEventClassResolver} class Javadoc CODER-NOTE (never reach the handler SPI at
     * all, so a HistoryLevel mismatch check for them is moot).
     */
    private static final Map<String, HistoryEventType> REPRESENTATIVE_EVENT_TYPE = Map.ofEntries(
            Map.entry(HistoryClassNames.PROCINST, HistoryEventTypes.PROCESS_INSTANCE_START),
            Map.entry(HistoryClassNames.ACTINST, HistoryEventTypes.ACTIVITY_INSTANCE_START),
            Map.entry(HistoryClassNames.VARINST, HistoryEventTypes.VARIABLE_INSTANCE_CREATE),
            Map.entry(HistoryClassNames.TASKINST, HistoryEventTypes.TASK_INSTANCE_CREATE),
            Map.entry(HistoryClassNames.INCIDENT, HistoryEventTypes.INCIDENT_CREATE),
            Map.entry(HistoryClassNames.CASEINST, HistoryEventTypes.CASE_INSTANCE_CREATE),
            Map.entry(HistoryClassNames.DETAIL, HistoryEventTypes.VARIABLE_INSTANCE_UPDATE_DETAIL),
            Map.entry(HistoryClassNames.IDENTITYLINK, HistoryEventTypes.IDENTITY_LINK_ADD),
            Map.entry(HistoryClassNames.OP_LOG, HistoryEventTypes.USER_OPERATION_LOG),
            Map.entry(HistoryClassNames.EXT_TASK_LOG, HistoryEventTypes.EXTERNAL_TASK_CREATE),
            Map.entry(HistoryClassNames.JOB_LOG, HistoryEventTypes.JOB_CREATE),
            Map.entry(HistoryClassNames.DECINST, HistoryEventTypes.DMN_DECISION_EVALUATE),
            Map.entry(HistoryClassNames.BATCH, HistoryEventTypes.BATCH_START));

    private HistoryBootstrapValidator() {
    }

    public static void validate(ProcessEngineConfigurationImpl configuration,
            HistoryClassificationProperties classification, String engineId) {
        for (String historyClass : classification.getAuditCriticalClasses()) {
            HistoryEventType representativeType = REPRESENTATIVE_EVENT_TYPE.get(historyClass);
            if (representativeType == null) {
                continue; // unknown/unreachable class -- nothing meaningful to check here
            }
            boolean produced = configuration.getHistoryLevel().isHistoryEventProduced(representativeType, null);
            if (!produced) {
                HistoryLevelAuditCriticalMismatchWarning warning =
                        new HistoryLevelAuditCriticalMismatchWarning(historyClass, engineId);
                log.warn("Audit-critical class configured but current HistoryLevel would not produce its events "
                        + "-- outbox will simply never receive this class until HistoryLevel is raised",
                        kv("history_class", historyClass), kv("engine_id", engineId),
                        kv("history_level", configuration.getHistoryLevel().getName()), kv("warning", warning));
            }
        }
    }
}
