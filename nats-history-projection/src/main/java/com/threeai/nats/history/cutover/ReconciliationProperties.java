package com.threeai.nats.history.cutover;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.threeai.nats.core.history.HistoryClassNames;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code history.reconciliation.*} (`08_config.md` §5).
 *
 * <p><b>CODER-NOTE (auditCriticalClasses field, beyond the LLD sketch):</b> {@code
 * ReconciliationJob} needs to know which classes are audit-critical for BA-Q2's clean-definition
 * branch, but this module (`nats-history-projection`) is engine-neutral and does not share config
 * with {@code HistoryClassificationProperties} (engine-side, `camunda-nats-channel`/
 * `cadenzaflow-nats-channel`) — the two config trees are independent by module-boundary design
 * (`02_package_structure.md` §3). Defaults to the same PO-Q5 set; tenants who override the
 * audit-critical set on the engine side should mirror the override here too.
 */
@ConfigurationProperties(prefix = "history.reconciliation")
public class ReconciliationProperties {

    private String cron = "0 0 3 * * *";
    private int cleanStreakTargetDefault = 7;
    private Map<String, Integer> cleanStreakTargetOverrides = new HashMap<>();
    private Map<String, Long> bulkEpsilonOverrides = new HashMap<>();
    private Set<String> auditCriticalClasses = new LinkedHashSet<>(HistoryClassNames.DEFAULT_AUDIT_CRITICAL_CLASSES);

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public int getCleanStreakTargetDefault() {
        return cleanStreakTargetDefault;
    }

    public void setCleanStreakTargetDefault(int cleanStreakTargetDefault) {
        if (cleanStreakTargetDefault <= 0) {
            // VAL_RECONCILIATION_WINDOW_N_INVALID -- config rejected, default (7) retained.
            return;
        }
        this.cleanStreakTargetDefault = cleanStreakTargetDefault;
    }

    public Map<String, Integer> getCleanStreakTargetOverrides() {
        return cleanStreakTargetOverrides;
    }

    public void setCleanStreakTargetOverrides(Map<String, Integer> cleanStreakTargetOverrides) {
        this.cleanStreakTargetOverrides = cleanStreakTargetOverrides;
    }

    public Map<String, Long> getBulkEpsilonOverrides() {
        return bulkEpsilonOverrides;
    }

    public void setBulkEpsilonOverrides(Map<String, Long> bulkEpsilonOverrides) {
        this.bulkEpsilonOverrides = bulkEpsilonOverrides;
    }

    public int cleanStreakTargetFor(String historyClass) {
        return cleanStreakTargetOverrides.getOrDefault(historyClass, cleanStreakTargetDefault);
    }

    public long bulkEpsilonFor(String historyClass) {
        return bulkEpsilonOverrides.getOrDefault(historyClass, 0L);
    }

    public Set<String> getAuditCriticalClasses() {
        return auditCriticalClasses;
    }

    public void setAuditCriticalClasses(Set<String> auditCriticalClasses) {
        this.auditCriticalClasses = auditCriticalClasses;
    }
}
