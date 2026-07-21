package com.threeai.nats.history.governance;

import java.time.Duration;
import java.time.Period;
import java.util.HashMap;
import java.util.Map;

import com.threeai.nats.core.history.HistoryClassNames;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** {@code history.retention.*} (`08_config.md` §6). */
@ConfigurationProperties(prefix = "history.retention")
public class RetentionProperties {

    private int bulkDefaultDays = 90;
    /** ISO-8601 duration (PO-Q7 example: legal 7-year retention for audit-critical classes). */
    private String auditCriticalDefaultWindow = "P7Y";
    private Map<String, String> perClassOverrides = new HashMap<>();

    public int getBulkDefaultDays() {
        return bulkDefaultDays;
    }

    public void setBulkDefaultDays(int bulkDefaultDays) {
        this.bulkDefaultDays = bulkDefaultDays;
    }

    public String getAuditCriticalDefaultWindow() {
        return auditCriticalDefaultWindow;
    }

    public void setAuditCriticalDefaultWindow(String auditCriticalDefaultWindow) {
        this.auditCriticalDefaultWindow = auditCriticalDefaultWindow;
    }

    public Map<String, String> getPerClassOverrides() {
        return perClassOverrides;
    }

    public void setPerClassOverrides(Map<String, String> perClassOverrides) {
        this.perClassOverrides = perClassOverrides;
    }

    /** Resolves the retention window for a class as a {@link Duration} (days for bulk, Period-converted for audit-critical). */
    public Duration windowFor(String historyClass) {
        String override = perClassOverrides.get(historyClass);
        if (override != null) {
            return Duration.ofDays(Period.parse(override).getDays()
                    + (long) Period.parse(override).getMonths() * 30
                    + (long) Period.parse(override).getYears() * 365);
        }
        boolean auditCritical = HistoryClassNames.DEFAULT_AUDIT_CRITICAL_CLASSES.contains(historyClass);
        if (auditCritical) {
            Period p = Period.parse(auditCriticalDefaultWindow);
            return Duration.ofDays((long) p.getYears() * 365 + (long) p.getMonths() * 30 + p.getDays());
        }
        return Duration.ofDays(bulkDefaultDays);
    }

    /** Legal minimum guard: an override for an audit-critical class may never be SHORTER than the default. */
    public boolean isBelowLegalMinimum(String historyClass, Duration proposedWindow) {
        if (!HistoryClassNames.DEFAULT_AUDIT_CRITICAL_CLASSES.contains(historyClass)) {
            return false;
        }
        Period legalMinimum = Period.parse(auditCriticalDefaultWindow);
        Duration legalMinimumDuration = Duration.ofDays(
                (long) legalMinimum.getYears() * 365 + (long) legalMinimum.getMonths() * 30 + legalMinimum.getDays());
        return proposedWindow.compareTo(legalMinimumDuration) < 0;
    }
}
