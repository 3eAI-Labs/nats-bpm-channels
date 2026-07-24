package com.threeai.nats.cibseven.history;

import java.util.LinkedHashSet;
import java.util.Set;

import com.threeai.nats.core.history.HistoryClassNames;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code spring.nats.cibseven.history.*} — audit-critical/bulk classification map + pseudonymization
 * opt-in (PO-Q5, BA-Q4/Q5, `08_config.md` §1). Deliberately a SEPARATE config-tree from
 * {@code A2Properties}/{@code SubscriptionConfig} (basamak-1 `A2ConsumerConfig` naming-collision
 * lesson, `03_classes/1_handler_outbox.md` §4).
 *
 * <p>cadenzaflow ayna uses the identical class shape under prefix
 * {@code spring.nats.cadenzaflow.history}.
 */
@ConfigurationProperties(prefix = "spring.nats.cibseven.history")
public class HistoryClassificationProperties {

    /** PO-Q5 default — kiracı override edebilir. */
    private Set<String> auditCriticalClasses = new LinkedHashSet<>(HistoryClassNames.DEFAULT_AUDIT_CRITICAL_CLASSES);

    /** US-G3 — kiracı opt-in (default false). */
    private boolean pseudonymizationOptIn = false;

    /** Pseudonym keyed-hash anahtarı (OpenBao/deploy-secret referansı — bkz. {@code PseudonymTokenGenerator} CODER-NOTE). */
    private String tenantKeyId;

    /** Rotasyon takibi. */
    private int tenantKeyVersion = 1;

    public Set<String> getAuditCriticalClasses() {
        return auditCriticalClasses;
    }

    public void setAuditCriticalClasses(Set<String> auditCriticalClasses) {
        this.auditCriticalClasses = auditCriticalClasses;
    }

    public boolean isPseudonymizationOptIn() {
        return pseudonymizationOptIn;
    }

    public void setPseudonymizationOptIn(boolean pseudonymizationOptIn) {
        this.pseudonymizationOptIn = pseudonymizationOptIn;
    }

    public String getTenantKeyId() {
        return tenantKeyId;
    }

    public void setTenantKeyId(String tenantKeyId) {
        this.tenantKeyId = tenantKeyId;
    }

    public int getTenantKeyVersion() {
        return tenantKeyVersion;
    }

    public void setTenantKeyVersion(int tenantKeyVersion) {
        this.tenantKeyVersion = tenantKeyVersion;
    }

    public boolean isAuditCritical(String historyClass) {
        return auditCriticalClasses.contains(historyClass);
    }
}
