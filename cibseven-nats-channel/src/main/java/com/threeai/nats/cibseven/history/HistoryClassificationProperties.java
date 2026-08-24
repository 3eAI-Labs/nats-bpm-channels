package com.threeai.nats.cibseven.history;

import java.util.LinkedHashSet;
import java.util.Set;

import com.threeai.nats.core.history.HistoryClassNames;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code spring.nats.cibseven.history.*} — audit-critical/bulk classification map + pseudonymization
 * opt-in (PO-Q5, BA-Q4/Q5). Deliberately a SEPARATE config-tree from
 * {@code A2Properties}/{@code SubscriptionConfig} (increment 1 `A2ConsumerConfig`
 * naming-collision lesson).
 *
 * <p>The cadenzaflow mirror uses the identical class shape under prefix
 * {@code spring.nats.cadenzaflow.history}.
 */
@ConfigurationProperties(prefix = "spring.nats.cibseven.history")
public class HistoryClassificationProperties {
    /**
     * G4-P/3 (2026-08-25): post-commit publish varsayilan ASYNC (sinirli-ucus; tavanda
     * caller bloklanir). Kontrat degismez — kayip ayni telafi yoluna duser. {@code false}
     * = eski senkron yol (kacis kapisi).
     */
    private boolean asyncPublish = true;

    private int asyncPublishMaxInFlight = 256;

    public boolean isAsyncPublish() {
        return asyncPublish;
    }

    public void setAsyncPublish(boolean asyncPublish) {
        this.asyncPublish = asyncPublish;
    }

    public int getAsyncPublishMaxInFlight() {
        return asyncPublishMaxInFlight;
    }

    public void setAsyncPublishMaxInFlight(int asyncPublishMaxInFlight) {
        this.asyncPublishMaxInFlight = asyncPublishMaxInFlight;
    }


    /**
     * Master switch for the whole history-offload capability. When false, none of the
     * offload beans are created and the engine keeps its own default DB history handler —
     * ACT_HI_* behaves exactly as it does without this library on the classpath.
     *
     * <p>It exists because the capability was previously gated only on the presence of a
     * DataSource, and every engine has one: putting the jar on the classpath to use A2
     * alone silently activated history offload too, which then requires the HISTORY /
     * DLQ_HISTORY streams and the compact_history_outbox table or every event fails to
     * publish. The datasheet's "every capability is independent and opt-in" was not true
     * of this one.     *
     * <p>Default is OFF. The capability activates only when this is set explicitly, which is
     * what "independent and opt-in" has to mean if the phrase is to be true: putting the
     * library on the classpath for one capability must not start the others.
     */
    private boolean enabled = false;

    /** PO-Q5 default — tenants may override. */
    private Set<String> auditCriticalClasses = new LinkedHashSet<>(HistoryClassNames.DEFAULT_AUDIT_CRITICAL_CLASSES);

    /** US-G3 — tenant opt-in (default false). */
    private boolean pseudonymizationOptIn = false;

    /** Pseudonym keyed-hash key (OpenBao/deploy-secret reference — see {@code PseudonymTokenGenerator} CODER-NOTE). */
    private String tenantKeyId;

    /** Rotation tracking. */
    private int tenantKeyVersion = 1;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

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
