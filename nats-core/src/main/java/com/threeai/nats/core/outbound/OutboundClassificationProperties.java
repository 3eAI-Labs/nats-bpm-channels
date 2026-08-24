package com.threeai.nats.core.outbound;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code spring.nats.outbound.*} — outbound message-type → critical/best-effort classification +
 * per-type process-variable allowlist (increment 4, decision D-C'; modelled on increment 2's
 * {@code HistoryClassificationProperties}/{@code isAuditCritical} pattern).
 *
 * <p><b>CODER-NOTE (placement — nats-core, NOT per-engine mirrored):</b> unlike
 * {@code HistoryClassificationProperties} (which increment 2 deliberately duplicated per engine
 * module under separate prefixes {@code spring.nats.camunda.history}/{@code
 * spring.nats.cadenzaflow.history} to avoid a defensive naming collision — see that class's
 * Javadoc), this class lives ONCE in {@code nats-core} under the single engine-neutral prefix
 * {@code spring.nats.outbound}, per the increment 4 planning brief's explicit module placement. This
 * is also the structurally correct modeling choice: a message TYPE's criticality (e.g.
 * {@code "order.created"}) is a business/tenant-wide decision, not an engine-specific one — the
 * SAME type should classify identically regardless of which engine (camunda/cadenzaflow) emits it;
 * the {@code <engineId>} subject segment ({@link OutboundSubjectBuilder}) already disambiguates the
 * two downstream. If a tenant ever co-deploys both engine modules in the SAME Spring context, both
 * auto-configurations bind the SAME bean/prefix — a deliberate simplification, flagged here for
 * visibility (CODER-QUESTIONS in the implementation return report).
 *
 * <p><b>CODER-NOTE (default classification):</b> {@code criticalTypes} defaults to EMPTY — every
 * unrecognized message type is {@link OutboundClassification#BEST_EFFORT} unless explicitly opted
 * into the durable/at-least-once path. Increment 2's {@code HistoryClassificationProperties} shipped
 * a CURATED non-empty default (fixed {@code ACT_HI_*} class names the engine itself produces);
 * outbound message TYPES are 100% tenant/business-defined strings with no engine-known default set
 * to seed — an empty default is the only defensible choice here.
 */
@ConfigurationProperties(prefix = "spring.nats.outbound")
public class OutboundClassificationProperties {
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
     * Master switch for the outbound-handoff capability. When false the relay, its leader
     * lease and the outbox writer are not created and BPMN message-throw/send-task keep the
     * engine's own behaviour.
     *
     * <p>Like history offload, this was previously gated only on a DataSource bean being
     * present — which every engine has — so the capability activated on classpath presence
     * alone and provisioned its KV leader bucket at every boot.     *
     * <p>Default is OFF. The capability activates only when this is set explicitly, which is
     * what "independent and opt-in" has to mean if the phrase is to be true: putting the
     * library on the classpath for one capability must not start the others.
     */
    private boolean enabled = false;

    /** D-C' — tenant opt-in into the critical/at-least-once outbox+relay path. Default: none (best-effort). */
    private Set<String> criticalTypes = new LinkedHashSet<>();

    /**
     * Per-type allowlist of process-variable names captured IN-TX and included in the outbound
     * payload's {@code variables} object (A2/increment 2 {@code variableAllowlist} precedent — PII
     * minimization by default). Default is EMPTY for every type — the identity-only envelope
     * (processInstanceId/businessKey/messageType) is preserved unless a type explicitly opts in.
     */
    private Map<String, List<String>> variableAllowlist = new LinkedHashMap<>();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Set<String> getCriticalTypes() {
        return criticalTypes;
    }

    public void setCriticalTypes(Set<String> criticalTypes) {
        this.criticalTypes = criticalTypes;
    }

    public Map<String, List<String>> getVariableAllowlist() {
        return variableAllowlist;
    }

    public void setVariableAllowlist(Map<String, List<String>> variableAllowlist) {
        this.variableAllowlist = variableAllowlist;
    }

    public OutboundClassification classify(String messageType) {
        return criticalTypes.contains(messageType) ? OutboundClassification.CRITICAL : OutboundClassification.BEST_EFFORT;
    }

    /** @return the configured allowlist for {@code messageType}, or an empty list (default — no capture) if unconfigured. */
    public List<String> variableAllowlistFor(String messageType) {
        return variableAllowlist.getOrDefault(messageType, List.of());
    }
}
