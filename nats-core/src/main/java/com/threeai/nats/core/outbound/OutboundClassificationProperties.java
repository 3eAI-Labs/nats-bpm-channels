package com.threeai.nats.core.outbound;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code spring.nats.outbound.*} — outbound message-type → critical/best-effort classification +
 * per-type process-variable allowlist (docs/09-outbound-handoff.md D-C', basamak-2
 * {@code HistoryClassificationProperties}/{@code isAuditCritical} deseni şablon).
 *
 * <p><b>CODER-NOTE (placement — nats-core, NOT per-engine mirrored):</b> unlike
 * {@code HistoryClassificationProperties} (which basamak-2 deliberately duplicated per engine
 * module under separate prefixes {@code spring.nats.camunda.history}/{@code
 * spring.nats.cadenzaflow.history} to avoid a defensive naming collision — see that class's
 * Javadoc), this class lives ONCE in {@code nats-core} under the single engine-neutral prefix
 * {@code spring.nats.outbound}, per the basamak-4 planning brief's explicit module placement. This
 * is also the structurally correct modeling choice: a message TYPE's criticality (e.g.
 * {@code "order.created"}) is a business/tenant-wide decision, not an engine-specific one — the
 * SAME type should classify identically regardless of which engine (camunda/cadenzaflow) emits it;
 * the {@code <engineId>} subject segment ({@link OutboundSubjectBuilder}) already disambiguates the
 * two downstream. If a tenant ever co-deploys both engine modules in the SAME Spring context, both
 * auto-configurations bind the SAME bean/prefix — a deliberate simplification, flagged here for
 * visibility (CODER-QUESTIONS in the phase-5 return report).
 *
 * <p><b>CODER-NOTE (default classification):</b> {@code criticalTypes} defaults to EMPTY — every
 * unrecognized message type is {@link OutboundClassification#BEST_EFFORT} unless explicitly opted
 * into the durable/at-least-once path. Basamak-2's {@code HistoryClassificationProperties} shipped
 * a CURATED non-empty default (fixed {@code ACT_HI_*} class names the engine itself produces);
 * outbound message TYPES are 100% tenant/business-defined strings with no engine-known default set
 * to seed — an empty default is the only defensible choice here.
 */
@ConfigurationProperties(prefix = "spring.nats.outbound")
public class OutboundClassificationProperties {

    /** D-C' — tenant opt-in into the critical/at-least-once outbox+relay path. Default: none (best-effort). */
    private Set<String> criticalTypes = new LinkedHashSet<>();

    /**
     * Per-type allowlist of process-variable names captured IN-TX and included in the outbound
     * payload's {@code variables} object (A2/basamak-2 {@code variableAllowlist} precedent — PII
     * minimization by default). Default is EMPTY for every type — the identity-only envelope
     * (processInstanceId/businessKey/messageType) is preserved unless a type explicitly opts in.
     */
    private Map<String, List<String>> variableAllowlist = new LinkedHashMap<>();

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
