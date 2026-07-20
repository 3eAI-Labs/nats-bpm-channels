# 03.6 — EPIC-G: Retention / Erasure / Pseudonymization

**Modül:** `nats-history-projection` (`com.threeai.nats.history.governance.*`, `com.threeai.nats.history.vault.*`); saf-fonksiyon üretici `com.threeai.nats.core.history.PseudonymTokenGenerator` (`nats-core`, engine-side de kullanır — `02_package_structure.md` not).
**Kaynak ADR:** 0018 (retention), 0017 (erasure), 0016 (pseudonymization vault, ARCH-Q2).
**HLD:** §3.6.1…§3.6.3.

---

## 1. `RetentionEnforcementJob` (BR-PII-001, ADR-0018)

```java
package com.threeai.nats.history.governance;

public class RetentionEnforcementJob {

    public RetentionEnforcementJob(DataSource projectionDataSource, RetentionAuditLogger auditLogger,
            RetentionProperties properties /* bulk=90d default, audit-critical=legal (e.g. 7y), per-class override */) { ... }

    /** Scheduled. Per class table: finds partitions whose upper bound is older than the class's
     *  retention window, DETACH+DROP (DB_SCHEMA.md §2.6, bulk-DELETE VACUUM cost avoided --
     *  docker-proven mechanism, DB_SCHEMA.md §5 item 2). Writes retention_audit_log BEFORE dropping
     *  is NOT possible (partition name only known post-scan) -- audit row is written in the SAME
     *  transaction as the DROP where supported, else immediately after with a reconciling read-back;
     *  if the audit write itself fails post-drop -> SYS_RETENTION_AUDIT_LOG_WRITE_FAILED (CRITICAL
     *  page, compliance-invariant violation). Job-level failure (DB unreachable) ->
     *  SYS_RETENTION_JOB_FAILED (log-only, retried next period). */
    @Scheduled(cron = "${history.retention.cron:0 30 3 * * *}")
    public void enforceRetention();

    /** Bootstrap/config-validation: tenant override below legal minimum for an audit-critical class
     *  -> VAL_RETENTION_OVERRIDE_BELOW_LEGAL_MINIMUM (rejected, requires DPO/legal sign-off). */
    public void validateRetentionOverrides(RetentionProperties properties);
}
```

**BA-Q8 (retention × pseudonymization etkileşimi):** `enforceRetention()` sınıfın `legal_hold` bayrağına ve **yalnız** sınıfın kendi `retention_window`'una bakar — bir satırın `user_id_pseudonymized=true` olması retention penceresini KISALTMAZ (ADR-0016/0018 — kayıt yaşam döngüsü değişmez, yalnız alan tersinebilirliği).

**Bağımlılık:** BR-PII-001, FR-G1, US-G1, ADR-0018.

---

## 2. `ErasurePipeline` + `ErasureScopeResolver` (BR-PII-002/005, ADR-0017)

```java
package com.threeai.nats.history.governance;

public class ErasurePipeline {

    public ErasurePipeline(DataSource projectionDataSource, ErasureScopeResolver scopeResolver,
            ErasureAuditLogger auditLogger, HistoryQueryApi verificationQuery) { ... }

    /** Entry point for a data-subject erasure request. Routes by class: audit-critical target ->
     *  BUS_ERASURE_REQUEST_LEGAL_HOLD_BLOCKED (rejected, pseudonymization alternative surfaced if
     *  opt-in available -- see §3). Bulk target -> delegates to scopeResolver.resolve(...) first. */
    public ErasureRequestOutcome requestErasure(String subjectKey, ErasureTargetScope scope);

    /** Soft-delete -> anonymize on the resolved (confirmed) scope, per bulk class table
     *  (deleted_at/anonymized_at columns, DB_SCHEMA.md §2.6). Writes erasure_audit_log. On SQL
     *  failure -> SYS_ERASURE_PIPELINE_FAILED (retry + alert). After completion, calls
     *  verificationQuery to confirm the subject's PII no longer surfaces via HistoryQueryApi --
     *  failure -> RES_ERASURE_VERIFICATION_FAILED (CRITICAL, KVKK 30d SLA). */
    protected void executeAnonymization(UUID requestId, List<CandidateInstance> confirmedScope);
}

public class ErasureScopeResolver {
    /** BA-Q6: businessKey/userId resolves to >1 time-disjoint instance groups -> writes
     *  erasure_scope_confirmation (candidate_instances JSONB) and returns AMBIGUOUS
     *  (VAL_ERASURE_SUBJECT_KEY_AMBIGUOUS) -- pipeline does NOT auto-trigger. Single unambiguous
     *  group -> returns RESOLVED, pipeline proceeds directly. */
    public ScopeResolution resolve(String subjectKey);

    /** Called once the requester confirms a subset of erasure_scope_confirmation.candidate_instances. */
    public void confirmScope(UUID requestId, List<String> confirmedInstanceIds, String confirmedBy);
}
```

**Bağımlılık:** BR-PII-002/005, FR-G2, US-G2, ADR-0017. DB: `DB_SCHEMA.md §2.7`.

---

## 3. `PseudonymizationVault` (BR-PII-003/004, ADR-0016, ARCH-Q2)

```java
package com.threeai.nats.core.history;

/** Pure, no I/O -- deterministic keyed-hash (BA-Q5). Called IN-TX by CompactHistoryOutboxWriter
 *  (engine side) AND by PseudonymizationVaultClient (projection side, for map-write). Lives in
 *  nats-core so both call sites share the exact same algorithm/tenant-key version. */
public class PseudonymTokenGenerator {
    public String generate(String realValue, String tenantKeyId, int tenantKeyVersion);
}
```

```java
package com.threeai.nats.history.vault;

/** Vault-DB CLIENT -- talks to the physically separate pseudonym-vault Postgres (DB_SCHEMA.md §0/§3).
 *  Never called from the audit-critical hot path directly; invoked downstream/async from
 *  HistoryProjectionConsumer (BA-Q5) once the projection row carrying pseudonym_token is committed. */
public class PseudonymizationVaultClient {

    public PseudonymizationVaultClient(DataSource vaultDataSource, VaultAccessAuditor auditor) { ... }

    /** INSERT pseudonym_map (idempotent -- ON CONFLICT (pseudonym_token) DO NOTHING, since the same
     *  deterministic token from the same real value is expected to repeat). Writes vault_access_audit
     *  (operation=WRITE). Unreachable vault -> SYS_PSEUDONYM_VAULT_UNAVAILABLE, caller retries;
     *  audit-critical outbox/relay/NATS flow is NEVER blocked by this (BA-Q5). */
    public void persistMapping(String pseudonymToken, String engineId, String realUserId, int tenantKeyVersion, String sourceClass);

    /** DELETE FROM pseudonym_map -- hard delete, irreversible (ADR-0016). Called by ErasurePipeline
     *  when an erasure request targets a pseudonymized audit-critical record. Writes
     *  vault_access_audit (operation=DELETE) -> BUS_PSEUDONYM_MAP_ENTRY_DELETED. */
    public void deleteMapping(String pseudonymToken, String requestedBy);

    /** Rare, authorized re-identification -- requires an explicit reason string, always audited
     *  (operation=REIDENTIFY_ATTEMPT). Unauthorized caller -> AUTH_PSEUDONYM_VAULT_ACCESS_DENIED
     *  (CRITICAL, security-page) -- access-control decision made by the caller's authz layer BEFORE
     *  this method is invoked; this method logs granted=false too if invoked without authorization
     *  (defense-in-depth, DP-16). */
    public Optional<String> reidentify(String pseudonymToken, String requesterIdentity, String reason, boolean authorized);
}
```

**İzolasyon (ARCH-Q2 KARAR):** `PseudonymizationVaultClient`'ın `DataSource` bean'i **AYRI** bir Spring `@ConfigurationProperties("history.vault.datasource")` ağacından beslenir — `ProjectionStore`'un `DataSource`'uyla ASLA paylaşılmaz (fiziksel izolasyon, `08_config.md §1`).

**Bağımlılık:** BR-PII-003/004, FR-G3, US-G3, ADR-0016. DB: `DB_SCHEMA.md §3`.
