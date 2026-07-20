# 03.3 — EPIC-C: Sorgu-API + Cockpit-Körleşme

**Modül:** `nats-history-projection` (`com.threeai.nats.history.query.*`).
**Kaynak ADR:** 0014 (read-only REST, çekirdek-4, ARCH-Q4 dağıtım+authz).
**HLD:** §3.3.1…§3.3.2. **Kontrat:** `api/openapi.yaml` (tek doğruluk kaynağı — burada inline şema TEKRARLANMAZ, ADR-0014).

---

## 1. `HistoryQueryApi` — çekirdek-4, read-only (BR-QRY-001/003, ADR-0014)

```java
package com.threeai.nats.history.query;

/** Embeddable service bean (ARCH-Q4 "gömülebilir" mod -- tenant wires it directly into their own
 *  gateway) AND, when nats-history-projection is run standalone, exposed via HistoryQueryController
 *  (REST, opsiyonel standalone mod). Authz is delegated to HistoryQueryAuthzSpi (pluggable). */
public class HistoryQueryApi {

    public HistoryQueryApi(ProjectionStore projectionStore, PiiMaskingService maskingService,
            HistoryQueryAuthzSpi authzSpi) { ... }

    /** çekirdek-4 desen 1: processInstanceId -> full history (all classes present for that instance
     *  across all 15 class tables). VAL_QUERY_UNSUPPORTED_PATTERN if request carries aggregation
     *  flags. AUTH_QUERY_ACCESS_DENIED if authzSpi.check(...) fails. */
    public ProcessInstanceHistoryResponse getProcessInstanceHistory(String processInstanceId, QueryContext ctx);

    /** çekirdek-4 desen 2 (businessKey) / desen 3 (time-range + processDefinition) -- both resolve
     *  against process_instance_history, paginated (page/size + meta.total). */
    public PagedResponse<ProcessInstanceSummary> listProcessInstanceHistory(ProcessInstanceListQuery query, QueryContext ctx);

    /** çekirdek-4 desen 4: instance -> activity/task/variable history, paginated, PII-masked per role
     *  (PiiMaskingService -- variable value, operator identity, free text masked when
     *  ctx.hasPiiViewPermission()==false, DP-15; BUS_QUERY_PII_MASKED informational marker in
     *  response meta). */
    public PagedResponse<ActivityHistoryEntry> listActivityHistory(String processInstanceId, PageRequest page, QueryContext ctx);
    public PagedResponse<TaskHistoryEntry> listTaskHistory(String processInstanceId, PageRequest page, QueryContext ctx);
    public PagedResponse<VariableHistoryEntry> listVariableHistory(String processInstanceId, PageRequest page, QueryContext ctx);
}
```

**`HistoryQueryAuthzSpi` (pluggable, ARCH-Q4):**

```java
package com.threeai.nats.history.query;

/** Tenant provides the implementation (Keycloak/APISIX/JWT -- deployment-specific, SRS §4.7). The
 *  openapi contract's envelope/masking/pattern-scope is fixed regardless of this SPI's binding. */
public interface HistoryQueryAuthzSpi {
    boolean isAuthorized(QueryContext ctx, QueryOperation operation);
    boolean hasPiiViewPermission(QueryContext ctx);
}
```

**Kapsam (BA-Q3, cutover-bağımsız):** `listProcessInstanceHistory`/vb. sınıf filtreler UYGULAMAZ — projeksiyonda VAR olan her sınıf sunulur; yanıtın `cutoverState` alanı bilgilendiricidir (`class_cutover_state.state`, `03_classes/4_cutover_reconciliation.md` §2.2'den okunur), teknik filtre DEĞİLDİR.

**Bağımlılık:** BR-QRY-001/003, FR-C1, US-C1, ADR-0014.

---

## 2. Cockpit-körleşme dokümantasyonu (BR-QRY-002, ADR-0014) — kod DEĞİL, sınıf-başına harita

Kod üretmez; teslimat = tablo (aşağıda) + operatör runbook notu (`99_deployment.md` §6). Kaynak: HLD §11 kalem 1 (Camunda manual doğrulaması — Cockpit history görünümleri `ACT_HI_*`'e bağlı, enterprise-only, ayrı DB'ye yazım görünümleri bozar).

| Cockpit history görünümü | Bağlı ACT_HI sınıf(lar)ı | Cutover sonrası davranış | Telafi |
|---|---|---|---|
| Process Instance History (activity/variable timeline) | ACTINST, VARINST, DETAIL | Sınıf cutover'landıysa görünüm o sınıf için BOŞ/eksik | `HistoryQueryApi.listActivityHistory`/`listVariableHistory` |
| Task History | TASKINST, IDENTITYLINK, COMMENT, ATTACHMENT | Aynı | `listTaskHistory` |
| User Operation Log (audit) | OP_LOG | Aynı (audit-kritik, nihayetinde cutover'lanır — BR-HDL-005) | Sorgu-API + `operation_log_history` (pseudonym-aware) |
| Incidents | INCIDENT | Aynı | Sorgu-API |
| Process Definition History (instance list) | PROCINST | Aynı | `listProcessInstanceHistory` (businessKey/zaman-aralığı) |
| **Runtime Cockpit** (task-list, process-diagram, `ACT_RU_*`) | — (runtime tablolar) | **ETKİLENMEZ** (FR-C2) | Gerekmez |

**Geri-dönüş (BR-CUT-003):** sınıf `enableDefaultDbHistoryEventHandler`-eşdeğeri geri açılınca (LLD-Q3 mekanizması + rolling-restart), Cockpit-history görünümü otomatik geri gelir (ACT_HI yeniden dolmaya başlar) — kod değişikliği gerekmez.

**Bağımlılık:** BR-QRY-002, FR-C2, US-C2, ADR-0014.
