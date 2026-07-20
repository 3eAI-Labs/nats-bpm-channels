# 99 — Deployment & Konfigürasyon Notları

---

## 1. `application.yml` örnek konfigürasyon (engine tarafı, Camunda — cadenzaflow ayna `spring.nats.cadenzaflow.history`)

```yaml
spring:
  nats:
    camunda:
      history:
        audit-critical-classes: [OP_LOG, INCIDENT, EXT_TASK_LOG]
        pseudonymization-opt-in: false
        tenant-key-id: "tenant-42-history-pseudonym"
        tenant-key-version: 1
        outbox:
          relay-cycle-period-seconds: 30
          stuck-threshold-multiplier: 5
```

**`nats-history-projection` örnek (`application.yml`, motor-dışı servis):**

```yaml
history:
  projection:
    datasource:
      jdbc-url: jdbc:postgresql://projection-pg:5432/history_projection
    partition-count: 8
  vault:
    datasource:
      jdbc-url: jdbc:postgresql://vault-pg:5432/pseudonym_vault    # AYRI örnek -- projection ile PAYLAŞILMAZ
      vault-column-encryption-key-ref: "openbao:secret/history-vault/column-key"
  reconciliation:
    cron: "0 0 3 * * *"
    clean-streak-target-default: 7
  retention:
    bulk-default-days: 90
    audit-critical-default-window: "P7Y"
  cutover:
    volume-priority-order: [DETAIL, VARINST, ACTINST, JOB_LOG, TASKINST, PROCINST, COMMENT, ATTACHMENT, IDENTITYLINK, DECINST, CASEINST, BATCH, EXT_TASK_LOG, INCIDENT, OP_LOG]
```

Bootstrap sırası (engine node): `NatsTransportSecurityGuard` (basamak-1, devralınan) → `HistoryClassificationProperties`/`HistoryOutboxProperties` yükleme + `VAL_HISTORY_LEVEL_AUDIT_CRITICAL_MISMATCH` guard → `JetStreamKvManager.ensureBucket("history-relay-leader", ...)` + `ensureBucket("history-cutover-state", ...)` → `ClassCutoverStateRegistry.loadAtBootstrap()` → `NatsHistoryEventHandler` kurulumu (`customHistoryEventHandlers` ekleme, `enableDefaultDbHistoryEventHandler=false` SABİT) → engine start.

---

## 2. Stream/KV provisioning

`HistoryStreamProvisioner.ensureHistoryStreams(...)` (`03_classes/5_bench.md §2`) `HISTORY` (Limits, `SubjectTransform Partition(8,3)`, 7g default retention) ve `DLQ_HISTORY` (Limits, 14g default, ayrı-stream CQ-6) stream'lerini sağlar. Prod'da PR-ile-provisioning kuralı (`NATS_JETSTREAM.md §5` kural 4/5, basamak-1'den devralınan disiplin) AYNEN geçerli: `deployment/nats/streams/history.yaml`, `deployment/nats/streams/dlq-history.yaml`, `deployment/nats/kv/history-relay-leader.yaml`, `deployment/nats/kv/history-cutover-state.yaml`.

---

## 3. Partition rebalance runbook (LLD-Q2/ARCH-Q3 — bakım-penceresi, CANLI DEĞİL)

1. Yeni instance başlatımını (yeni process instance'lar için publish) DURDUR veya kısa bir dondurma penceresi (birkaç dakika) kabul et.
2. Tüm `HistoryProjectionConsumer` partition'larının lag'inin (`nats.history.projection.lag_seconds`) 0'a yaklaştığını doğrula.
3. `HISTORY` stream `SubjectTransform`'unu yeni `N`'e güncelle (`nats stream edit` veya `deployment/nats/streams/history.yaml` PR).
4. `history.projection.partition-count` config'ini `N_yeni`'e güncelle, `nats-history-projection` replika sayısını `N_yeni`'e ayarla (`partitionAssignment` veya `replicaOrdinal % N_yeni`).
5. Yeni consumer'ları başlat, publish'i devam ettir.

**Rolling-restart flip runbook (ARCH-Q5 — cutover uygulama mekanizması):**
1. `CutoverControlPlane.requestCutover(...)` → `history-cutover-state` KV'sine `cutover.<engineId>.<class>=true` yazılır, `class_cutover_state.state=CUTOVER_TALEP`.
2. Engine node replikaları SIRAYLA rolling-restart edilir (K8s `RollingUpdate` veya eşdeğer — deploy-spesifik, bu repo tetikleyiciyi SAĞLAMAZ, yalnız KV sinyalini üretir).
3. Her replika restart'ta `ClassCutoverStateRegistry.loadAtBootstrap()` yeni KV değerini okur → o sınıf için `internalDbDelegate.handleEvent(...)` çağrısı ARTIK YAPILMAZ.
4. Tüm replikalar restart tamamlanınca (health-check yeşil) → `class_cutover_state.state=CUTOVERLANMIS`, `cutover_applied_at=now()`.
5. Apply başarısız (KV yazımı veya restart-orkestrasyon hatası) → `SYS_CUTOVER_CONFIG_APPLY_FAILED`, dual-run DEVAM eder (fail-safe).

---

## 4. Projeksiyon/Kasa PG kiracı-owned RTO/RPO şablonu (NFR-R8)

`TENANT_PII_CHECKLIST_TEMPLATE.md` (basamak-2 genişletmesi) aşağıdaki iki alanı ZORUNLU kiracı-parametresi olarak taşımalıdır (Phase 5'te şablona eklenecek):

| Parametre | Önerilen default | Not |
|---|---|---|
| Projeksiyon PG RPO | ≤15dk (WAL-archiving/`pg_basebackup` sıklığı) | Store kesintisi audit KAYBI üretmez (outbox/relay dayanıklılığı) — yalnız projeksiyon-lag büyür |
| Projeksiyon PG RTO | ≤1sa (standby/restore) | |
| Pseudonym kasası RPO/RTO | Projeksiyon ile AYNI sınıf (L4-bitişik, ARCH-Q2) ama AYRI backup rotasyonu (izolasyon ilkesi, DP-16) | |

---

## 5. Boundary-timer / bulk-sınıf modelleme rehberi — pointer

Basamak-1'in `99_deployment.md §3` boundary-timer rehberi bu basamakta DEĞİŞMEZ (Flowable-özgü, D-G kapsam dışı).

## 6. Cockpit-körleşme operatör notu (US-C2)

`03_classes/3_query_api.md §2` tablosu — sınıf cutover'landığında ilgili Cockpit-history görünümünün karardığı ve `HistoryQueryApi`'nin telafi ettiği, runbook'a (kiracı ops dokümantasyonu, Phase 5/6) taşınır.

## 7. Silinen/değişen dosya YOK

Basamak-1'in aksine (JavaDelegate phase-out), basamak-2 mevcut hiçbir sınıfı SİLMEZ — yalnız EKLER (yeni `nats-history-projection` modülü + engine-side yeni sınıflar).

## 8. pom.xml değişiklikleri

`02_package_structure.md §1` (root `<modules>` + yeni `nats-history-projection`).
