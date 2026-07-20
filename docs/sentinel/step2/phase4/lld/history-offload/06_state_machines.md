# 06 — Durum Makineleri (Implementasyon Bağlaması)

**Kaynak (tek doğruluk — DEĞİŞTİRİLMEZ):** `BUSINESS_LOGIC.md §2.1` (sınıf-bazlı cutover yaşam döngüsü), `§2.2` (kompakt outbox satırı yaşam döngüsü), `§2.3` (custody-transfer), `§2.4` (PII yaşam döngüsü). Bu dosya o diyagramları TEKRARLAMAZ — her geçişi hangi LLD sınıfının uyguladığını bağlar.

---

## 1. Sınıf-bazlı cutover yaşam döngüsü (`BUSINESS_LOGIC.md §2.1`) → sınıf bağlaması

| Geçiş | Uygulayan sınıf | Metot/mekanizma |
|---|---|---|
| `[*] → DUAL_RUN` (bootstrap) | `ClassCutoverStateStore` (init) + `ClassCutoverStateRegistry` (engine-side boot okuması) | `class_cutover_state` satırı INSERT (default) |
| `DUAL_RUN → RECONCILING` | `ReconciliationJob` | `reconcileAllClasses()` |
| `RECONCILING → DUAL_RUN` (fark tespit) | `ReconciliationJob` | `evaluateGate(...)` → streak sıfırla |
| `RECONCILING → N_GUN_TEMIZ` | `ReconciliationJob` | `evaluateGate(...)` → streak ≥ N |
| `N_GUN_TEMIZ → CUTOVER_TALEP` | `CutoverControlPlane` | `requestCutover(...)` — hacim-öncelikli sıra kontrolü |
| `CUTOVER_TALEP → CUTOVERLANMIS` | `CutoverControlPlane` | KV yazımı + rolling-restart tetikleyici + health-check onayı (LLD-Q3) |
| `CUTOVER_TALEP → N_GUN_TEMIZ` (apply fail) | `CutoverControlPlane` | `SYS_CUTOVER_CONFIG_APPLY_FAILED` — kapı açık kalır |
| `CUTOVERLANMIS → DUAL_RUN` | `CutoverRollback` | `rollback(...)` |

**LLD-özgü ek not (ARCH-Q5 somutlaştırması):** `CUTOVER_TALEP → CUTOVERLANMIS` geçişi, basamak-1'in `08_config.md §1.4` "bootstrap-time validator" desenine benzer şekilde, **engine node restart'ının** `ClassCutoverStateRegistry.loadAtBootstrap()`'unu tetiklediği ANDA tamamlanır — `CutoverControlPlane` bu tamamlanmayı bir health-check/deployment-status sinyaliyle GÖZLEMLER (deploy-spesifik, `99_deployment.md §3`).

---

## 2. Kompakt outbox satırı yaşam döngüsü (`BUSINESS_LOGIC.md §2.2`) → sınıf bağlaması

| Geçiş | Uygulayan sınıf |
|---|---|
| `[*] → CREATED_IN_TX` | `CompactHistoryOutboxWriter.write(...)` |
| `CREATED_IN_TX → ROLLED_BACK` | — (pasif; tx rollback, engine motoru) |
| `CREATED_IN_TX → PENDING_RELAY` | — (pasif; tx commit) |
| `PENDING_RELAY → PUBLISHED_AWAITING_PUBACK` | `HistoryOutboxRelay.relayRow(...)` |
| `PUBLISHED_AWAITING_PUBACK → DELETED` | `HistoryOutboxRelay.relayRow(...)` (PubAck sonrası DELETE) |
| `PUBLISHED_AWAITING_PUBACK → PENDING_RELAY` (retry) | `HistoryOutboxRelay.relayRow(...)` (publish/PubAck fail) |
| `PENDING_RELAY → PENDING_RELAY` (leader-devri) | `SweepLeaderLease` (basamak-1 sınıf, yeniden kullanım) |
| `PENDING_RELAY → STUCK` | `HistoryOutboxRelay.checkStuckRows()` |
| `STUCK → PENDING_RELAY` | `HistoryOutboxRelay.relayCycle()` (otomatik, leader kurtarılınca) |

---

## 3. Custody-transfer (`BUSINESS_LOGIC.md §2.3`) → sınıf bağlaması

| Geçiş | Uygulayan sınıf |
|---|---|
| `PENDING → IN_FLIGHT` | JetStream runtime (instance-partition push) — uygulama kodu yok |
| `IN_FLIGHT → ACKED` | `HistoryProjectionConsumer.onMessage(...)` (merge-upsert veya stale no-op) |
| `IN_FLIGHT → NAKED_BACKOFF` | `HistoryProjectionConsumer.onMessage(...)` (`SYS_PROJECTION_WRITE_FAILED`) |
| `IN_FLIGHT → DLQ_ROUTED` | `HistoryDlqConsumer.routeToDlq(...)` |
| `DLQ_ROUTED → ACKED` | `DlqPublisher` (nats-core, basamak-1 reuse) dönüşü + caller ack |
| `DLQ_ROUTED → RETRY_DLQ_PUBLISH → DLQ_ROUTED` | `DlqPublisher` `FAILED_*` dönüşü + caller nak |

---

## 4. PII yaşam döngüsü (`BUSINESS_LOGIC.md §2.4`) → sınıf bağlaması

| Geçiş | Uygulayan sınıf |
|---|---|
| `[*] → ACTIVE` | `ProjectionStore.upsertEntity`/`insertLogEvent` (ilk yazım) |
| `ACTIVE → RETENTION_EXPIRED` | `RetentionEnforcementJob.enforceRetention()` |
| `ACTIVE → ERASURE_REQUESTED` | `ErasurePipeline.requestErasure(...)` |
| `RETENTION_EXPIRED → SOFT_DELETED` (bulk) | `RetentionEnforcementJob` (partition DROP/DETACH) |
| `ERASURE_REQUESTED → SOFT_DELETED` (bulk) | `ErasurePipeline.executeAnonymization(...)` |
| `ERASURE_REQUESTED → LEGAL_HOLD_KORUNUYOR` (audit-kritik) | `ErasurePipeline.requestErasure(...)` (`legal_hold=TRUE` kontrolü) |
| `SOFT_DELETED → ANONYMIZED` | `ErasurePipeline.executeAnonymization(...)` |
| `LEGAL_HOLD_KORUNUYOR → PSEUDONYMIZED` | `PseudonymTokenGenerator` (tx-içi) + `PseudonymizationVaultClient.persistMapping(...)` (downstream) |
| `PSEUDONYMIZED → MAP_ENTRY_DELETED` | `PseudonymizationVaultClient.deleteMapping(...)` |
| `LEGAL_HOLD_KORUNUYOR → LEGAL_HOLD_KORUNUYOR` (retention süresi, BA-Q8) | `RetentionEnforcementJob` (`legal_hold` satırları için AYNI retention penceresi, pseudonymized OLSA BİLE) |
