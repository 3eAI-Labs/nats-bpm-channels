# Error Registry — Basamak-2 (42 Kod)

**Sentinel fazı:** Phase 4 — Developer (LLD). **Kaynak:** `docs/sentinel/step2/phase2/EXCEPTION_CODES.md` (42 kod, kategori taksonomisi — **DEĞİŞTİRİLMEZ**, yalnız Java sınıfı/log-format/metrik/alarm bağlaması eklenir), `ERROR_HANDLING_GUIDELINE.md §1/§3/§6`.
**Uyarlama notu:** basamak-1 `docs/sentinel/phase4/ERROR_REGISTRY.md`'nin aynı "Statü" odaklı (ack/nak/DLQ/retry/CRITICAL-page/build-fail) uyarlaması burada da geçerlidir — HTTP-status yerine mesajlaşma/DB/job statüsü.
**Durum:** Taslak — Levent faz-4 onayına sunuluyor.

---

## 1. Hata sınıfı hiyerarşisi (`com.threeai.nats.core.history.exception` + engine/projeksiyon sınıflarının kendi catch-dalları)

Basamak-1 deseni AYNEN: yalnız **kontrol akışını kesen** (bootstrap-fail, CRITICAL-invariant) kodlar gerçek Java exception sınıfı olarak modellenir; geri kalanı ilgili LLD sınıfının catch/if dalında yapılandırılmış log+metrik+(gerekirse) alarm olarak somutlaşır.

```java
package com.threeai.nats.core.history.exception;

/** Bootstrap-time fail-fast DEĞİL (BA-Q4 kararı — WARN, hard-reject değil) -- ama tip olarak
 *  ayrıştırılabilir kalması için bir işaretleyici sınıf. */
public class HistoryLevelAuditCriticalMismatchWarning { /* log-only marker, exception DEĞİL */ }

/** Compliance-invariant ihlalleri -- gerçek exception, CRITICAL page tetikler. */
public class RetentionAuditLogWriteFailedException extends RuntimeException { ... }         // SYS_RETENTION_AUDIT_LOG_WRITE_FAILED
public class ErasureVerificationFailedException extends RuntimeException { ... }            // RES_ERASURE_VERIFICATION_FAILED
public class PseudonymVaultAccessDeniedException extends SecurityException { ... }          // AUTH_PSEUDONYM_VAULT_ACCESS_DENIED
```

Diğer 39 kod için ayrı exception sınıfı YOK — her biri ilgili sınıfın (`docs/sentinel/step2/phase4/lld/history-offload/03_classes/*.md`) davranışında somutlaşır.

---

## 2. MDC alan seti (basamak-1 `07_errors.md §2` genişlemesi)

| MDC alanı | Ne zaman | Kaynak |
|---|---|---|
| `trace_id` | Her zaman | `X-Cadenzaflow-Trace-Id` (devralınan) |
| `history_class` | Her zaman | Subject'ten / event'ten |
| `engine_id` | Her zaman | Subject'ten |
| `process_instance_id` | Her zaman | Subject'ten (PSEUDONYMOUS, DP-1 uyumlu — PII DEĞİL) |
| `partition_index` | Projeksiyon consumer yollarında | `HistoryProjectionConsumer` |
| `subject_key` | Erasure yollarında (yalnız kimlik, DEĞER DEĞİL) | `ErasurePipeline` |
| `pseudonym_token` | Pseudonymization yollarında | `PseudonymizationVaultClient` |
| `history_class_retention_window` | Retention yollarında | `RetentionEnforcementJob` |

**DP-1/DP-14 uyumu:** payload/business-key/operatör-kimliği/variable **değeri** hiçbir MDC alanına yazılmaz.

---

## 3. Kod → LLD bağlaması (42/42)

### 3.1 Handler/Config (3)

| # | Kod | Statü | Fırlatan/Loglayan sınıf | Log | Metrik/Alarm |
|---|---|---|---|---|---|
| 1 | `VAL_HISTORY_CLASS_UNCLASSIFIED` | fail-safe-bulk+WARN | `NatsHistoryEventHandler` (`03_classes/1_handler_outbox.md §1`) | WARN | — (config-zamanı-benzeri, runtime tespit) |
| 2 | `VAL_HISTORY_LEVEL_AUDIT_CRITICAL_MISMATCH` | deployment-time WARN (kalıcı) | Bootstrap guard, `08_config.md §1` | WARN (her boot) | — |
| 3 | `VAL_HISTORY_STREAM_PROVISIONING_MISSING` | ERROR(prod)/WARN(bench) | `HistoryStreamProvisioner` (`03_classes/5_bench.md §2`) | ERROR/WARN | — |

### 3.2 Outbox/Relay (4)

| # | Kod | Statü | Fırlatan/Loglayan sınıf | Log | Metrik/Alarm |
|---|---|---|---|---|---|
| 4 | `SYS_OUTBOX_RELAY_PUBLISH_FAILED` | retry/backoff | `HistoryOutboxRelay.relayRow(...)` | WARN→ERROR | `nats.history.outbox.relayed{outcome=failed}` |
| 5 | `SYS_OUTBOX_RELAY_LEADER_LOST` | failover (transparent) | `HistoryOutboxRelay` + `SweepLeaderLease` (reuse) | WARN | — (leader-devri, rutin) |
| 6 | `SYS_OUTBOX_ROW_STUCK` | ops-alert | `HistoryOutboxRelay.checkStuckRows()` | WARN→ERROR | `nats.history.outbox.oldest_row_age_seconds` — `10_metrics.md §2.4` |
| 7 | `BUS_OUTBOX_DUPLICATE_RELAY_DELIVERY` *(bilgilendirici)* | no-op | `HistoryOutboxRelay` (dedup yutar, downstream) | DEBUG/INFO | — |

### 3.3 Projeksiyon-Consumer (4)

| # | Kod | Statü | Fırlatan/Loglayan sınıf | Log | Metrik/Alarm |
|---|---|---|---|---|---|
| 8 | `BUS_PROJECTION_STALE_EVENT_DISCARDED` *(beklenen)* | no-op | `ProjectionStore.upsertEntity(...)` (`03_classes/2_relay_projection.md §3`) | DEBUG | `nats.history.projection.stale_discarded` |
| 9 | `BUS_MERGE_UPSERT_CONFLICT_AMBIGUOUS` | WARN+tie-break | `ProjectionStore.upsertEntity(...)` (eşit `stream_sequence`, teorik) | WARN | — |
| 10 | `SYS_PROJECTION_WRITE_FAILED` | nak, redelivery | `HistoryProjectionConsumer.onMessage(...)` | ERROR | — |
| 11 | `SYS_PROJECTION_SCHEMA_DRIFT` | consumer durur+DLQ | `HistoryProjectionConsumer.onMessage(...)` | ERROR | `nats.history.dlq.routed{reason=schema_drift}` |

### 3.4 History-DLQ (3)

| # | Kod | Statü | Fırlatan/Loglayan sınıf | Log | Metrik/Alarm |
|---|---|---|---|---|---|
| 12 | `BUS_HISTORY_DELIVERY_BUDGET_EXCEEDED` | DLQ-route | `HistoryDlqConsumer.routeToDlq(...)` | WARN | `nats.history.dlq.routed` |
| 13 | `SYS_HISTORY_DLQ_PUBLISH_FAILED` | nak+alert | `HistoryDlqConsumer.routeToDlq(...)` | ERROR | — |
| 14 | `RES_HISTORY_DLQ_ACCESS_DENIED` | reddedilir | `HistoryDlqInspectionConsumer` + subject-ACL (`09_security/1_transport_authz.md §2`) | WARN (security-log) | — |

### 3.5 Sorgu-API (4)

| # | Kod | Statü | Fırlatan/Loglayan sınıf | Log | Metrik/Alarm |
|---|---|---|---|---|---|
| 15 | `VAL_QUERY_UNSUPPORTED_PATTERN` | reddedilir | `HistoryQueryApi` (`03_classes/3_query_api.md §1`) | WARN | — |
| 16 | `AUTH_QUERY_ACCESS_DENIED` | reddedilir | `HistoryQueryAuthzSpi.isAuthorized(...)` çağıran taraf | WARN | — |
| 17 | `BUS_QUERY_PII_MASKED` *(bilgilendirici)* | yanıt-maskeli | `PiiMaskingService.mask(...)` (`09_security/2_pii_protection.md §2`) | DEBUG/INFO | — |
| 18 | `RES_HISTORY_INSTANCE_NOT_FOUND` *(beklenen)* | 404-eşdeğeri | `HistoryQueryApi.getProcessInstanceHistory(...)` | DEBUG/INFO | — |

### 3.6 Reconciliation (4)

| # | Kod | Statü | Fırlatan/Loglayan sınıf | Log | Metrik/Alarm |
|---|---|---|---|---|---|
| 19 | `SYS_RECONCILIATION_JOB_FAILED` | log-only, döngü atlanır | `ReconciliationJob.reconcileAllClasses()` (`03_classes/4_cutover_reconciliation.md §1`) | ERROR | — |
| 20 | `BUS_RECONCILIATION_DIFF_DETECTED` | streak-reset | `ReconciliationJob.evaluateGate(...)` | WARN | `nats.history.reconciliation.diff_count` |
| 21 | `RES_RECONCILIATION_DIFF_THRESHOLD_EXCEEDED` | ops-alert | `ReconciliationJob` | WARN→ERROR | `10_metrics.md §2.5` |
| 22 | `VAL_RECONCILIATION_WINDOW_N_INVALID` | config reddedilir | `ReconciliationProperties` validasyonu (`08_config.md §5`) | ERROR | — |

### 3.7 Cutover (3)

| # | Kod | Statü | Fırlatan/Loglayan sınıf | Log | Metrik/Alarm |
|---|---|---|---|---|---|
| 23 | `BUS_CUTOVER_GATE_NOT_MET` | reddedilir | `CutoverControlPlane.requestCutover(...)` | WARN | — |
| 24 | `SYS_CUTOVER_CONFIG_APPLY_FAILED` | retry+alert | `CutoverControlPlane.requestCutover(...)` (KV yazımı/restart-orkestrasyonu fail) | ERROR | — |
| 25 | `BUS_CUTOVER_ROLLBACK_TRIGGERED` *(operasyonel)* | audit-logged | `CutoverRollback.rollback(...)` | INFO | — |

### 3.8 Bench (4)

| # | Kod | Statü | Fırlatan/Loglayan sınıf | Log | Metrik/Alarm |
|---|---|---|---|---|---|
| 26 | `BUS_BENCH_HISTORY_METRIC_REGRESSION` | **build-fail (sert kapı)** | `HistoryDbWriteOpReport.passesHardGate()` (`03_classes/5_bench.md §1`) | ERROR (rapor) | — |
| 27 | `SYS_BENCH_HISTORY_ENVIRONMENT_UNAVAILABLE` | warn-only | `HistoryBenchScenario` (Testcontainers abort) | WARN | — |
| 28 | `SYS_BENCH_HISTORY_SLI_DRIFT` | warn-only | `HistoryBenchScenario` (destekleyici SLI karşılaştırma) | WARN | — |
| 29 | `BUS_BENCH_BASELINE_MISSING` | warn-only | `HistoryBenchScenario` (basamak-1 baseline eksik) | WARN | — |

### 3.9 Retention (4)

| # | Kod | Statü | Fırlatan/Loglayan sınıf | Log | Metrik/Alarm |
|---|---|---|---|---|---|
| 30 | `SYS_RETENTION_JOB_FAILED` | log-only | `RetentionEnforcementJob.enforceRetention()` (`03_classes/6_governance.md §1`) | ERROR | — |
| 31 | `BUS_RETENTION_WINDOW_BREACH_DETECTED` *(beklenen)* | silme uygulanır | `RetentionEnforcementJob.enforceRetention()` | INFO | `nats.history.retention.deleted_rows` |
| 32 | `VAL_RETENTION_OVERRIDE_BELOW_LEGAL_MINIMUM` | reddedilir | `RetentionEnforcementJob.validateRetentionOverrides(...)` | ERROR (config reddi) | — |
| 33 | `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED` | **CRITICAL — on-call page** | `RetentionEnforcementJob.enforceRetention()` (`RetentionAuditLogWriteFailedException`) | **CRITICAL** | `10_metrics.md §2.1` — `for: 0m` |

### 3.10 Erasure (5)

| # | Kod | Statü | Fırlatan/Loglayan sınıf | Log | Metrik/Alarm |
|---|---|---|---|---|---|
| 34 | `BUS_ERASURE_REQUEST_ACCEPTED` *(bilgilendirici)* | pipeline tetiklenir | `ErasurePipeline.requestErasure(...)` (`03_classes/6_governance.md §2`) | INFO | — |
| 35 | `BUS_ERASURE_REQUEST_LEGAL_HOLD_BLOCKED` *(beklenen)* | reddedilir | `ErasurePipeline.requestErasure(...)` (`legal_hold=TRUE` kontrolü) | WARN | — |
| 36 | `VAL_ERASURE_SUBJECT_KEY_AMBIGUOUS` | kapsam-onayı istenir | `ErasureScopeResolver.resolve(...)` | WARN | — |
| 37 | `SYS_ERASURE_PIPELINE_FAILED` | retry+alert | `ErasurePipeline.executeAnonymization(...)` | ERROR | — |
| 38 | `RES_ERASURE_VERIFICATION_FAILED` | **CRITICAL — compliance-risk** | `ErasurePipeline.executeAnonymization(...)` (`ErasureVerificationFailedException`) | **CRITICAL** | `10_metrics.md §2.3` |

### 3.11 Pseudonymization (4)

| # | Kod | Statü | Fırlatan/Loglayan sınıf | Log | Metrik/Alarm |
|---|---|---|---|---|---|
| 39 | `BUS_PSEUDONYMIZATION_APPLIED` *(bilgilendirici)* | no-op (başarı) | `PseudonymTokenGenerator.generate(...)` + `PseudonymizationVaultClient.persistMapping(...)` (`03_classes/6_governance.md §3`) | INFO | `nats.history.vault.access{operation=WRITE}` |
| 40 | `SYS_PSEUDONYM_VAULT_UNAVAILABLE` | downstream retry | `PseudonymizationVaultClient.persistMapping(...)` | ERROR | — |
| 41 | `AUTH_PSEUDONYM_VAULT_ACCESS_DENIED` | **CRITICAL — security-page** | `PseudonymizationVaultClient.reidentify(...)` (`PseudonymVaultAccessDeniedException`) | **CRITICAL** | `10_metrics.md §2.2` — `for: 0m` |
| 42 | `BUS_PSEUDONYM_MAP_ENTRY_DELETED` *(bilgilendirici)* | audit-logged | `PseudonymizationVaultClient.deleteMapping(...)` | INFO | `nats.history.vault.access{operation=DELETE}` |

---

## 4. Sayım doğrulaması (görev talimatı: "sayarak doğrula")

**42/42** — `EXCEPTION_CODES.md §12` kaynak toplamıyla birebir (11 kaynak grubu: 3+4+4+3+4+4+3+4+4+5+4=42). Kategori dağılımı (bu fazda TEKRAR sayıldı):

| Kategori | Bu kayıtta sayım | Kod # listesi |
|---|---|---|
| `VAL_` | 7 | 1,2,3,15,22,32,36 |
| `BUS_` | 15 | 7,8,9,12,17,20,23,25,26,29,31,34,35,39,42 |
| `RES_` | 4 | 14,18,21,38 |
| `SYS_` | 14 | 4,5,6,10,11,13,19,24,27,28,30,33,37,40 |
| `AUTH_` | 2 | 16,41 |
| **Toplam** | **42** | 7+15+4+14+2=42 ✓ |

**Not (ÇÖZÜLDÜ 2026-07-20):** `EXCEPTION_CODES.md §12` kategori-özeti tablosundaki `RES_ = 3` drift'i (bu registry'nin ilk yazımında tespit edilmişti) kaynağında düzeltildi — `EXCEPTION_CODES.md §12` artık `RES_ = 4` taşır; iki belge kırılımda da hemfikir (phase4-review F-003 kapanışı).

**3 CRITICAL kod:** `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED`, `RES_ERASURE_VERIFICATION_FAILED`, `AUTH_PSEUDONYM_VAULT_ACCESS_DENIED` — basamak-1 `SYS_SENTINEL_WORKER_CONFLICT` ciddiyet sınıfıyla eşdeğer, hepsi `10_metrics.md`'de `for: 0m` (anında sayfalama) alarmıyla eşleşir.

---

## 5. İzlenebilirlik

Bu kayıt `EXCEPTION_CODES.md §9-§13`'ün (42 satır) BİREBİR ÜSTÜNE kuruludur — kod adları/statüleri/BR-FR-US bağlantıları oradan DEĞİŞTİRİLMEDEN alınmıştır; bu belge yalnız Java sınıfı + log/MDC/metrik/alarm katmanını ekler.
