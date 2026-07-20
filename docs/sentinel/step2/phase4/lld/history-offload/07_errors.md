# 07 — Hata Kodları (Köprü)

Tam kayıt (42 kod → Java sınıf/hiyerarşi, log formatı, MDC alanları, metrik bağlaması, alarm kanalı) tek doğruluk kaynağı olarak burada **DEĞİL**, ayrı Phase 4 teslimatında yaşar:

**→ [`docs/sentinel/step2/phase4/ERROR_REGISTRY.md`](../../ERROR_REGISTRY.md)**

## Sınıf → kod üretim noktası (hızlı bakış — tam ayrıntı ERROR_REGISTRY.md'de)

| Sınıf (bu modül) | Ürettiği kod(lar) |
|---|---|
| `NatsHistoryEventHandler` | `VAL_HISTORY_CLASS_UNCLASSIFIED`, `VAL_HISTORY_LEVEL_AUDIT_CRITICAL_MISMATCH` |
| `CompactHistoryOutboxWriter` | (dolaylı — outbox satırı yazımı, kod fırlatmaz; hata DB constraint ihlaliyse `SYS_OUTBOX_RELAY_PUBLISH_FAILED` DEĞİL, engine-native tx-fail) |
| `HistoryPostCommitPublisher` | (dolaylı — basamak-1 `EXT_JETSTREAM_PUBLISH_UNAVAILABLE` deseninin history izdüşümü, ayrı kod DEĞİL — WARN log) |
| `HistoryOutboxRelay` | `SYS_OUTBOX_RELAY_PUBLISH_FAILED`, `SYS_OUTBOX_RELAY_LEADER_LOST`, `SYS_OUTBOX_ROW_STUCK`, `BUS_OUTBOX_DUPLICATE_RELAY_DELIVERY` (bilgilendirici) |
| `HistoryProjectionConsumer` | `BUS_PROJECTION_STALE_EVENT_DISCARDED`, `BUS_MERGE_UPSERT_CONFLICT_AMBIGUOUS`, `SYS_PROJECTION_WRITE_FAILED`, `SYS_PROJECTION_SCHEMA_DRIFT` |
| `HistoryDlqConsumer` / `HistoryDlqInspectionConsumer` | `BUS_HISTORY_DELIVERY_BUDGET_EXCEEDED`, `SYS_HISTORY_DLQ_PUBLISH_FAILED`, `RES_HISTORY_DLQ_ACCESS_DENIED` |
| `HistoryQueryApi` | `VAL_QUERY_UNSUPPORTED_PATTERN`, `AUTH_QUERY_ACCESS_DENIED`, `BUS_QUERY_PII_MASKED` (bilgilendirici), `RES_HISTORY_INSTANCE_NOT_FOUND` |
| `ReconciliationJob` | `SYS_RECONCILIATION_JOB_FAILED`, `BUS_RECONCILIATION_DIFF_DETECTED`, `RES_RECONCILIATION_DIFF_THRESHOLD_EXCEEDED`, `VAL_RECONCILIATION_WINDOW_N_INVALID` |
| `CutoverControlPlane` | `BUS_CUTOVER_GATE_NOT_MET`, `SYS_CUTOVER_CONFIG_APPLY_FAILED` |
| `CutoverRollback` | `BUS_CUTOVER_ROLLBACK_TRIGGERED` (bilgilendirici) |
| `HistoryBenchScenario` / `HistoryDbWriteOpReport` | `BUS_BENCH_HISTORY_METRIC_REGRESSION`, `SYS_BENCH_HISTORY_ENVIRONMENT_UNAVAILABLE`, `SYS_BENCH_HISTORY_SLI_DRIFT`, `BUS_BENCH_BASELINE_MISSING` |
| `RetentionEnforcementJob` | `SYS_RETENTION_JOB_FAILED`, `BUS_RETENTION_WINDOW_BREACH_DETECTED` (bilgilendirici), `VAL_RETENTION_OVERRIDE_BELOW_LEGAL_MINIMUM`, `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED` (**CRITICAL**) |
| `ErasurePipeline` / `ErasureScopeResolver` | `BUS_ERASURE_REQUEST_ACCEPTED` (bilgilendirici), `BUS_ERASURE_REQUEST_LEGAL_HOLD_BLOCKED`, `VAL_ERASURE_SUBJECT_KEY_AMBIGUOUS`, `SYS_ERASURE_PIPELINE_FAILED`, `RES_ERASURE_VERIFICATION_FAILED` (**CRITICAL**) |
| `PseudonymizationVaultClient` / `PseudonymTokenGenerator` | `BUS_PSEUDONYMIZATION_APPLIED` (bilgilendirici), `SYS_PSEUDONYM_VAULT_UNAVAILABLE`, `AUTH_PSEUDONYM_VAULT_ACCESS_DENIED` (**CRITICAL**), `BUS_PSEUDONYM_MAP_ENTRY_DELETED` (bilgilendirici) |
| `HistoryStreamProvisioner` | `VAL_HISTORY_STREAM_PROVISIONING_MISSING` |

**Sayım:** 42/42 basamak-2 kodu bu tabloda bir üretici sınıfa bağlıdır (`ERROR_REGISTRY.md §6` sayarak doğrulama). Basamak-1'in 23 kodu bu modülün DIŞINDADIR (A2 dispatch akışı, `docs/sentinel/phase4/ERROR_REGISTRY.md`'de kalır, DEĞİŞTİRİLMEZ).
