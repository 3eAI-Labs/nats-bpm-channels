# 10 — Metrikler

**Kaynak:** HLD §7 (NFR-O1…O3), basamak-1 `NatsChannelMetrics` (`[07§4]` taban, genişletilir — yeni sınıf İCAT EDİLMEZ).

---

## 1. Yeni sayaçlar/gauge'lar (`NatsChannelMetrics` genişlemesi)

| Metrik | Tip | Tag'ler (düşük-kardinalite, DP-1/DP-2) | Üreten sınıf |
|---|---|---|---|
| `nats.history.outbox.written` | Counter | `history_class`, `engine_id` | `CompactHistoryOutboxWriter` |
| `nats.history.outbox.relayed` | Counter | `history_class`, `outcome`(published/failed) | `HistoryOutboxRelay` |
| `nats.history.outbox.oldest_row_age_seconds` | Gauge | `engine_id` | `HistoryOutboxRelay.checkStuckRows()` — `SYS_OUTBOX_ROW_STUCK` sinyali |
| `nats.history.postcommit.published` | Counter | `history_class` | `HistoryPostCommitPublisher` |
| `nats.history.projection.consumed` | Counter | `history_class`, `partition` | `HistoryProjectionConsumer` |
| `nats.history.projection.stale_discarded` | Counter | `history_class` | `HistoryProjectionConsumer` (`BUS_PROJECTION_STALE_EVENT_DISCARDED`) |
| `nats.history.projection.lag_seconds` | Gauge (p95) | `history_class`, `partition` | `HistoryProjectionConsumer` (event→query-store gecikmesi, NFR-P3 SLI) |
| `nats.history.dlq.routed` | Counter | `history_class`, `reason` | `HistoryDlqConsumer` |
| `nats.history.reconciliation.diff_count` | Gauge | `history_class` | `ReconciliationJob` |
| `nats.history.reconciliation.clean_streak_days` | Gauge | `history_class` | `ReconciliationJob` |
| `nats.history.cutover.state` | Gauge (enum-encoded) | `history_class` | `CutoverControlPlane` |
| `nats.history.retention.deleted_rows` | Counter | `history_class`, `action`(drop/detach) | `RetentionEnforcementJob` |
| `nats.history.erasure.processed` | Counter | `history_class`, `action` | `ErasurePipeline` |
| `nats.history.vault.access` | Counter | `operation`, `granted` | `PseudonymizationVaultClient` (DP-16 — `pseudonym_token`/gerçek değer TAG'E GİRMEZ) |

**Birincil metrik (NFR-P1/FR-E1, TEK sert kapı):** `HistoryDbWriteOpReport` bench raporu — Prometheus'a YAYINLANMAZ (test-zamanı, CI artifact), `03_classes/5_bench.md §1`.

---

## 2. Alarm tanımları

### 2.1 `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED` — CRITICAL + page (basamak-1 `SYS_SENTINEL_WORKER_CONFLICT` ciddiyet sınıfı)

```yaml
- alert: HistoryRetentionAuditLogWriteFailed
  expr: increase(nats_history_retention_audit_write_failures_total[1m]) > 0
  for: 0m
  labels: { severity: critical }
  annotations: { summary: "Retention deletion succeeded but audit-log write failed -- compliance-invariant violated" }
```

### 2.2 `AUTH_PSEUDONYM_VAULT_ACCESS_DENIED` — CRITICAL + security-page

```yaml
- alert: PseudonymVaultUnauthorizedAccess
  expr: increase(nats_history_vault_access_total{operation="REIDENTIFY_ATTEMPT",granted="false"}[1m]) > 0
  for: 0m
  labels: { severity: critical }
```

### 2.3 `RES_ERASURE_VERIFICATION_FAILED` — CRITICAL, compliance-risk

```yaml
- alert: HistoryErasureVerificationFailed
  expr: increase(nats_history_erasure_processed_total{action="verification_failed"}[5m]) > 0
  labels: { severity: critical }
  annotations: { summary: "Erasure completed but PII still surfaces via Query API -- KVKK 30d SLA risk" }
```

### 2.4 `SYS_OUTBOX_ROW_STUCK` — çarpan-tabanlı eşik (LLD-Q5, BA-Q7)

```yaml
- alert: HistoryOutboxRowStuck
  expr: nats_history_outbox_oldest_row_age_seconds > 150   # 5x30s default, LLD-Q5
  labels: { severity: warning }
  annotations: { summary: "Outbox row exceeds 5x relay-cycle-period -- relay/leader stuck suspicion" }
```

### 2.5 Reconciliation eşik-aşımı

```yaml
- alert: HistoryReconciliationDiffSustained
  expr: increase(nats_history_reconciliation_diff_count[1h]) > 0
  labels: { severity: warning }
```

### 2.6 CB OPEN (DLQ-bridge, basamak-1 deseni yeniden kullanım)

`cb-history-dlq-inspection` — basamak-1 `10_metrics.md §4.4` `DlqBridgeCircuitBreakerOpen` alarmıyla AYNI şablon, yeni CB adı.

**Bağımlılık:** NFR-O1/O2/O3, ADR-0015/0016/0017/0018.
