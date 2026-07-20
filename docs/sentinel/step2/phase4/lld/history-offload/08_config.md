# 08 — Konfigürasyon Sınıfları

**Kaynak:** ADR-0010 (outbox/relay), ADR-0011 (partition), ADR-0015 (cutover/reconciliation, ARCH-Q5), ADR-0016 (kasa, ARCH-Q2), ADR-0018 (retention), PO-Q4/Q5/Q7, LLD-Q1…5 (`01_overview.md`).

**Basamak-1 dersi (config sınıf-adlandırma çakışması, `A2ConsumerConfig`) burada uygulanır:** aşağıdaki sınıfların HİÇBİRİ mevcut `A2Properties`/`SubscriptionConfig`/`UmbrellaLockProperties` ile İSİM/PREFİX ÇAKIŞMAZ — hepsi `spring.nats.<engine>.history.*` (engine-side) veya `history.*` (`nats-history-projection`, motor-dışı) yeni bir config-ağacı altındadır.

---

## 1. Engine-side config (`camunda-nats-channel`/`cadenzaflow-nats-channel`, `spring.nats.<engine>.history.*`)

```java
@ConfigurationProperties(prefix = "spring.nats.camunda.history")   // cadenzaflow ayna: "spring.nats.cadenzaflow.history"
public class HistoryClassificationProperties {
    private Set<String> auditCriticalClasses = Set.of("OP_LOG", "INCIDENT", "EXT_TASK_LOG");  // PO-Q5 default
    private boolean pseudonymizationOptIn = false;         // US-G3, kiracı opt-in
    private String tenantKeyId;                            // pseudonym keyed-hash anahtarı (OpenBao/deploy-secret referansı)
    private int tenantKeyVersion = 1;                       // rotasyon takibi
}

@ConfigurationProperties(prefix = "spring.nats.camunda.history.outbox")
public class HistoryOutboxProperties {
    private long relayCyclePeriodSeconds = 30;              // LLD "#3" RTO türetme girdisi
    private int stuckThresholdMultiplier = 5;               // LLD-Q5 (BA-Q7 default)
}
```

**Bootstrap-time guard'lar (basamak-1 `UmbrellaLockValidator` desenine paralel):**
- `VAL_HISTORY_CLASS_UNCLASSIFIED`: motor upgrade ile eklenen sınıf haritada yoksa → fail-safe bulk + WARN (hard-reject DEĞİL, `NatsHistoryEventHandler` içinde runtime-tespit, bootstrap'ta DEĞİL — sınıf kümesi motor upgrade'iyle DEĞİŞEBİLİR).
- `VAL_HISTORY_LEVEL_AUDIT_CRITICAL_MISMATCH`: bootstrap'ta `HistoryLevel.isHistoryEventProduced(...)` her `auditCriticalClasses` üyesi için kontrol edilir; üretilmiyorsa **WARN** (BA-Q4 KARAR — hard-reject DEĞİL, basamak-1'in `VAL_UMBRELLA_LOCK_TOO_SHORT` deseninden BİLİNÇLİ sapma).

**Bağımlılık:** BR-HDL-002/003/007, PO-Q5, BA-Q4/Q5.

---

## 2. `ClassCutoverStateRegistry` KV bucket — `history-cutover-state` (LLD-Q3)

| Alan | Değer | Kaynak |
|---|---|---|
| Bucket adı | `history-cutover-state` | LLD-Q3 (ARCH-Q5'in engine-node'a ULAŞTIRILMA mekanizması) |
| Replikasyon | 3 (prod) | `NATS_JETSTREAM.md` genel kural (persistent KV) |
| TTL | 0 (kalıcı — cutover kararları geçici DEĞİL) | LLD kararı |
| History | 5 (son 5 flip/rollback'in izlenebilirliği) | LLD kararı |
| Anahtar | `cutover.<engineId>.<historyClass>` → değer `"true"`/`"false"` | `CutoverControlPlane`/`CutoverRollback` yazar |
| Okuma | `ClassCutoverStateRegistry.loadAtBootstrap()` — **yalnız engine boot'ta**, ARCH-Q5 rolling-restart kararına uyumlu (`01_overview.md` "Hot-reconfigure" kapanışı) | Engine node (`camunda-nats-channel`/ayna) |
| Yazma | `CutoverControlPlane.requestCutover(...)` / `CutoverRollback.rollback(...)` | `nats-history-projection` |

---

## 3. `history-relay-leader` KV bucket (basamak-1 `SweepLeaderLease` yeniden kullanım)

| Alan | Değer | Kaynak |
|---|---|---|
| Bucket adı | `history-relay-leader` (basamak-1 `a2-sweep-leader`'DAN AYRI — farklı amaç) | ADR-0010 + basamak-1 ADR-0002 |
| TTL | `2·relayCyclePeriodSeconds` = 60s (default 30s döngü) | LLD "Phase3'ün devrettiği doğrulamalar #3" (RTO türetme) |
| Anahtar | `relay-leader.<engineId>` (motor-başına izole — basamak-1 LLD-Q1 dersi ÖNCEDEN uygulandı) | `03_classes/2_relay_projection.md` §1 |
| Sınıf | `SweepLeaderLease` (basamak-1, `nats-core`, YENİDEN KULLANILIR — yalnız bucket/TTL/key parametreleri farklı) | ADR-0002 |

---

## 4. Projeksiyon partition config (`nats-history-projection`, `history.projection.*`)

```java
@ConfigurationProperties(prefix = "history.projection")
public class HistoryProjectionProperties {
    private int partitionCount = 8;                          // LLD-Q2 (ARCH-Q3 N default)
    private List<Integer> partitionAssignment = List.of();   // boşsa: replicaOrdinal % partitionCount
}
```

**Rebalance runbook:** `99_deployment.md §3` (bakım-penceresi prosedürü, N değişimi CANLI DEĞİL).

---

## 5. Reconciliation/Cutover config (`nats-history-projection`, `history.reconciliation.*` / `history.cutover.*`)

```java
@ConfigurationProperties(prefix = "history.reconciliation")
public class ReconciliationProperties {
    private String cron = "0 0 3 * * *";                      // daily default
    private int cleanStreakTargetDefault = 7;                 // PO-Q4 default N
    private Map<String, Integer> cleanStreakTargetOverrides = Map.of();  // sınıf-başına override
    private Map<String, Long> bulkEpsilonOverrides = Map.of();           // BA-Q2, default 0
}

@ConfigurationProperties(prefix = "history.cutover")
public class HistoryCutoverProperties {
    private List<String> volumePriorityOrder = List.of("DETAIL", "VARINST", "ACTINST", /* ... */);  // BR-HDL-005
}
```

**Config-zamanı reddi:** `N ≤ 0` → `VAL_RECONCILIATION_WINDOW_N_INVALID` (config reddedilir, default 7g korunur).

---

## 6. Retention config (`nats-history-projection`, `history.retention.*`)

```java
@ConfigurationProperties(prefix = "history.retention")
public class RetentionProperties {
    private int bulkDefaultDays = 90;                         // PO-Q7 default
    private String auditCriticalDefaultWindow = "P7Y";        // ISO-8601 duration, yasal-saklama örneği
    private Map<String, String> perClassOverrides = Map.of(); // kiracı override, DPO onayına işaretli
}
```

**Guard:** kiracı bir audit-kritik sınıfın retention'ını yasal-asgarinin ALTINA çekmeye çalışırsa → `VAL_RETENTION_OVERRIDE_BELOW_LEGAL_MINIMUM` (reddedilir, hukuki/DPO onayı gerekir — mekanizma bu repoda YOK, yalnız reddeder).

---

## 7. Pseudonym kasası config (`nats-history-projection`, `history.vault.*`) — İZOLE `DataSource`

```java
@ConfigurationProperties(prefix = "history.vault.datasource")
public class PseudonymVaultDataSourceProperties {
    private String jdbcUrl;             // AYRI Postgres örneği (ARCH-Q2) — history.projection.datasource ile PAYLAŞILMAZ
    private String username;
    private String vaultColumnEncryptionKeyRef;  // OpenBao/deploy-secret referansı, pgcrypto sembolik-anahtar KAYNAĞI (değeri KOD İÇİNDE YOK)
}
```

**Bağımlılık:** ARCH-Q2, ADR-0016, DP-16.
