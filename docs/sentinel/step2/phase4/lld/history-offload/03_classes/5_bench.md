# 03.5 — EPIC-E/F: Metrik/Bench + Devreden Borçlar

**Modül:** `nats-bpm-bench` (basamak-1 modülü genişler, `com.threeai.nats.bench.history.*`).
**Kaynak ADR:** 0015 (D-F metriği/sert kapı), 0013+0019 (stream provisioning).
**HLD:** §3.5.1…§3.5.5. **Test tasarımı:** `TEST_SPECIFICATIONS.md` (bu dosya yalnız sınıf iskeletini taşır, tekrar YOK).

---

## 1. `HistoryBenchScenario` + `HistoryDbWriteOpReport` (BR-OBS-001/003, ADR-0015)

```java
package com.threeai.nats.bench.history;

/** Two modes, same scenario -- DB_HISTORY_BASELINE (default AUDIT HistoryLevel, no offload) vs
 *  HISTORY_OFFLOAD (cutover'd classes per HistoryCutoverProperties test fixture). Extends the
 *  basamak-1 BenchScenarioRunner pattern (nats-bpm-bench/03_classes/5_bench.md, basamak-1). */
public class HistoryBenchScenario {

    public HistoryBenchScenario(BenchEnvironment env, PgStatStatementsSnapshotter snapshotter) { ... }

    /** Runs N process instances through full lifecycle (start -> activities -> variables -> complete),
     *  captures pg_stat_statements before/after (D-F fingerprint methodology, basamak-1 reused). */
    public HistoryDbWriteOpReport run(BenchMode mode, int instanceCount);
}

public class HistoryDbWriteOpReport {
    public long actHiWriteOpCount;         // MUST be 0 in HISTORY_OFFLOAD mode for cut-over classes
    public long compactOutboxRowCount;     // audit-critical: <=1/tx (LLD-Q1 -- companion rows tracked separately)
    public long compactOutboxPayloadRowCount; // large-payload companion, reported but NOT part of the hard-gate count (LLD-Q1)
    public boolean passesHardGate();       // false -> BUS_BENCH_HISTORY_METRIC_REGRESSION (build-fail)
}
```

**Sert kapı (D-F, TEK):** `passesHardGate()==false` → build-fail (`BUS_BENCH_HISTORY_METRIC_REGRESSION`). Reconciliation-temizliği AYRI bir kapıdır (cutover kapısı, `03_classes/4_cutover_reconciliation.md` §1) — burada test EDİLMEZ.

**Bağımlılık:** BR-OBS-001/003, FR-E1/E3, US-E1/E3, ADR-0015.

---

## 2. Stream provisioning genişlemesi (BR-DBT-002, ADR-0019/0013)

```java
package com.threeai.nats.bench.history;

/** Extends basamak-1 BenchEnvironment.ensureStreams() -- adds HISTORY (Limits, SubjectTransform
 *  Partition(8,3), 7g retention) and DLQ_HISTORY (Limits, 14g, separate stream CQ-6) provisioning.
 *  Missing provisioning -> VAL_HISTORY_STREAM_PROVISIONING_MISSING. */
public class HistoryStreamProvisioner {
    public void ensureHistoryStreams(JetStreamStreamManager streamManager, Connection connection);
}
```

**Bağımlılık:** BR-DBT-002, FR-F2, US-F2, ADR-0019/0013.

---

## 3. `RelayFailoverBenchScenario` (§01_overview.md "Phase3'ün devrettiği doğrulamalar #5")

```java
package com.threeai.nats.bench.history;

/** Design only in this LLD -- actual measurement is phase5.5 scope (task instruction explicit
 *  deferral). Kills the current history-relay-leader KV holder mid-cycle, measures time-to-recover
 *  (new leader acquires lease + resumes relayCycle()) against the lease TTL bound (LLD "#3" RTO). */
public class RelayFailoverBenchScenario {
    public RelayFailoverReport run(BenchEnvironment env, int engineNodeReplicaCount);
}
```

Tam senaryo tasarımı: `TEST_SPECIFICATIONS.md` (f).

---

## 4. Devreden borç triyajı (US-F3, BR-DBT-003) — kod DEĞİL, dokümantasyon

HLD §10 tablosu (borç #1,3,4,5,6 triyajı) doğrudan geçerlidir — bu LLD kod üretmez, yalnız triyaj kararının HLD'de KAPANDIĞINI teyit eder. `01_overview.md` "Phase3'ün devrettiği doğrulamalar #2 (borç #6 sweep captured-variables)" — history publish captured-payload tamlığı, `CompactHistoryOutboxWriter`'ın `payload_scalar`/`payload_large_ref` ayrımıyla (§`03_classes/1_handler_outbox.md` §2) doğrudan ele alınır: relay yeniden-publish ettiğinde payload eksik KALMAZ (referans hedefi `compact_history_outbox_payload`'da hâlâ mevcuttur, satır silinene kadar).

**Bağımlılık:** BR-DBT-003, FR-F3, US-F3.
