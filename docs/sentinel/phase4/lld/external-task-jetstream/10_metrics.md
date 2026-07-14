# 10 — Metrik Genişletmeleri

**Kaynak:** HLD §8, FR-D2/NFR-O1, `NatsChannelMetrics.java:15-82` (mevcut taban).

---

## 1. `NatsChannelMetrics` genişlemesi (yeni metotlar, mevcut sınıfa eklenir)

```java
// nats-core/.../metrics/NatsChannelMetrics.java — mevcut sınıfa EKLENIR (silinen/değişen metot YOK)

public Counter sweepRepublishCount(String topic) {
    return Counter.builder("nats.a2.sweep.republish")
            .tag("topic", topic).register(registry);
}

public Counter dlqPublishFailureCount(String subject, String channel) {
    return Counter.builder("nats.jetstream.dlq.publish.failures")
            .tag("subject", subject).tag("channel", channel).register(registry);
}

public Counter failureEventCorrelationMissCount(String channel) {
    return Counter.builder("nats.flowable.failure_event.correlation_miss")
            .tag("channel", channel).register(registry);
}

/** SYS_SENTINEL_WORKER_CONFLICT — CRITICAL+page kanalının metrik tarafı (ERROR_REGISTRY.md §4.1). */
public Counter sentinelWorkerConflictCount(String topic) {
    return Counter.builder("nats.a2.sentinel_worker_conflict")
            .tag("topic", topic).register(registry);
}

public Timer dispatchLatencyTimer(String topic) {
    return Timer.builder("nats.a2.dispatch.latency")   // commit -> worker deliver (NFR-P2, soft SLI)
            .tag("topic", topic).register(registry);
}

/** En-yaşlı-orphan yaşı — gauge, A2OrphanSweep her döngüde günceller. */
public void registerOldestOrphanAgeGauge(String topic, Supplier<Number> ageSecondsSupplier) {
    Gauge.builder("nats.a2.sweep.oldest_orphan_age_seconds", ageSecondsSupplier)
            .tag("topic", topic).register(registry);
}
```

**Tag disiplini (DP-1/DP-2/NFR-S1):** yalnız `subject`/`channel`/`topic` (düşük kardinalite, PII-içermez). `externalTaskId`, `businessKey`, payload değeri **hiçbir tag'e YAZILMAZ** — mevcut disiplinin devamı.

---

## 2. Resilience4j CB metrikleri (özel sayaç GEREKMEZ)

`DlqBridgeCircuitBreakerFactory.create(...)` (`03_classes/1_nats_core_common.md` §4) `TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(...).bindTo(registry)` çağırır — `resilience4j_circuitbreaker_state`, `resilience4j_circuitbreaker_calls`, vb. standart Micrometer isimleriyle **otomatik** yayınlanır. Bu LLD **yeni bir CB-özel sayaç tanımlamaz** — Resilience4j'nin kendi binder'ı yeterli (DRY).

---

## 3. Infra-düzeyi metrikler (bu repo kodu ÜRETMEZ — açıklık için listelenir)

| Metrik (HLD §8 / FR-D2) | Kaynak | Bu repo'nun rolü |
|---|---|---|
| `fetchAndLock` QPS (hot-path 0 doğrulaması) | `pg_stat_statements` (PostgreSQL) | Bench modülü `PgStatStatementsSnapshotter` ile OKUR, üretmez |
| HikariCP aktif connection | Spring Boot Actuator + Micrometer HikariCP binder (otomatik, `HikariDataSource` bean'i varsa) | Ek kod GEREKMEZ — Spring Boot auto-config zaten sağlar |
| `ACT_RU_EXT_TASK` lock-wait | `pg_locks` / `pg_stat_activity` (özel exporter veya `postgres_exporter`) | Bu repo kapsamı DIŞI (DevOps/platform gözlemlenebilirlik katmanı) |

---

## 4. Alarm tanımları — ERROR_REGISTRY.md'ye köprü

Tam alarm eşikleri (`failure_event_correlation_miss` eşik-alarmı, `SYS_SENTINEL_WORKER_CONFLICT` CRITICAL+page kanalı, CB OPEN geçiş alarmı, en-yaşlı-orphan-yaşı alarmı): **`docs/sentinel/phase4/ERROR_REGISTRY.md` §4**. Burada tekrarlanmaz.

**Bağımlılık:** FR-D2, NFR-O1, US-D2, BR-OBS-002.
