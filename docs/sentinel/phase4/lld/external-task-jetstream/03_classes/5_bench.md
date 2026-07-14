# 03.5 — `nats-bpm-bench` (Yeni Modül)

**Kaynak:** HLD §2.9, ADR-0007 §4 (yeni beşinci Maven modülü). Paket kökü: `com.threeai.nats.bench`.
**Sorumluluk:** Aynı BPMN senaryosunu iki modda (native-poll baseline ↔ A2-push) Testcontainers ile koşturup BR-OBS-001'in **tek sert kabul kapısını** üretmek.

> Tam test-tasarım detayı (senaryo adımları, `pg_stat_statements` sorgu-sayım metodolojisi, iki-modlu koşum akışı): `docs/sentinel/phase4/TEST_SPECIFICATIONS.md` §Bench. Bu dosya yalnız **sınıf iskeletini** tanımlar.

---

## 1. Sınıf iskeleti

```java
package com.threeai.nats.bench;

/** Testcontainers ile PG + engine (Camunda veya CadenzaFlow) + NATS + N worker ayağa kaldırır. */
public class BenchEnvironment implements AutoCloseable {
    // PostgreSQLContainer<?> postgres  (pg_stat_statements extension aktif — shared_preload_libraries)
    // GenericContainer<?> nats        (JetStream aktif, ephemeral R1 — dev/preflight modu, NATS_JETSTREAM.md §3)
    // ProcessEngine engine             (embedded, Testcontainers PG'ye bağlı)
    // List<SimulatedWorker> workers    (N worker — job tüket, reply üret; native-poll modunda fetchAndLock da simüle eder)
}

public enum BenchMode { NATIVE_POLL_BASELINE, A2_PUSH }

/** Tek senaryo: M adet external-task instance başlat, hepsinin complete olmasını bekle, DB-roundtrip say. */
public interface BenchScenario {
    DbRoundTripReport run(BenchEnvironment env, BenchMode mode, int taskCount);
}

public class ExternalTaskLifecycleScenario implements BenchScenario { ... }   // BR-OBS-003 tek senaryo, iki modda

/** pg_stat_statements queryid bazlı fingerprint sayaçları — US-D1 birincil metrik. */
public class PgStatStatementsSnapshotter {
    public Snapshot capture(DataSource ds) { ... }   // SELECT queryid, query, calls FROM pg_stat_statements
    public DbRoundTripDelta diff(Snapshot before, Snapshot after) { ... }
}

/** BR-OBS-001'in kabul tablosunu üretir: Task INSERT, Poll, fetchAndLock UPDATE, complete tx, sweep okuması. */
public record DbRoundTripReport(
        long taskInsertCount, long pollQueryCount, long fetchAndLockCount,
        long completeTxCount, long sweepReadCount, BenchMode mode) {

    /** BUS_BENCH_METRIC_REGRESSION — TEK sert kapı (Q7). */
    public boolean passesHardGate() {
        if (mode == BenchMode.A2_PUSH) {
            return pollQueryCount == 0 && fetchAndLockCount == 0;   // NFR-P1 doğrulaması
        }
        return true;   // baseline modunda kapı YOK — yalnız karşılaştırma referansı üretir
    }
}

/** Destekleyici SLI'lar — SYS_BENCH_SLI_DRIFT (warn-only, sert kapı DEĞİL). */
public record SupportingSliReport(
        double dispatchLatencyP95Ms, double lockWaitMsAvg, int hikariActiveConnectionsAvg) {
    public boolean withinSoftTargets() {
        return dispatchLatencyP95Ms <= 200.0;   // NFR-P2 — yalnız RAPORLANIR, FAIL ETMEZ
    }
}
```

---

## 2. JUnit tag ve CI konumu

```java
@Tag("bench")   // BR-OBS-003 — nightly/manuel, ana CI'yı BLOKLAMAZ
class ExternalTaskLifecycleBenchTest {

    @Test
    void a2PushProducesZeroPollAndZeroFetchAndLock() {
        try (BenchEnvironment env = BenchEnvironment.start()) {                 // Docker yoksa: SYS_BENCH_ENVIRONMENT_UNAVAILABLE (warn, skip)
            DbRoundTripReport baseline = new ExternalTaskLifecycleScenario().run(env, NATIVE_POLL_BASELINE, 1000);
            DbRoundTripReport target   = new ExternalTaskLifecycleScenario().run(env, A2_PUSH, 1000);
            assertThat(target.passesHardGate()).isTrue();                       // BUS_BENCH_METRIC_REGRESSION → build-fail
            assertThat(target.taskInsertCount()).isEqualTo(baseline.taskInsertCount());     // artmıyor
            assertThat(target.completeTxCount()).isEqualTo(baseline.completeTxCount());     // artmıyor
            BenchReportWriter.write(baseline, target, supportingSliOf(target));  // karşılaştırmalı rapor (nightly artifact)
        } catch (ContainerLaunchException dockerUnavailable) {
            Assumptions.abort("SYS_BENCH_ENVIRONMENT_UNAVAILABLE — Docker/Testcontainers yok, ana CI bloklanmaz");
        }
    }
}
```

`@Tag("bench")` mevcut dört modülün Testcontainers entegrasyon-test altyapısı üstüne kurulur (D-F doğrulaması, HLD §11 kalem 5) — yeni bir Testcontainers wiring **icat edilmez**, mevcut `*IntegrationTest.java` desenleri (`CamundaInboundIntegrationTest`, `JetStreamInboundIntegrationTest`, vb.) referans alınır.

---

## 3. `fetchAndLock` fingerprint izolasyonu (HLD §11 kalem 5 — uyarı)

`PgStatStatementsSnapshotter`, `fetchAndLock` SQL ailesini (native `FetchExternalTasksCmd`'nin ürettiği sorgular) `queryid` ile izler. **Uyarı (HLD doğrulaması):** `IN`-list arity (fetch edilen topic sayısı) değişirse parse-tree değişebilir → aynı mantıksal sorgu **birden çok** `queryid`'e bölünebilir. Bu yüzden `DbRoundTripReport.fetchAndLockCount()` **tek bir queryid değil, önceden bilinen fetchAndLock SQL-imza ailesinin toplamıdır** (BR-OBS-001 ölçüm notu — `PgStatStatementsSnapshotter` bu aileyi bir `Set<String> knownFetchAndLockQueryPrefixes` ile eşler, tam `queryid` eşitliği değil).

---

## 4. Bağımlılık ve modül kurulumu

- `pom.xml` (root): `<module>nats-bpm-bench</module>` eklenir (`02_package_structure.md` §1).
- `nats-bpm-bench/pom.xml`: `nats-core`, `camunda-nats-channel`, `cadenzaflow-nats-channel`, `flowable-nats-channel` + Testcontainers (`postgresql`, `nats` modülü veya `GenericContainer`) + JUnit 5 test-scope bağımlılıkları. Tam POM içeriği **Phase 5** kapsamıdır.

**Bağımlılık:** BR-OBS-001/002/003, FR-D1/D2/D3, US-D1/D2/D3, ADR-0007 §4.
