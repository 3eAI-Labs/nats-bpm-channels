# Test Specifications — Devredilen Doğrulamalar + Bench Tasarımı

**Sentinel fazı:** Phase 4 — Developer (LLD). **Kaynak:** `docs/sentinel/phase3/HLD.md` §11 (4 kalem `⏭ phase4/5'te test ile doğrulanacak` olarak işaretlenmiş), ADR-0005 §2 (guard test), BR-OBS-001/002/003.
Bu belge, phase3'ün devrettiği **4 test görevini** somut test-spesifikasyonuna (sınıf adı, kurulum, adım, assertion, geçme kriteri) çevirir + `nats-bpm-bench` modülünün tam test tasarımını tanımlar. **Kod YAZILMAZ** (Phase 5 kapsamı) — yalnız test **tasarımı**.

---

## (a) Flush-öncesi `lock()` tek-INSERT guard testi (ADR-0005 §2 · BR-A2-002 · US-A2)

**Doğrulanacak iddia:** `A2ExternalTaskBehavior.execute()`'te `createAndInsert(...)` + aynı-tx `task.lock(SENTINEL, L)` çağrısı **tek bir SQL INSERT** üretir — flush sonrası ikinci bir `UPDATE ACT_RU_EXT_TASK` **YOK**.

**Test sınıfı:** `CamundaA2GuardTest` (`camunda-nats-channel/src/test/.../a2/`) + ayna `CadenzaFlowA2GuardTest` (`cadenzaflow-nats-channel`).

**Altyapı farkı (önemli):** mevcut `*IntegrationTest.java` sınıfları (`CamundaInboundIntegrationTest` vb.) yalnız **NATS Testcontainers** kullanır ve `RuntimeService`'i **Mockito ile mock'lar** — gerçek bir `ProcessEngine`/DB YOKTUR (bu fazda `CamundaInboundIntegrationTest.java:17-45` okunarak doğrulandı). Bu guard testi **gerçek bir embedded `ProcessEngine`** (H2 in-memory veya Testcontainers PostgreSQL) ve **gerçek SQL-sayaç** gerektirir — mevcut desenin **genişletilmesi**, tekrarı değil.

**Kurulum:**
1. Embedded `ProcessEngine` (H2, `jdbcUrl=jdbc:h2:mem:guard-test`) + `datasource-proxy` (`net.ttddyy:datasource-proxy`, JDBC-seviyesi SQL interceptor) ile sarmalanmış `DataSource`.
2. BPMN modeli: tek `camunda:type="external" camunda:topic="a2-guard-topic"` service-task.
3. `A2BpmnParseListener` A2-topic listesine `a2-guard-topic` eklenmiş şekilde kayıtlı.

**Adımlar:**
1. `datasource-proxy`'nin SQL sayaç listener'ı **sıfırlanır**.
2. Süreç başlatılır (`runtimeService.startProcessInstanceByKey(...)`) — bu, `A2ExternalTaskBehavior.execute()`'i tetikler.
3. Sayaç, `ACT_RU_EXT_TASK` tablosuna karşı çalışan **tüm** DML ifadelerini (INSERT/UPDATE) toplar.

**Assertion (geçme kriteri):**
- `ACT_RU_EXT_TASK`'a karşı **tam olarak 1 INSERT**, **0 UPDATE**.
- `WORKER_ID_` ve `LOCK_EXP_TIME_` kolonları INSERT ifadesinin **parametre listesinde** dolu (ayrı bir sonraki-UPDATE'te DEĞİL).

**Regresyon rolü (ADR-0005 §2):** engine upgrade sonrası bu test **kırmızıya düşerse**, `createAndInsert`/`lock`'un iç davranışı değişmiş demektir → A2 aktivasyonu **fail-closed** bloklanmalıdır (upgrade runbook, `99_deployment.md` §4).

---

## (b) Core-publish'te `Nats-Msg-Id` dedup çalışmadığı varsayımının testi (HLD §11 kalem 4b)

**Doğrulanacak/reddedilecek varsayım:** `DlqPublisher`'ın core-NATS fallback yolu (`connection.publish(...)`, JetStream publish DEĞİL) ile yayınlanan bir mesajın `Nats-Msg-Id` header'ı, hedef stream'in `duplicate_window`'u tarafından **dedup'lanıyor mu, yoksa çift kayıt mı oluşuyor**?

**Test sınıfı:** `CoreNatsFallbackDedupTest` (`nats-core/src/test/.../dlq/`), gerçek NATS Testcontainers (mevcut `GenericContainer("nats:2.10-alpine").withCommand("--jetstream")` deseni).

**Kurulum:** `dlq.>` subject'ine bound bir stream, `duplicate_window=120s`.

**Adımlar:**
1. `connection.publish(subject, headersWithNatsMsgId, data)` (core NATS, JetStream API DEĞİL) — **aynı** `Nats-Msg-Id` ile **iki kez**, aralarında < 1s.
2. Stream'in mesaj sayısını `jsm.getStreamInfo(streamName).getStreamState().getMsgCount()` ile oku.

**Assertion:**
- **Eğer `msgCount == 1`:** dedup core-publish'te de ÇALIŞIYOR — HLD §11 kalem 4b'nin "belirsiz" notu **çözüldü**, `DlqPublisher`'ın `PUBLISHED_CORE_FALLBACK` yolu için ek bir güvenlik notu gerekmez.
- **Eğer `msgCount == 2`:** dedup ÇALIŞMIYOR — HLD'nin "güvenli varsayım: fallback dedup'a güvenme" notu **doğrulanmış** olur; `DlqPublisher` dokümantasyonuna (`03_classes/1_nats_core_common.md` §2.3) bu sınırlama **kalıcı not** olarak eklenir (kod değişikliği gerekmez, yalnız dokümantasyon kesinleşir).

**Her iki sonuç da testin BAŞARILI kabul edildiği bir "karakterizasyon testi"dir** (assertion sonuca göre iki dalda da geçerli olacak şekilde yazılır — `assertThat(msgCount).isIn(1L, 2L)` + sonuca göre `System.out`/rapor notu, CI'ı KIRMAZ). Amaç varsayımı **belgelemek**, bir "doğru" değeri zorlamak değil.

---

## (c) Boundary-timer `ACT_RU_TIMER_JOB` maliyet ölçümü (HLD §11 kalem 6b · BR-FLW-004 · US-B4)

**Doğrulanacak iddia:** opt-in boundary-timer modellenmiş bir aktivitenin, instance başına `ACT_RU_TIMER_JOB` tablosuna **tam olarak** 1 INSERT (timer aktivasyonu) + 1 DELETE (timer tetiklenme/iptal) maliyeti getirdiği.

**Test sınıfı:** `BoundaryTimerCostTest` (`flowable-nats-channel/src/test/.../escalation/`), `datasource-proxy` + embedded Flowable engine (H2).

**Kurulum:** İki BPMN modeli — (A) SLA'sız (timer YOK, yalnız DLQ→failure-event), (B) opt-in boundary-timer'lı (aynı süreç + boundary timer event).

**Adımlar:**
1. Model (A) çalıştırılır, `ACT_RU_TIMER_JOB` DML sayacı alınır → **beklenen: 0**.
2. Model (B) çalıştırılır, aynı sayaç alınır → **beklenen: 1 INSERT** (timer job aktivasyonu).
3. Timer süresi ilerletilir (`processEngineConfiguration.getClock().setCurrentTime(...)`, test-clock deseni) — timer tetiklenir.
4. Sayaç tekrar okunur → **beklenen: +1 DELETE** (toplam: 1 INSERT + 1 DELETE).

**Assertion:** Model (A) maliyeti **sıfır**; Model (B) maliyeti **yalnız kendi instance'ında ödenir** — bu, BR-FLW-004'ün "maliyet yalnız o modellerde ödenir" iddiasının doğrudan kanıtıdır.

---

## (d) `eventReceived` eşleşmeyen-event davranış testi (HLD §11 kalem 6c · BR-FLW-005 · US-B5)

**Doğrulanacak/karakterize edilecek davranış:** `EventRegistry.eventReceived(...)`, bekleyen subscription'ı olmayan bir event'te **exception mi fırlatır, yoksa sessizce no-op mu döner**?

**Test sınıfı:** `EventReceivedNoMatchBehaviorTest` (`flowable-nats-channel/src/test/.../escalation/`), embedded Flowable engine.

**Adımlar:**
1. Hiçbir bekleyen subscription'ı olmayan bir `InboundChannelModel` + `NatsInboundEvent` (rastgele correlation key) hazırlanır.
2. `eventRegistry.eventReceived(model, event)` doğrudan çağrılır (`try/catch` ile sarmalı).
3. Sonuç gözlemlenir: (i) exception fırlarsa hangi tip (`FlowableException` alt sınıfı?), (ii) fırlamazsa dönüş değeri/yan-etki nedir.

**Assertion (sonuca göre `FailureEventBridge` tasarımı sabitlenir — `03_classes/4_flowable.md` §2 LLD-QUESTION'ı bu testle kapanır):**
- **Exception fırlıyorsa:** `FailureEventBridge.handleDlqMessage(...)`'daki `catch (NoMatchingSubscriptionException)` dalı (veya gözlemlenen gerçek tip) **doğru tiple** güncellenir.
- **Sessizce dönüyorsa:** `FailureEventBridge` bir **sonuç-nesnesi kontrolü** ekler (`eventReceived`'ın dönüş değeri veya yan-etkisi üzerinden "match oldu mu" tespiti) — bu durumda mevcut LLD pseudo-code'u (§2, `03_classes/4_flowable.md`) **bir dallanma revizyonu** gerektirir (Phase 5'te, bu test sonucuna göre).

**Bu test, Phase 4 LLD'nin bilinçli olarak açık bıraktığı tek dallanma noktasıdır** — `manifest.md` "Open Questions" listesindeki 2. madde ile eşleşir.

---

## Bench Modülü Test Tasarımı (`nats-bpm-bench` — BR-OBS-001/002/003 · US-D1/D2/D3)

### Senaryo (tek, iki modda — `ExternalTaskLifecycleScenario`)

1. **Ortam:** Testcontainers `PostgreSQLContainer` (`pg_stat_statements` extension `shared_preload_libraries`'e eklenmiş özel imaj veya `postgres:16` + `CREATE EXTENSION pg_stat_statements`) + embedded `ProcessEngine` (bu PG'ye bağlı) + `GenericContainer` NATS (JetStream) + N simüle worker (basit NATS client, gerçek iş mantığı yok — yalnız job tüket→reply üret).
2. **İki mod:**
   - `NATIVE_POLL_BASELINE`: klasik `camunda:type="external"` (A2 SWAP EDİLMEMİŞ) + bir `fetchAndLock` poll-loop simülasyonu (test-içi basit poller, gerçek worker SDK değil — yalnız baseline referansı üretmek için).
   - `A2_PUSH`: aynı BPMN modeli, ama topic A2-topic listesinde — `A2ExternalTaskBehavior` aktif.
3. **Ölçüm noktaları:** `PgStatStatementsSnapshotter.capture(dataSource)` senaryo öncesi/sonrası — `pg_stat_statements`'tan `queryid`/`calls` farkı.

### Sorgu-sayım metodolojisi (`pg_stat_statements` queryid)

1. Senaryo öncesi: `SELECT pg_stat_statements_reset();` (izole ölçüm için, yalnız test-DB'de).
2. Senaryo koşulur (N task, tam yaşam döngüsü: doğum→dispatch→complete).
3. Senaryo sonrası: `SELECT queryid, query, calls FROM pg_stat_statements WHERE query ILIKE '%ACT_RU_EXT_TASK%' ORDER BY calls DESC;`
4. Sınıflandırma (`knownFetchAndLockQueryPrefixes` — `03_classes/5_bench.md` §3): `fetchAndLock`/`selectExternalTasksForTopics` ailesi sorguları ayrı `queryid`'lerde toplanabilir (IN-list arity uyarısı, HLD §11 kalem 5) — bench bu aileyi **toplar**, tek queryid eşitliği aramaz.
5. `INSERT INTO ACT_RU_EXT_TASK` ve `complete`'in ürettiği `DELETE FROM ACT_RU_EXT_TASK` sorguları ayrı queryid'lerdir — bunlar **A2_PUSH modunda baseline'a göre artmamalı** (BR-OBS-001).

### İki-modlu koşum akışı

```
for mode in [NATIVE_POLL_BASELINE, A2_PUSH]:
    env = BenchEnvironment.start()
    pg_stat_statements_reset()
    scenario.run(env, mode, taskCount=1000)
    report[mode] = PgStatStatementsSnapshotter.capture(dataSource).classify()
assert report[A2_PUSH].pollQueryCount == 0
assert report[A2_PUSH].fetchAndLockCount == 0
assert report[A2_PUSH].taskInsertCount == report[NATIVE_POLL_BASELINE].taskInsertCount
assert report[A2_PUSH].completeTxCount == report[NATIVE_POLL_BASELINE].completeTxCount
```

### CI entegrasyonu

`@Tag("bench")`, nightly cron + manuel tetikleme (`mvn test -Dgroups=bench`); ana CI pipeline'ı (`mvn test`, tag hariç) **bloklanmaz**. Docker/Testcontainers ortamı yoksa (`SYS_BENCH_ENVIRONMENT_UNAVAILABLE`) test **abort** edilir (JUnit `Assumptions`), FAIL değil.

### Rapor çıktısı

`BenchReportWriter` (nightly CI artifact) — iki-mod karşılaştırmalı tablo (bu belgenin `DB_ACCESS_MAP.md` §4 tablosuyla **aynı format**) + destekleyici SLI'lar (dispatch p95, lock-wait, HikariCP) — yalnız **rapor**, `BUS_BENCH_METRIC_REGRESSION` hariç hiçbiri build'i kırmaz.

---

## Özet — test-spesifikasyon sayımı

| Kalem | Test sınıfı (önerilen) | Tür | Build'i kırar mı? |
|---|---|---|---|
| (a) tek-INSERT guard | `CamundaA2GuardTest` + `CadenzaFlowA2GuardTest` | Entegrasyon (yeni altyapı: embedded engine+DB) | Evet — regresyon dedektörü (ADR-0005) |
| (b) core-publish dedup | `CoreNatsFallbackDedupTest` | Entegrasyon (mevcut NATS Testcontainers deseni) | Hayır — karakterizasyon |
| (c) boundary-timer maliyet | `BoundaryTimerCostTest` | Entegrasyon (yeni: embedded Flowable engine+DB) | Hayır — ölçüm/rapor |
| (d) `eventReceived` no-match | `EventReceivedNoMatchBehaviorTest` | Entegrasyon (embedded Flowable engine) | Hayır — karakterizasyon, LLD dallanma kararını kilitler |
| Bench (iki-modlu) | `ExternalTaskLifecycleBenchTest` (+ `nats-bpm-bench` modülü) | Testcontainers (PG+engine+NATS+worker) | **Yalnız** `BUS_BENCH_METRIC_REGRESSION` |

**Toplam: 5 test-spesifikasyonu** (4 devredilen + 1 bench-modülü tasarımı).
