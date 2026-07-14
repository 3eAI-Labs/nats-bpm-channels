# DB / Entity Erişim Haritası — Basamak-1

**Sentinel fazı:** Phase 4 — Developer (LLD). **Kaynak:** `docs/sentinel/phase3/HLD.md` §4 (veri akışı), §5 (ölçekleme); `docs/06-external-task-over-jetstream.md` §5.6/§7; ADR-0001/0002/0003.
**Tez (kilitli, DEĞİŞTİRİLMEZ):** `ACT_RU_EXT_TASK` = transactional outbox; basamak-1 **yeni tablo eklemez**. Bu belge LLD_GUIDELINE §2.1'in "DB şeması" talebini bu projeye özgü biçimde karşılar: **yeni DDL YOK** (aşağıda gerekçe), yalnız mevcut şemaya erişim haritası + tek yeni kalıcı-durum deposu (NATS JetStream KV — SQL değil).

---

## 0. Neden migration dizini/DDL YOK (mimari gerekçe, DATABASE_GUIDELINE'dan sapma DEĞİL)

`LLD_GUIDELINE §2.1` ve `DATABASE_GUIDELINE` her LLD'nin bir DB şeması (CREATE TABLE, migration dosyaları) tanımlamasını varsayar — bu varsayım **yeni bir tablo/entity yaratan** projeler içindir. Basamak-1'in HLD §1'de kilitlenmiş "dürüst tavan" tezi tam tersini söyler: **mevcut motor şeması (`ACT_RU_EXT_TASK`, `ACT_RU_VARIABLE`, `ACT_RU_EVENT_SUBSCR`, `ACT_RU_TIMER_JOB`) değişmez** — bu tablolar zaten Camunda7/CadenzaFlow/Flowable motorlarının **kendi** migration mekanizmalarıyla (Liquibase/DDL script, motor-native) yönetilir; bu repo (`nats-bpm-channels`) bir **eklenti/bağlayıcı katmanıdır**, motor şemasının sahibi DEĞİLDİR ve ona **asla DDL uygulamaz**. Basamak-1'in tek yeni kalıcı durumu (`a2-sweep-leader` lease anahtarı) **NATS JetStream KV**'de yaşar — bu SQL değildir, `docs/04_design/db/migrations/*.sql` kapsamına girmez.

**Sonuç:** `docs/sentinel/phase4/db/migrations/` dizini **kasıtlı olarak oluşturulmamıştır**. Faz-review bunu bir eksiklik olarak değil, yukarıdaki gerekçeyle **kapanmış bir karar** olarak değerlendirmelidir (phase-review'a not: bu bir "atlanmış görev" değil, HLD §1'in "sıfır DB lock satılmaz / mevcut şema korunur" ilkesinin doğal sonucudur).

---

## 1. Mevcut motor şeması — erişim haritası (yeni tablo YOK)

### 1.1 `ACT_RU_EXT_TASK` (A2 — outbox)

| Kolon (kullanılan) | Bileşen | Erişim türü |
|---|---|---|
| `ID_`, `TOPIC_NAME_`, `WORKER_ID_`, `LOCK_EXP_TIME_`, `RETRIES_`, `SUSPENSION_STATE_`, `EXECUTION_ID_`, `PROC_INST_ID_`, `PROC_DEF_ID_` | `A2ExternalTaskBehavior` | **INSERT** (`createAndInsert` + `lock`, tek INSERT — `ExternalTaskEntity.java:568-588,471-474`) |
| Aynı kolonlar | `A2OrphanSweep` (fetchable-parite SELECT) | **SELECT** (read-only, `FOR UPDATE` YOK — `ExternalTaskManager.selectExternalTasksForTopics`, `ExternalTask.xml` ~205-235) |
| `WORKER_ID_`, `LOCK_EXP_TIME_` | `A2OrphanSweep` (re-lock) | **UPDATE** (`LockExternalTaskCmd` → `ExternalTaskEntity.lock(...)`, yalnız publish-adayı satırlar için) |
| `WORKER_ID_`, `LOCK_EXP_TIME_` (temizlenir) | `A2OrphanSweep` (telafi-unlock, ADR-0003) | **UPDATE** (`UnlockExternalTaskCmd` → `ExternalTaskEntity.unlock()`, yalnız publish-fail dalında, **nadir**) |
| Satır **silinir** | `A2CompletionBridge` (complete) | **DELETE** (native `complete()` akışı — `deleteFromExecutionAndRuntimeTable`, motor-native, bu repo tetikler ama SQL'i motor üretir) |
| `RETRIES_`, `LOCK_EXP_TIME_` | `A2IncidentBridge` (`handleFailure(retries=0, retryDuration=0)`) | **UPDATE** (`ExternalTaskEntity.failed(...)` → `setRetriesAndManageIncidents(0)`) |

### 1.2 `ACT_RU_VARIABLE` (process değişkenleri — RESTRICTED/PII)

| Bileşen | Erişim |
|---|---|
| `A2ExternalTaskBehavior` (execute — değişken okuma, dolaylı) | SELECT (motor-native execution context) |
| `A2CompletionBridge` (`complete(..., vars)`) | INSERT/UPDATE (worker'ın döndürdüğü değişkenler motora yazılır — motor-native) |

**PII notu:** bu kolonlar `DATA_CLASSIFICATION.md` §2.3'te zaten envanterlenmiş (RESTRICTED/PII); basamak-1 bu tabloya **yeni bir kolon eklemez**, yalnız mevcut motor davranışını (worker sonucu → değişken yazımı) çağırır.

### 1.3 `ACT_RU_EVENT_SUBSCR` (Flowable — Event Registry wait-state)

| Bileşen | Erişim |
|---|---|
| `FailureEventBridge` (`eventReceived(...)` çağrısı) | SELECT+UPDATE (motor-native correlate — `EventRegistry` iç mekanizması, bu repo doğrudan SQL yazmaz) |

### 1.4 `ACT_RU_TIMER_JOB` (Flowable — yalnız opt-in boundary-timer modellenmişse)

| Bileşen | Erişim |
|---|---|
| Motor-native (BPMN boundary timer, kod YOK) | INSERT (aktivasyon) + DELETE (tetiklendiğinde/iptal) — instance-başına, **yalnız opt-in modellerde** (BR-FLW-004) |

**Maliyet ölçümü:** `TEST_SPECIFICATIONS.md` (c) — bu LLD'nin katkısı kod değil, ölçüm-testi tasarımıdır.

---

## 2. Fetchable-parite SQL (A2OrphanSweep — birebir native paritesi)

```sql
-- ExternalTask.xml fetchable predicate (bu fazda bizzat okundu, satır ~217-235'e tekabül eden blok):
select RES.* from ACT_RU_EXT_TASK RES
where
  (RES.LOCK_EXP_TIME_ is null or RES.LOCK_EXP_TIME_ <= :now)
  and (RES.SUSPENSION_STATE_ is null or RES.SUSPENSION_STATE_ = 1)
  and (RES.RETRIES_ is null or RES.RETRIES_ > 0)
  and RES.TOPIC_NAME_ in (:a2Topics)   -- sweep'in eklediği TEK ek filtre
```

**Java çağrı yüzeyi:** `ExternalTaskManager.selectExternalTasksForTopics(Collection<TopicFetchInstruction>, int, boolean, List<QueryOrderingProperty>)` (`ExternalTaskManager.java:73-94`, mevcut public metod) — **yeni mapped statement YAZILMAZ**, mevcut `selectExternalTasksForTopics` çağrılır (bkz. `03_classes/2_camunda_a2.md` §3.2).

---

## 3. KV bucket şeması (NATS JetStream KV — `a2-sweep-leader`, tek yeni kalıcı depo)

| Alan | Değer |
|---|---|
| Bucket adı | `a2-sweep-leader` |
| Replikasyon | 3 |
| TTL | 240s (`2·S`, S=120s default) |
| History | 1 |
| Anahtar sayısı | 1 (`leader`) |
| Değer şeması | UTF-8 string, node kimliği (ör. `pod-name` veya `hostname:pid`) — **PSEUDONYMOUS, PII YOK** |
| Erişim (yazma) | `SweepLeaderLease.tryAcquireOrRenew()` — `kv.create`/`kv.update` |
| Erişim (okuma) | `SweepLeaderLease.tryAcquireOrRenew()` — `kv.get` (mevcut sahibi kontrolü) |
| Oluşturma | `JetStreamKvManager.ensureBucket(...)` (bootstrap, `08_config.md` §3, `99_deployment.md` §2) |

**DATA_CLASSIFICATION.md §2.3 kapsam notu:** bu karar (ADR-0002) KV katmanını "basamak-1'de opsiyonel"den **aktif**e çevirir — lease anahtarı PII taşımaz, TTL'li (§2.3 zaten bunu öngörmüştü).

---

## 4. Happy-path DB-op sayımı (BR-OBS-001 taban çizgisi — bench'in birincil metriği)

| Bileşen | Baseline (native-poll) | A2-push hedef | Ölçüm sınıfı |
|---|---|---|---|
| Task INSERT (+ sentinel-kilit, aynı INSERT) | 1 | **1** (artmıyor) | `DbRoundTripReport.taskInsertCount` |
| Poll sorguları (amortize, N_worker×f_poll÷throughput) | N_worker × f_poll ÷ throughput | **0** | `DbRoundTripReport.pollQueryCount` |
| `fetchAndLock` UPDATE | 1 | **0** | `DbRoundTripReport.fetchAndLockCount` — **TEK sert kapı (Q7/BUS_BENCH_METRIC_REGRESSION)** |
| `complete` token-move tx (P2, dürüst tavan) | 1 | **1** (artmıyor) | `DbRoundTripReport.completeTxCount` |
| Sweep okuması (amortize ≤1 read / S / cluster) | — | **≈0** (leader-only, seyrek) | `DbRoundTripReport.sweepReadCount` |

Bu tablo `docs/06-external-task-over-jetstream.md` §5.6'nın **birebir aynısıdır** (tekrar değil — kaynak referans, LLD burada yalnız hangi Java sınıfının/alanının bu sayıyı ürettiğini bağlar, bkz. `03_classes/5_bench.md`).

---

## 5. İzlenebilirlik

BR-A2-001/002/004/005/013 (DB erişimi), BR-OBS-001 (metrik), ADR-0001/0002/0003 (KV+sweep), FR-A1/A2/A5/A6/D1, US-A1/A2/A3/D1.
