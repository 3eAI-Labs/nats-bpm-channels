# Strateji: History Offload — ACT_HI Yazımını DB'den NATS'a Taşı (Basamak-2)

**Bağlam:** `05-db-offload-strategy.md` §6.7 merdiveni, basamak-2. Basamak-1 (dispatch push) v0.2.0 olarak yayında (`9e04b16`).
**Durum:** Taslak — açık kararlar tek tek Levent onayına sunuluyor ([[decision-workflow-preference]] kalıbı).
**Tarih:** 2026-07-15

---

## 0. Bu belge nasıl kullanılır (Sentinel girişi)

Basamak-2'nin karar dokümanı — `06-external-task-over-jetstream.md`'nin basamak-1'deki rolü. §6'daki açık kararlar kilitlendikçe Sentinel phase1 (PO) girdisi olur. Kanıt disiplini aynıdır: motor davranışı iddiaları file:line'lı; doğrulanmamış varsayımlar etiketli; effort tahmini yok.

## 1. Karar özeti (kilitli)

*(Henüz karar kilitlenmedi — §6 sırayla soruluyor.)*

## 2. Neden bu belge

`ACT_HI_*` tabloları engine DB'sinin **en büyük yazım hacmi** (docs/05 §6.7 satır 2): her process-instance adımı, task, variable güncellemesi, job log'u history satırları üretir ve **runtime state ile aynı ACID transaction'ında** DB'ye yazılır. Basamak-2, bu yazımı NATS'a (ve oradan bir async query-store'a) taşıyarak birincil DB'den kaldırır. SPI-katmanı iş; fork core'a dokunmaz.

## 3. Kanıt: fork history altyapısı (2026-07-15 keşfi — `~/Workspaces/cadenzaflow/cadenzaflow-bpm-platform/engine`)

Fork history alanında upstream Camunda 7 ile **birebir** (tek commit: paket rename `org.cadenzaflow.*`) — Camunda history desenleri doğrudan geçerli.

- **SPI arayüzü:** `impl/history/handler/HistoryEventHandler.java:38-53` — `handleEvent(HistoryEvent)` + `handleEvents(List)`. Javadoc (:26-29) async/MQ implementasyona açıkça kapı bırakır; fork'ta böyle bir implementasyon YOK.
- **Default zincir:** `DbHistoryEventHandler.java:40` (DB'ye yazar) · `CompositeHistoryEventHandler.java:33,38` (delege listesi) · `CompositeDbHistoryEventHandler.java:70-72` (listeye default DB handler ekler).
- **⚠️ KRİTİK — çağrı tx-İÇİ ve senkron:** `HistoryEventProcessor.java:73-85` → `historyEventHandler.handleEvent(singleEvent)` aynı thread'de; `DbHistoryEventHandler.java:172-174` command-context `DbEntityManager`'ını kullanır; `CommandContext.java:186-197` `flushSessions()` → `transactionContext.commit()` — **history INSERT'leri runtime state ile TEK transaction'da.** 20+ çağrı noktası (`ExecutionEntity.java:375,504`, `PvmExecutionImpl.java:1218`, listener'lar...).
- **Resmi genişletme noktaları (fork değişikliği GEREKMEZ):** `ProcessEngineConfigurationImpl.java:757-769` (alanlar), `:2788-2796` (`initHistoryEventHandler()` — yalnız null iken kurar), `:3876-3898` (setter'lar). Üç senaryo: (1) `customHistoryEventHandlers` + default-DB açık → DB + custom yan yana; (2) `enableDefaultDbHistoryEventHandler=false` → yalnız custom; (3) `setHistoryEventHandler(...)` → tam ikame.
- **History level:** `HistoryLevel.java:56-82` (`isHistoryEventProduced`), NONE/ACTIVITY/AUDIT/FULL (`HistoryLevelNone.java:27-39` → hiç event üretilmez, handler'a ulaşmaz; default AUDIT). Filtreleme producer aşamasında — custom handler'a yalnız seviyenin ürettiği event'ler gelir.
- **ACT_HI tablo haritası:** 16+ entity → tablo eşlemesi (`HistoricProcessInstanceEntity`→ACT_HI_PROCINST, `HistoricActivityInstanceEntity`→ACT_HI_ACTINST, `HistoricVariableInstanceEntity`→ACT_HI_VARINST, `HistoricDetailEntity`→ACT_HI_DETAIL, TASKINST, INCIDENT, IDENTITYLINK, JOB_LOG, EXT_TASK_LOG, DECINST±, BATCH, CASEINST±, OP_LOG, COMMENT, ATTACHMENT; mapping XML: `mapping/entity/Historic*.xml`). Variable byte'ları ayrıca `ByteArrayEntity(..., ResourceTypes.HISTORY)` (`DbHistoryEventHandler.java:97-105`).
- **Async/spool YOK:** yazım yolunda kuyruk/spool sıfır; `deleteHistoricProcessInstancesAsync`/`cleanUpHistoryAsync` yalnız SİLME batch'leri.

## 4. Tasarım uzayı ve peşinen işaretli tehlikeler

**REDDEDİLEN (kanıt gereği) — handler içinde senkron NATS publish:** `handleEvent` tx-içi/aynı-thread olduğundan (a) NATS latency'si engine komutunu bloklar; (b) publish exception'ı `CommandContext.close`'da commit'i atlatır → **history yayını runtime transaction'ını rollback eder.** Basamak-1'in in-tx-blocking dersinin (docs/06 §3) history izdüşümü — yeniden açılmaz.

**Basamak-1'den KRİTİK FARK — outbox yok olma problemi:** Basamak-1'de `ACT_RU_EXT_TASK` satırı transactional outbox'tı; kaçan publish sweep'le telafi edilirdi. Basamak-2'nin nihai hedefi ACT_HI yazımını **kaldırmak** — DB handler kapatıldığında kaçan publish'in telafi kaynağı da yok olur: post-commit publish penceresinde çökme = **kalıcı audit kaybı**. At-least-once isteniyorsa dayanıklı bir ara kayıt (kompakt outbox / öncelikli sınıflar için) veya kayıp-kabulü (at-most-once) bilinçli seçilmelidir → **D-A**. docs/05 D2 interim posture bu ayrımı zaten tanımlar: *kritik → dayanıklı handoff (at-least-once); kritik değil → post-commit (at-most-once).*

**Yeniden kullanılabilir basamak-1 varlıkları:** post-commit `TransactionListener` deseni (D-A/D-C), `DlqPublisher` + DLQ kontratı + custody-transfer ack ilkesi (D-E), KV-lease leader (`SweepLeaderLease`), asyncapi kontrat-first (ADR-0006), `nats-bpm-bench` (D-F metriği history moduna genişler), ayrı-stream deployment dersi (CQ-6).

## 5. Basamak-1'den devreden borçlar (planlama girdisi)

| # | Borç | Kaynak |
|---|---|---|
| 1 | `allow-unsafe-lock-duration` runbook/on-call devri | RELEASE-DECISIONS Q3 |
| 2 | F-2 kalanı: `BenchEnvironment.ensureStreams()` + prod stream provisioning (WorkQueue) DevOps takibi | phase6 F-2 kapanış notu |
| 3 | MINOR: scheduler shutdown await (`A2SubscriptionRegistrar`) | phase6 review |
| 4 | MINOR: `FailureEventBridge` önceden-var-olan NonMatchingEventConsumer'ı eziyor | phase6 review |
| 5 | TEST_SPEC (c): Flowable boundary-timer `ACT_RU_TIMER_JOB` maliyet ölçümü | phase4/5.5 |
| 6 | Sweep re-publish captured-variables taşımaz (Javadoc'lu açık) | fix-paketi CODER-NOTES |
| 7 | **Bench İLK GERÇEK KOŞUSU** — basamak-1 hard-gate sayıları (poll=0 kanıtı + DB-op taban çizgisi) = **basamak-2'nin hedef tavanı kanıtı** | D-F kapanış kriteri |

## 6. Açık kararlar (sırayla Levent onayına)

- **D-A — Yayın ve tutarlılık deseni:** custom handler in-tx'te ne yapar; at-least-once nasıl sağlanır (kompakt outbox / post-commit at-most-once / event-sınıfı-bazlı hibrit)? Cutover-sonrası telafi kaynağı sorusu burada çözülür. — **sıradaki karar.**
- **D-B — Query-store seçimi:** history sorguları (Cockpit dahil) nereden okur — ayrı Postgres/ClickHouse/ES projeksiyonu mu, JetStream stream + on-demand projeksiyon mu? Consumer = asyncapi kontratlı projeksiyon servisi.
- **D-C — Geçiş stratejisi + Cockpit kaderi:** dual-run (DB+NATS) → doğrulama → cutover (`enableDefaultDbHistoryEventHandler=false`); Cockpit history UI'ı ACT_HI'dan okur — cutover sonrası ne olur (query-store'a yönlendirme / kayıp kabulü / Cockpit-history'siz işletim)?
- **D-D — Tablo/event kapsamı:** tümü mü, hacim-öncelikli alt küme mi (DETAIL/VARINST/ACTINST önce)? OP_LOG/COMMENT/ATTACHMENT gibi düşük-hacim + yüksek-audit sınıfların yeri.
- **D-E — DLQ/ack + dedup:** history event'leri için `Nats-Msg-Id` anahtarı (event id?), D-E(basamak-1) kontratının yeniden kullanımı, sıralama gereksinimleri (aynı instance'ın event'leri sıralı mı tüketilmeli — JetStream ordering / partition anahtarı).
- **D-F — Başarı metriği:** bench'e history modu — normalize "DB yazım-op / process-adımı" metriği; baseline = mevcut AUDIT seviyesi.
- **D-G — Flowable tarafı:** Flowable history mimarisi farklı (async history zaten var mı? — doğrulanacak); kapsam bu basamakta Camunda/CadenzaFlow mu, üç motor mu?

## 7. Doğrulama notları

- Fork history altyapısı haritası → ✅ DOĞRULANDI (2026-07-15, Explore keşfi — §3'teki tüm file:line'lar).
- Flowable history mimarisi (async history job / handler SPI'ı) → doğrulanacak (D-G öncesi).
- Cockpit history UI'ının ACT_HI bağımlılık yüzeyi → doğrulanacak (D-C öncesi).
- `handleEvents(List)` batch yolunun gerçek kullanım sıklığı (composite tek tek `handleEvent`'e düşüyor — `CompositeHistoryEventHandler.java:100-105`) → phase3'te doğrulanacak.
