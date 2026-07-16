# Strateji: History Offload — ACT_HI Yazımını DB'den NATS'a Taşı (Basamak-2)

**Bağlam:** `05-db-offload-strategy.md` §6.7 merdiveni, basamak-2. Basamak-1 (dispatch push) v0.2.0 olarak yayında (`9e04b16`).
**Durum:** Taslak — açık kararlar tek tek Levent onayına sunuluyor ([[decision-workflow-preference]] kalıbı).
**Tarih:** 2026-07-15

---

## 0. Bu belge nasıl kullanılır (Sentinel girişi)

Basamak-2'nin karar dokümanı — `06-external-task-over-jetstream.md`'nin basamak-1'deki rolü. §6'daki açık kararlar kilitlendikçe Sentinel phase1 (PO) girdisi olur. Kanıt disiplini aynıdır: motor davranışı iddiaları file:line'lı; doğrulanmamış varsayımlar etiketli; effort tahmini yok.

## 1. Karar özeti (kilitli)

1. **D-A (2026-07-15): Hibrit, event-sınıfı-bazlı tutarlılık.** Audit-kritik sınıflar (OP_LOG, INCIDENT, EXT_TASK_LOG — düşük hacim) → tx-içi **kompakt outbox** + relay/delete (**at-least-once**, audit kaybı imkansız); bulk sınıflar (DETAIL, VARINST, ACTINST — hacmin ~%90'ı) → **post-commit listener** (**at-most-once**, sıfır DB yazımı). Sınıflandırma konfigürable. docs/05 D2 interim-posture'ının history izdüşümü. REDDEDİLEN: handler-içi senkron publish (tx coupling, §4); tek-desen-hepsi varyantları (tam-outbox: yazım kalır; tam-post-commit: sessiz audit kaybı).
2. **D-B (2026-07-16): Query-store = engine DB'sinden AYRI Postgres projeksiyonu.** Denormalize/sorgu-odaklı şema; consumer = asyncapi-kontratlı projeksiyon servisi (ADR-0006 deseni). Contention domain ayrılır, SQL/ops bilgisi yeniden kullanılır, KVKK retention SQL'le. Hacim zorlarsa ClickHouse'a evrim ayrı karar (kontrat sabit → consumer değişimi izole). REDDEDİLEN: JetStream-only (rastgele-erişim audit sorguları için yetersiz); ClickHouse-şimdi (yeni ops yüzeyi, ihtiyaç kanıtlanmadan).
3. **D-C (2026-07-16): Kademeli sınıf-bazlı cutover + minimal sorgu-API.** Dual-run boyunca sınıf-başına reconciliation raporu (projeksiyon ↔ ACT_HI); sınıf N gün temiz kalınca yalnız o sınıfın DB yazımı kapatılır (hacim-öncelikli sıra: DETAIL→VARINST→ACTINST→…). Cutover'lanan sınıflar için Cockpit history körleşir; karşılığında basamak-2 teslimatına query-store üstünde **minimal history sorgu-API'si** dahildir. Geri dönüş = sınıfı yeniden açmak (konfig). REDDEDİLEN: big-bang (etki yüzeyi/geri dönüş büyük), kalıcı dual-run (yazım hacmi kalkmıyor — §6.7 hedefiyle çelişir).
4. **D-D (2026-07-16, D-A/D-C'nin sonucu — sorulmadan kapatıldı):** Kapsam = **tüm ACT_HI event sınıfları** (sınıf-bazlı makine hepsini taşır); cutover SIRASI hacim-öncelikli. İstisna yok — düşük-hacim/yüksek-audit sınıflar zaten D-A'da outbox yoluna ayrıldı.
5. **D-E (2026-07-16): Instance-anahtarlı sıra + merge-upsert.** Subject şeması `history.<engineId>.<class>.<processInstanceId>` (aynı instance aynı subject'te → stream sırası korunur); projeksiyon consumer'ı instance-anahtarıyla partition'lı (aynı instance hep aynı işleyicide); güvenlik ağı: **idempotent merge-upsert** (geç/eski event yeni state'i ezmez). Dedup: `Nats-Msg-Id = <historyEventId>:<eventType>`. DLQ: basamak-1 D-E kontratı AYNEN (`dlq.history.>`, header-korumalı, custody-transfer ack; ayrı-stream şartı [CQ-6] history stream'leri için de geçerli). REDDEDİLEN: sırasız+salt-upsert (çatışma-çözüm karmaşası), global tek-consumer (throughput tavanı).
6. **D-F (2026-07-16, basamak-1 D-F deseninin izdüşümü — sorulmadan kapatıldı):** Birincil metrik = **process-adımı başına normalize DB yazım-op'u** (baseline: mevcut AUDIT seviyesi dual-run öncesi; hedef: cutover'lanan sınıflarda history-yazım bileşeni **0**, outbox bileşeni yalnız audit-kritik sınıflarda ≤1 kompakt satır/tx). `nats-bpm-bench`'e history modu eklenir; sert kapı yalnız normalize metrik (PO-Q7 ilkesi), reconciliation-temizliği cutover kapısıdır (D-C). Destekleyici SLI: projeksiyon gecikmesi (event→query-store p95), reconciliation fark sayacı.

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

- **D-A — Yayın ve tutarlılık deseni:** ✅ **ÇÖZÜLDÜ (2026-07-15)** = hibrit sınıf-bazlı (bkz. §1 madde 1).
- **D-B — Query-store seçimi:** ✅ **ÇÖZÜLDÜ (2026-07-16)** = ayrı Postgres projeksiyonu (bkz. §1 madde 2).
- **D-C — Geçiş stratejisi + Cockpit kaderi:** ✅ **ÇÖZÜLDÜ (2026-07-16)** = kademeli sınıf-bazlı + sorgu-API (§1 madde 3).
- **D-D — Tablo/event kapsamı:** ✅ **KAPATILDI (2026-07-16, D-A/D-C sonucu)** = tüm sınıflar, hacim-öncelikli sıra (§1 madde 4).
- **D-E — Sıralama + dedup + DLQ:** ✅ **ÇÖZÜLDÜ (2026-07-16)** = instance-anahtarlı sıra + merge-upsert (§1 madde 5).
- **D-F — Başarı metriği:** ✅ **KAPATILDI (2026-07-16, basamak-1 deseni)** = normalize DB yazım-op/adım + bench history modu (§1 madde 5).
- **D-G — Flowable tarafı:** Flowable history mimarisi farklı (async history zaten var mı? — **kanıt keşfi sürüyor, 2026-07-16**); kapsam bu basamakta Camunda/CadenzaFlow mu, üç motor mu? — **keşif raporu sonrası sorulacak (son açık karar).**

## 7. Doğrulama notları

- Fork history altyapısı haritası → ✅ DOĞRULANDI (2026-07-15, Explore keşfi — §3'teki tüm file:line'lar).
- Flowable history mimarisi (async history job / handler SPI'ı) → doğrulanacak (D-G öncesi).
- Cockpit history UI'ının ACT_HI bağımlılık yüzeyi → doğrulanacak (D-C öncesi).
- `handleEvents(List)` batch yolunun gerçek kullanım sıklığı (composite tek tek `handleEvent`'e düşüyor — `CompositeHistoryEventHandler.java:100-105`) → phase3'te doğrulanacak.
