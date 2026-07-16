# USER STORIES — Basamak-2: History Offload (ACT_HI → NATS → async query-store)

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Sentinel fazı:** Phase 1 — Product Owner (basamak-2)
**Yerleşim:** `docs/sentinel/step2/phase1/` (PO-Q1 ONAYLANDI 2026-07-17)
**Kapsam:** `05-db-offload-strategy.md` §6.7 **basamak-2** (History offload) — `07-history-offload.md`'in somutlaştırdığı iş
**Girdi:** `docs/07-history-offload.md` (D-A…D-G KİLİTLİ, 2026-07-15/16)
**Tarih:** 2026-07-16 (açılış) / 2026-07-17 (PO kararları işlendi)
**Durum:** Onaylı (2026-07-17) — PO-Q1…7 cevaplandı (bkz. §4 PO Karar Kaydı)

> **Dokümantasyon dili Türkçe; kod/tanımlayıcılar İngilizce.** Motor/history-SPI davranışına dair her iddia `docs/07 §3/§7`'de **DOĞRULANMIŞ** file:line kanıtına dayanır. Doğrulanmamış varsayımlar açıkça "phase3'te doğrulanacak" etiketlidir. **Effort/story-point tahmini içermez** (workspace kuralı). **Kilitli kararlar (D-A…D-G) değiştirilmez; reddedilen/ertelenenler yeniden açılmaz.**

---

## 0. Kapsam sınırı (bu fazı okurken)

**KAPSAM İÇİ (basamak-2 kod + doküman teslimatı, `07 §6` durum notu):**
- Fork-ailesi (Camunda 7 + CadenzaFlow) custom composite `HistoryEventHandler` — sınıf-bazlı hibrit yol: audit-kritik → tx-içi **kompakt outbox** + relay/delete (at-least-once); bulk → **post-commit** publisher (at-most-once, sıfır DB yazımı) [D-A].
- Ayrı Postgres **projeksiyon servisi** (asyncapi-kontratlı consumer, instance-anahtarlı partition + merge-upsert, denormalize sorgu-şeması) [D-B/D-E].
- **Minimal history sorgu-API'si** + Cockpit-history körleşme telafisi/dokümantasyonu [D-C].
- Sınıf-başına **reconciliation** raporu + kademeli sınıf-bazlı **cutover** (hacim-öncelikli, reconciliation-kapılı) + geri-dönüş konfigürasyonu [D-C/D-D].
- **Metrik/bench history modu** (normalize DB yazım-op/adım + projeksiyon gecikmesi SLI + reconciliation fark sayacı) [D-F].
- History wire-contract: subject `history.<engineId>.<class>.<processInstanceId>`, dedup `Nats-Msg-Id=<eventId>:<type>`, DLQ **basamak-1 D-E kontratı AYNEN** (`dlq.history.>`, header-korumalı, custody-transfer, ayrı-stream [CQ-6]) [D-E].
- Basamak-1'den **devreden borçlar** (docs/07 §5) — özellikle **bench'in İLK GERÇEK KOŞUSU** basamak-2 baseline tavan-kanıtı olarak.

**KAPSAM DIŞI (bilinçli, yeniden açılmaz — bkz. `SRS.md §7`):**
- Handler-içi senkron NATS publish (D-A'da REDDEDİLDİ — tx coupling, `07 §4`).
- Tam-outbox (tüm sınıflar outbox) / tam-post-commit (tüm sınıflar post-commit) (D-A'da REDDEDİLDİ).
- JetStream-only query-store (D-B'de REDDEDİLDİ); ClickHouse-şimdi (D-B'de ERTELENDİ — hacim-tetikli ayrı karar).
- Big-bang cutover / kalıcı dual-run (D-C'de REDDEDİLDİ).
- Sırasız+salt-upsert / global tek-consumer (D-E'de REDDEDİLDİ).
- **Flowable history offload = basamak-2b** (D-G'de ERTELENDİ — ayrı kısa karar dokümanı); üç-motor-birlikte (REDDEDİLDİ).
- token-move/completion tx kaldırılması (basamak-6, P2); büyük değişken externalization (basamak-3); DB sharding (basamak-5).

---

## 1. Persona / roller

| Kod | Persona | Bağlam |
|---|---|---|
| **P1** | **Migrasyon / Platform Sahibi** | Camunda 8 lisansından kaçan; birincil DB'nin **en büyük yazım hacmini** (`ACT_HI_*`) DB-dışına taşımak isteyen (`07 §2`) |
| **P2** | **Platform / Ops (SRE)** | Reconciliation, projeksiyon gecikmesi, cutover, history-DLQ ve SLI'ları izleyen operatör |
| **P3** | **Denetim & Uyum Sorumlusu** | Audit-kritik history'nin (OP_LOG operatör kimlikleri, INCIDENT, EXT_TASK_LOG) **asla kaybolmamasını** garanti eden (D-A at-least-once yolu) |
| **P4** | **Bakımcı Mühendis (3eAI Labs)** | DB-offload tezini **ölçülebilir** kanıtla gösteren; bench history modu sahibi (`07 §5` borç #7) |
| **P5** | **Veri Koruma Sorumlusu (DPO)** | History = PII yüzeyi; projeksiyon DB retention/erasure, KVKK silme-hakkı ↔ denetim-izi gerilimini yöneten (bkz. `DATA_CLASSIFICATION.md`) |
| **P6** | **Raporlama / Sorgu Kullanıcısı** | Cutover sonrası Cockpit-history körleştiği için minimal history sorgu-API'sine geçen; geçmiş instance/task/variable sorgulayan |
| **P7** | **Süreç Geliştirici** | BPMN modelleyen; history'yi debug/audit için kullanan; Cockpit-history körleşmesinden etkilenen |

**Öncelik ölçeği (MoSCoW):** M = Must (basamak-2 kapanışı için zorunlu), S = Should, C = Could.
**PO-Q6 kararı (2026-07-17):** basamak-1 Q6 ilkesiyle aynı — **tüm S kalemleri (US-C2, D3, E2, F2, G3) basamak-2 kapanışına DAHİLDİR**; yalnız **C** (US-F3) backlog'a bırakılır. S yalnız göreli önem sırasını gösterir.

---

## 2. Epic haritası

| Epic | Başlık | US aralığı | İlgili karar |
|---|---|---|---|
| **EPIC-A** | Fork-ailesi history handler + hibrit tutarlılık yolu (outbox/post-commit) | US-A1 … US-A6 | D-A, D-D, D-E (publish), D-G |
| **EPIC-B** | Relay + Postgres projeksiyon servisi (+ history wire-contract & DLQ) | US-B1 … US-B5 | D-A (relay), D-B, D-E |
| **EPIC-C** | Minimal history sorgu-API'si + Cockpit-körleşme telafisi | US-C1 … US-C2 | D-C |
| **EPIC-D** | Reconciliation + kademeli sınıf-bazlı cutover | US-D1 … US-D3 | D-C, D-D |
| **EPIC-E** | Metrik & bench history modu | US-E1 … US-E3 | D-F |
| **EPIC-F** | Basamak-1'den devreden borçlar (bench İLK GERÇEK KOŞU baseline dahil) | US-F1 … US-F3 | `07 §5` |
| **EPIC-G** | Projeksiyon retention & KVKK erasure pipeline (PO-Q2/Q7 kararı) | US-G1 … US-G3 | D-B, PO-Q2/Q7 |

Toplam: **25 user story.**

---

## EPIC-A — Fork-ailesi history handler + hibrit tutarlılık yolu

### US-A1 — Custom composite `HistoryEventHandler` plug-in (fork motor değişmez, dual-run yetenekli)
**P1 Migrasyon/Platform Sahibi** olarak, custom bir history handler'ı **resmi genişletme noktalarıyla** takmak istiyorum; **böylece** `ACT_HI` yazımı DB-dışına yönlendirilirken fork motor kodu **değişmesin** ve dual-run (DB + custom yan yana) mümkün olsun.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Custom handler `ProcessEngineConfigurationImpl` resmi genişletme noktalarıyla takılır — **fork motor kodu değişmez** (`:757-769` alanlar, `:2788-2796` `initHistoryEventHandler()` yalnız null iken kurar, `:3876-3898` setter'lar).
- [ ] **Dual-run senaryosu (1):** `customHistoryEventHandlers` + default-DB açık → DB **ve** custom handler yan yana koşar (reconciliation için — US-D1).
- [ ] **Cutover senaryosu (2):** sınıf-başına `enableDefaultDbHistoryEventHandler=false` (veya eşdeğer) → o sınıf için yalnız custom yol (US-D2).
- [ ] Camunda 7 ve CadenzaFlow **tek adapter'ı** paylaşır (byte-ayna, basamak-1 deseni); **Flowable kapsam dışı** (D-G — basamak-2b).
- [ ] Handler'a yalnız **konfigüre `HistoryLevel`'in ürettiği** event'ler gelir (`HistoryLevel.isHistoryEventProduced`; default AUDIT); NONE → hiç event üretilmez, handler'a ulaşmaz.
**Dayanak:** `07 §3` (SPI `HistoryEventHandler.java:38-53`; genişletme noktaları `ProcessEngineConfigurationImpl.java:757-769,2788-2796,3876-3898`; `HistoryLevel.java:56-82`, `HistoryLevelNone.java:27-39`), D-G (fork ailesi tek SPI), D-A (dual-run).
**Bağımlılık:** yok (kanca noktası; tüm EPIC-A'nın temeli).

---

### US-A2 — Event-sınıfı sınıflandırması (audit-kritik ↔ bulk), konfigürable
**P3 Denetim & Uyum Sorumlusu** olarak, her `ACT_HI` event-sınıfının bir **tutarlılık katmanına** atanmasını (audit-kritik ↔ bulk) ve bunun **konfigürable** olmasını istiyorum; **böylece** audit-kritik sınıflar at-least-once yola, bulk sınıflar at-most-once yola yönlensin.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Audit-kritik **nihai default küme (PO-Q5 kararı 2026-07-17)** = **{OP_LOG, INCIDENT, EXT_TASK_LOG}** (düşük hacim, yüksek denetim değeri) — **konfigürable**; bulk küme = **{DETAIL, VARINST, ACTINST, PROCINST, TASKINST, IDENTITYLINK, COMMENT, ATTACHMENT, …}** (hacmin ~%90'ı).
- [ ] **PO-Q5 kararı:** IDENTITYLINK / COMMENT / ATTACHMENT — PII-yoğun olsalar da — **bulk yolda** kalır (at-most-once); PII koruması tutarlılık katmanından değil, projeksiyon retention/erasure'dan (EPIC-G) gelir. Konfigle audit-kritik'e taşınabilirler; seçim rehberi `TENANT_PII_CHECKLIST_TEMPLATE.md`'de.
- [ ] Sınıflandırma routing'i sürer: audit-kritik → outbox yolu (US-A3); bulk → post-commit yolu (US-A4).
- [ ] Konfig değişimi **fork rebuild gerektirmez** (runtime/deploy config).
**Dayanak:** `07 §1` madde 1 (D-A), `07 §3` (ACT_HI 16+ sınıf haritası); PO-Q5 kararı (2026-07-17).
**Bağımlılık:** US-A1.

---

### US-A3 — Audit-kritik yol: tx-içi kompakt outbox (at-least-once handoff)
**P3 Denetim & Uyum Sorumlusu** olarak, audit-kritik history event'lerinin **oluşturuldukları transaction içinde** kompakt bir outbox satırına yazılmasını istiyorum; **böylece** DB handler kapandığında bile (post-commit penceresinde çökme) **audit kaybı imkansız** olsun (at-least-once).
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Audit-kritik event → aynı tx'te **≤1 kompakt outbox satırı** (tam `ACT_HI` satırı değil; hedef D-F: audit-kritikte outbox bileşeni ≤1 kompakt satır/tx).
- [ ] Outbox satırı, relay'in (US-B1) event'i yeniden kurabilmesi/publish edebilmesi için gerekli referansı taşır (kesin şema → phase4 LLD).
- [ ] **Outbox-yok-olma problemi çözülür:** publish öncesi çökme → outbox satırı **hayatta kalır** → relay devralır → kalıcı audit kaybı YOK (`07 §4` kritik fark).
- [ ] Handler-içi **senkron publish YOK** (D-A'da REDDEDİLDİ — `07 §4`): outbox yazımı DB'dir, NATS publish relay'e (tx-dışı) bırakılır.
- [ ] at-least-once + downstream idempotent (dedup `Nats-Msg-Id`, merge-upsert US-B2) ile çift teslim yutulur.
**Dayanak:** `07 §1` madde 1 (D-A), `07 §4` ("outbox yok olma problemi"), `07 §1` madde 7 (D-F outbox ≤1 kompakt satır).
**Bağımlılık:** US-A2.

---

### US-A4 — Bulk yol: post-commit publisher (at-most-once, sıfır DB yazımı)
**P1 Migrasyon/Platform Sahibi** olarak, bulk history event'lerinin **post-commit `TransactionListener`** ile ve **sıfır DB yazımıyla** yayınlanmasını istiyorum; **böylece** history hacminin ~%90'ı (DETAIL/VARINST/ACTINST) birincil DB'den tamamen kalksın.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Bulk event → `TransactionState.COMMITTED` listener'ında yayınlanır; sınıf cutover olduktan sonra **`ACT_HI` INSERT'i yok** (US-D2).
- [ ] In-tx blocking YOK (handler-içi senkron publish REDDEDİLDİ — `07 §4`): publish exception'ı runtime transaction'ını **rollback edemez**.
- [ ] at-most-once **bilinçli kabul edilir**: post-commit çökme penceresi = o bulk event için kayıp — dual-run'da reconciliation (US-D1) boşlukları tespit eder; kalıcı kayıp bulk sınıfta kabul (D-A).
- [ ] Basamak-1 post-commit `TransactionListener` deseni yeniden kullanılır (`07 §4` reusable).
**Dayanak:** `07 §1` madde 1 (D-A), `07 §4` (post-commit at-most-once + reusable asset).
**Bağımlılık:** US-A2.

---

### US-A5 — Tüm-sınıf kapsamı + history-level farkındalığı (D-D)
**P1 Migrasyon/Platform Sahibi** olarak, **tüm** `ACT_HI` event-sınıflarının sınıf-bazlı makine tarafından taşınmasını istiyorum; **böylece** cutover'dan sonra hiçbir sınıf DB'ye yazmaya devam etmesin (history-yazım tamamen kalksın — hacim-öncelikli sıra).
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Kapsam = **tüm `ACT_HI` event sınıfları** (D-D — istisna yok; düşük-hacim/yüksek-audit sınıflar zaten D-A'da outbox yoluna ayrıldı).
- [ ] Handler yalnız konfigüre `HistoryLevel`'in ürettiği event'leri alır (ACTIVITY vs AUDIT vs FULL farklı event kümesi üretir — seviye-başına kapsam dokümante).
- [ ] Cutover **sıralaması** hacim-öncelikli: DETAIL→VARINST→ACTINST→… (US-D2 uygular).
- [ ] `handleEvents(List)` batch yolunun gerçek kullanım sıklığı (composite tek tek `handleEvent`'e düşüyor — `CompositeHistoryEventHandler.java:100-105`) **phase3'te doğrulanacak**.
**Dayanak:** `07 §1` madde 4 (D-D), `07 §3` (HistoryLevel + ACT_HI sınıf haritası), `07 §7` (batch yolu doğrulaması phase3).
**Bağımlılık:** US-A1, US-A2.

---

### US-A6 — Instance-anahtarlı subject şeması + publish dedup id (D-E, publish tarafı)
**P2 Platform/Ops** olarak, history event'lerinin **instance-anahtarlı** bir subject'e (`history.<engineId>.<class>.<processInstanceId>`) ve `Nats-Msg-Id=<eventId>:<type>` dedup id'siyle yayınlanmasını istiyorum; **böylece** aynı instance'ın event'leri stream sırasını korusun ve çift-teslim dedup'lansın.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Subject şeması: `history.<engineId>.<class>.<processInstanceId>` — aynı instance aynı subject'te → **stream sırası korunur** (D-E).
- [ ] Her publish `Nats-Msg-Id = <historyEventId>:<eventType>` taşır (dedup, D-E).
- [ ] Hem outbox-relay (US-B1) hem post-commit (US-A4) publish'i aynı subject/dedup şemasını kullanır.
- [ ] Kanonik tanım history wire-contract'ında (asyncapi, US-B4) yaşar (ADR-0006 deseni).
**Dayanak:** `07 §1` madde 5 (D-E subject + dedup).
**Bağımlılık:** US-A3, US-A4; kanonik tanım US-B4.

---

## EPIC-B — Relay + Postgres projeksiyon servisi

### US-B1 — Outbox relay (audit-kritik): outbox → NATS, PubAck-sonrası-delete, leader-elected
**P3 Denetim & Uyum Sorumlusu** olarak, kompakt outbox satırlarını NATS'a yayınlayıp **yalnız PubAck sonrası silen** leader-elected bir relay istiyorum; **böylece** audit-kritik yol at-least-once garantisini (custody-transfer) versin.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Relay **tek node/leader**'da koşar (basamak-1 KV-lease leader `SweepLeaderLease` yeniden kullanılır — `07 §4`).
- [ ] Relay outbox satırını okur → `history.<engine>.<class>.<instanceId>`'e `Nats-Msg-Id=<eventId>:<type>` ile publish eder → **yalnız PubAck sonrası** satırı siler (custody-transfer).
- [ ] Çökme-güvenli: yayınlanmamış satırlar hayatta kalır, relay devralır (at-least-once).
- [ ] Publish başarısız → retry/backoff; PubAck'ten **önce asla delete yok**.
- [ ] Relay okuması amortize edilir (hot-path'e yük bindirmez — SLI US-E2).
**Dayanak:** `07 §1` madde 1 (D-A at-least-once), `07 §4` (reusable `SweepLeaderLease`, custody-transfer ilkesi), `07 §1` madde 5 (D-E subject/dedup).
**Bağımlılık:** US-A3, US-A6.

---

### US-B2 — Postgres projeksiyon consumer: instance-anahtarlı partition + merge-upsert
**P1 Migrasyon/Platform Sahibi** olarak, history stream'ini tüketip **ayrı bir Postgres** projeksiyonuna yazan, **instance-anahtarıyla partition'lı** ve **idempotent merge-upsert** yapan bir consumer istiyorum; **böylece** aynı instance hep aynı işleyicide sırayla işlensin ve geç/eski event yeni state'i ezmesin.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Consumer history stream'ini tüketir; **instance-anahtarıyla partition'lı** (aynı `processInstanceId` → aynı işleyici) → per-instance sıra korunur (D-E).
- [ ] **Idempotent merge-upsert**: geç/eski event yeni state'i **ezmez** (güvenlik ağı — D-E).
- [ ] Dedup `Nats-Msg-Id=<eventId>:<type>` ile (D-E).
- [ ] Yazım hedefi **engine DB'sinden AYRI Postgres** (contention domain ayrılır — D-B).
- [ ] Consumer **asyncapi-kontratlıdır** (ADR-0006 deseni, US-B4).
- [ ] REDDEDİLEN: sırasız+salt-upsert, global tek-consumer (D-E) — yeniden açılmaz.
**Dayanak:** `07 §1` madde 2 (D-B ayrı Postgres), `07 §1` madde 5 (D-E partition + merge-upsert + dedup).
**Bağımlılık:** US-B1, US-A4 (publish tarafı), US-B4 (kontrat).

---

### US-B3 — Denormalize, sorgu-odaklı projeksiyon şeması (KVKK retention SQL ile)
**P6 Raporlama/Sorgu Kullanıcısı** olarak, projeksiyon şemasının **denormalize/sorgu-odaklı** olmasını istiyorum; **böylece** minimal sorgu-API'si (EPIC-C) verimli sunulsun ve KVKK retention/silme SQL'le uygulanabilsin.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Şema **denormalize/sorgu-odaklı** (ACT_HI normalize düzeninin aynası değil — D-B).
- [ ] Minimal sorgu-API'sinin (US-C1) erişim desenlerini destekler.
- [ ] **KVKK retention/silme SQL'le uygulanabilir** (D-B gerekçesi; ayrıntı `DATA_CLASSIFICATION.md` DP-9/DP-10).
- [ ] Şema evrimi ileride **ClickHouse'a izole** edilebilir (wire-contract sabit → yalnız consumer değişir); **ClickHouse-şimdi REDDEDİLDİ** (D-B, hacim-tetikli ayrı karar).
**Dayanak:** `07 §1` madde 2 (D-B denormalize + KVKK SQL + ClickHouse evrim izolasyonu).
**Bağımlılık:** US-B2.

---

### US-B4 — History wire-contract (asyncapi) — basamak-1 kontratını genişletir
**P2 Platform/Ops** olarak, history stream'inin **makine-okunur** bir asyncapi kontratıyla tanımlanmasını istiyorum; **böylece** subject/mesaj/header/dedup/DLQ şeması tek doğrulanabilir artefakt'ta (ADR-0006) yaşasın.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] `history.<engineId>.<class>.<processInstanceId>` subject, mesaj/header şemaları, dedup id (`<eventId>:<type>`), DLQ kontratı basamak-1 asyncapi'sine (ADR-0006) eklenir.
- [ ] **DLQ: basamak-1 D-E kontratı AYNEN** — `dlq.history.>`, header-korumalı byte-ayna, custody-transfer ack; **ayrı-stream şartı [CQ-6]** history stream'leri için de geçerli.
- [ ] Basamak-1 DLQ substratı (DlqPublisher + kontrat) yeniden kullanılır (`07 §4` reusable).
**Dayanak:** `07 §1` madde 5 (D-E — DLQ basamak-1 aynen + CQ-6), `07 §4` (reusable ADR-0006, DLQ substratı).
**Bağımlılık:** yok (substrat/kontrat; US-A6/US-B2/US-B5 buna bağlı).

---

### US-B5 — History DLQ runtime davranışı (custody-transfer, sessiz kayıp yok)
**P2 Platform/Ops** olarak, teslim edilemeyen history event'inin **sessizce kaybolmadan** `dlq.history.>`'a düşmesini ve orada ops'a görünür olmasını istiyorum; **böylece** projeksiyon consumer'ı zehirli mesajda takılmasın ve audit izi kaybolmasın.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Delivery bütçesi bitince mesaj `dlq.history.<...>`'a yönlenir; **header-korumalı byte-ayna** kopya (basamak-1 US-C1 deseni).
- [ ] Custody-transfer: consumer **DLQ-PubAck'ten önce ack-drop yapmaz** (basamak-1 US-C2 ilkesi).
- [ ] DLQ payload = history event payload → **PII yüzeyi** (`DATA_CLASSIFICATION.md` DP-13); DLQ retention/erişim kararı gerekir.
- [ ] Ops görünürlüğü: history-DLQ metrik/alert (US-E2).
**Dayanak:** `07 §1` madde 5 (D-E — DLQ basamak-1 aynen), `07 §4` (custody-transfer, DLQ substratı reusable).
**Bağımlılık:** US-B4.

---

## EPIC-C — Minimal history sorgu-API'si + Cockpit-körleşme telafisi

### US-C1 — Projeksiyon üstünde minimal history sorgu-API'si
**P6 Raporlama/Sorgu Kullanıcısı** olarak, cutover'lanan sınıflar için **projeksiyon store'u** üstünde minimal bir read-only history sorgu-API'si istiyorum; **böylece** Cockpit-history körleşmesine rağmen geçmiş instance/activity/variable sorgulanabilir kalsın.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] **PO-Q3 kararı (2026-07-17): kapsam = çekirdek-4 okuma deseni** — (1) `processInstanceId` → tam geçmiş, (2) `businessKey` → instance listesi, (3) zaman-aralığı + `processDefinition` → instance listesi, (4) instance → task/activity/variable geçmişi.
- [ ] Protokol **REST + JSON**, **sayfalamalı (paginated)**, **read-only**.
- [ ] Okuma **projeksiyon Postgres**'ten (D-B), engine DB'sinden **değil**.
- [ ] Agregasyon / analitik / raporlama görünümleri **KAPSAM DIŞI** (PO-Q3 — `SRS.md §7` reddedilen tablosu).
- [ ] Yanıtlarda **erişim kontrolü + PII maskeleme** (bkz. `DATA_CLASSIFICATION.md` DP-15); sorgu-API loglarına PII değeri yazılmaz (DP-1 devralınır).
- [ ] Bu API, basamak-2 teslimatına **dahildir** (D-C: Cockpit körleşmesinin karşılığı).
**Dayanak:** `07 §1` madde 3 (D-C — "minimal history sorgu-API'si dahildir"); PO-Q3 kararı (2026-07-17).
**Bağımlılık:** US-B2, US-B3.

---

### US-C2 — Cockpit-history körleşme dokümantasyonu + migrasyon rehberi
**P7 Süreç Geliştirici** olarak, hangi sınıf cutover olduğunda **hangi Cockpit-history görünümünün körleştiğinin** dokümante edilmesini istiyorum; **böylece** operatör hangi görünümün karardığını ve nereye (sorgu-API) bakacağını bilsin.
**Öncelik:** S
**Kabul kriterleri:**
- [ ] Cockpit history UI'ının **`ACT_HI` bağımlılık yüzeyi** dokümante edilir (`07 §7`: "doğrulanacak (D-C öncesi)" → **phase3'te doğrulanacak**; bu US o doğrulamanın teslimat yeridir).
- [ ] Sınıf-başına: o sınıf cutover olunca hangi Cockpit-history görünümü körleşir.
- [ ] **Runtime Cockpit (`ACT_RU_*`) etkilenmez** — yalnız history görünümleri körleşir.
- [ ] Geri-dönüş (sınıfı yeniden açma, US-D3) o sınıfın Cockpit-history'sini geri getirir.
**Dayanak:** `07 §1` madde 3 (D-C — cutover'lanan sınıflar için Cockpit körleşir), `07 §7` (Cockpit `ACT_HI` bağımlılığı phase3'te doğrulanacak).
**Bağımlılık:** US-C1, US-D2.

---

## EPIC-D — Reconciliation + kademeli sınıf-bazlı cutover

### US-D1 — Sınıf-başına reconciliation raporu (projeksiyon ↔ ACT_HI), dual-run boyunca
**P2 Platform/Ops** olarak, dual-run boyunca **sınıf-başına** projeksiyon ↔ `ACT_HI` karşılaştırma raporu istiyorum; **böylece** cutover öncesi drift/boşluk tespit edilsin.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Dual-run'da (default DB handler AÇIK + custom handler) sınıf-başına fark raporu: projeksiyon satırları ↔ `ACT_HI` satırları.
- [ ] Reconciliation **fark sayacı** SLI üretilir (D-F destekleyici SLI).
- [ ] Rapor hangi sınıfların **"N gün temiz"** olduğunu gösterir → cutover kapısı. **PO-Q4 kararı (2026-07-17): N = sınıf-başına konfigürable, default 7 gün** ("kalibre edilebilir başlangıç" — LLD-Q3 deseni; erken üretim gözlemiyle ayarlanır).
- [ ] Bulk at-most-once boşluklarını (US-A4) ve merge-upsert çatışmalarını (US-B2) tespit eder.
- [ ] **Reconciliation-temizliği cutover kapısıdır** (D-C); normalize DB-yazım metriği (US-E1) ise yazım-azaltma sert kapısıdır — **iki ayrı kapı** (D-F).
- [ ] Rapor **PII değeri sızdırmaz** (yalnız sayaç/id; `DATA_CLASSIFICATION.md` DP-14).
**Dayanak:** `07 §1` madde 3 (D-C reconciliation), `07 §1` madde 7 (D-F fark sayacı + iki-kapı ayrımı).
**Bağımlılık:** US-A1 (dual-run), US-B2.

---

### US-D2 — Kademeli sınıf-bazlı cutover (hacim-öncelikli, reconciliation-kapılı)
**P1 Migrasyon/Platform Sahibi** olarak, DB history yazımını **sınıf-başına** ve yalnız **N gün temiz** reconciliation'dan sonra, **hacim-öncelikli** sırayla kapatmak istiyorum; **böylece** offload kademeli ve düşük-riskli olsun.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Cutover = o sınıf için default DB handler'ını kapatma (**sınıf-başına konfig**).
- [ ] Kapı: sınıf reconciliation'da (US-D1) **N gün temiz** (N = sınıf-başına konfig, default **7 gün** — PO-Q4 2026-07-17).
- [ ] Sıra **hacim-öncelikli**: DETAIL→VARINST→ACTINST→… (D-D).
- [ ] **Big-bang REDDEDİLDİ**; **kalıcı dual-run REDDEDİLDİ** (yazım hacmi gerçekten kalkmalı — §6.7 hedefi) — yeniden açılmaz.
- [ ] Cutover sonrası o sınıfın `ACT_HI` yazım bileşeni = **0** (D-F hedefi, US-E1 ile ölçülür).
**Dayanak:** `07 §1` madde 3 (D-C kademeli), madde 4 (D-D hacim-öncelikli sıra), madde 7 (D-F hedef 0).
**Bağımlılık:** US-D1, US-A2.

---

### US-D3 — Cutover geri-dönüşü (sınıfı yeniden açma)
**P2 Platform/Ops** olarak, cutover'lanmış bir sınıfı **konfigle yeniden açabilmek** (DB handler'ı geri-etkinleştirme) istiyorum; **böylece** hatalı bir cutover anında tersine çevrilebilsin.
**Öncelik:** S
**Kabul kriterleri:**
- [ ] Geri-dönüş = o sınıf için default DB handler'ını yeniden etkinleştirme (**yalnız konfig, kod değişikliği yok** — D-C).
- [ ] O sınıfın Cockpit-history'sini geri getirir (US-C2).
- [ ] Geri-dönüş yolu runbook'ta dokümante.
- [ ] Geri-dönüşte dual-run yeniden başlar; yeniden-cutover öncesi reconciliation yeniden doğrular (US-D1).
**Dayanak:** `07 §1` madde 3 (D-C — "Geri dönüş = sınıfı yeniden açmak (konfig)").
**Bağımlılık:** US-D2.

---

## EPIC-E — Metrik & bench history modu

### US-E1 — Normalize birincil metrik: adım başına DB yazım-op (history modu)
**P4 Bakımcı Mühendis** olarak, basamak-2'nin kazancını **donanım-bağımsız** bir metrikle (process-adımı başına normalize DB yazım-op'u) ölçmek istiyorum; **böylece** "cutover'lanan sınıflarda history-yazım bileşeni = 0, outbox bileşeni yalnız audit-kritikte ≤1 kompakt satır/tx" iddiası doğrudan kanıtlansın.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Metrik bileşenleri: adım başına `ACT_HI` yazım-op (cutover'lanan sınıflarda hedef **0**), outbox yazım-op (audit-kritikte **≤1 kompakt satır/tx**), baseline = **dual-run öncesi mevcut AUDIT seviyesi**.
- [ ] Ölçüm `pg_stat_statements` fingerprint (veya datasource-proxy); history-write SQL'inin ayrı fingerprint verdiği **phase3'te doğrulanacak** (basamak-1 D-F metodolojisi yeniden kullanılır).
- [ ] **Bu normalize metrik yazım-azaltmanın TEK sert kapısıdır** (D-F/PO-Q7 ilkesi); reconciliation-temizliği (US-D1) cutover kapısıdır — **iki ayrı kapı**.
**Dayanak:** `07 §1` madde 7 (D-F normalize metrik + sert kapı).
**Bağımlılık:** US-A3, US-A4, US-D2.

---

### US-E2 — Destekleyici SLI'lar: projeksiyon gecikmesi + reconciliation farkı
**P2 Platform/Ops** olarak, projeksiyon gecikmesi (event→query-store p95) ve reconciliation fark sayacı SLI'larını istiyorum; **böylece** dual-run/cutover sağlığı sürekli izlenebilsin.
**Öncelik:** S
**Kabul kriterleri:**
- [ ] Projeksiyon gecikmesi **p95** (event publish → query-store'da görünür) — **SLI, sert kapı DEĞİL**.
- [ ] Reconciliation fark sayacı (US-D1).
- [ ] History stream'leri için DLQ/nak/ack sayaçları (basamak-1 `NatsChannelMetrics` tabanı yeniden kullanılır).
- [ ] Mevcut Micrometer tabanı üstüne kurulur (basamak-1 deseni).
**Dayanak:** `07 §1` madde 7 (D-F destekleyici SLI: projeksiyon gecikmesi + fark sayacı), `07 §4` (reusable `NatsChannelMetrics`).
**Bağımlılık:** US-E1, US-D1.

---

### US-E3 — `nats-bpm-bench` history modu
**P4 Bakımcı Mühendis** olarak, aynı senaryoyu **iki modda** (DB-history baseline ↔ offload-edilmiş history) koşan bir bench modu istiyorum; **böylece** baseline+hedef kontrollü üretilsin.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Bench history modu: DB-history baseline (default AUDIT) ↔ offload-edilmiş history (cutover'lanmış).
- [ ] US-E1 normalize metriğini iki mod için üretir; kabul = US-E1 hedefleri.
- [ ] Basamak-1 bench altyapısı üstüne kurulur (D-F modülü genişletilir — `07 §4` reusable `nats-bpm-bench`).
- [ ] `@Tag("bench")`, CI'da nightly/manuel; basamak-2 teslimatına **dahildir**.
**Dayanak:** `07 §1` madde 7 (D-F bench history modu), `07 §4` (reusable bench).
**Bağımlılık:** US-E1, US-F1 (basamak-1 bench ilk koşusu baseline'ı).

---

## EPIC-F — Basamak-1'den devreden borçlar (`07 §5`)

### US-F1 — Bench İLK GERÇEK KOŞU: basamak-1 sert-kapı sayıları = basamak-2 hedef-tavan baseline'ı
**P4 Bakımcı Mühendis** olarak, basamak-1 bench'inin **ilk gerçek koşusunun** (poll=0 kanıtı + DB-op taban çizgisi) yürütülmesini istiyorum; **böylece** basamak-2'nin somut hedef-tavan baseline'ı olsun (`07 §5` borç #7).
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Basamak-1 bench ilk gerçek koşusu üretir: `fetchAndLock`=0 kanıtı + DB yazım-op taban çizgisi sayıları.
- [ ] Bu sayılar basamak-2 history-modu baseline referansı olur (US-E1/US-E3'e besleme).
- [ ] Basamak-1 **D-F kapanış kriteri** kapanır (`07 §5` borç #7).
**Dayanak:** `07 §5` borç #7 (bench İLK GERÇEK KOŞU = basamak-2 hedef tavanı kanıtı).
**Bağımlılık:** yok (US-E3'ü besler).

---

### US-F2 — Bench + prod stream provisioning'in history stream'lerine genişletilmesi (borç #2)
**P2 Platform/Ops (DevOps)** olarak, `BenchEnvironment.ensureStreams()` + prod stream provisioning'in history stream'lerini de kapsamasını istiyorum; **böylece** bench ve prod, history + DLQ stream'lerini doğru sağlasın.
**Öncelik:** S
**Kabul kriterleri:**
- [ ] `ensureStreams()` history stream'i + `dlq.history.>`'i sağlar (ayrı-stream [CQ-6]).
- [ ] Prod stream provisioning DevOps takibi (basamak-1 F-2 kapanış notunun history'ye genişlemesi).
- [ ] Basamak-1 ayrı-stream deployment dersi (CQ-6) history stream'lerine uygulanır.
**Dayanak:** `07 §5` borç #2 (F-2 kalanı: ensureStreams + prod provisioning), `07 §1` madde 5 (CQ-6).
**Bağımlılık:** US-B4.

---

### US-F3 — Devreden borç backlog triyajı (borçlar #1, #3, #4, #5, #6)
**P4 Bakımcı Mühendis** olarak, basamak-1'in kalan devreden borçlarının basamak-2 kapsamında **triyaj edilmesini** (hangisi basamak-2-ilgili, hangisi basamak-1 kuyruğu) istiyorum; **böylece** hiçbir borç sessizce düşmesin.
**Öncelik:** C
**Kabul kriterleri:**
- [ ] Borç #1 — `allow-unsafe-lock-duration` runbook/on-call devri (RELEASE-DECISIONS Q3): basamak-2 ilgisi triyaj edilir (muhtemelen basamak-1 ops kuyruğu — history L-parametresi yok).
- [ ] Borç #3 — scheduler shutdown await (`A2SubscriptionRegistrar`, MINOR): history relay/consumer scheduler'ına **aynı desen** uygulanmalı mı? triyaj.
- [ ] Borç #4 — `FailureEventBridge` önceden-var-olan `NonMatchingEventConsumer`'ı eziyor (MINOR): basamak-1 kuyruğu; history kapsamına girmiyor → not düş.
- [ ] Borç #5 — Flowable boundary-timer `ACT_RU_TIMER_JOB` maliyet ölçümü (TEST_SPEC c): Flowable basamak-2b'ye ertelendi (D-G) → basamak-2b ile bağla.
- [ ] Borç #6 — Sweep re-publish captured-variables taşımaz: basamak-1 external-task kuyruğu; history publish'inin **captured-payload tamlığı** için aynı sınıf-hatanın history'de tekrarlanmadığı doğrulanır → not düş.
- [ ] Her borç için: **basamak-2-ilgili / basamak-1-kuyruğu / basamak-2b** etiketi + kısa gerekçe.
**Dayanak:** `07 §5` borç tablosu (#1–#6).
**Bağımlılık:** yok.

---

## EPIC-G — Projeksiyon retention & KVKK erasure pipeline (PO-Q2/Q7 kararı)

> **PO-Q7 kararı (2026-07-17):** retention default'u, erasure pipeline'ı ve pseudonymization mekanizması **üçü de basamak-2 teslimatıdır.** History = kalıcı PII yüzeyi olduğundan (projeksiyon store, `DATA_CLASSIFICATION.md`), veri-yaşam döngüsü artık bir **kod teslimatıdır** (basamak-1'de yalnız retention konfigü vardı; basamak-2 aktif silme/anonimleştirme getirir). PO-Q2 katmanlı politikasını uygular.

### US-G1 — Sınıf-bazlı projeksiyon retention enforcement
**P5 Veri Koruma Sorumlusu (DPO)** olarak, projeksiyon store'daki history'nin **sınıf-bazlı** ve otomatik bir retention işiyle temizlenmesini istiyorum; **böylece** PII gereksiz süre saklanmasın ve retention KVKK/GDPR minimizasyonuna uysun.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Otomatik retention job (scheduled) sınıf-başına retention penceresi uygular (`DATA_GOVERNANCE §3.3` deseni).
- [ ] **PO-Q7 default'ları:** bulk sınıflar **90 gün** (kiracı override); audit-kritik sınıflar **yasal-saklama** süresi (örn. denetim yükümlülüğü, kiracı override).
- [ ] Retention silmesi audit-log kaydı üretir (`DATA_GOVERNANCE §3.3` — "audit log entry for every deletion").
- [ ] Retention penceresi **kiracı-konfigürable**; policy `TENANT_PII_CHECKLIST_TEMPLATE.md` history-katmanında kaydedilir.
**Dayanak:** PO-Q7 kararı (2026-07-17); D-B (KVKK retention SQL'le); `DATA_CLASSIFICATION.md` DP-9, §5; `DATA_GOVERNANCE §3.1/§3.3`.
**Bağımlılık:** US-B3 (denormalize şema, SQL retention).

---

### US-G2 — Bulk sınıf PII erasure pipeline (silme-hakkı)
**P5 Veri Koruma Sorumlusu (DPO)** olarak, bulk sınıflardaki PII için projeksiyon-DB üstünde bir **erasure/anonimleştirme pipeline'ı** istiyorum; **böylece** KVKK/GDPR silme-hakkı talepleri (data subject) yerine getirilebilsin.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Data-subject anahtarına (örn. businessKey / userId) göre bulk sınıf PII'ları (VARINST/DETAIL değerleri, TASKINST name/description, serbest metinler) **silinir/anonimleştirilir** (soft-delete → anonymize, `DATA_GOVERNANCE §4.1` Right to Erasure).
- [ ] Pipeline SQL-uygulanabilir (D-B — denormalize şema); erasure işlemi audit-log'lanır (kim-neyi-ne-zaman sildi).
- [ ] **PO-Q2 katmanı (2):** bulk PII erasure'a **tabidir** (audit-kritik'ten farklı — US-G3).
- [ ] Erasure tamlığı doğrulanabilir (erasure sonrası sorgu-API o PII'yi döndürmez).
**Dayanak:** PO-Q2 kararı katman-2 (2026-07-17); D-B; `DATA_CLASSIFICATION.md` DP-10; `DATA_GOVERNANCE §4.1`.
**Bağımlılık:** US-B3, US-G1.

---

### US-G3 — Audit-kritik pseudonymization (kimlik↔takma-ad kasası)
**P3 Denetim & Uyum Sorumlusu** olarak, audit-kritik kayıtlarda (OP_LOG operatör kimlikleri, INCIDENT) **pseudonymization seçeneği** istiyorum; **böylece** denetim izinin **yapısı korunurken** silme-hakkı, kimlik↔takma-ad haritasının o kaydını silerek gerçekleştirilebilsin.
**Öncelik:** S
**Kabul kriterleri:**
- [ ] **PO-Q2 katmanı (3):** audit-kritik kayıtta PII alanı (userId) **tersinmez takma-ada** çevrilir; kimlik↔takma-ad haritası **ayrı bir kasada** tutulur (`DATA_CLASSIFICATION.md` DP-16).
- [ ] **Silme = harita kaydını silmek** → takma-ad tersinmez olur (denetim izinin yapısı korunur, re-identification imkânsız).
- [ ] Audit-kritik sınıflar **yasal-saklama istisnası (PO-Q2 katman-1)** altında erasure'dan muaf tutulabilir; pseudonymization bunun **opt-in tamamlayıcısıdır** (kiracı seçer).
- [ ] Kasaya erişim en yüksek koruma (L4-bitişik: re-identification anahtarı); erişim audit-log'lanır.
**Dayanak:** PO-Q2 kararı katman-3 (2026-07-17); `DATA_CLASSIFICATION.md` §6, DP-11, DP-16.
**Bağımlılık:** US-G1; ilgili audit-kritik yol US-A3.

---

## 3. İzlenebilirlik özeti (US → kilitli karar / kanıt)

| US | İlgili kilitli karar | Birincil kanıt (`07 §3/§7` DOĞRULANMIŞ) |
|---|---|---|
| US-A1 | D-G, D-A | `HistoryEventHandler.java:38-53`; `ProcessEngineConfigurationImpl.java:757-769,2788-2796,3876-3898`; `HistoryLevel.java:56-82` |
| US-A2 | D-A | `07 §1` madde 1; `07 §3` ACT_HI sınıf haritası |
| US-A3 | D-A | `07 §4` (outbox yok-olma); `07 §1` madde 7 (≤1 kompakt satır) |
| US-A4 | D-A | `07 §4` (post-commit reusable); `HistoryEventProcessor.java:73-85` (tx-içi senkron → in-handler REDDEDİLDİ) |
| US-A5 | D-D | `07 §1` madde 4; `CompositeHistoryEventHandler.java:100-105` (batch phase3) |
| US-A6 | D-E | `07 §1` madde 5 (subject + dedup) |
| US-B1 | D-A, D-E | `07 §4` (`SweepLeaderLease`, custody-transfer) |
| US-B2 | D-B, D-E | `07 §1` madde 2 (ayrı Postgres); madde 5 (partition + merge-upsert) |
| US-B3 | D-B | `07 §1` madde 2 (denormalize + KVKK SQL + ClickHouse izolasyon) |
| US-B4 | D-E | `07 §1` madde 5 (DLQ basamak-1 aynen + CQ-6); ADR-0006 |
| US-B5 | D-E | `07 §1` madde 5; basamak-1 US-C1/US-C2 (custody-transfer) |
| US-C1 | D-C | `07 §1` madde 3 (minimal sorgu-API dahil) |
| US-C2 | D-C | `07 §1` madde 3; `07 §7` (Cockpit `ACT_HI` bağımlılığı phase3) |
| US-D1 | D-C, D-F | `07 §1` madde 3 (reconciliation); madde 7 (fark sayacı) |
| US-D2 | D-C, D-D | `07 §1` madde 3/4 (kademeli + hacim-öncelikli) |
| US-D3 | D-C | `07 §1` madde 3 (geri-dönüş = konfig) |
| US-E1 | D-F | `07 §1` madde 7 (normalize metrik, sert kapı) |
| US-E2 | D-F | `07 §1` madde 7 (projeksiyon gecikmesi + fark sayacı SLI) |
| US-E3 | D-F | `07 §1` madde 7 (bench history modu); `07 §4` reusable bench |
| US-F1 | — (`07 §5` #7) | `07 §5` borç #7 (bench ilk koşu = hedef tavan) |
| US-F2 | — (`07 §5` #2) | `07 §5` borç #2; CQ-6 |
| US-F3 | — (`07 §5` #1,3,4,5,6) | `07 §5` borç tablosu |
| US-G1 | D-B, PO-Q7 | PO-Q7 kararı; `DATA_GOVERNANCE §3.1/§3.3`; DP-9 |
| US-G2 | PO-Q2 (katman-2) | PO-Q2 kararı; `DATA_GOVERNANCE §4.1`; DP-10 |
| US-G3 | PO-Q2 (katman-3) | PO-Q2 kararı; `DATA_CLASSIFICATION.md §6`; DP-11/DP-16 |

---

## 4. PO Karar Kaydı (Q→A, 2026-07-17)

> Basamak-2 phase1 kapanışında Levent'e sunulan 7 PO-QUESTION ve verilen kararlar (**sorular korunur, cevaplar eklenir**). Hiçbir karar kilitli D-A…D-G'yi değiştirmez; yalnız docs/07'de bilinçle **açık/konfigürable bırakılmış parametreleri** sabitler.

| # | Soru (PO-QUESTION) | Verilen karar (2026-07-17) | Bu fazda uygulanışı |
|---|---|---|---|
| **PO-Q1** | Yerleşim: basamak-2 teslimatları `docs/sentinel/step2/phase1/` altında mı kalsın? | **ONAYLANDI** — `docs/sentinel/step2/phase1/` kalır. | Konum korundu; `GUIDELINES_MANIFEST.yaml` layout_deviation "onaylandı 2026-07-17" notu. |
| **PO-Q2** | KVKK silme-hakkı ↔ denetim-izi saklama gerilimi nasıl uzlaştırılsın? | **KATMANLI politika ONAYLANDI:** (1) audit-kritik sınıflar **yasal-saklama istisnası** (hukuki dayanak DPO doğrulamasına işaretli); (2) bulk sınıf PII → projeksiyon-DB **erasure pipeline**; (3) audit-kritik kayıtlarda **pseudonymization seçeneği** (kimlik↔takma-ad haritası ayrı kasada; silme = harita kaydını silmek). | `DATA_CLASSIFICATION.md §6` **karar** olarak yazıldı; DP-10 netleşti, DP-16 eklendi; **EPIC-G** (US-G1/G2/G3) eklendi. |
| **PO-Q3** | Sorgu-API kapsam sınırı? | **çekirdek-4 okuma deseni ONAYLANDI:** (1) processInstanceId→tam geçmiş, (2) businessKey→instance listesi, (3) zaman-aralığı+definition→liste, (4) instance→task/activity/variable geçmişi; **REST+JSON, sayfalamalı, read-only**. Agregasyon/analitik **KAPSAM DIŞI**. | US-C1 çekirdek-4 ile netleşti; `SRS.md §7` reddedilen tablosuna "sorgu-API agregasyon/analitik" eklendi; FR-C1/IR-6 güncellendi. |
| **PO-Q4** | Reconciliation "N gün temiz" değeri? | **sınıf-başına konfig, default 7 gün** ("kalibre edilebilir başlangıç" — LLD-Q3 deseni). | US-D1/US-D2 + FR-D1 default 7g olarak sabitlendi. |
| **PO-Q5** | Audit-kritik sınıf listesi kesinleştirme? | **default = {OP_LOG, INCIDENT, EXT_TASK_LOG} + konfig ONAYLANDI**; IDENTITYLINK/COMMENT/ATTACHMENT **bulk yolda**. | US-A2/FR-A3 nihaileşti; `DATA_CLASSIFICATION.md §2.1` tutarlılık sütunu güncellendi; `TENANT_PII_CHECKLIST_TEMPLATE.md`'e sınıf-seçim rehberi maddesi eklendi. |
| **PO-Q6** | Should-kapsam: tüm S kalemleri kapanışa dahil mi? | **Tüm S DAHİL** (US-C2, D3, E2, F2, G3); **US-F3 (C) backlog**. | §1 MoSCoW notu karara çevrildi. |
| **PO-Q7** | Projeksiyon retention default + erasure pipeline + TENANT template materyalizasyonu? | **Üçü de basamak-2 teslimatı:** retention **sınıf-bazlı** (bulk 90g / audit-kritik yasal-saklama, kiracı override); **erasure pipeline kod teslimatı**; **TENANT_PII_CHECKLIST history-genişletmesi ayrı dosya**. | **EPIC-G** (US-G1 retention, US-G2 erasure, US-G3 pseudonymization); yeni dosya `TENANT_PII_CHECKLIST_TEMPLATE.md`; `DATA_CLASSIFICATION.md §5/§8` güncellendi. |

---

*Fonksiyonel/non-fonksiyonel gereksinim türetimi: `SRS.md`. PII ve veri hassasiyeti eşlemesi + KVKK gerilimi: `DATA_CLASSIFICATION.md`. Manifest: `GUIDELINES_MANIFEST.yaml`.*
