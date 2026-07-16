# SRS — Software Requirements Specification
## Basamak-2: History Offload (ACT_HI → NATS → async query-store)

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Sentinel fazı:** Phase 1 — Product Owner (basamak-2)
**Yerleşim:** `docs/sentinel/step2/phase1/` (PO-Q1)
**Kapsam:** `05-db-offload-strategy.md` §6.7 **basamak-2** (`07-history-offload.md`'in gereksinimleştirilmesi)
**Girdi:** `docs/07-history-offload.md` (D-A…D-G KİLİTLİ, 2026-07-15/16)
**Tarih:** 2026-07-16 (açılış) / 2026-07-17 (PO kararları işlendi)
**Durum:** Onaylı (2026-07-17) — PO-Q1…7 cevaplandı (bkz. §8 PO Karar Kaydı); phase1-review KOŞULLU ONAY bulguları F-001…F-005 kapatıldı (2026-07-17: anchor'lar v4.0'a pinlendi, NFR-R8 eklendi)
**Sürüm:** 1.0 (basamak-2)

> Bu SRS, LLD değildir; **ne** yapılacağını (gereksinim) sabitler, **nasıl** yapılacağını (tasarım) `07`'ye + phase3/phase4'e bırakır. History-SPI/motor davranış iddiaları `07 §3/§7`'de **DOĞRULANMIŞ** `file:line` kanıtına dayanır. Doğrulanmamış varsayımlar "phase3'te doğrulanacak" etiketlidir. **Effort tahmini içermez.** Kilitli D-A…D-G kararları değiştirilmez.

---

## 1. Giriş

### 1.1 Amaç
Bu belge, basamak-2'nin — `ACT_HI_*` history yazımının engine DB'sinin ACID transaction'ından NATS'a ve oradan **ayrı bir async query-store'a** taşınmasının — yazılım gereksinimlerini tanımlar. `ACT_HI_*`, engine DB'sinin **en büyük yazım hacmidir** (`05 §6.7` satır 2; `07 §2`): her process-adımı/task/variable güncellemesi/job-log satırı, runtime state ile **aynı transaction'da** yazılır. Basamak-2 bu yazımı **SPI katmanında** kaldırır; **fork core'a dokunmaz**. Kapsam = **Camunda 7 + CadenzaFlow** (Flowable = basamak-2b, D-G).

### 1.2 Kapsam
**Kapsam içi:** custom composite `HistoryEventHandler` (sınıf-bazlı hibrit yol: audit-kritik → tx-içi kompakt outbox + relay/delete at-least-once; bulk → post-commit at-most-once) [D-A]; ayrı Postgres projeksiyon servisi (asyncapi-kontratlı, instance-anahtarlı partition + merge-upsert, denormalize sorgu-şeması) [D-B/D-E]; minimal history sorgu-API'si (çekirdek-4 okuma deseni, REST+JSON, PO-Q3) + Cockpit-körleşme telafisi [D-C]; sınıf-başına reconciliation (N=7g default, PO-Q4) + kademeli sınıf-bazlı cutover (hacim-öncelikli, reconciliation-kapılı) + geri-dönüş [D-C/D-D]; history wire-contract (subject `history.<engineId>.<class>.<processInstanceId>`, dedup `Nats-Msg-Id=<eventId>:<type>`, DLQ basamak-1 D-E kontratı aynen + CQ-6) [D-E]; metrik/bench history modu [D-F]; **projeksiyon retention (sınıf-bazlı) + KVKK erasure pipeline + pseudonymization** (PO-Q2/Q7 katmanlı politikası); basamak-1 devreden borçlar (`07 §5`, özellikle bench İLK GERÇEK KOŞU baseline).

**Kapsam dışı (bkz. §7):** handler-içi senkron publish, tam-outbox, tam-post-commit (D-A REDDEDİLDİ); JetStream-only store, ClickHouse-şimdi (D-B); big-bang cutover, kalıcı dual-run (D-C); sırasız+salt-upsert, global tek-consumer (D-E); Flowable history/basamak-2b, üç-motor-birlikte (D-G); token-move tx kaldırılması (basamak-6), büyük değişken externalization (basamak-3), DB sharding (basamak-5).

### 1.3 Tanımlar & kısaltmalar
| Terim | Açıklama |
|---|---|
| **ACT_HI_\*** | Camunda/CadenzaFlow history tabloları (PROCINST, ACTINST, VARINST, DETAIL, TASKINST, OP_LOG, INCIDENT, EXT_TASK_LOG, …) |
| **Audit-kritik sınıf** | Kaybı kabul edilemez event sınıfı (default {OP_LOG, INCIDENT, EXT_TASK_LOG}); at-least-once yol |
| **Bulk sınıf** | Yüksek-hacim event sınıfı (DETAIL/VARINST/ACTINST — hacmin ~%90'ı); at-most-once yol |
| **Kompakt outbox** | Audit-kritik yolda tx-içi ≤1 satırlık dayanıklı ara-kayıt; relay onu NATS'a taşıyıp siler |
| **Post-commit publish** | `TransactionState.COMMITTED` listener'ında, tx-dışı, sıfır-DB-yazımı yayın |
| **Projeksiyon store** | Engine DB'sinden AYRI, denormalize/sorgu-odaklı Postgres (query-store) |
| **Merge-upsert** | Idempotent birleştirmeli upsert; geç/eski event yeni state'i ezmez |
| **Reconciliation** | Dual-run'da projeksiyon ↔ ACT_HI sınıf-başına fark raporu (cutover kapısı) |
| **Cutover** | Bir sınıfın DB history yazımının (default DB handler) konfigle kapatılması |
| **CQ-6** | Basamak-1 ayrı-DLQ-stream deployment şartı (history stream'lerine de uygulanır) |

### 1.4 Referanslar
- `docs/01-vision-roadmap.md` (VISION — 7/7 doğrulandı, basamak-2'de değişmedi)
- `docs/05-db-offload-strategy.md` §6.7 (merdiven; basamak-2 satırı, D2 interim posture)
- `docs/07-history-offload.md` (**birincil tasarım girdisi**; D-A…D-G çözülü)
- `docs/sentinel/phase1/*` (basamak-1 teslimatları — biçim/olgunluk + DP-1…8 devri)
- `docs/sentinel/phase3/ADR/0006-*` (asyncapi contract-first — history kontratının tabanı)
- `USER_STORIES.md`, `DATA_CLASSIFICATION.md` (bu faz kardeş teslimatları)

---

## 2. Genel açıklama

### 2.1 Ürün perspektifi
`nats-bpm-channels`, üç açık kaynak BPM motoru için ortak NATS.io/JetStream katmanıdır (`01 §5.1`). Basamak-2, history yazım yolunu **motor-içi in-tx DB handler**'dan **motor-dışı push + async projeksiyon**'a taşır. Fork history alanı upstream Camunda 7 ile **birebir** (tek commit: paket rename `org.cadenzaflow.*`) → Camunda history desenleri doğrudan geçerli (`07 §3`).

### 2.2 Ana fonksiyonlar (özet)
1. Custom composite `HistoryEventHandler`'ı resmi genişletme noktalarıyla tak (fork değişmez); dual-run yetenekli.
2. Event-sınıfını audit-kritik/bulk olarak sınıflandır (konfigürable).
3. Audit-kritik → tx-içi kompakt outbox + leader-elected relay (at-least-once).
4. Bulk → post-commit publisher (at-most-once, sıfır DB yazımı).
5. History'yi instance-anahtarlı subject'e dedup id'siyle yayınla; ayrı Postgres'e instance-partition'lı merge-upsert consumer'la yaz.
6. Minimal history sorgu-API'si sun; Cockpit-history körleşmesini dokümante et.
7. Sınıf-başına reconciliation + kademeli hacim-öncelikli cutover + geri-dönüş.
8. Normalize DB-yazım-op/adım metriği + bench history modu ile kanıtla.
9. Basamak-1 devreden borçları (bench ilk koşu baseline dahil) kapat/triyaj et.

### 2.3 Kullanıcı sınıfları
`USER_STORIES.md §1`: P1 Migrasyon/Platform Sahibi, P2 Platform/Ops, P3 Denetim & Uyum, P4 Bakımcı Mühendis, P5 DPO, P6 Raporlama/Sorgu, P7 Süreç Geliştirici.

### 2.4 İşletim ortamı & teknoloji kısıtları
- Java 21+ (build Java 21 gerektirir; sistem default Java 25 Mockito'yu kırar — memory `build-requires-java21`).
- Spring Boot 3.3+, jnats 2.20+, NATS/JetStream 2.10+ (`01 §6`).
- Engine sürümleri: Camunda 7.24+, CadenzaFlow 1.2+ (Flowable **kapsam dışı** — basamak-2b, D-G).
- CadenzaFlow **fork'tur**; history offload **motor kodunu değiştirmez** — yalnız resmi genişletme noktaları (`ProcessEngineConfigurationImpl` history handler alanları/setter'ları — `07 §3`).
- Birincil DB PostgreSQL (metrik `pg_stat_statements`); projeksiyon store **ayrı Postgres** (D-B).

### 2.5 Varsayımlar & bağımlılıklar
- **Doğrulanmış (`07 §3`, 2026-07-15):** fork history alanı = upstream Camunda 7 birebir; SPI `HistoryEventHandler.java:38-53` (Javadoc :26-29 async/MQ'ya açık, fork'ta impl YOK); default zincir `DbHistoryEventHandler.java:40`/`CompositeHistoryEventHandler.java:33,38`/`CompositeDbHistoryEventHandler.java:70-72`; **çağrı tx-içi + senkron** `HistoryEventProcessor.java:73-85` → `DbHistoryEventHandler.java:172-174` (command-context DbEntityManager) → `CommandContext.java:186-197` (flushSessions→commit); resmi genişletme noktaları `ProcessEngineConfigurationImpl.java:757-769,2788-2796,3876-3898` (fork değişikliği GEREKMEZ); `HistoryLevel.java:56-82`; ACT_HI 16+ sınıf haritası + `ByteArrayEntity(...,HISTORY)` `DbHistoryEventHandler.java:97-105`; yazım yolunda spool/async YOK.
- **Doğrulanmış (`07 §7`, 2026-07-16):** Flowable 7.1'de hazır async-history producer YOK → D-G kararının kanıt tabanı (Flowable = basamak-2b).
- **Phase3'te doğrulanacak:** Cockpit history UI'ının `ACT_HI` bağımlılık yüzeyi (D-C öncesi); `handleEvents(List)` batch yolunun gerçek sıklığı (`CompositeHistoryEventHandler.java:100-105`); `pg_stat_statements` history-write fingerprint izolasyonu; projeksiyon merge-upsert çatışma-çözüm kenar durumları; kompakt outbox satırının tek INSERT'e bindiği.

---

## 3. Fonksiyonel gereksinimler (FR)

> Notasyon: **FR-x** [öncelik M/S/C] — gereksinim → *kaynak US* → *kanıt/karar*.

### 3.1 Fork-ailesi history handler + hibrit yol (EPIC-A)

**FR-A1** [M] — Sistem, custom composite `HistoryEventHandler`'ı `ProcessEngineConfigurationImpl` resmi genişletme noktalarıyla takmalı; **fork motor kodu değişmemeli**; dual-run (default DB handler + custom yan yana) desteklenmeli. → US-A1 → `ProcessEngineConfigurationImpl.java:757-769,2788-2796,3876-3898`; `HistoryEventHandler.java:38-53`; D-G/D-A.

**FR-A2** [M] — Handler yalnız konfigüre `HistoryLevel`'in ürettiği event'leri almalı (NONE → hiç; default AUDIT); Camunda 7 ve CadenzaFlow **tek adapter'ı** paylaşmalı (byte-ayna). → US-A1/US-A5 → `HistoryLevel.java:56-82`, `HistoryLevelNone.java:27-39`; D-G.

**FR-A3** [M] — Sistem, her `ACT_HI` event-sınıfını **audit-kritik ↔ bulk** olarak sınıflandırmalı (**konfigürable**, fork rebuild gerektirmeden). **Nihai default audit-kritik küme (PO-Q5 2026-07-17) = {OP_LOG, INCIDENT, EXT_TASK_LOG}**; IDENTITYLINK/COMMENT/ATTACHMENT **bulk yolda** (PII koruması EPIC-G retention/erasure'dan gelir), konfigle audit-kritik'e taşınabilir. → US-A2 → `07 §1` madde 1 (D-A); PO-Q5.

**FR-A4** [M] — Audit-kritik event, oluşturulduğu transaction içinde **≤1 kompakt outbox satırına** yazılmalı; NATS publish tx-dışı relay'e bırakılmalı (handler-içi senkron publish **yasak**). Publish öncesi çökmede outbox satırı hayatta kalmalı (kalıcı audit kaybı YOK). → US-A3 → `07 §4` (outbox yok-olma), `07 §1` madde 7 (D-F ≤1 satır); D-A.

**FR-A5** [M] — Bulk event, `TransactionState.COMMITTED` listener'ında tx-dışı yayınlanmalı; cutover sonrası **sıfır `ACT_HI` INSERT** üretmeli; publish exception'ı runtime tx'i **rollback edememeli**; at-most-once bilinçli kabul (kayıp reconciliation ile tespit). → US-A4 → `07 §4` (post-commit reusable); `HistoryEventProcessor.java:73-85` (in-handler senkron → REDDEDİLDİ); D-A.

**FR-A6** [M] — Kapsam **tüm `ACT_HI` event sınıfları** olmalı (D-D, istisna yok); cutover **sıralaması** hacim-öncelikli (DETAIL→VARINST→ACTINST→…). → US-A5 → `07 §1` madde 4 (D-D).

**FR-A7** [M] — History event'leri `history.<engineId>.<class>.<processInstanceId>` subject'ine `Nats-Msg-Id=<historyEventId>:<eventType>` ile yayınlanmalı (hem relay hem post-commit aynı şema). → US-A6 → `07 §1` madde 5 (D-E).

### 3.2 Relay + Postgres projeksiyon servisi (EPIC-B)

**FR-B1** [M] — Kompakt outbox'ı NATS'a taşıyan relay **leader-elected** olmalı (basamak-1 `SweepLeaderLease`); publish → **PubAck sonrası** outbox satırını silmeli (custody-transfer); PubAck'ten önce delete yok. → US-B1 → `07 §4` (reusable, custody-transfer); D-A.

**FR-B2** [M] — Projeksiyon consumer, history stream'ini **instance-anahtarıyla partition'lı** tüketmeli (aynı instanceId → aynı işleyici, sıra korunur); **idempotent merge-upsert** yapmalı (geç/eski event yeni state'i ezmez); dedup `Nats-Msg-Id=<eventId>:<type>`; yazım hedefi **engine DB'sinden AYRI Postgres**. → US-B2 → `07 §1` madde 2/5 (D-B/D-E).

**FR-B3** [M] — Projeksiyon şeması **denormalize/sorgu-odaklı** olmalı; minimal sorgu-API erişim desenlerini desteklemeli; **KVKK retention/silme SQL'le** uygulanabilir olmalı; ClickHouse'a evrim wire-contract sabit kalarak izole edilebilmeli (**ClickHouse-şimdi kapsam dışı**). → US-B3 → `07 §1` madde 2 (D-B).

**FR-B4** [M] — History wire-contract asyncapi (ADR-0006) olarak tanımlanmalı: subject, mesaj/header şemaları, dedup id, **DLQ = basamak-1 D-E kontratı aynen** (`dlq.history.>`, header-korumalı byte-ayna, custody-transfer, **ayrı-stream [CQ-6]**). → US-B4 → `07 §1` madde 5 (D-E); ADR-0006.

**FR-B5** [M] — Teslim edilemeyen history event'i `dlq.history.<...>`'a **header-korumalı byte-ayna** kopyayla düşmeli; consumer **DLQ-PubAck'ten önce ack-drop yapmamalı** (custody-transfer); DLQ payload PII yüzeyi olarak retention/erişim kararına bağlanmalı (`DATA_CLASSIFICATION.md` DP-13). → US-B5 → `07 §1` madde 5 (D-E); basamak-1 US-C1/US-C2.

### 3.3 Sorgu-API + Cockpit-körleşme telafisi (EPIC-C)

**FR-C1** [M] — Sistem, **projeksiyon store** üstünde **read-only minimal history sorgu-API'si** sunmalı (cutover'lanan sınıflar için). **Kapsam (PO-Q3 2026-07-17) = çekirdek-4 okuma deseni:** (1) `processInstanceId`→tam geçmiş, (2) `businessKey`→instance listesi, (3) zaman-aralığı+`processDefinition`→instance listesi, (4) instance→task/activity/variable geçmişi; **REST+JSON, sayfalamalı, read-only**. Agregasyon/analitik **kapsam dışı** (§7). Yanıtlarda erişim kontrolü + PII maskeleme (`DATA_CLASSIFICATION.md` DP-15), loglara PII değeri yazılmamalı. → US-C1 → `07 §1` madde 3 (D-C); PO-Q3.

**FR-C2** [S] — Sistem dokümantasyonu, sınıf-başına hangi Cockpit-history görünümünün cutover'da körleştiğini belirtmeli; **runtime Cockpit (`ACT_RU_*`) etkilenmemeli**; Cockpit `ACT_HI` bağımlılık yüzeyi **phase3'te doğrulanacak** (D-C öncesi). → US-C2 → `07 §7` (Cockpit bağımlılığı doğrulanacak); D-C.

### 3.4 Reconciliation + kademeli cutover (EPIC-D)

**FR-D1** [M] — Sistem, dual-run boyunca **sınıf-başına** projeksiyon ↔ `ACT_HI` reconciliation raporu ve **fark sayacı** SLI üretmeli; hangi sınıfların **N gün temiz** olduğunu göstermeli (**reconciliation-temizliği cutover kapısıdır**); rapor **PII değeri sızdırmamalı** (`DATA_CLASSIFICATION.md` DP-14). **N = sınıf-başına konfig, default 7 gün** (PO-Q4 2026-07-17; "kalibre edilebilir başlangıç"). → US-D1 → `07 §1` madde 3/7 (D-C/D-F); PO-Q4.

**FR-D2** [M] — Sistem, sınıf-başına DB history yazımını (default DB handler) **konfigle** kapatabilmeli; kapı = sınıf N gün temiz; sıra **hacim-öncelikli**; cutover sonrası o sınıfın `ACT_HI` yazım bileşeni = **0**. **Big-bang / kalıcı dual-run REDDEDİLDİ.** → US-D2 → `07 §1` madde 3/4/7 (D-C/D-D/D-F).

**FR-D3** [S] — Sistem, cutover'lanmış sınıfı **konfigle yeniden açabilmeli** (default DB handler geri-etkin, kod değişikliği yok); geri-dönüş o sınıfın Cockpit-history'sini geri getirmeli; runbook'ta dokümante. → US-D3 → `07 §1` madde 3 (D-C).

### 3.5 Metrik & bench history modu (EPIC-E)

**FR-E1** [M] — Sistem, **process-adımı başına normalize DB yazım-op** metriğini üretmeli: cutover'lanan sınıflarda `ACT_HI` bileşeni **0**, outbox bileşeni audit-kritikte **≤1 kompakt satır/tx**, baseline = dual-run öncesi AUDIT seviyesi. **Bu normalize metrik yazım-azaltmanın TEK sert kapısıdır** (D-F/PO-Q7 ilkesi; reconciliation-temizliği cutover kapısıdır — iki ayrı kapı). Ölçüm `pg_stat_statements` fingerprint (izolasyon **phase3'te doğrulanacak**). → US-E1 → `07 §1` madde 7 (D-F).

**FR-E2** [S] — Sistem destekleyici SLI'ları yayınlamalı: projeksiyon gecikmesi (event→query-store **p95** — SLI, sert kapı değil), reconciliation fark sayacı, history-stream DLQ/nak/ack sayaçları (basamak-1 `NatsChannelMetrics` üstüne). → US-E2 → `07 §1` madde 7 (D-F).

**FR-E3** [M] — `nats-bpm-bench` history modu, aynı senaryoyu **DB-history baseline ↔ offload-edilmiş history** iki modda koşmalı; FR-E1 metriğini iki mod için üretmeli; basamak-1 bench altyapısı üstüne kurulmalı; basamak-2 teslimatına dahil olmalı. → US-E3 → `07 §1` madde 7 (D-F); `07 §4` reusable bench.

### 3.6 Devreden borçlar (EPIC-F)

**FR-F1** [M] — Basamak-1 bench'inin **ilk gerçek koşusu** yürütülmeli (`fetchAndLock`=0 kanıtı + DB yazım-op taban çizgisi); bu sayılar basamak-2 history-modu baseline referansı olmalı (basamak-1 D-F kapanış kriteri). → US-F1 → `07 §5` borç #7.

**FR-F2** [S] — `BenchEnvironment.ensureStreams()` + prod stream provisioning history stream'i + `dlq.history.>`'i (ayrı-stream [CQ-6]) kapsayacak şekilde genişletilmeli. → US-F2 → `07 §5` borç #2.

**FR-F3** [C] — Basamak-1 kalan borçları (#1 unsafe-lock runbook, #3 scheduler shutdown await, #4 FailureEventBridge/NonMatchingEventConsumer, #5 Flowable timer maliyeti, #6 sweep captured-variables) **basamak-2-ilgili / basamak-1-kuyruğu / basamak-2b** olarak triyaj edilmeli. → US-F3 → `07 §5` borç #1,3,4,5,6.

### 3.7 Projeksiyon retention & KVKK erasure (EPIC-G — PO-Q2/Q7)

**FR-G1** [M] — Sistem, projeksiyon store'da **sınıf-bazlı otomatik retention** uygulamalı (scheduled job, `DATA_GOVERNANCE v4.0 §4.4`); **default (PO-Q7): bulk 90 gün, audit-kritik yasal-saklama süresi** (kiracı override); her silme audit-log'lanmalı. → US-G1 → PO-Q7; D-B; `DATA_CLASSIFICATION.md` DP-9.

**FR-G2** [M] — Sistem, bulk sınıf PII'ları için **erasure/anonimleştirme pipeline'ı** sunmalı (KVKK/GDPR silme-hakkı; data-subject anahtarına göre; SQL-uygulanabilir; soft-delete→anonymize; erasure audit-log'lanır; erasure sonrası sorgu-API o PII'yi döndürmemeli). → US-G2 → PO-Q2 katman-2; `DATA_CLASSIFICATION.md` DP-10; `DATA_GOVERNANCE v4.0 §2.5`; `compliance/KVKK v1.0 §2.1/§4.3`; `compliance/GDPR v1.0 §2.5`.

**FR-G3** [S] — Sistem, audit-kritik kayıtlar için **pseudonymization seçeneği** sunmalı: PII alanı (userId) tersinmez takma-ada çevrilir, **kimlik↔takma-ad haritası ayrı kasada**; silme = harita kaydını silmek (denetim yapısı korunur, re-identification imkânsız); kasa L4-bitişik korunur + erişim audit'i. → US-G3 → PO-Q2 katman-3; `DATA_CLASSIFICATION.md` §6, DP-11/DP-16.

---

## 4. Non-fonksiyonel gereksinimler (NFR)

### 4.1 Performans & ölçeklenebilirlik
**NFR-P1** [M] — Cutover'lanan sınıflarda process-adımı başına `ACT_HI` yazım-op'u **0** olmalı (birincil kazanç). → FR-E1.
**NFR-P2** [M] — Audit-kritik yolda tx-içi ek yazım **≤1 kompakt outbox satırı/tx** olmalı (tam ACT_HI satırından çok küçük). → FR-A4/FR-E1.
**NFR-P3** [S] — Projeksiyon gecikmesi (event→query-store) **p95 izlenen SLI hedefi** olmalı — **sert kapı değil** (sert kapı yalnız NFR-P1/FR-E1). → FR-E2.
**NFR-P4** [M] — Relay okuması + reconciliation, birincil DB hot-path'ine anlamlı yük bindirmemeli (amortize, batch/leader-elected). → FR-B1/FR-D1.
**NFR-P5** [S] — Projeksiyon consumer instance-anahtarlı partition'la **yatay ölçeklenebilir** olmalı (global tek-consumer REDDEDİLDİ). → FR-B2.

### 4.2 Güvenilirlik & tutarlılık
**NFR-R1** [M] — Audit-kritik sınıflar **at-least-once** olmalı; **kalıcı audit kaybı imkansız** olmalı (kompakt outbox handoff). → FR-A4/FR-B1.
**NFR-R2** [M] — Bulk sınıflar **at-most-once** olmalı; kayıp penceresi bilinçli kabul, dual-run'da reconciliation ile tespit edilebilir olmalı. → FR-A5/FR-D1.
**NFR-R3** [M] — Custody-transfer: hiçbir history mesajı kalıcılık el değiştirmeden ACK'lenmemeli; **sessiz kayıp olmamalı** (zehirli mesaj DLQ'ya ya da nak'a). → FR-B1/FR-B5.
**NFR-R4** [M] — Sıralama garanti edilmeli: aynı instance aynı subject/partition (stream sırası) + **merge-upsert güvenlik ağı** (geç/eski event yeni state'i ezmez). → FR-A7/FR-B2.
**NFR-R5** [M] — `dual-write` kalıcılaşmamalı: dual-run **geçici** (reconciliation kapısı); cutover sonrası DB yazımı gerçekten kalkmalı (kalıcı dual-run REDDEDİLDİ). → FR-D2.
**NFR-R6** [M] — Idempotency: tüm consumer'lar dedup (`Nats-Msg-Id=<eventId>:<type>`) + merge-upsert ile çift/geç teslimi güvenle yutmalı. → FR-B2.
**NFR-R7** [S] — Cutover **geri-döndürülebilir** olmalı (sınıfı yeniden açma, konfig). → FR-D3.
**NFR-R8** [S] — **Erişilebilirlik/kurtarma zarfı (phase1-review F-003):** yeni stateful yüzeyler için duruş açıkça şudur — (1) **relay HA** basamak-1 `SweepLeaderLease` lider-devri deseninden **devralınır** (lider düşerse lease TTL içinde devir; relay kesintisi audit verisi KAYBETMEZ — kompakt outbox birikir, custody-transfer korunur; NFR-R1/R3); (2) **projeksiyon Postgres'in HA/failover/RTO-RPO hedefi kiracı-owned'dır** (gömülebilir-kütüphane duruşu, §4.7) — store kesintisi projeksiyon-lag'ini büyütür (NFR-P3 SLI), audit kaybı üretmez; kiracı RTO/RPO beklentisini `TENANT_PII_CHECKLIST` üzerinden verir. Relay lider-devri süresi phase3'te doğrulanacak. → FR-B1; NFR-P3.

### 4.3 Güvenlik & veri koruma
> History = **PII yüzeyinin ta kendisi**. Ayrıntı: `DATA_CLASSIFICATION.md` (basamak-1 DP-1…8 devralınır; basamak-2 DP-9…16 eklenir).
**NFR-S1** [M] — Payload/business-key/operatör-kimliği **değerleri** loglara ve metrik tag'lerine yazılmamalı (basamak-1 DP-1 devralınır; history akışına genişler). → `DATA_CLASSIFICATION.md` DP-1.
**NFR-S2** [M] — Projeksiyon Postgres bir **L3 (PII) store**'dur: at-rest şifreleme (AES-256), erişim kontrolü, retention/silme (SQL) uygulanmalı. → `DATA_CLASSIFICATION.md` DP-9.
**NFR-S3** [M] — KVKK/GDPR **silme-hakkı** projeksiyon store üstünde uygulanabilir olmalı. **PO-Q2 katmanlı politikası (2026-07-17, ÇÖZÜLDÜ):** (1) audit-kritik sınıflar **yasal-saklama istisnası** (hukuki dayanak DPO doğrulaması), (2) bulk PII **erasure pipeline** (FR-G2), (3) audit-kritik **pseudonymization seçeneği** (FR-G3). → `DATA_CLASSIFICATION.md §6`, DP-10; FR-G2/FR-G3.
**NFR-S4** [M] — History stream + `dlq.history.>` **at-rest PII** taşır: TLS + erişim kontrolü + retention; ayrı-stream [CQ-6]; DLQ byte-ayna kopya PII'yi retention süresince tutar. → `DATA_CLASSIFICATION.md` DP-13.
**NFR-S5** [M] — Kompakt outbox (engine DB'de, audit-kritik) history payload'ı geçici taşır → kaynak event ile aynı sınıf; relay silmesiyle kısa maruziyet. → `DATA_CLASSIFICATION.md` DP-12.
**NFR-S6** [M] — Reconciliation raporları ve sorgu-API yanıtları/log'ları PII değeri sızdırmamalı (maskeleme/sayaç-only). → `DATA_CLASSIFICATION.md` DP-14/DP-15.
**NFR-S7** [S] — NATS transport güvenliği (TLS + NKey/JWT) production'da zorunlu; history subject'lerine subject-level authz. *Detay phase3.* → basamak-1 NFR-S3 devralınır.
**NFR-S8** [S] — Pseudonymization kasası (kimlik↔takma-ad haritası, FR-G3) **re-identification anahtarı** taşıdığından en yüksek koruma altında (L4-bitişik, `DATA_GOVERNANCE v4.0 §2.1`) tutulmalı: ayrı depo, erişim en-az-yetki + audit, projeksiyon store'dan izole. → `DATA_CLASSIFICATION.md` DP-16; FR-G3.

### 4.4 Gözlemlenebilirlik
**NFR-O1** [M] — Tüm history publish/consume/DLQ/nak/ack olayları Micrometer sayaçlarıyla ölçülmeli (`NatsChannelMetrics` üstüne). → FR-E2.
**NFR-O2** [M] — Reconciliation fark sayacı ve projeksiyon gecikmesi sürekli izlenebilir olmalı (dual-run/cutover sağlığı). → FR-D1/FR-E2.
**NFR-O3** [S] — Bench history modu çıktısı CI'da nightly/manuel üretilebilmeli, baseline ↔ offload karşılaştırmalı. → FR-E3.

### 4.5 Taşınabilirlik & bakım
**NFR-M1** [M] — History offload, fork motor kodunu **değiştirmemeli** (yalnız resmi genişletme noktaları); impl-sınıf bağımlılığı (`ProcessEngineConfigurationImpl` history alanları, command-context) upgrade'lerde izlenecek yüzey olarak dokümante edilmeli. → `07 §3`.
**NFR-M2** [M] — Camunda 7 ↔ CadenzaFlow history adapter'ı **birebir taşınabilir** olmalı (paket adı dışında fark yok; tek adapter + byte-ayna). → `07 §3` (fork birebir); D-G.
**NFR-M3** [M] — History wire-contract asyncapi'de **tek makine-okunur artefakt** olmalı (ADR-0006); projeksiyon consumer o kontrata bağlı olmalı. → FR-B4.
**NFR-M4** [S] — Query-store, wire-contract sabit kalarak ileride ClickHouse'a **izole** biçimde evrilebilmeli (yalnız consumer değişir). → FR-B3.
**NFR-M5** [S] — Flowable history offload (basamak-2b) aynı wire-contract/query-store'a bağlanabilecek biçimde tasarlanmalı (D-G — desen 2'de kanıtlandıktan sonra yalnız adapter işi). → `07 §1` madde 6 (D-G).

### 4.6 Uyumluluk (lisans)
**NFR-L1** [M] — Tüm bağımlılıklar Apache 2.0 uyumlu kalmalı; toplam lisans maliyeti $0 (`01 §6`). Projeksiyon store = Postgres (Apache-2.0-uyumlu PostgreSQL lisansı).

### 4.7 Platform-compliance kapsam notu
> `PRODUCT_OWNER_GUIDELINE §2.1/§2` "Admin Interface [REQUIRED]" ve "Platform Compliance [BLOCKING]" (Keycloak/APISIX/Prometheus/Loki/…) kalemleri, **gömülebilir OSS kütüphane** doğasıyla basamak-1'de olduğu gibi (manifest `ui-ux-guidelines`/`frontend-security` disabled) **doğrudan uygulanmaz**: bu repo bir 3eAI iç servisi değil, kiracının kendi stack'ine gömdüğü Apache-2.0 bir kütüphanedir. Sorgu-API (FR-C1) **read-only servistir, merkezi Admin-UI değildir**; kimlik/authz **pluggable** (kiracı-sağlar) olmalı ve deployment rehberinde dokümante edilmelidir. Bu duruş basamak-1 ile tutarlıdır; farklı bir zorunluluk isteniyorsa PO kararı gerekir.

---

## 5. Arayüz / wire-contract gereksinimleri

**IR-1 — Subject şeması:** `history.<engineId>.<class>.<processInstanceId>` (instance-anahtarlı, stream sırası); DLQ `dlq.history.<orijinal-subject>` (tek `dlq.history.>` stream, ayrı-stream [CQ-6]). → `07 §1` madde 5.

**IR-2 — Header'lar:** basamak-1 `BpmHeaders` (`X-Cadenzaflow-Trace-Id`, `-Business-Key`, `-Idempotency-Key`) devralınır; history-özgü meta (engineId, class, eventType, historyEventId, processInstanceId) kanonik olarak history asyncapi'sinde tanımlanır (phase3/4). Business-Key masking basamak-1 DP-8 önerisi (normatif değil) history akışına da uygulanır.

**IR-3 — Dedup:** `Nats-Msg-Id = <historyEventId>:<eventType>` (D-E). → `07 §1` madde 5.

**IR-4 — Ack/nak semantiği:** custody-transfer (basamak-1 D-E aynen); transient → nakWithDelay; delivery bütçesi bitince → `dlq.history.>`; relay outbox delete **yalnız PubAck sonrası**. → FR-B1/FR-B5.

**IR-5 — Projeksiyon consumer kontratı:** asyncapi-kontratlı (ADR-0006); instance-anahtarlı partition; idempotent merge-upsert; ayrı Postgres. → FR-B2/FR-B4.

**IR-6 — Sorgu-API arayüzü:** **REST + JSON, sayfalamalı, read-only**; projeksiyon Postgres'ten; **çekirdek-4 okuma deseni** (processInstanceId→tam geçmiş; businessKey→instance listesi; zaman-aralığı+definition→liste; instance→task/activity/variable geçmişi — PO-Q3); erişim kontrolü + PII maskeleme; agregasyon/analitik kapsam dışı. → FR-C1.

**IR-7 — Stream tipleri:** history stream (sıra-korumalı; tip phase3'te netleşir — WorkQueue değil, çünkü tek-tüketici-alır değil, projeksiyon consumer + reconciliation okur); DLQ → limits-based (basamak-1 default 14 gün), ayrı stream [CQ-6]. → `07 §1` madde 5.

---

## 6. İzlenebilirlik matrisi (FR ↔ US ↔ kilitli karar)

| FR | US | Kilitli karar | Öncelik |
|---|---|---|---|
| FR-A1, FR-A2 | US-A1 (US-A5) | D-G / D-A | M |
| FR-A3 | US-A2 | D-A | M |
| FR-A4 | US-A3 | D-A | M |
| FR-A5 | US-A4 | D-A | M |
| FR-A6 | US-A5 | D-D | M |
| FR-A7 | US-A6 | D-E | M |
| FR-B1 | US-B1 | D-A / D-E | M |
| FR-B2 | US-B2 | D-B / D-E | M |
| FR-B3 | US-B3 | D-B | M |
| FR-B4 | US-B4 | D-E | M |
| FR-B5 | US-B5 | D-E | M |
| FR-C1 | US-C1 | D-C | M |
| FR-C2 | US-C2 | D-C | S |
| FR-D1 | US-D1 | D-C / D-F | M |
| FR-D2 | US-D2 | D-C / D-D | M |
| FR-D3 | US-D3 | D-C | S |
| FR-E1 | US-E1 | D-F | M |
| FR-E2 | US-E2 | D-F | S |
| FR-E3 | US-E3 | D-F | M |
| FR-F1 | US-F1 | `07 §5` #7 | M |
| FR-F2 | US-F2 | `07 §5` #2 | S |
| FR-F3 | US-F3 | `07 §5` #1,3,4,5,6 | C |
| FR-G1 | US-G1 | D-B / PO-Q7 | M |
| FR-G2 | US-G2 | PO-Q2 (katman-2) | M |
| FR-G3 | US-G3 | PO-Q2 (katman-3) | S |

---

## 7. Kapsam dışı & reddedilen/ertelenen (yeniden açılmaz)

| Öğe | Durum | Kaynak |
|---|---|---|
| Handler-içi senkron NATS publish | **REDDEDİLDİ** (tx coupling: NATS latency engine'i bloklar; publish exception commit'i atlatıp runtime tx'i rollback eder) | `07 §4` / D-A |
| Tam-outbox (tüm sınıflar outbox) | **REDDEDİLDİ** (DB yazımı kalır — §6.7 hedefiyle çelişir) | `07 §1` madde 1 / D-A |
| Tam-post-commit (tüm sınıflar post-commit) | **REDDEDİLDİ** (sessiz audit kaybı — audit-kritik sınıf koruması yok) | `07 §1` madde 1 / D-A |
| JetStream-only query-store | **REDDEDİLDİ** (rastgele-erişim audit sorguları için yetersiz) | `07 §1` madde 2 / D-B |
| ClickHouse-şimdi | **ERTELENDİ** (hacim-tetikli ayrı karar; kontrat sabit → consumer değişimi izole) | `07 §1` madde 2 / D-B |
| Big-bang cutover | **REDDEDİLDİ** (etki yüzeyi/geri-dönüş büyük) | `07 §1` madde 3 / D-C |
| Kalıcı dual-run | **REDDEDİLDİ** (yazım hacmi kalkmıyor — §6.7 hedefiyle çelişir) | `07 §1` madde 3 / D-C |
| Sırasız + salt-upsert | **REDDEDİLDİ** (çatışma-çözüm karmaşası) | `07 §1` madde 5 / D-E |
| Sorgu-API agregasyon / analitik / raporlama görünümleri | **KAPSAM DIŞI** (sorgu-API = çekirdek-4 okuma deseni) | PO-Q3 (2026-07-17) |
| Global tek-consumer | **REDDEDİLDİ** (throughput tavanı) | `07 §1` madde 5 / D-E |
| Flowable history offload (**basamak-2b**) | **ERTELENDİ** (7.1'de hazır async-history yok; SPI `HistoryManager` Camunda ile arayüz-paylaşımı ~0; audit-kritik harita yeniden yapılmalı) | `07 §1` madde 6 / `07 §7` / D-G |
| Üç-motor-birlikte | **REDDEDİLDİ** (kapsam ~2×, iki SPI riski tek teslimatta) | `07 §1` madde 6 / D-G |
| Flowable-süresiz-dışarıda | **REDDEDİLDİ** (üç-motor-eşitliği vizyonunu kırar → basamak-2b planlı) | `07 §1` madde 6 / D-G |
| token-move/completion tx kaldırılması | **KAPSAM DIŞI** → basamak-6 (`05 §6.7` strangler basamağı; "P2" persona değil) | `05 §6.7` |
| Büyük değişken externalization / DB sharding | **KAPSAM DIŞI** → basamak 3 / 5 | `05 §6.7` |

---

## 8. PO Karar Kaydı (Q→A, 2026-07-17)

**Kilitli teknik kararlar:** D-A…D-G **tamamı çözülü** (`07 §1`, 2026-07-15/16). Bu SRS onları **değiştirmez**; yalnız gereksinimleştirir.

**PO-QUESTIONS cevaplandı (2026-07-17)** — SRS'e etkileri (sorular korunur, kararlar eklenir; tam kayıt `USER_STORIES.md §4`):

| # | Karar | SRS'e etki |
|---|---|---|
| **PO-Q1** | Yerleşim `docs/sentinel/step2/phase1/` ONAYLANDI | Konum korundu |
| **PO-Q2** | KVKK **katmanlı politika** (yasal-saklama istisnası + bulk erasure + audit-kritik pseudonymization) | **NFR-S3** güncellendi; **FR-G2/FR-G3** eklendi (§3.7); NFR-S8 |
| **PO-Q3** | Sorgu-API = **çekirdek-4 okuma deseni** (REST+JSON, sayfalamalı, read-only); agregasyon kapsam dışı | **FR-C1** + **IR-6** güncellendi; §7 reddedilen tablosuna eklendi |
| **PO-Q4** | Reconciliation N = **sınıf-başına konfig, default 7g** | **FR-D1** güncellendi |
| **PO-Q5** | Audit-kritik = **{OP_LOG, INCIDENT, EXT_TASK_LOG}** + konfig; IDENTITYLINK/COMMENT/ATTACHMENT bulk | **FR-A3** nihaileşti |
| **PO-Q6** | Tüm **S** kapanışa dahil; US-F3 (C) backlog | §1.2 kapsam; matris öncelikleri |
| **PO-Q7** | Retention (sınıf-bazlı) + erasure pipeline + TENANT template = basamak-2 teslimatı | **FR-G1/G2/G3** (§3.7); NFR-S2/S3; yeni dosya `TENANT_PII_CHECKLIST_TEMPLATE.md` |

**Phase3'e taşınan doğrulamalar:** Cockpit `ACT_HI` bağımlılık yüzeyi; `handleEvents(List)` batch sıklığı; `pg_stat_statements` history-write fingerprint izolasyonu; projeksiyon merge-upsert kenar durumları; kompakt outbox tek-INSERT bindirmesi (§2.5'te işaretli).
