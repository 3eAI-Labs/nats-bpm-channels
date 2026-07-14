# SRS — Software Requirements Specification
## Basamak-1: External Task / Event-Driven Work Offload over JetStream

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Sentinel fazı:** Phase 1 — Product Owner
**Kapsam:** `05-db-offload-strategy.md` §6.7 **basamak-1** (`06-external-task-over-jetstream.md`'in gereksinimleştirilmesi)
**Tarih:** 2026-07-14
**Durum:** Onaylı (2026-07-14) — PO-QUESTIONS cevaplandı (bkz. §8 PO Karar Kaydı)
**Sürüm:** 1.0

> Bu SRS, LLD değildir; **ne** yapılacağını (gereksinim) sabitler, **nasıl** yapılacağını (tasarım) `06`'ya + phase3/phase4'e bırakır. Motor/adapter davranış iddiaları `file:line` kanıtlıdır. Doğrulanmamış varsayımlar "phase3'te doğrulanacak" etiketlidir. **Effort tahmini içermez.**

---

## 1. Giriş

### 1.1 Amaç
Bu belge, basamak-1'in — external task acquisition / dispatch'in DB-transaction polling'inden JetStream push'a taşınması — yazılım gereksinimlerini tanımlar. İki motor idiomu (Camunda 7 / CadenzaFlow **A2**, Flowable **Event Registry**) ortak bir JetStream substratı üzerinde buluşur. Amaç, motorun tek-DB dispatch yükünü DB-dışına almak; **token-move/completion transaction bilinçli olarak kalır** (P2, basamak-6'ya kadar).

### 1.2 Kapsam
**Kapsam içi:** custom `ExternalTaskActivityBehavior` + post-commit publisher, soğuk orphan-sweep, inbound completion-bridge, DLQ bridge'leri (Camunda incident-bridge + Flowable failure-event bridge), ortak JetStream substratı + wire-contract fix'leri (**5 fix:** DLQ header kaybı, koşulsuz ack, DLQ dedup id, trace-header adı — Q2 2026-07-14; boş-body sessiz-ack — phase2 BAQ-5 2026-07-14, BR-SUB-007), Testcontainers yük-bench modülü, JavaDelegate outbound tam phase-out.

**Kapsam dışı (bkz. §7):** hot central poller, timer-only escalation, advisory-tabanlı DLQ tespiti (üçü REDDEDİLDİ), heartbeat (D-H), gRPC ön kapısı (D-G), token-move tx kaldırılması (basamak-6), history offload (basamak-2), büyük değişken externalization (basamak-3), DB sharding (basamak-5).

### 1.3 Tanımlar & kısaltmalar
| Terim | Açıklama |
|---|---|
| **A2** | Camunda 7 / CadenzaFlow için "job-push external task over JetStream" idiomu |
| **Outbox** | `ACT_RU_EXT_TASK` tablosu; tek DB yazımı doğruluk kaynağı, push onun idempotent türevi |
| **Sentinel workerId** | Küme-geneli tek sabit workerId (örn. `a2-jetstream-bridge`); task doğumda bununla kilitlenir |
| **Şemsiye kilit (L)** | Sentinel lockDuration; JetStream teslimat bütçesini kapsayan değer |
| **W / M / S / ε** | `ack-wait` / `maxDeliver` / sweep periyodu / işleme payı |
| **DLQ** | Dead-letter queue; `dlq.>` subject uzayında tek ortak stream |
| **Custody-transfer** | Ack yalnız kalıcılık el değiştirdikten sonra verilir ilkesi |
| **WorkQueue** | Her mesaj tek tüketiciye giden JetStream stream tipi |
| **P1/P2** | Kilitli mimari ilkeler (`05 §3`): lock-free ⇐ single-writer; completion-lock ancak state-ownership NATS'a geçerse kalkar |

### 1.4 Referanslar
- `docs/01-vision-roadmap.md` (VISION — üst çerçeve)
- `docs/05-db-offload-strategy.md` (omurga; §3 ilkeler, §6.7 merdiven, §7 wire-contract, §8 açık kararlar)
- `docs/06-external-task-over-jetstream.md` (**birincil tasarım girdisi**; D-A…D-F çözülü, D-G/D-H ertelenmiş)
- `docs/04-async-request-reply-design.md` (async desen — A2 tarafından supersede)
- `USER_STORIES.md`, `DATA_CLASSIFICATION.md` (bu faz kardeş teslimatları)

---

## 2. Genel açıklama

### 2.1 Ürün perspektifi
`nats-bpm-channels`, üç açık kaynak BPM motoru (Flowable, Camunda 7, CadenzaFlow) için ortak bir NATS.io/JetStream messaging katmanıdır (`01 §5.1`). Mevcut mimari üç bağlayıcı + `nats-core`'dan oluşur. Basamak-1, outbound yolunu **motor-içi in-tx JavaDelegate**'ten **motor-dışı push**'a taşır; inbound correlation altyapısının büyük kısmı yeniden kullanılır.

### 2.2 Ana fonksiyonlar (özet)
1. External task'ı doğumda sentinel-kilitli oluştur, commit sonrası push et (A2).
2. Yayınlanmamış çökme-orphan'larını soğuk sweep ile topla.
3. Worker sonucunu inbound bridge ile completion'a bağla (A2 `complete` / Event Registry correlate).
4. Delivery bütçesi bitince DLQ → incident (Camunda) / failure-event (Flowable).
5. Ortak DLQ/ack/dedup wire-contract'ını uygula; mevcut 5 kontrat açığını kapat (FR-C1..C3, C7; 5. fix boş-body — phase2 BAQ-5 kararı, FR-C2/FR-B2 kapsamında BR-SUB-007 ile tanımlı, ayrı FR açılmadı).
6. Başarıyı normalize DB-round-trip metriği + Testcontainers bench ile kanıtla.
7. JavaDelegate outbound'u tam phase-out et.

### 2.3 Kullanıcı sınıfları
`USER_STORIES.md §1`: P1 Süreç Geliştirici, P2 Worker Geliştirici, P3 Platform/Ops, P4 Migrasyon Sahibi, P5 Bakımcı Mühendis, P6 Veri Koruma Sorumlusu.

### 2.4 İşletim ortamı & teknoloji kısıtları
- Java 21+ (build Java 21 gerektirir; sistem default Java 25 Mockito'yu kırar — memory `build-requires-java21`).
- Spring Boot 3.3+, jnats 2.20+, NATS/JetStream 2.10+ (`01 §6`).
- Engine sürümleri: Flowable 7.1+, Camunda 7.24+, CadenzaFlow 1.2+ (`01 §6`).
- CadenzaFlow **fork'tur** (3eAI Labs); A2 fork **motor kodunu değiştirmez** — yalnız desteklenen plugin extension point'leri (`BpmnParseListener`, `preParseListeners`).
- PostgreSQL birincil hedef DB (metrik `pg_stat_statements` ile); MySQL/InnoDB alternatif (lock-wait metriği farklı).

### 2.5 Varsayımlar & bağımlılıklar
- **Doğrulanmış:** external-task subsystem CadenzaFlow fork'unda mevcut, upstream-Camunda7 davranışı (`06 §9`: `org.cadenzaflow...cmd.*ExternalTaskCmd` + `ExternalTask.xml`).
- **Doğrulanmış:** `complete` yalnız workerId eşitliği kontrol eder, lock-expiry kontrol etmez (`HandleExternalTaskCmd.java:89-91`).
- **Doğrulanmış:** Micrometer tabanı + Testcontainers altyapısı dört modülde mevcut (`06 §9` D-F).
- **Phase3'te doğrulanacak:** `pg_stat_statements` `fetchAndLock` fingerprint izolasyonu; `duplicate_window`/`msg.inProgress()`/advisory JetStream davranışları; Flowable Event Registry yakalama biçimleri + boundary-timer maliyeti + `eventReceived` geç-event davranışı; custom behavior'da flush-öncesi `lock()`'un tek INSERT ürettiği.

---

## 3. Fonksiyonel gereksinimler (FR)

> Notasyon: **FR-x** [öncelik M/S/C] — gereksinim → *kaynak US* → *kanıt*.

### 3.1 A2 — outbound push & lifecycle (EPIC-A)

**FR-A1** [M] — Sistem, A2-topic'li (`camunda:type="external"`) bir external task oluşturulduğunda, worker'lara **JetStream push** ile teslimat yapmalı; happy-path'te **hiçbir `fetchAndLock` sorgusu koşmamalı**. → US-A1 → `06 §5.1/§5.5`.

**FR-A2** [M] — Sistem, external task satırını **onu oluşturan transaction içinde** custom `ExternalTaskActivityBehavior` ile `createAndInsert` + `lock(SENTINEL, L)` yaparak kilitlemeli; kilit alanları aynı INSERT'e binmeli (ek DB yazısı üretmemeli). → US-A2 → `ExternalTaskEntity.java:568-588,472-474`; `BpmnParse.java:2564`; `ProcessEngineConfigurationImpl.java:687,2189`.

**FR-A3** [M] — `SENTINEL` workerId **küme-geneli tek sabit** olmalı ve audit için payload'da taşınmalı. → US-A2 → `06 §5.4`.

**FR-A4** [M] — Sistem, task'ı yayınlamayı `TransactionState.COMMITTED` listener'ında, oluşturan node'un elindeki entity ile, **DB sorgusu yapmadan** gerçekleştirmeli. → US-A3 → `06 §5.3`; `TransactionContext.java:49`, `TransactionState.java:25`.

**FR-A5** [M] — Sistem, yayınlanmamış çökme-orphan'ları için **tek node/leader**'da koşan, engine fetchable-predicate'iyle birebir sorgulayan, `SELECT FOR UPDATE` **kullanmayan** bir soğuk sweep sağlamalı; periyodu (S, default 120s) yapılandırılabilir olmalı. → US-A3 → `06 §5.3/§5.4`; `ExternalTask.xml:220-222`.

**FR-A6** [M] — Sweep, yayın öncesi sentinel re-lock yapmalı (aynı workerId, her zaman geçer) ve `retries=0` (DLQ'lanmış) task'ı **asla** yeniden yayınlamamalı. → US-A3 → `LockExternalTaskCmd.java:50-61`; `ExternalTask.xml:222`.

**FR-A7** [M] — Inbound bridge, `jobs.<topic>.reply` mesajını `externalTaskService.complete(extTaskId, SENTINEL, vars)` çağrısına bağlamalı; business-error → `handleBpmnError`, transient → `handleFailure`. → US-A4 → `06 §5.2`; `HandleExternalTaskCmd.java:89-91`.

**FR-A8** [M] — Sistem, şemsiye koşulunu `L ≥ M·W + Σbackoff + S + ε` sağlamalı; default'lar W=30s, M=4, S=120s, ε=60s → alt sınır **307s**, default **L=320s** (13s marj). W topic-başına override edilebilmeli; L default'u parametrelerden türetilebilmeli. → US-A5 → `06 §5.4` (phase-review MAJOR-B düzeltmesi 2026-07-14).

**FR-A9** [M] — Sistem heartbeat kullanmamalı; W·M sert tavan olmalı (`msg.inProgress()` ve engine `extendLock` **kullanılmaz**). → US-A5 → `ExtendLockOnExternalTaskCmd.java:46-47`.

**FR-A10** [M] — Delivery bütçesi (`deliveryCount > M`) bitince mesaj `dlq.jobs.<topic>`'a yönlenmeli; incident-bridge `handleFailure(..., retries=0)` ile **incident** üretmeli (Cockpit görünürlüğü). → US-A6 → `06 §5.4/§7`.

**FR-A11** [M] — `retries=0` task fetchable-predicate dışı olduğundan sweep tarafından dirilmemeli; operatör Cockpit-retry verirse task yeniden fetchable olmalı ve sweep onu doğal olarak yeniden yayınlamalı. → US-A6 → `ExternalTask.xml:222`.

**FR-A12** [M] — L sonrası gelen reply yine başarılı complete olmalı; ikinci (çift) complete "task yok" ile karşılaşınca **yakalanıp ACK** edilmeli (idempotency anahtarı = `externalTaskId`). → US-A7 → `HandleExternalTaskCmd.java:89-91`; `06 §5.4`.

**FR-A13** [S] — A2 task'ı doğuştan kilitli olduğundan legacy external-task poller'lar onu fetch edememeli; A2 olmayan external task'lar etkilenmemeli. → US-A8 → `06 §5.4` yan kazanç (a).

### 3.2 Flowable — Event Registry basamak-1 (EPIC-B)

**FR-B1** [M] — Flowable outbound, `NatsOutboundEventChannelAdapter.sendEvent(...)` ile motor-dışı yapılmalı; native push idiomu (subscribe + queue-group + `eventReceived`) korunmalı. → US-B1 → `NatsInboundEventChannelAdapter.java:49,88`; `NatsOutboundEventChannelAdapter.java:29`.

**FR-B2** [M] — Flowable JetStream inbound yolu **ack + DLQ + dedup** sağlamalı (`maxDeliver+1` DLQ tespiti + `Nats-Msg-Id`/correlation idempotency); core adapter'ın ack'siz/log-only yolu basamak-1 kritik iş için kullanılmamalı. → US-B2 → `JetStreamInboundEventChannelAdapter.java:75-77,133-146,152`.

**FR-B3** [M] — DLQ'ya düşen mesaj **aynı correlation key'lerle** failure-event'e çevrilip `eventRegistry.eventReceived(...)`'a sokulmalı → bekleyen instance escalation path'ini işlemeli; happy-path ek DB maliyeti sıfır olmalı. → US-B3 → `06 §6.2`.

**FR-B4** [S] — Opt-in boundary timer, yalnız gerçek wall-clock deadline'ı olan modellerde modellenebilmeli; timer-job maliyeti yalnız o modellerde ödenmeli. → US-B4 → `06 §6.2` (maliyet phase3'te doğrulanacak).

**FR-B5** [S] — Escalation interrupting ise geç sonuç **ack + log + metric** ile drop edilmeli; non-interrupting modellenirse işlenmeli. → US-B5 → `06 §6.2` (`eventReceived` geç davranış phase3'te doğrulanacak).

### 3.3 Ortak substrat & wire-contract fix'leri (EPIC-C)

**FR-C1** [M] — `publishToDlq`, orijinal payload byte'larını **ve** orijinal header'ların tamamını kopyalamalı; ek meta header'lar eklemeli (`X-Cadenzaflow-Dlq-Original-Subject`, `-Dlq-Delivery-Count`, `-Dlq-Reason`, `-Dlq-Timestamp`). *Mevcut açık:* yalnız `msg.getData()` yayınlanıyor. → US-C1 → **bizzat doğrulandı** `JetStreamInboundEventChannelAdapter.java:218,227`; cadenzaflow `JetStreamMessageCorrelationSubscriber.java:210,219`.

**FR-C2** [M] — Ack **yalnız kalıcılık el değiştirdikten sonra** verilmeli (custody-transfer). `dlqSubject==null` iken mesaj discard **edilmemeli**; DLQ publish başarısızsa **nak** edilmeli; `dlq-of-dlq` yok → işlenemezse nak + alert. *Mevcut açık:* `dlqSubject==null`'da discard + koşulsuz ack; DLQ-publish fail'de sadece log sonra ack. → US-C2 → **bizzat doğrulandı** `:141-145,211-214,222-235`; cadenzaflow `:123-127`.

**FR-C3** [M] — Her DLQ publish `Nats-Msg-Id = <orijinal-msg-id>.dlq` taşımalı (çift DLQ kaydına karşı). *Mevcut açık:* `Nats-Msg-Id` yok. → US-C3 → **bizzat doğrulandı** `:218`.

**FR-C4** [S] — Tüm DLQ trafiği **tek** `DLQ` stream'inde (`dlq.>`, limits-based retention default 14 gün, WorkQueue değil) toplanmalı; tüketiciler subject filtresiyle ayrışmalı (`dlq.jobs.>` → incident-bridge, event-channel DLQ → failure-event bridge). → US-C4 → `06 §7`.

**FR-C5** [M] — İş dağıtım stream'i **WorkQueue** tipinde olmalı; `Nats-Msg-Id` dedup (A2: `externalTaskId`; Event Registry: correlation key); `duplicate_window` yapılandırılabilir (default 2dk) ve L>window durumunda pencere-dışı çiftlerin apply-zamanı idempotency ile yutulduğu dokümante edilmeli. → US-C5 → `06 §7`; `05 §7`.

**FR-C6** [M] — Transient hata `nakWithDelay` ile ele alınmalı (üstel backoff `2^(n-1)`s, cap 30s — mevcut ortak desen). → US-C2 → `JetStreamInboundEventChannelAdapter.java:204-208`.

**FR-C7** [M] — Trace header standardize edilmeli: **yazma** yalnız `X-Cadenzaflow-Trace-Id` (`BpmHeaders.java:12`) üretmeli; **okuma** iki adı da fallback ile kabul etmeli (önce `X-Cadenzaflow-Trace-Id`, yoksa `X-Trace-Id`). *Mevcut açık:* okuma `X-Trace-Id` bekliyor, yazma `X-Cadenzaflow-Trace-Id`. → US-C6 → `06 §7` kontrat-fix #4 (Q2 2026-07-14); **bizzat gözlemlendi** `JetStreamInboundEventChannelAdapter.java:119` (okuma) vs `BpmHeaders.java:12` (yazma).

### 3.4 Gözlemlenebilirlik & bench (EPIC-D)

**FR-D1** [M] — Sistem, task-yaşamdöngüsü başına DB round-trip metriğini üretmeli; kabul: poll + `fetchAndLock` bileşenleri **0**, INSERT/complete bileşenleri **artmıyor**. Ölçüm `pg_stat_statements` fingerprint (veya datasource-proxy). **Bu metrik basamak-1 kapanışının TEK sert kabul kapısıdır (Q7 2026-07-14).** → US-D1 → `06 §5.6` (fingerprint izolasyonu phase3'te doğrulanacak).

**FR-D2** [S] — Sistem, destekleyici SLI'ları yayınlamalı: `fetchAndLock` QPS (hot-path 0/s), lock-wait (~0), HikariCP aktif connection (düşer), dispatch latency (**p95 ≤ 200ms — SLI hedefi, sert kapı değil, Q7**), failure sayaçları + sweep-republish + en-yaşlı-orphan yaşı; mevcut `NatsChannelMetrics` üstüne kurulmalı. → US-D2 → `NatsChannelMetrics.java:25-63`.

**FR-D3** [M] — Testcontainers yük-bench modülü, aynı senaryoyu native-poll ↔ A2-push iki modda koşmalı (`@Tag("bench")`), FR-D1 metriğini iki mod için üretmeli; basamak-1 teslimatına dahil olmalı. → US-D3 → `06 §5.6`.

### 3.5 Phase-out & idiom (EPIC-E)

**FR-E1** [M] — Tüm JavaDelegate outbound (senkron dahil, fast-RPC istisnası **yok**) üç motorda kaldırılmalı: Camunda `NatsPublishDelegate.java:17`/`JetStreamPublishDelegate.java:17`/`NatsRequestReplyDelegate.java:19`; CadenzaFlow karşılıkları; Flowable `requestreply/NatsRequestReplyDelegate.java:19`. → US-E1 → `06 §3`; `05 §9`.

**FR-E2** [S] — Doküman, iş dağıtımı (A2/Event Registry) ↔ saf message-correlation (gerçek dış event) ayrımını netleştirmeli; `correlateWithResult()` yapısı A2 completion-bridge'e evrilmeli, saf event-bekleme için korunmalı. → US-E2 → `06 §1.5`.

---

## 4. Non-fonksiyonel gereksinimler (NFR)

### 4.1 Performans & ölçeklenebilirlik
**NFR-P1** [M] — Happy-path'te external task doğumu → worker teslim, **`fetchAndLock` sorgusu içermemeli** (poll bileşeni = 0). → FR-D1.
**NFR-P2** [S] — Dispatch latency (commit → worker deliver) **p95 ≤ 200ms** — **izlenen SLI hedefi, sert kabul kapısı değil** (Q7 2026-07-14; sert kapı yalnız NFR-P1/FR-D1). → FR-D2.
**NFR-P3** [S] — Aynı yükte HikariCP aktif connection sayısı, native-poll baseline'a göre **düşmeli** (connection-tutma ayağı).
**NFR-P4** [S] — `ACT_RU_EXT_TASK` lock-wait ~0 olmalı (`pg_locks` / `innodb_row_lock_waits`).
**NFR-P5** [M] — Sweep DB okuması amortize ≤ 1 read / S(120s) / cluster olmalı (hot-path'e yük bindirmemeli).

### 4.2 Güvenilirlik & tutarlılık
**NFR-R1** [M] — Teslimat **at-least-once** olmalı; tüm tüketiciler idempotent (dedup + apply-zamanı idempotency). → `05 P3`.
**NFR-R2** [M] — Custody-transfer ilkesi: hiçbir mesaj kalıcılık el değiştirmeden ACK'lenmemeli; **sessiz mesaj kaybı olmamalı** (poison mesaj DLQ'ya ya da nak'a gitmeli). → FR-C2.
**NFR-R3** [M] — Çökme-orphan'ları (yayınlanmamış task) ≤ L+S (~7dk) içinde toplanmalı. → FR-A5.
**NFR-R4** [M] — İki redelivery saati çakışmamalı: JetStream tek otorite, engine kilidi şemsiye. → FR-A8.
**NFR-R5** [M] — `dual-write` üretilmemeli: `ACT_RU_EXT_TASK` outbox'tur; push onun idempotent türevidir. → `06 §5.3`.
**NFR-R6** [S] — DLQ→escalation ile bekleyen token **leak** olmamalı (Flowable failure-event / Camunda incident). → FR-A10, FR-B3.

### 4.3 Güvenlik & veri koruma
**NFR-S1** [M] — Payload/business-key/header değerleri **loglara ve metrik tag'lerine yazılmamalı** (bkz. `DATA_CLASSIFICATION.md`); metrik tag'leri yalnız `subject`/`channel` gibi düşük-kardinalite, PII-içermeyen alanlar olmalı.
**NFR-S2** [M] — DLQ stream'i orijinal payload'ı byte-aynen kopyaladığından (FR-C1) payload'daki hassas veri DLQ retention (default 14 gün) boyunca kalır; DLQ stream'ine **erişim kontrolü + saklama politikası** uygulanmalı (bkz. `DATA_CLASSIFICATION.md §5`).
**NFR-S3** [S] — NATS transport güvenliği (TLS + NKey/JWT auth) yapılandırılabilir olmalı; production için zorunlu tutulmalı (`01 §9` entegrasyon). *Not:* kimlik/authz mekanizmasının basamak-1 kapsamındaki detayı **phase3'te netleşecek**.
**NFR-S4** [M] — Business-key ve process değişkenleri motor-dışı polyglot worker'lara (güven sınırı dışına) header/payload olarak geçtiğinden, worker güven sınırı ve erişim kontrolü dokümante edilmeli.

### 4.4 Gözlemlenebilirlik
**NFR-O1** [M] — Tüm inbound/outbound/DLQ/nak/ack olayları Micrometer sayaçlarıyla ölçülmeli (`NatsChannelMetrics` üstüne). → FR-D2.
**NFR-O2** [S] — `X-Cadenzaflow-Trace-Id` MDC'ye taşınmalı (mevcut desen `JetStreamInboundEventChannelAdapter.java:119-122` `X-Trace-Id`→MDC — **Q2 2026-07-14:** okuma iki adı da fallback ile kabul eder, yazma yalnız `X-Cadenzaflow-Trace-Id`; bkz. FR-C7 / US-C6).
**NFR-O3** [S] — Bench çıktısı CI'da nightly/manuel üretilebilir olmalı, sonuç raporu karşılaştırmalı (baseline ↔ A2). → FR-D3.

### 4.5 Taşınabilirlik & bakım
**NFR-M1** [M] — A2, CadenzaFlow fork motor kodunu **değiştirmemeli** (yalnız plugin extension point). Impl-sınıf bağımlılığı (`ExternalTaskEntity`, `TransactionContext`) upgrade'lerde izlenecek yüzey olarak dokümante edilmeli. → `06 §5.4`.
**NFR-M2** [M] — A2 Camunda 7 ↔ CadenzaFlow **birebir taşınabilir** olmalı (paket adı dışında fark yok). → `06 §9`.
**NFR-M3** [M] — İki idiom (A2 + Event Registry) **aynı wire-contract'ı** yayıp tüketmeli → worker ekosistemi paylaşılmalı. → `05 P5/§7`.
**NFR-M4** [S] — Wire-contract, ileride Track A (basamak-6 native engine) tarafından **dışarıya aynı** verilebilecek biçimde tanımlanmalı (worker'lar değişmeden bağlanabilmeli). → `05 §7`.

### 4.6 Uyumluluk (lisans)
**NFR-L1** [M] — Tüm bağımlılıklar Apache 2.0 uyumlu kalmalı; toplam lisans maliyeti $0 (`01 §6`).

---

## 5. Arayüz / wire-contract gereksinimleri

**IR-1 — Subject şeması:** `jobs.<type>` (A2 job) + `jobs.<type>.reply` (A2 reply); Event Registry channel subject'leri + inbound channel; DLQ `dlq.<orijinal-subject>` (tek `dlq.>` stream). → `06 §7`.

**IR-2 — Header'lar (mandatory):** `X-Cadenzaflow-Trace-Id`, `-Business-Key`, `-Idempotency-Key` (mevcut kod: `BpmHeaders.java:12-14` — yalnız bu üç sabit); async: `-Correlation-Id`, `-Reply-Subject` (**yeni kontrat header'ları** — kodda henüz yok, phase5'te `BpmHeaders`'a eklenir; kanonik tanım `phase3/api/asyncapi.yaml`); DLQ meta: `X-Cadenzaflow-Dlq-Original-Subject`, `-Dlq-Delivery-Count`, `-Dlq-Reason`, `-Dlq-Timestamp` (yeni, D-E).
- **Trace-header (Q2 2026-07-14):** yazma yalnız `X-Cadenzaflow-Trace-Id`; okuma `X-Trace-Id` fallback kabul eder (FR-C7).
- **Business-Key (Q4 2026-07-14, normatif DEĞİL):** kiracıya hash/mask **önerilir**; kod zorunluluğu değildir, karar kiracının (bkz. `DATA_CLASSIFICATION.md` DP-7, `TENANT_PII_CHECKLIST_TEMPLATE.md`).

**IR-3 — Dedup:** `Nats-Msg-Id` = A2 `externalTaskId` / Event Registry correlation key; DLQ publish'te `<orijinal-msg-id>.dlq`.

**IR-4 — Ack/nak semantiği:** custody-transfer (FR-C2); transient → `nakWithDelay` (`2^(n-1)`s, cap 30s); `deliveryCount > M` → DLQ.

**IR-5 — Worker protokolü:** request/job → reply/complete; correlation-id birebir echo; reply-önce-ack (at-least-once); business-error de bir reply'dır (error-reply → ack).

**IR-6 — Stream tipleri:** iş dağıtımı → WorkQueue; DLQ → limits-based (retention default 14 gün).

---

## 6. İzlenebilirlik matrisi (FR ↔ US ↔ açık karar)

| FR | US | Açık karar (çözüldü) | Öncelik |
|---|---|---|---|
| FR-A1 | US-A1 | — | M |
| FR-A2, FR-A3 | US-A2 | D-C | M |
| FR-A4, FR-A5, FR-A6 | US-A3 | D-A/D-B | M |
| FR-A7 | US-A4 | — | M |
| FR-A8, FR-A9 | US-A5 | D-B | M |
| FR-A10, FR-A11 | US-A6 | D-D/D-E | M |
| FR-A12 | US-A7 | D-B | M |
| FR-A13 | US-A8 | D-C | S |
| FR-B1 | US-B1 | — | M |
| FR-B2 | US-B2 | D-E | M |
| FR-B3 | US-B3 | D-D | M |
| FR-B4 | US-B4 | D-D | S |
| FR-B5 | US-B5 | D-D | S |
| FR-C1 | US-C1 | D-E | M |
| FR-C2, FR-C6 | US-C2 | D-E | M |
| FR-C3 | US-C3 | D-E | M |
| FR-C4 | US-C4 | D-E | S |
| FR-C5 | US-C5 | — | M |
| FR-C7 | US-C6 | D-E (Q2 fix #4) | M |
| FR-D1 | US-D1 | D-F | M |
| FR-D2 | US-D2 | D-F | S |
| FR-D3 | US-D3 | D-F | M |
| FR-E1 | US-E1 | — | M |
| FR-E2 | US-E2 | — | S |

---

## 7. Kapsam dışı & reddedilen (yeniden açılmaz)

| Öğe | Durum | Kaynak |
|---|---|---|
| Hot central poller | **REDDEDİLDİ** (N-node `fetchAndLock` contention = P1 ihlali) | `06 §5.3` D-A |
| Timer-only escalation | **REDDEDİLDİ** (tespit gecikmesi = SLA; her instance timer-job yazısı) | `06 §6.2` D-D |
| DLQ→ops-only escalation | **REDDEDİLDİ** (token leak) | `06 §6.2` D-D |
| Advisory-tabanlı DLQ tespiti (`MAX_DELIVERIES`) | **REDDEDİLDİ** (best-effort, poison sessizce sıkışır) | `06 §7` D-E |
| Post-commit `lock()` / lazy kilit | **REDDEDİLDİ** (+1 UPDATE / migration guard kaybı) | `06 §5.4` D-C |
| İdiom-başına ayrı DLQ stream | **REDDEDİLDİ** (2× provisioning) | `06 §7` D-E |
| Yalnız-mutlak-QPS / latency-öncelikli metrik | **REDDEDİLDİ** (ortam-bağımlı / dolaylı) | `06 §5.6` D-F |
| InProgress heartbeat | **ERTELENDİ** → D-H (basamak-1 sonrası) | `06 §8` D-H |
| gRPC worker ön kapısı | **ERTELENDİ** → D-G (ayrı belge) | `06 §8` D-G |
| token-move/completion tx kaldırılması | **KAPSAM DIŞI** → basamak-6 (P2) | `05 §6.7` |
| History offload / büyük değişken / DB sharding | **KAPSAM DIŞI** → basamak 2/3/5 | `05 §6.7` |

---

## 8. PO Karar Kaydı (Q→A, 2026-07-14)

Phase-1 PO-QUESTION'larının hepsi Levent tarafından cevaplandı; SRS'i etkileyen kararlar (sorular korunur, cevaplar eklenir):

| # | Soru | Karar | SRS'e etki |
|---|---|---|---|
| **Q1** | Teslimat konumu | `docs/sentinel/phase1/` tek klasör ONAYLANDI | Konum korundu |
| **Q2** | Trace header tutarsızlığı | Basamak-1'de düzeltilir (yazma tek ad, okuma fallback) | **FR-C7** eklendi; §1.2 "4 fix"; IR-2 notu; matris |
| **Q3** | DLQ retention vs PII | 14g default + kiracı-bazlı konfig ONAYLANDI | NFR-S2 korunur; ayrıntı `DATA_CLASSIFICATION.md` DP-3/§5 |
| **Q4** | Business-Key masking | Normatif olmayan öneri (kod değişikliği yok) | IR-2 + NFR-S1 öneri notu; `DATA_CLASSIFICATION.md` DP-7 |
| **Q5** | Field-level PII checklist | Basamak-1 doküman teslimatı (EVET) | `TENANT_PII_CHECKLIST_TEMPLATE.md` referansı |
| **Q6** | "Should" kalemleri kapsam | Altısı da basamak-1'e DAHİL | Tüm S-FR'ler kapsam-içi (§3) |
| **Q7** | Bench p95 sert kapı mı | SLI hedefi; sert kapı yalnız DB-roundtrip | **NFR-P2 → S**; FR-D1 tek sert kapı; FR-D2 SLI notu |

Tam Q→A karar kaydı: `USER_STORIES.md §4`.

**Teknik açık kararlar durumu:** D-A…D-F **çözülü**; D-G/D-H bilinçli ertelenmiş (§7). Phase3'e taşınan doğrulama kalemleri §2.5'te ve ilgili FR notlarında "phase3'te doğrulanacak" olarak işaretlidir.
