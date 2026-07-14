# USER STORIES — Basamak-1: External Task / Event-Driven Work Offload over JetStream

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Sentinel fazı:** Phase 1 — Product Owner
**Kapsam:** `05-db-offload-strategy.md` §6.7 **basamak-1** (Dispatch push) — `06-external-task-over-jetstream.md`'in somutlaştırdığı iş
**Tarih:** 2026-07-14
**Durum:** Onaylı (2026-07-14) — PO-QUESTIONS cevaplandı (bkz. §4 PO Karar Kaydı)

> **Dokümantasyon dili Türkçe; kod/tanımlayıcılar İngilizce.** Motor/adapter davranışına dair her iddia `file:line` kanıtlıdır (`06 §9`'da doğrulanmış kanıtlar temel alındı). Doğrulanmamış varsayımlar açıkça "phase3'te doğrulanacak" etiketlidir. **Effort tahmini içermez** (workspace kuralı).

---

## 0. Kapsam sınırı (bu fazı okurken)

**KAPSAM İÇİ (basamak-1 kod teslimatı, `06 §8` durum notu):** custom `ExternalTaskActivityBehavior` + post-commit publisher (D-A/D-C), soğuk orphan-sweep (D-B), inbound completion-bridge, DLQ bridge'leri (Camunda incident-bridge + Flowable failure-event bridge — D-D/D-E), ortak JetStream substratı + `06 §7` kontrat-fix listesi (**4 fix:** DLQ header kaybı, koşulsuz ack, DLQ dedup id, trace-header adı — Q2 kararı 2026-07-14), Testcontainers yük-bench modülü (D-F), JavaDelegate outbound **tam phase-out**.

**KAPSAM DIŞI (bilinçli, yeniden açılmaz):** hot central poller (D-A'da REDDEDİLDİ), timer-only escalation (D-D'de REDDEDİLDİ), advisory-tabanlı DLQ tespiti (D-E'de REDDEDİLDİ), InProgress heartbeat (D-H — ertelendi), gRPC worker ön kapısı (D-G — ertelendi), token-move/completion transaction'ın kaldırılması (P2 — basamak-6), history offload (basamak-2), büyük değişken externalization (basamak-3), DB sharding (basamak-5).

---

## 1. Persona / roller

| Kod | Persona | Bağlam |
|---|---|---|
| **P1** | **Süreç Geliştirici** | BPMN modelleyen, `camunda:type="external"` topic'i / Event Registry channel tanımlayan entegratör |
| **P2** | **Worker Geliştirici** | Polyglot; JetStream'den job tüketip sonucu `*.reply`/inbound event olarak yazan motor-dışı worker yazarı |
| **P3** | **Platform / Ops (SRE)** | DLQ, incident, connection ve SLI metriklerini izleyen operatör |
| **P4** | **Migrasyon Sahibi** | Camunda 8 lisansından kaçan; Camunda 7 / CadenzaFlow / Flowable üstünde push-tabanlı iş dağıtımı isteyen |
| **P5** | **Bakımcı Mühendis** | 3eAI Labs; DB-offload tezini ölçülebilir kanıtla göstermek isteyen |
| **P6** | **Veri Koruma Sorumlusu** | Payload/header içindeki PII akışını ve saklama süresini yöneten (bkz. `DATA_CLASSIFICATION.md`) |

**Öncelik ölçeği (MoSCoW):** M = Must (basamak-1 kapanışı için zorunlu), S = Should, C = Could. **Q6 kararı (2026-07-14): tüm S kalemleri (US-A8, B4, B5, C4, D2, E2) basamak-1 kapanışına DAHİLDİR** — takip işine ertelenmez; S yalnız göreli önem sırasını gösterir.

---

## 2. Epic haritası

| Epic | Başlık | US aralığı |
|---|---|---|
| **EPIC-A** | Camunda 7 / CadenzaFlow — A2 external-task-over-JetStream | US-A1 … US-A8 |
| **EPIC-B** | Flowable — Event Registry basamak-1 olgunluğu | US-B1 … US-B5 |
| **EPIC-C** | Ortak JetStream substratı & wire-contract (DLQ/ack/dedup + trace-header) | US-C1 … US-C6 |
| **EPIC-D** | Gözlemlenebilirlik & başarı metriği (SLI + bench) | US-D1 … US-D3 |
| **EPIC-E** | JavaDelegate phase-out & idiom netliği | US-E1 … US-E2 |

Toplam: **24 user story.**

---

## EPIC-A — Camunda 7 / CadenzaFlow: A2 external-task-over-JetStream

### US-A1 — fetchAndLock polling'ini JetStream push ile değiştir
**P4 Migrasyon Sahibi** olarak, external task'larımın worker'lara **push** ile dağıtılmasını istiyorum; **böylece** `fetchAndLock` poll/lock storm'u (N worker × `SELECT FOR UPDATE`) kalksın ve motor DB'sinin dispatch yükü DB-dışına taşınsın.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Native external-task **semantiği korunur**: BPMN `camunda:type="external"` topic'i modellenmiş bir aktivite, worker tarafında değişiklik gerekmeden JetStream `jobs.<topic>` üzerinden teslim edilir.
- [ ] Happy-path'te external task doğduğunda **hiçbir `fetchAndLock` sorgusu koşmaz** (ne worker ne merkezi poller). Kanıt: `pg_stat_statements`'ta `fetchAndLock` fingerprint hit sayısı hot-path'te 0 (bkz. US-D1).
- [ ] Worker'a teslim edilen job mesajı `Nats-Msg-Id = externalTaskId` taşır (dedup).
- [ ] Bir A2 topic'i için birden çok worker (queue-group) çalışırken **tek worker** işi alır (WorkQueue claim).
- [ ] Redeliver'da worker kodu değişmeden idempotent tüketebilir (dedup + at-least-once dokümante).
**Dayanak:** `06 §5.1`, `§5.2`, `§5.5` (kalkan: fetchAndLock poll/lock storm); REDDEDİLEN hot poller → `06 §5.3` (yeniden açılmaz).
**Bağımlılık:** US-A2 (kilit doğumda), US-C5 (WorkQueue stream).

---

### US-A2 — External task'ı doğumda in-tx sentinel-kilitli oluştur
**P5 Bakımcı Mühendis** olarak, external task satırının **oluşturulduğu transaction içinde** sentinel workerId ile kilitlenmesini istiyorum; **böylece** completion-yolu ön şartı (kilit) **sıfır ek DB yazısıyla** karşılansın ve migrasyon döneminde legacy poller A2 task'ını fetch edemesin.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Custom `ExternalTaskActivityBehavior`, A2-topic'li aktivitelerde `BpmnParseListener` (`preParseListeners`) ile takılır — **fork motor kodu değişmez** (plugin deseni).
- [ ] `createAndInsert(...)` ile üretilen entity, flush'tan önce `lock(SENTINEL, L)` çağrılır → kilit alanları **aynı INSERT'e biner**; entegrasyon testi ikinci bir `UPDATE` üretilmediğini kanıtlar (bkz. `06 §9` phase3 doğrulaması).
- [ ] `SENTINEL` workerId **küme-geneli tek sabittir** (örn. `a2-jetstream-bridge`); payload'da audit için taşınır.
- [ ] Doğuştan kilitli task, native fetchable-predicate dışındadır → legacy poller onu asla fetch edemez (`ExternalTask.xml:220-222`).
- [ ] `L` (sentinel lockDuration) topic-başına override edilebilir; default **320s** (bkz. US-A5).
**Dayanak:** `06 §5.4` (D-C), `§9` (D-C kanca noktaları DOĞRULANDI): `ExternalTaskEntity.java:568-588` (createAndInsert kilitsiz doğurur), `:472-474` (lock), `BpmnParse.java:2564`, `ProcessEngineConfigurationImpl.java:687,2189`. REDDEDİLEN: post-commit `lock()`, complete-önü lazy kilit (`06 §5.4`) — yeniden açılmaz.
**Bağımlılık:** yok (kanca noktası; US-A1/A3'ün temeli).

---

### US-A3 — Post-commit publish + soğuk orphan-sweep
**P3 Platform/Ops** olarak, external task'ın **onu oluşturan node tarafından commit sonrası (tx dışı) yayınlanmasını** ve çökme kaynaklı yayınlanmamış task'ların **seyrek, read-only** bir sweep ile toplanmasını istiyorum; **böylece** happy-path'te DB sorgusu olmadan at-most-once yayın + net at-least-once teslim garantisi kurulsun.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Task'ı oluşturan node, `TransactionState.COMMITTED` listener'ında elindeki entity ile yayınlar — **DB sorgusu yok, cross-node yarış yok** (herkes yalnız kendi yarattığını yayınlar).
- [ ] Orphan-sweep **tek node/leader**'da koşar; sorgu, engine fetchable-predicate'inin birebir aynısıdır (lock süresi dolmuş/yok **ve** `retries≠0` **ve** süreç askıda değil — `SUSPENSION_STATE_`, yalnız A2 topic'leri) — `SELECT FOR UPDATE` **kullanmaz**.
- [ ] Sweep periyodu `S` yapılandırılabilir; default **120s**.
- [ ] Sweep, yayın öncesi sentinel re-lock yapar (aynı workerId → her zaman geçer, `LockExternalTaskCmd.java:50-61`); DLQ'lanmış (`retries=0`) task'ı **asla** yeniden yayınlamaz.
- [ ] Çift yayın (post-commit + sweep) `Nats-Msg-Id` dedup'u ile stream'de yutulur; pencere dışı çift, inbound complete-idempotency ile yutulur (US-A7).
**Dayanak:** `06 §5.3` (D-A), `§5.4` (sweep kriteri = fetchable-parite). REDDEDİLEN hot central poller (P1 ihlali) — yeniden açılmaz.
**Bağımlılık:** US-A2, US-A5.

---

### US-A4 — Inbound completion-bridge (reply → `externalTaskService.complete`)
**P4 Migrasyon Sahibi** olarak, worker'ın `jobs.<topic>.reply`'a yazdığı sonucun external task'ı **tamamlamasını** (token ilerlesin) istiyorum; **böylece** mevcut inbound correlation altyapısı yeniden kullanılıp iş sonucu motora bağlansın.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Inbound bridge, reply mesajını alıp `externalTaskService.complete(extTaskId, SENTINEL, vars)` çağırır (mevcut `NatsMessageCorrelationSubscriber` `correlateWithResult()` desenini evrilterek — `06 §5.2`).
- [ ] `complete` başarılı olduktan **sonra** reply mesajı ACK'lenir (custody-transfer; bkz. US-C2).
- [ ] Worker business-error dönerse `handleBpmnError(...)`, transient hata için `handleFailure(...)` çağrılır — native lifecycle korunur.
- [ ] `complete`, `SENTINEL` workerId eşitliği ile geçer (`HandleExternalTaskCmd.java:89-91` — yalnız workerId eşitliği kontrol edilir).
- [ ] `complete` = kısa token-move tx **kalır** (P2, dürüst tavan); bu US onu kaldırmayı iddia etmez.
**Dayanak:** `06 §5.2`, `§5.4` madde 1; `06 §9` (`complete()` lock zorunluluğu DOĞRULANDI).
**Bağımlılık:** US-A2 (sentinel kilit — complete ön şartı), US-C2.

---

### US-A5 — Şemsiye kilit / tek redelivery otoritesi (JetStream)
**P5 Bakımcı Mühendis** olarak, redelivery saatinin **tek otorite** olarak JetStream (`ack-wait`=W, `maxDeliver`=M) olmasını ve engine sentinel kilidinin bunları **kapsayan bir şemsiye** olmasını istiyorum; **böylece** iki rakip redelivery saati (engine lock vs JetStream) çakışmasın ve geç gelen complete başarısız olmasın.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Şemsiye koşulu tutar: `L ≥ M·W + Σbackoff + S + ε`.
- [ ] Default değerler yapılandırılabilir ve şu ilişkiyi sağlar: W=30s, M=4, S=120s, ε=60s, Σbackoff=1+2+4=7s → alt sınır **307s**; default **L=320s** (13s marj — phase-review MAJOR-B düzeltmesi 2026-07-14).
- [ ] W topic-başına override edilebilir (uzun işli topic büyük W seçer); L default'u W/M/S/ε'den türetilebilir olmalı (elle tutarsız L yazımına karşı validasyon/uyarı).
- [ ] Heartbeat **yok**; W·M sert tavandır (`msg.inProgress()` ve engine `extendLock` **kullanılmaz** — `ExtendLockOnExternalTaskCmd.java:46-47`).
**Dayanak:** `06 §5.4` (D-B), `§9` (D-B davranışları DOĞRULANDI, fork değişikliği gerektirmez). REDDEDİLEN heartbeat (D-H) — yeniden açılmaz.
**Bağımlılık:** US-A1, US-C5.

---

### US-A6 — DLQ → incident bridge (Cockpit görünürlüğü)
**P3 Platform/Ops** olarak, delivery bütçesi (M) bitip DLQ'ya düşen bir A2 job'ının motorda **incident** üretmesini istiyorum; **böylece** kalıcı worker arızası Cockpit'ten görünür olsun ve operatör retry verebilsin.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] `deliveryCount > M` → DLQ subject'i (`dlq.jobs.<topic>`) + `dlq.jobs.>` tüketen incident-bridge → `handleFailure(..., retries=0)` → **incident** (Cockpit).
- [ ] `retries=0` task fetchable-predicate dışıdır → sweep onu **asla** yeniden yayınlamaz (`ExternalTask.xml:222`).
- [ ] Operatör Cockpit'ten retry verirse task yeniden fetchable olur → sweep doğal olarak yeniden yayınlar (Cockpit-retry JetStream'e geri akar).
- [ ] Tespit gecikmesi = W·M; bu yol için **SLA beklenmez** (dokümante).
**Dayanak:** `06 §5.4` madde 3, `§7` (D-E post-DLQ idiom-özel), `§8` (D-D/D-E).
**Bağımlılık:** US-C1 (DLQ header preservation — incident-bridge correlation için), US-C4.

---

### US-A7 — Geç-complete idempotency
**P2 Worker Geliştirici** olarak, L dolduktan **sonra** ya da çift-teslim sonucu gelen ikinci bir complete'in sistemi bozmamasını istiyorum; **böylece** at-least-once teslimatın bedeli sessizce ve güvenle yutulsun.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] L sonrası gelen reply yine başarılı complete olur (expiry kontrolü yok — `HandleExternalTaskCmd.java:89-91`); sahiplik el değiştirmez (tek `SENTINEL` workerId).
- [ ] Çift işlenmiş işin ikinci complete'i "task yok" ile karşılaşır → **yakalanır + ACK** (idempotency anahtarı = `externalTaskId`).
- [ ] `Nats-Msg-Id` dedup penceresi (`duplicate_window`, default 2dk) < L (320s) olduğu durumda pencere-dışı çiftin **complete-idempotency** tarafından yutulduğu test edilir.
**Dayanak:** `06 §5.4` madde 4 + "Dürüst sınır".
**Bağımlılık:** US-A4.

---

### US-A8 — Migrasyon guard: legacy poller ile bir arada çalışma
**P4 Migrasyon Sahibi** olarak, A2'ye geçiş döneminde çalışmaya devam eden **legacy external-task poller'ların** A2 task'larına dokunamamasını istiyorum; **böylece** geçiş kademeli ve güvenli olsun.
**Öncelik:** S
**Kabul kriterleri:**
- [ ] A2 task'ı doğuştan sentinel-kilitli olduğundan native fetchable-predicate dışıdır → herhangi bir `fetchAndLock` çağrısı onu döndürmez.
- [ ] A2 olmayan (klasik) external task'lar etkilenmez — behavior swap yalnız A2-topic'li aktivitelerde uygulanır.
**Dayanak:** `06 §5.4` "Yan kazançlar (a)".
**Bağımlılık:** US-A2.

---

## EPIC-B — Flowable: Event Registry basamak-1 olgunluğu

### US-B1 — In-tx blocking delegate'i `sendEvent` ile değiştir
**P4 Migrasyon Sahibi** olarak, Flowable outbound işinin motorun DB transaction'ı **dışında** çalışmasını istiyorum; **böylece** in-tx blocking round-trip (tez ihlali) kalksın ve outbound = Event Registry `sendEvent` olsun.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] `requestreply/NatsRequestReplyDelegate.java:19` (in-tx blocking) **kaldırılır**.
- [ ] Outbound `NatsOutboundEventChannelAdapter.sendEvent(...)` üzerinden yapılır (`:29`); iş motor-dışı tüketicide koşar, sonuç inbound event olarak correlate edilir.
- [ ] Native push idiom'u korunur (`NatsInboundEventChannelAdapter.java:49` subscribe + queue-group; `:88` `eventReceived`).
**Dayanak:** `06 §6.2`, `§3` (delegate phase-out kanıtı); `06 §6.1` (native push kanıtı file:line).
**Bağımlılık:** US-E1.

---

### US-B2 — JetStream sağlamlığı: ack + DLQ + dedup
**P3 Platform/Ops** olarak, Flowable JetStream inbound yolunun **ack + DLQ + dedup** ile çalışmasını istiyorum; **böylece** core adapter'ın "ack yok, hatada sadece log" davranışı yerine exactly-once-ish teslimat garantisi gelsin.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] JetStream variant kullanılır (`jetstream/JetStreamInboundEventChannelAdapter.java:152` `eventReceived`); core adapter'ın ack'siz/log-only yolu basamak-1 kritik iş için kullanılmaz.
- [ ] `maxDeliver+1` deseni ile DLQ tespiti çalışır (`:75-77`, `:133-146`).
- [ ] Dedup `Nats-Msg-Id`/correlation idempotency ile yapılır (`06 §7`).
- [ ] Custody-transfer ack ilkesi uygulanır (bkz. US-C2).
**Dayanak:** `06 §6.2`, `§9` (D-E kanıtları DOĞRULANDI: `JetStreamInboundEventChannelAdapter.java:75-77,133-146`).
**Bağımlılık:** US-C1, US-C2, US-C3.

---

### US-B3 — DLQ → failure-event bridge (default escalation)
**P1 Süreç Geliştirici** olarak, worker'ın kalıcı ölümünde DLQ'ya düşen mesajın **aynı correlation key'lerle** bir *failure-event*'e çevrilip Event Registry'ye geri sokulmasını istiyorum; **böylece** bekleyen instance model escalation path'ini işlesin (token leak olmasın).
**Öncelik:** M
**Kabul kriterleri:**
- [ ] DLQ-bridge, DLQ mesajını **aynı correlation key'leri koruyarak** (BpmHeaders) failure-event'e çevirir → `eventRegistry.eventReceived(...)` → bekleyen instance'a correlate olur.
- [ ] Happy-path ek DB maliyeti **sıfır** (timer-job yazısı yok).
- [ ] Tespit gecikmesi = W·M; **SLA beklenmez** (dokümante).
- [ ] Model escalation yakalama biçimleri (event-based gateway / event-registry boundary event / event subprocess) **phase3'te doğrulanacak** (`06 §9` D-D).
**Dayanak:** `06 §6.2` (D-D default). REDDEDİLEN: timer-only, DLQ→ops-only (`06 §6.2`) — yeniden açılmaz.
**Bağımlılık:** US-C1 (DLQ header preservation — failure-event correlate edemezse leak kalır).

---

### US-B4 — Opt-in boundary timer escalation (wall-clock deadline)
**P1 Süreç Geliştirici** olarak, worker canlı ama iş **deadline'ı aşarsa** bunu yakalayan opsiyonel bir boundary timer istiyorum; **böylece** gerçek wall-clock SLA'sı olan modeller saat-tabanlı escalation'a sahip olsun.
**Öncelik:** S
**Kabul kriterleri:**
- [ ] Boundary timer **yalnız gerçek deadline'ı olan modellerde** modellenir (opt-in); default değildir.
- [ ] Timer-job satır maliyeti (`ACT_RU_TIMER_JOB` instance-başına INSERT+DELETE) yalnız o modellerde ödenir — maliyet **phase3'te doğrulanacak** (`06 §9` D-D b).
**Dayanak:** `06 §6.2` (D-D opt-in). REDDEDİLEN: timer-only default — yeniden açılmaz.
**Bağımlılık:** US-B3.

---

### US-B5 — Geç-sonuç politikası (drop / non-interrupting)
**P2 Worker Geliştirici** olarak, escalation sonrası gelen **geç sonucun** öngörülebilir davranmasını istiyorum; **böylece** eşleşecek subscription bulunmadığında sessiz veri kaybı ya da hata patlaması olmasın.
**Öncelik:** S
**Kabul kriterleri:**
- [ ] Escalation interrupting ise geç sonuç correlate edecek subscription bulamaz → **ack + log + metric** (drop).
- [ ] Non-interrupting modellenirse geç sonuç yine işlenir (model kararı).
- [ ] `eventReceived`'ın eşleşmeyen (geç) event'teki davranışı (sessiz drop mu, hata mı) **phase3'te doğrulanacak** (`06 §9` D-D c).
**Dayanak:** `06 §6.2` "Geç-sonuç politikası".
**Bağımlılık:** US-B3.

---

## EPIC-C — Ortak JetStream substratı & wire-contract

### US-C1 — DLQ mesajında orijinal header'ları koru (contract-fix #1)
**P3 Platform/Ops** olarak, DLQ'ya yönlendirilen mesajın **orijinal header'larını aynen** taşımasını istiyorum; **böylece** correlation key'siz DLQ mesajı yüzünden failure-event bridge (US-B3) / incident-bridge (US-A6) correlate edememesi sorunu ortadan kalksın.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] `publishToDlq`, orijinal payload byte'larını **ve** orijinal header'ların tamamını kopyalar (mevcut kod yalnız `msg.getData()` yayınlıyor — `JetStreamInboundEventChannelAdapter.java:218,227` / cadenzaflow `JetStreamMessageCorrelationSubscriber.java:210,219`).
- [ ] Ek meta header'lar eklenir: `X-Cadenzaflow-Dlq-Original-Subject`, `-Dlq-Delivery-Count`, `-Dlq-Reason`, `-Dlq-Timestamp`.
- [ ] DLQ subject şeması `dlq.<orijinal-subject>`.
- [ ] Test: DLQ mesajından correlation key okunabildiği ve bridge'in correlate edebildiği doğrulanır.
**Dayanak:** `06 §7` (contract açık #1), `§9` (D-E kanıtları DOĞRULANDI). Bu fazda **bizzat doğrulandı:** `publishToDlq` `:218/:227` yalnız `data` yayınlıyor, header yok.
**Bağımlılık:** yok (fix; US-A6/US-B3 buna bağlı).

---

### US-C2 — Custody-transfer ack: koşulsuz ack'i kaldır (contract-fix #2)
**P3 Platform/Ops** olarak, mesajın **yalnız kalıcılık el değiştirdikten sonra** ACK'lenmesini istiyorum; **böylece** DLQ-publish başarısızlığında yaşanan **sessiz poison-mesaj kaybı** (custody-transfer ihlali) ortadan kalksın.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Ack sırası: worker → reply-PubAck-sonrası-ack (business-error da bir reply'dır → error-reply → ack); engine-inbound → complete/correlate dönüşü-sonrası-ack; DLQ yolu → **DLQ-PubAck-sonrası-ack**.
- [ ] `dlqSubject == null` iken mesaj **discard edilmez**; DLQ publish başarısızsa **nak** edilir (mevcut kod `dlqSubject==null`'da discard + koşulsuz `msg.ack()` yapıyor — `JetStreamInboundEventChannelAdapter.java:141-145,211-214` / cadenzaflow `:123-127`).
- [ ] `dlq-of-dlq` **yok**: bridge DLQ mesajını işleyemezse **nak + alert**, asla ack-drop.
- [ ] Transient hata → `nakWithDelay` (üstel backoff `2^(n-1)`s, cap 30s — mevcut ortak desen `:204-208`).
**Dayanak:** `06 §7` (contract açık #2), custody-transfer ilkesi. Bu fazda **bizzat doğrulandı:** `:211-214` `dlqSubject==null` → `return` sonra caller `:145` `msg.ack()`; `:222-235` both-publish-fail sadece log, yine ack.
**Bağımlılık:** yok (fix).

---

### US-C3 — DLQ publish'te `Nats-Msg-Id` (contract-fix #3)
**P3 Platform/Ops** olarak, DLQ publish'inin `Nats-Msg-Id = <orijinal-msg-id>.dlq` taşımasını istiyorum; **böylece** çökme-sonrası çift DLQ kaydı (duplicate) engellensin.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Her DLQ publish `Nats-Msg-Id = <orijinal-msg-id>.dlq` ile yapılır (mevcut kod DLQ publish'te `Nats-Msg-Id` set etmiyor — `:218`).
- [ ] Aynı poison mesajın iki kez DLQ'ya yönlenmesi durumunda stream dedup tek kayıt tutar (pencere içinde).
**Dayanak:** `06 §7` (contract açık #3). Bu fazda **bizzat doğrulandı:** `publishToDlq` `:218` `jetStream.publish(dlqSubject, data)` — id yok.
**Bağımlılık:** US-C1.

---

### US-C4 — Tek ortak DLQ stream topolojisi (`dlq.>`)
**P3 Platform/Ops** olarak, tüm DLQ trafiğinin **tek** `DLQ` stream'inde (`dlq.>`) toplanmasını ve tüketicilerin subject filtresiyle ayrışmasını istiyorum; **böylece** ops tek yüzeyden izlesin, idiom-başına ayrı stream provisioning/izleme yükü olmasın.
**Öncelik:** S
**Kabul kriterleri:**
- [ ] Tek `DLQ` stream, retention **limits-based** (default 14 gün), **WorkQueue değil**.
- [ ] `dlq.jobs.>` → Camunda/CadenzaFlow incident-bridge (US-A6); event-channel DLQ'ları → Flowable failure-event bridge (US-B3).
- [ ] İdiom-başına ayrı DLQ stream **kullanılmaz** (REDDEDİLDİ — `06 §7`).
**Dayanak:** `06 §7` (D-E topoloji).
**Bağımlılık:** US-C1.

---

### US-C5 — İş dağıtımı için WorkQueue stream + dedup penceresi
**P2 Worker Geliştirici** olarak, iş dağıtım stream'inin **WorkQueue** tipinde (her mesaj tek tüketici, nack→redeliver) ve `Nats-Msg-Id` dedup pencereli olmasını istiyorum; **böylece** tek-worker-alır semantiği ve idempotent teslim garanti altına alınsın.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] `jobs.<type>` / event channel subject'leri **WorkQueue** stream'inde tanımlıdır.
- [ ] `Nats-Msg-Id` dedup: A2'de `externalTaskId`, Event Registry'de correlation key.
- [ ] Stream `duplicate_window` yapılandırılabilir; default 2dk olduğu ve L>window durumunda pencere-dışı çiftlerin apply-zamanı idempotency ile yutulduğu dokümante (US-A7).
- [ ] Subject şeması: `jobs.<type>` + `*.reply` (A2) / inbound channel (Event Registry).
**Dayanak:** `06 §7`, `05 §7` (ortak wire-contract).
**Bağımlılık:** yok (substrat temeli).

---

### US-C6 — Trace header adı standardizasyonu (contract-fix #4)
**P2 Worker Geliştirici** olarak, trace header adının tel boyunca **tutarlı** olmasını ve geçiş döneminde eski/yeni adın birlikte çalışmasını istiyorum; **böylece** izleme korelasyonu (`Trace-Id`) kopmasın.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] **Yazma tarafı** yalnız `X-Cadenzaflow-Trace-Id` üretir (`BpmHeaders.java:12`); hiçbir yerde `X-Trace-Id` yazımı kalmaz.
- [ ] **Okuma tarafı** iki adı da kabul eder (fallback): önce `X-Cadenzaflow-Trace-Id`, yoksa `X-Trace-Id` (mevcut MDC okuması `JetStreamInboundEventChannelAdapter.java:119` — eski üreticilerle geriye uyum).
- [ ] MDC/trace korelasyonu her iki header adında da çalışır; test her iki girdiyle doğrular.
**Dayanak:** `06 §7` kontrat-fix #4 (Q2 kararı 2026-07-14). Bu fazda **bizzat gözlemlendi:** okuma `JetStreamInboundEventChannelAdapter.java:119` `X-Trace-Id`; standart yazım `BpmHeaders.java:12` `X-Cadenzaflow-Trace-Id`.
**Bağımlılık:** yok (fix).

---

## EPIC-D — Gözlemlenebilirlik & başarı metriği

### US-D1 — Normalize birincil metrik: task-yaşamdöngüsü başına DB round-trip
**P5 Bakımcı Mühendis** olarak, basamak-1'in DB kazancını **donanım-bağımsız** bir metrikle (task başına DB round-trip) ölçmek istiyorum; **böylece** "poll + fetchAndLock bileşenleri = 0, INSERT/complete artmıyor" iddiası doğrudan kanıtlansın.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Metrik bileşenleri raporlanır: Task INSERT (1, sentinel kilit dahil), Poll sorguları (**0**), `fetchAndLock` UPDATE (**0**), `complete` token-move tx (1 — dürüst tavan), Sweep okuması (amortize ≈ ~0).
- [ ] Ölçüm `pg_stat_statements` fingerprint sayaçları (veya datasource-proxy) ile yapılır; `fetchAndLock` SQL'inin ayrı fingerprint verdiği **phase3'te doğrulanacak** (`06 §9` D-F).
- [ ] Kabul: birincil metrikte poll + fetchAndLock bileşenleri **0**, INSERT/complete bileşenleri **artmıyor**.
- [ ] **Bu normalize DB-roundtrip metriği, basamak-1 kapanışının TEK sert kabul kapısıdır** (Q7 kararı 2026-07-14); latency SLI'ları (US-D2) sert kapı değildir.
**Dayanak:** `06 §5.6` (D-F birincil metrik). REDDEDİLEN: yalnız-mutlak-QPS, latency-öncelikli metrik — yeniden açılmaz.
**Bağımlılık:** US-A1, US-A2, US-A4.

---

### US-D2 — Destekleyici SLI katmanı
**P3 Platform/Ops** olarak, ops gerçeğini yansıtan destekleyici SLI'lar istiyorum; **böylece** tez ayakları (connection-tutma, lock-wait, dispatch latency, failure sayaçları) sürekli izlenebilsin.
**Öncelik:** S
**Kabul kriterleri:**
- [ ] `fetchAndLock` QPS: hot-path **0/s**; yalnız sweep ≤ 1 FOR-UPDATE'siz read / 120s / cluster.
- [ ] `ACT_RU_EXT_TASK` lock-wait ~0 (`pg_locks` / `innodb_row_lock_waits`).
- [ ] HikariCP aktif connection aynı yükte düşer (connection-tutma ayağı).
- [ ] Dispatch latency (commit → worker deliver): **p95 ≤ 200ms** — **izlenen SLI hedefi, sert kabul kapısı DEĞİL** (Q7 kararı 2026-07-14; sert kapı yalnız US-D1 normalize DB-roundtrip metriği).
- [ ] Failure-path sayaçları: mevcut `NatsChannelMetrics` (dlq/nak/ack + `processingTimer` — `NatsChannelMetrics.java:25-63`) + sweep-republish sayacı + en-yaşlı-orphan yaşı.
- [ ] Yeni sayaçlar mevcut Micrometer tabanı üstüne kurulur (`nats-core/.../metrics/NatsChannelMetrics.java`).
**Dayanak:** `06 §5.6` (destekleyici SLI'lar), `§9` (D-F altyapı DOĞRULANDI: Micrometer tabanı mevcut).
**Bağımlılık:** US-D1.

---

### US-D3 — Testcontainers yük-bench modülü (native-poll ↔ A2-push)
**P5 Bakımcı Mühendis** olarak, aynı senaryoyu **iki modda** (native-poll baseline ↔ A2-push) koşan bir bench modülü istiyorum; **böylece** baseline+hedef kontrollü üretilsin ve kapanış kriteri muğlak kalmasın.
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Bench modülü PG + engine + NATS + N simüle worker ayağa kaldırır (Testcontainers); dört modülde mevcut entegrasyon-test altyapısı üstüne kurulur (`06 §9` D-F DOĞRULANDI).
- [ ] Aynı senaryo native-poll ve A2-push modlarında koşar; `@Tag("bench")`, CI'da nightly/manuel.
- [ ] Bench çıktısı US-D1 birincil metriğini iki mod için üretir; kabul kriteri US-D1'deki tablo hedefleri.
- [ ] Bench, basamak-1 kod teslimatına **dahildir**.
**Dayanak:** `06 §5.6` (metodoloji). REDDEDİLEN: bench'siz staging ölçümü — yeniden açılmaz.
**Bağımlılık:** US-D1, US-A1.

---

## EPIC-E — JavaDelegate phase-out & idiom netliği

### US-E1 — JavaDelegate outbound tam phase-out (üç motor)
**P5 Bakımcı Mühendis** olarak, JavaDelegate tabanlı outbound'un (senkron dahil) **tamamen** kaldırılmasını istiyorum; **böylece** iş motorun DB transaction'ı içinde koşmasın (tez ihlali kalksın).
**Öncelik:** M
**Kabul kriterleri:**
- [ ] Camunda: `NatsPublishDelegate.java:17`, `JetStreamPublishDelegate.java:17`, `NatsRequestReplyDelegate.java:19` kaldırılır (üçü de `implements JavaDelegate` → in-tx).
- [ ] CadenzaFlow: karşılık gelen üç delegate kaldırılır (`cadenzaflow-nats-channel/.../outbound/`).
- [ ] Flowable: `requestreply/NatsRequestReplyDelegate.java:19` kaldırılır.
- [ ] Fast-RPC istisnası **yok** (2026-06-28 kararı — `05 §9`); in-transaction blocking round-trip (`NatsRequestReplyDelegate.java:56`, timeout 30s) tamamen elenir.
- [ ] Migrasyon rehberi: delegate kullanan modeller A2 (Camunda/CadenzaFlow) / Event Registry (Flowable) idiom'una taşınır.
**Dayanak:** `06 §1.1`, `§3` (phase-out kanıtı file:line), `05 §9` (fast-RPC istisnası kaldırıldı).
**Bağımlılık:** US-A1 (Camunda/CadenzaFlow ikamesi), US-B1 (Flowable ikamesi).

---

### US-E2 — İdiom ayrımı netliği (A2 vs saf message-correlation)
**P1 Süreç Geliştirici** olarak, **iş dağıtımı** (A2 / Event Registry) ile **saf olay-bekleme** (message-correlation) arasındaki ayrımın net dokümante edilmesini istiyorum; **böylece** yanlış idiom seçimiyle (dispatch için message-correlation) tez ihlal edilmesin.
**Öncelik:** S
**Kabul kriterleri:**
- [ ] Doküman netleştirir: request-reply / iş dağıtımı → **A2** (external-task: lifecycle + Cockpit + `taskId`); saf message-correlation yalnız **gerçek dış event/callback/business-event** bekleme için kalır.
- [ ] Inbound subscriber'ın `correlateWithResult()` yapısı atılmaz → A2 completion-bridge'e evrilir (US-A4); saf event-bekleme yolu için korunur.
- [ ] `04-async-request-reply-design.md`'in message-correlation idiom'unun iş dağıtımı için **geçersiz** kılındığı (A2 supersede) belirtilir.
**Dayanak:** `06 §1.5`.
**Bağımlılık:** US-A4.

---

## 3. İzlenebilirlik özeti (US → açık karar / kanıt)

| US | İlgili açık karar (çözüldü) | Birincil kanıt |
|---|---|---|
| US-A1 | — (basamak-1 çekirdek) | `06 §5.1/§5.5` |
| US-A2 | D-C | `ExternalTaskEntity.java:568-588,472-474`, `BpmnParse.java:2564` |
| US-A3 | D-A | `06 §5.3`; `ExternalTask.xml:220-222` |
| US-A4 | — | `HandleExternalTaskCmd.java:89-91` |
| US-A5 | D-B | `06 §5.4`; `ExtendLockOnExternalTaskCmd.java:46-47` |
| US-A6 | D-D/D-E | `ExternalTask.xml:222`; `06 §7` |
| US-A7 | D-B | `HandleExternalTaskCmd.java:89-91` |
| US-A8 | D-C | `06 §5.4` yan kazanç (a) |
| US-B1 | — | `06 §3/§6.2`; `NatsOutboundEventChannelAdapter.java:29` |
| US-B2 | D-E | `JetStreamInboundEventChannelAdapter.java:75-77,133-146` |
| US-B3 | D-D | `06 §6.2` (phase3: yakalama biçimleri) |
| US-B4 | D-D | `06 §6.2` (phase3: timer maliyeti) |
| US-B5 | D-D | `06 §6.2` (phase3: `eventReceived` geç davranış) |
| US-C1 | D-E | `JetStreamInboundEventChannelAdapter.java:218,227` (bizzat doğrulandı) |
| US-C2 | D-E | `:141-145,211-214,222-235` (bizzat doğrulandı) |
| US-C3 | D-E | `:218` (bizzat doğrulandı) |
| US-C4 | D-E | `06 §7` topoloji |
| US-C5 | — | `06 §7`, `05 §7` |
| US-C6 | D-E (Q2 fix #4) | `JetStreamInboundEventChannelAdapter.java:119` (okuma) vs `BpmHeaders.java:12` (yazma) |
| US-D1 | D-F | `06 §5.6` (phase3: fingerprint izolasyonu) |
| US-D2 | D-F | `NatsChannelMetrics.java:25-63` |
| US-D3 | D-F | `06 §5.6` metodoloji |
| US-E1 | — | `06 §3`; delegate `:17/:19/:56` |
| US-E2 | — | `06 §1.5` |

---

---

## 4. PO Karar Kaydı (Q→A, 2026-07-14)

Phase-1 kapanışında Levent'e sunulan 7 PO-QUESTION ve verilen kararlar (sorular korunur, cevaplar eklenir):

| # | Soru (PO-QUESTION) | Verilen karar (2026-07-14) | Bu fazda uygulanışı |
|---|---|---|---|
| **Q1** | Teslimat konumu: `docs/sentinel/phase1/` mi, idiom-başına ayrı mı? | **Tek klasör, faz-bazlı ONAYLANDI** — `docs/sentinel/phase1/` kalır; taşıma yok. İdiom ayrımı epic düzeyinde yeterli. | Konum korundu; EPIC-A (A2) / EPIC-B (Flowable) ayrımı epic düzeyinde. |
| **Q2** | `X-Trace-Id` ↔ `X-Cadenzaflow-Trace-Id` tutarsızlığı basamak-1'de mi düzeltilsin? | **Basamak-1'de düzeltilir** — kontrat-fix listesine 4. madde (docs/06 §7). Kural: okuma iki adı da kabul (fallback), yazma yalnız `X-Cadenzaflow-Trace-Id`. | **US-C6** eklendi; §0 kapsam "4 fix"; izlenebilirlik tablosu güncellendi. |
| **Q3** | DLQ retention (14g) vs PII minimizasyonu? | **14g default + kiracı-bazlı konfig ONAYLANDI.** | `DATA_CLASSIFICATION.md` DP-3 + §5 notu: PII işleyen kiracı retention'ı kısaltmalı / erişimi kısıtlamalı. |
| **Q4** | `Business-Key` masking/hashing kontrata girsin mi? | **Normatif olmayan öneri-notu** (hash/mask önerilir, karar kiracının) — **kod değişikliği YOK.** | `SRS.md` IR-2/NFR-S1 + `DATA_CLASSIFICATION.md` DP-7 öneri notu olarak hizalandı. |
| **Q5** | Field-level PII checklist template'i basamak-1 teslimatı mı? | **EVET** — `TENANT_PII_CHECKLIST_TEMPLATE.md` oluşturulur. | Yeni dosya `docs/sentinel/phase1/TENANT_PII_CHECKLIST_TEMPLATE.md`; `DATA_CLASSIFICATION.md §6` referans verir. |
| **Q6** | "Should" kalemleri basamak-1'e dahil mi? | **Altısı da (US-A8, B4, B5, C4, D2, E2) DAHİL.** | §1 MoSCoW notu güncellendi; S kalemleri kapsam-içi. |
| **Q7** | Bench p95 ≤ 200ms sert kapı mı? | **İzlenen SLI hedefi, sert kapı DEĞİL.** Sert kapı yalnız normalize DB-roundtrip metriği. | US-D1 tek sert kapı olarak işaretlendi; US-D2 p95 SLI-hedefi olarak yumuşatıldı. |

---

*Detaylı fonksiyonel/non-fonksiyonel gereksinim türetimi: `SRS.md`. PII ve veri hassasiyeti eşlemesi: `DATA_CLASSIFICATION.md`. Kiracı-entegrasyon PII şablonu: `TENANT_PII_CHECKLIST_TEMPLATE.md`.*
