# BUSINESS LOGIC — Basamak-1: External Task / Event-Driven Work Offload over JetStream

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Sentinel fazı:** Phase 2 — Business Analyst
**Girdi:** `docs/sentinel/phase1/USER_STORIES.md` (24 US, 5 epic), `SRS.md` (30 FR + 23 NFR + 6 IR), `docs/06-external-task-over-jetstream.md` (D-A…D-F)
**Tarih:** 2026-07-14
**Durum:** Onaylı (2026-07-14) — BAQ-1…8 cevaplandı + phase-review KOŞULLU ONAY koşulları karşılandı

> Bu belge SRS'in **ne**'sini iş kuralına, süreç akışına ve durum makinesine çevirir. Her kural bir US/FR'ye bağlanır (§8 izlenebilirlik). Motor/adapter davranış iddiaları `file:line` kanıtlıdır — bu fazda **bizzat yeniden doğrulanan** kanıtlar `[BA-VERIFIED]` etiketiyle işaretlidir (phase1'in doğruladıklarının üstüne, bu fazda ek kaynak dosyaları okunarak teyit edildi); doğrulanamayan/phase3'e kalan iddialar `[phase3'te doğrulanacak]` etiketlidir. **Effort tahmini içermez.** Reddedilen kararlar (hot-poll, timer-only, advisory-DLQ, heartbeat/D-H, gRPC/D-G) bu belgede **yeniden açılmamıştır** — yalnız referans olarak anılır.

---

## 0. Kapsam ve yöntem notu

Bu iş mantığı belgesi 24 user story'nin tamamını kapsar (EPIC-A…E). BA Guideline §1 "Edge Case Rule" gereği her gereksinim için en az 3 hata/kenar-durum senaryosu tanımlanmıştır; bu süreçte SRS/US'de açıkça çözülmemiş **5 yeni kenar-durum bulgusu** ortaya çıktı — 4'ü kaynak koddan bizzat doğrulandı, 1'i tasarım analizinden türetildi (açık liste — review MINOR-2 düzeltmesi 2026-07-14): **(1)** sweep re-lock→re-publish sıralama açığı — BAQ-1 / BR-A2-013 (`LockExternalTaskCmd.java:50-61`); **(2)** Cockpit-retry residual-lock gecikmesi — BAQ-2 / BR-A2-010 (`SetExternalTaskRetriesCmd.java:48-51` lockExpiration'a dokunmaz); **(3)** boş-mesaj-gövdesi sessiz-ack — BAQ-5 (`JetStreamInboundEventChannelAdapter.java:124-131`); **(4)** sentinel workerId çakışma invariant'ı — BR-A2-003 / `SYS_SENTINEL_WORKER_CONFLICT` (`HandleExternalTaskCmd.java:89-91`); **(5)** `jobs.*` namespace çakışması — BAQ-4 (tasarım-türevi, kod-çıpasız). Bunlar ilgili iş kuralına eklenmiş ve §9 BA-QUESTIONS'a taşınmıştır. Bunlar **kapsam genişletmesi değildir**; mevcut US/FR'lerin içindeki tanımsız kenar durumlardır.

---

## 1. Süreç akışları (Mermaid)

### 1.1 A2 — Doğum, kilit, post-commit yayın, soğuk sweep (US-A1, A2, A3, A5, A8)

```mermaid
flowchart TD
    A["BPMN aktivite: camunda:type='external', A2 topic"] --> B["BpmnParseListener (preParseListeners) ExternalTaskActivityBehavior'ı takar"]
    B --> C["execute(): createAndInsert(...) — kilitsiz doğum"]
    C --> D["aynı tx içinde: task.lock(SENTINEL, L)"]
    D --> E["TransactionContext.addTransactionListener(COMMITTED, publish(task))"]
    E --> F{"COMMIT başarılı mı?"}
    F -->|Hayır - node COMMIT öncesi çöktü| G["Task hiç insert edilmedi — yok say"]
    F -->|Evet| H["COMMITTED listener tetiklenir: node kendi elindeki task'ı DB SORGUSUZ yayınlar"]
    H --> I["JetStream jobs.&lt;topic&gt; (WorkQueue, Nats-Msg-Id=externalTaskId)"]
    H -.->|"Çökme: commit oldu, publish() çalışmadı (node crash / broker unreachable)"| J["Orphan: DB'de var, JetStream'de yok"]
    J --> K["Soğuk orphan-sweep (S=120s, tek node/leader, FOR-UPDATE'siz read)"]
    K --> L{"Fetchable-parite: LOCK_EXP_ null/&lt;=now AND RETRIES_ null/&gt;0 AND SUSPENSION_STATE_ null/=1 AND A2 topic?"}
    L -->|Evet| M["Re-lock(SENTINEL,L) + re-publish"]
    L -->|"Hayır (retries=0 / suspended / lock taze)"| N["Atla (bkz. Karar Matrisi 3)"]
    M --> I
    I --> O["Legacy fetchAndLock poller: task doğuştan kilitli → asla döndürmez (US-A8)"]
```

**Not:** `createAndInsert` kilitsiz doğurur + entity döndürür — `[BA-VERIFIED]` `ExternalTaskEntity.java:568-588` (fork: `~/Workspaces/cadenzaflow/.../persistence/entity/ExternalTaskEntity.java`); `lock(workerId,lockDuration)` yalnız iki alan setter'ı — `[BA-VERIFIED]` `:471-474` (`workerId` + `lockExpirationTime` set edilir, ayrı DB çağrısı yok, flush ile aynı INSERT'e biner).

### 1.2 A2 — Completion yolu (US-A4, A7)

```mermaid
flowchart TD
    A["Worker: iş biter"] --> B["jobs.&lt;topic&gt;.reply'a yayınla (Nats-Msg-Id=externalTaskId)"]
    B --> C["Worker job mesajını ACK'ler (reply-önce-ack)"]
    C --> D["Engine-inbound bridge reply'ı tüketir"]
    D --> E{"Reply türü?"}
    E -->|Başarı| F["complete(extTaskId, SENTINEL, vars)"]
    E -->|BPMN business error| G["handleBpmnError(extTaskId, SENTINEL, errorCode, ...)"]
    E -->|Transient hata (worker error-reply'da işaretledi)| H["handleFailure(extTaskId, SENTINEL, ...)"]
    F --> I{"Task bulundu mu? (findExternalTaskById)"}
    I -->|Hayır — NotFoundException| J["Yakala + log WARN + ACK (idempotent yut, US-A7)"]
    I -->|Evet| K{"workerId eşit mi? (validateWorkerViolation)"}
    K -->|"Hayır — BadUserRequestException (ASLA OLMAMALI, invariant)"| L["CRITICAL: log ERROR + on-call page — invariant ihlali (BAQ-7 kararı: 2026-07-14)"]
    K -->|Evet| M["complete() çalışır → token ilerler"]
    M --> N["reply mesajı ACK'lenir (complete-sonrası-ack, custody-transfer)"]
    G --> N
    H --> N
    J -.->|"reply mesajı da ACK'lenir — idempotent yut"| N
```

**Not:** `execute()` akışı — `[BA-VERIFIED]` `HandleExternalTaskCmd.java:44-68`: satır 48-50 `findExternalTaskById` null → `EnsureUtil.ensureNotNull(NotFoundException.class, ...)`; satır 52-53 `validateWorkerViolation` true → `throw new BadUserRequestException(...)`. **Bu iki exception FARKLI tiptedir** — bridge implementasyonu bunları ayrı `catch` blokları ile ayırt edebilir (bkz. EXCEPTION_CODES.md RES_EXTERNAL_TASK_NOT_FOUND vs SYS_SENTINEL_WORKER_CONFLICT).

### 1.3 A2 — DLQ → incident → Cockpit-retry (US-A6)

```mermaid
flowchart TD
    A["jobs.&lt;topic&gt;.reply consumer: deliveryCount &gt; M"] --> B["publishToDlq: dlq.jobs.&lt;topic&gt; (header+Nats-Msg-Id korunur — US-C1/C3)"]
    B --> C["Orijinal reply mesajı ACK'lenir (DLQ-PubAck-sonrası-ack)"]
    C --> D["DLQ-bridge (dlq.jobs.&gt; tüketir)"]
    D --> E["handleFailure(extTaskId, SENTINEL, retries=0, retryDuration=0) — BAQ-2 kararı: sabit 0"]
    E --> F["setRetriesAndManageIncidents(0): areRetriesLeft() idi + şimdi 0 → createIncident(); lockExpirationTime=now+0=now (ANINDA süresi dolmuş)"]
    F --> G["Cockpit: incident görünür"]
    G --> H["Operatör Cockpit'ten Retry verir (ne zaman olursa olsun)"]
    H --> I["SetExternalTaskRetriesCmd.execute(): setRetriesAndManageIncidents(retries&gt;0) — lockExpirationTime'a dokunmaz ama zaten now'a eşitlenmişti"]
    I --> J["Satır ANINDA fetchable-parite sağlar (LOCK_EXP_TIME_ ≤ now zaten doğru) — sweep bir sonraki S döngüsünde re-lock+re-publish eder, gecikme YOK"]
```

**Not:** `[BA-VERIFIED]` `ExternalTaskEntity.java:402-419` (`failed(...)`: `lockExpirationTime = now + retryDuration`, ardından `setRetriesAndManageIncidents(retries)`); `:443-452` (`setRetriesAndManageIncidents`: `areRetriesLeft() && retries<=0 → createIncident()`; `!areRetriesLeft() && retries>0 → removeIncidents(true)`); `[BA-VERIFIED]` `SetExternalTaskRetriesCmd.java:48-51` (Cockpit-retry komutu **yalnız** `setRetriesAndManageIncidents(retries)` çağırır — `lockExpirationTime`'a **dokunmaz**). **BAQ-2 kararı (2026-07-14):** DLQ→incident bridge'in `handleFailure` çağrısı **daima `retryDuration=0`** kullanır → `lockExpirationTime = now` (uygulama açısından zaten geçmiş/eşit) → Cockpit-retry ne zaman verilirse verilsin satır **anında** fetchable-parite'i sağlar, residual-lock gecikmesi kalmaz. Bu, LLD/implementasyon seviyesinde bir sabit-parametre kararıdır (Phase 4'te `handleFailure` çağrı sitesine sabitlenir); BR-A2-010 buna göre güncellendi.

### 1.4 Flowable — Outbound/inbound + JetStream sağlamlığı (US-B1, B2)

```mermaid
flowchart TD
    A["Service-task / send-event aktivite"] --> B["NatsOutboundEventChannelAdapter.sendEvent(...) — motor DIŞI"]
    B --> C["JetStream jobs.&lt;type&gt; / event channel (WorkQueue)"]
    C --> D["Worker (motor-dışı, polyglot) işi yapar"]
    D --> E["Sonucu inbound channel'a yazar"]
    E --> F["JetStreamInboundEventChannelAdapter.handleMessage(msg)"]
    F --> G{"data boş mu?"}
    G -->|"Evet (BAQ-5 kararı: contract-fix #5)"| H["log WARN + metrics.dlqCount++ + publishToDlq + ACK (sessiz DEBUG+ack KALKAR)"]
    G -->|Hayır| I{"deliveryCount &gt; maxDeliver?"}
    I -->|Evet| J["publishToDlq + metrics.dlqCount + ACK"]
    I -->|Hayır| K["eventRegistry.eventReceived(inboundChannelModel, event)"]
    K --> L{"Exception?"}
    L -->|Hayır| M["metrics.ackCount + ACK"]
    L -->|Evet| N["metrics.nakCount + nakWithBackoff(2^(n-1)s, cap 30s)"]
```

**Not:** `[BA-VERIFIED]` bütün akış `JetStreamInboundEventChannelAdapter.java:118-176` (bizzat okundu, bu fazda). `NatsRequestReplyDelegate` (in-tx blocking) bu akıştan **çıkarılır** (US-B1/E1). **BAQ-5 kararı (2026-07-14):** boş mesaj gövdesi artık **5. kontrat açığı** olarak ele alınır — sessiz `log DEBUG + ACK` davranışı kalkar; yeni davranış WARN log + DLQ'ya yönlendirme + ACK (bkz. BR-SUB-007). *(LLD rafinesi 2026-07-15, phase4-review NIT-5: ACK **koşulludur** — yalnız DLQ publish başarılıysa; publish başarısızsa nak, asla ack-drop — BR-SUB-002/custody-transfer ile tutarlı.)* Aynı düzeltme cadenzaflow `JetStreamMessageCorrelationSubscriber.java:107-114`'teki özdeş desene de uygulanır (A2 tarafı da aynı açığı taşıyordu).

### 1.5 Flowable — Katmanlı escalation (US-B3, B4, B5)

```mermaid
flowchart TD
    A["Worker jobs-consumer: deliveryCount &gt; M"] --> B["DLQ (dlq.&lt;event-channel-subject&gt;, header+Nats-Msg-Id korunur)"]
    B --> C["Failure-event bridge (event-channel DLQ tüketir)"]
    C --> D["Aynı correlation key'lerle (BpmHeaders) failure-event oluştur"]
    D --> E["eventRegistry.eventReceived(...) — failure-event olarak geri sok"]
    E --> F{"Bekleyen subscription bulundu mu?"}
    F -->|"Evet (event-based gateway / boundary event / event subprocess — phase3'te doğrulanacak)"| G["Model escalation path'i işler"]
    F -->|"Hayır (BAQ-8, NFR-R6 token-leak riski)"| H["RES_FAILURE_EVENT_CORRELATION_MISS — WARN + metrik (tek olay: muhtemel benign yarış); süreklilik/eşik aşımı ayrı alarm tetikler (BAQ-8 kararı)"]
    I["Opt-in: BPMN boundary timer (yalnız gerçek wall-clock SLA'lı modeller)"] -.->|"Deadline aşıldı (worker canlı olsa bile)"| J["Timer tetiklenir — DLQ'dan BAĞIMSIZ"]
    K["Geç sonuç (worker/DLQ-bridge'den sonra gelen event)"] --> L{"Escalation interrupting mi?"}
    L -->|Evet| M["Subscription yok → ACK + log + metric (drop, US-B5)"]
    L -->|Hayır| N["Subscription hâlâ var → işlenir (model kararı)"]
```

### 1.6 Ortak DLQ/ack wire-contract düzeltmeleri (US-C1…C6 + BAQ-5 kararı: 5. fix)

```mermaid
flowchart TD
    Z["FIX#5 (BAQ-5, 2026-07-14): boş mesaj gövdesi → WARN + DLQ'ya yönlendir (BR-SUB-007)"] --> A
    A["Mesaj işlenemedi / deliveryCount&gt;M / boş body"] --> B["publishToDlq(msg)"]
    B --> C["FIX#1 (US-C1): orijinal header'lar AYNEN kopyalanır + meta header'lar eklenir (Dlq-Original-Subject/-Delivery-Count/-Reason/-Timestamp)"]
    C --> D["FIX#3 (US-C3): Nats-Msg-Id = &lt;orijinal-msg-id&gt;.dlq"]
    D --> E{"DLQ publish (JetStream) başarılı mı?"}
    E -->|Evet| F["Orijinal mesaj ACK'lenir (custody-transfer, FIX#2/US-C2)"]
    E -->|Hayır| G["Core-NATS fallback publish dene"]
    G --> H{"Fallback başarılı mı?"}
    H -->|Evet| F
    H -->|"Hayır (FIX#2: mevcut kod burada da ACK'liyordu — DÜZELTİLİR)"| I["nak + alert — asla ack-drop, dlqSubject==null da discard DEĞİL nak"]
    J["Trace header (FIX#4/US-C6)"] -.->|"Yazma: yalnız X-Cadenzaflow-Trace-Id"| K["Okuma: X-Cadenzaflow-Trace-Id yoksa X-Trace-Id fallback"]
    L["DLQ-bridge kendi işleyemezse (BAQ-6, 2026-07-14)"] -.->|"nak-backoff (2^(n-1)s, cap 30s)"| M["5 ardışık başarısızlık → Circuit Breaker OPEN (guideline §4.2)"]
    M -->|"30s sonra"| N["HALF_OPEN — sınırlı deneme"]
    N -->|"3 ardışık başarı"| O["CLOSED — normal işlem"]
    M -.->|"CB OPEN iken"| P["DLQ mesajları stream'de bekler (kalıcı, limits-based retention) — kayıp YOK"]
```

**Not (BAQ-5/BAQ-6, 2026-07-14 kararları):** Kontrat-fix listesi artık **5 madde**dir (FIX#1…#5); FIX#5 hem Flowable (`JetStreamInboundEventChannelAdapter.java:124-131`) hem A2 (`JetStreamMessageCorrelationSubscriber.java:107-114`) tarafında aynı düzeltmeyi alır. DLQ-bridge dayanıklılığı artık nak-backoff (standart `2^(n-1)s, cap 30s`) + circuit-breaker (ERROR_HANDLING_GUIDELINE §4.2 eşikleri: 5 ardışık hata→OPEN, 30s açık süre→HALF_OPEN, 3 ardışık başarı→CLOSED) kombinasyonudur; CB OPEN iken DLQ mesajları **kaybolmaz** — `DLQ` stream'i kalıcıdır (limits-based, 14g retention), mesajlar bekler ve CB CLOSED'a döndüğünde işlenir (bkz. BR-SUB-008).

### 1.7 JavaDelegate phase-out & idiom netliği (US-E1, E2)

```mermaid
flowchart TD
    A["Mevcut model: JavaDelegate outbound (in-tx)"] --> B{"Motor?"}
    B -->|Camunda/CadenzaFlow| C["NatsPublishDelegate / JetStreamPublishDelegate / NatsRequestReplyDelegate KALDIRILIR"]
    B -->|Flowable| D["requestreply/NatsRequestReplyDelegate KALDIRILIR"]
    C --> E{"İş dağıtımı mı, saf event-bekleme mi?"}
    D --> E
    E -->|İş dağıtımı| F["A2 (Camunda/CadenzaFlow) / Event Registry (Flowable) idiomuna taşı"]
    E -->|"Saf dış event/callback bekleme"| G["correlateWithResult() message-correlation KORUNUR (dispatch İÇİN kullanılmaz)"]
```

---

## 2. Durum makineleri

### 2.1 A2 External Task — türetilmiş durum makinesi

`ACT_RU_EXT_TASK` satırının bir `status` kolonu **yoktur** — durum üç kolonun (`LOCK_EXP_TIME_`, `RETRIES_`, `SUSPENSION_STATE_`) fonksiyonu olarak **türetilir**. Bu, klasik BA state-machine şablonundan sapmadır ve kasıtlıdır (kanıt: `[BA-VERIFIED]` `ExternalTask.xml` fetchable predicate, satır ~220-222'ye tekabül eden blok bu fazda bizzat okundu).

```mermaid
stateDiagram-v2
    [*] --> LOCKED_FRESH: createAndInsert()+lock(SENTINEL,L) aynı tx (BR-A2-002)
    LOCKED_FRESH --> COMPLETED: complete() başarılı (BR-A2-008)
    LOCKED_FRESH --> LOCKED_EXPIRED: L süresi dolar (worker/bridge yanıt vermedi)
    LOCKED_EXPIRED --> LOCKED_FRESH: sweep re-lock(SENTINEL,L)+re-publish, SIRA SABİT (BR-A2-005/013) [guard: fetchable-parite; re-lock→publish arası çökme kabul edilen nadir risk — BAQ-1]
    LOCKED_EXPIRED --> COMPLETED: geç complete() başarılı (BR-A2-011 — workerId hâlâ SENTINEL, expiry kontrolsüz)
    LOCKED_FRESH --> EXHAUSTED: deliveryCount>M → handleFailure(retries=0,retryDuration=0) (BR-A2-009)
    LOCKED_EXPIRED --> EXHAUSTED: deliveryCount>M → handleFailure(retries=0,retryDuration=0) (BR-A2-009)
    EXHAUSTED --> LOCKED_EXPIRED: Cockpit retry → setRetries(>0) [guard: LOCK_EXP_TIME_=now zaten sağlanmış (retryDuration=0, BAQ-2 kararı) — gecikme YOK]
    LOCKED_FRESH --> SUSPENDED: process/instance suspend
    LOCKED_EXPIRED --> SUSPENDED: process/instance suspend
    SUSPENDED --> LOCKED_EXPIRED: resume (predikat yeniden değerlendirilir — lock zaten expired ise)
    COMPLETED --> [*]
```

**Guard notu (BAQ-1 kararı, 2026-07-14):** `LOCKED_EXPIRED --> LOCKED_FRESH` geçişinde sweep'in **re-lock→publish sırası SABİTTİR** (re-lock ADIMI önce, JetStream publish ikinci). Bu iki adım arasında düğümün çökmesi/broker'ın erişilemez olması **kabul edilen nadir bir durumdur**: satır `LOCKED_FRESH` görünür (lock taze) ama teslim edilmemiş olabilir; bedeli **≤ +L (320s) ek gecikme**dir (bir sonraki fetchable-parite döngüsüne kadar) — kalıcı kayıp değil, sınırlı gecikmedir. Adımların atomik hale getirilmesi (ör. publish-önce-relock-sonra veya iki-fazlı commit) **Phase 3/4 tasarım kararına bırakılmıştır**; bu fazda iş kuralı olarak yalnız üst sınır (≤+L) ve mekanizma kararının aşağı akışa devri sabitlenmiştir (bkz. BR-A2-013, `SYS_SWEEP_REPUBLISH_FAILED`).

### 2.2 JetStream mesaj custody-transfer durumu (özet — üç rol ayrımı Karar Matrisi 1'de)

```mermaid
stateDiagram-v2
    [*] --> PENDING: publish (post-commit / sendEvent / sweep re-publish)
    PENDING --> IN_FLIGHT: consumer teslim alır (push, queue-group)
    IN_FLIGHT --> ACKED: işlem başarılı + yan-etki kalıcılaştı (custody-transfer ilkesi)
    IN_FLIGHT --> NAKED_BACKOFF: transient hata
    NAKED_BACKOFF --> IN_FLIGHT: redelivery (backoff 2^(n-1)s, cap 30s)
    IN_FLIGHT --> DLQ_ROUTED: deliveryCount > M (in-band tespit, consumer'ın kendisi yönlendirir)
    DLQ_ROUTED --> ACKED: orijinal mesaj ACK'lenir (yalnız DLQ-PubAck sonrası)
    DLQ_ROUTED --> RETRY_DLQ_PUBLISH: DLQ publish (JetStream+core-NATS) ikisi de başarısız
    RETRY_DLQ_PUBLISH --> DLQ_ROUTED: nak + alert (asla ack-drop) — bir sonraki redelivery'de tekrar dener
    ACKED --> [*]
```

**Not:** Bu diyagram özet/görselleştirmedir; her rolün (worker consumer / engine-inbound consumer / DLQ-bridge) tam guard-koşulları için bkz. `DECISION_MATRIX.md` §1.

---

## 3. İş Kuralları Kataloğu (BR-XXX)

> Format: `BR-{MODÜL}-{NO}`. Modüller: **A2** (EPIC-A), **FLW** (EPIC-B), **SUB** (EPIC-C substrat), **OBS** (EPIC-D), **MIG** (EPIC-E). Her kural US + FR'ye bağlıdır. **31 kural** (24 US'nin tamamı kapsanır — bkz. §8; sıfır boşluk; BAQ-5/BAQ-6 kararlarıyla BR-SUB-007/008 eklendi — review KOŞULLU ONAY sonrası güncelleme, 2026-07-14).

### BR-A2-001: Happy-path'te fetchAndLock sorgusu sıfır
**User Story:** US-A1 | **FR:** FR-A1 | **Öncelik:** Must

**Tanım:** A2-topic'li bir external task doğduğunda worker'a teslim, JetStream push ile yapılır; ne worker ne merkezi bir poller `fetchAndLock` (SELECT FOR UPDATE) çalıştırır.

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Task A2-topic'li VE happy-path (crash yok) | `fetchAndLock` fingerprint hit = 0 |
| 2 | N worker aynı topic'e queue-group ile bağlı | Tam olarak 1 worker işi alır (WorkQueue claim) |
| 3 | Worker redeliver alır (nak/timeout sonrası) | Worker kodu değişmeden idempotent işler (dedup + at-least-once) |

**Test senaryoları:**
| Senaryo | Girdi | Beklenen çıktı | Tür |
|---|---|---|---|
| Happy path | 1 task doğar | `pg_stat_statements` fetchAndLock=0 | Positive |
| Çoklu worker | 5 worker, 1 task | Yalnız 1 worker alır | Positive |
| Redeliver | ack-wait dolar | Aynı worker/başka worker idempotent işler | Edge case |

**Bağımlılık:** BR-A2-002 (kilit doğumda), BR-SUB-005 (WorkQueue stream).

---

### BR-A2-002: Doğumda in-tx sentinel kilit — sıfır ek DB yazısı
**User Story:** US-A2 | **FR:** FR-A2 | **Öncelik:** Must

**Tanım:** External task satırı, onu oluşturan transaction içinde `createAndInsert(...)` ile kilitsiz doğar, aynı tx'te flush'tan önce `lock(SENTINEL, L)` çağrılır → kilit alanları aynı INSERT'e biner.

**Kanıt:** `[BA-VERIFIED]` `ExternalTaskEntity.java:568-588` (`createAndInsert` — workerId/lockExpirationTime hiç set edilmiyor, yalnız `insert()` çağrılıyor); `:471-474` (`lock(String workerId, long lockDuration)` — yalnız iki alan setter'ı, DB round-trip yok).

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | A2-topic'li aktivite parse edilir | `BpmnParseListener` behavior'ı swap eder (`BpmnParse.java:2564`) |
| 2 | `execute()` çağrılır | `createAndInsert` + aynı tx'te `lock(SENTINEL,L)` |
| 3 | Flush gerçekleşir | Tek INSERT üretilir — ikinci bir UPDATE YOK |
| 4 | A2 olmayan (klasik) external task | Davranış değişmez — kilitsiz doğar (native) |

**Sınır değerler:** L=320s default; topic-başına override edilebilir (bkz. BR-A2-006 için alt sınır kısıtı).

**Test senaryoları:**
| Senaryo | Girdi | Beklenen çıktı | Tür |
|---|---|---|---|
| Happy path | A2-topic aktivite | 1 INSERT, kilit alanları dolu | Positive |
| Klasik ext-task | camunda:type=external, A2 değil | Kilitsiz doğar (davranış korunur) | Negative (regresyon kontrolü) |
| Flush-öncesi çift lock çağrısı | (savunma) | Tek INSERT'e biner, hata yok | Edge case |

**Bağımlılık:** BR-A2-003.

---

### BR-A2-003: SENTINEL workerId küme-geneli tek sabit
**User Story:** US-A2 | **FR:** FR-A3 | **Öncelik:** Must

**Tanım:** `SENTINEL` (örn. `a2-jetstream-bridge`) küme-geneli **tek** sabittir; node-başına farklı id KULLANILMAZ (aksi halde reply queue-group'tan farklı bir node tüketirse `complete()` workerId eşitliği kırılır). Id ayrıca payload'da audit için taşınır.

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Reply'ı queue-group'tan herhangi bir bridge node'u tüketir | `complete(extTaskId, SENTINEL, vars)` her node'da aynı sabitle çağrılır |
| 2 | Config'te SENTINEL değeri node'lar arası farklı yazılmış (config drift) | `SYS_SENTINEL_WORKER_CONFLICT` (bkz. EXCEPTION_CODES.md) — **CRITICAL + on-call page** (BAQ-7 kararı, 2026-07-14) — ASLA olmamalı invariant |

**Bağımlılık:** BR-A2-002, BR-A2-008.

---

### BR-A2-004: Post-commit publish — sorgusuz, yarışsız
**User Story:** US-A3 | **FR:** FR-A4 | **Öncelik:** Must

**Tanım:** Task'ı oluşturan node, `TransactionState.COMMITTED` listener'ında elindeki entity ile yayınlar. DB sorgusu YOK, cross-node yarış YOK (her node yalnız kendi yarattığını yayınlar).

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Commit başarılı | Aynı node, tx dışı, task'ı DB sorgusuz yayınlar |
| 2 | Node commit ÖNCESİ çöker | Task hiç insert edilmedi — yayın da yok, tutarlı |
| 3 | Node commit SONRASI ama publish() ÖNCESİ çöker | Orphan (DB'de var, JetStream'de yok) → sweep yakalar (BR-A2-005) |
| 4 | Çift yayın (post-commit + sweep aynı task'ı iki kez yayınlarsa) | `Nats-Msg-Id=externalTaskId` dedup penceresinde yutulur |

**Bağımlılık:** BR-A2-002, BR-A2-005.

---

### BR-A2-005: Soğuk sweep — fetchable-parite kriteri (+ re-publish güvenliği)
**User Story:** US-A3, US-A5 | **FR:** FR-A5, FR-A6 | **Öncelik:** Must

**Tanım:** Sweep, engine'in native fetchable predicate'inin (`LOCK_EXP_TIME_ null|≤now AND RETRIES_ null|>0 AND SUSPENSION_STATE_ null|=1`) birebir aynısını sorgular; `SELECT FOR UPDATE` KULLANMAZ; yayın öncesi sentinel re-lock yapar (aynı workerId → her zaman geçer).

**Kanıt:** `[BA-VERIFIED]` fetchable predicate WHERE bloğu (`ExternalTask.xml`, bu fazda bizzat okundu — `LOCK_EXP_TIME_`/`SUSPENSION_STATE_`/`RETRIES_` üç koşul AND'li); `[BA-VERIFIED]` `LockExternalTaskCmd.java:50-61` — re-lock ihlali yalnız *farklı worker + süresi DOLMAMIŞ kilit* kombinasyonunda (`validateWorkerViolation`: `workerValidation AND lockValidation`, `lockValidation` yalnız `existingLockExpirationTime != null && !now.after(existingLockExpirationTime)` iken true) → aynı SENTINEL workerId ile veya süresi dolmuş herhangi bir kilitle re-lock HER ZAMAN geçer.

**Koşullar (bkz. Karar Matrisi 3 — tam tablo):**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Lock expired/null AND retries≠0 AND not suspended AND A2 topic | Re-lock(SENTINEL,L) + re-publish |
| 2 | retries=0 (DLQ'lanmış) | Atla — asla yeniden yayınlama |
| 3 | Suspended (process/instance) | Atla — resume bekle |
| 4 | Lock hâlâ taze (LOCK_EXP_TIME_ > now) | Atla — in-flight, orphan değil |

**Edge-case (BAQ-1, bu fazda bulundu):** Re-lock (DB yazısı) başarılı olur ama ardından re-publish (JetStream) başarısız olursa, satır artık "taze kilitli" görünür (LOCK_EXP_TIME_ = now+L) ama **hiçbir yere teslim edilmemiştir**. Sonraki sweep döngüleri (≤ L/S ≈ 2-3 döngü, ~240-320s) bu satırı "in-flight" sanıp atlar → gerçek orphan, kendi taze kilidi yüzünden **görünmez** hale gelir. Re-lock ve re-publish adımlarının sıralaması/atomikliği bu belgenin kapsamı dışıdır (Phase 3/4 tasarım kararı) ama **iş kuralı olarak şart koşulur:** re-publish başarısız olursa satır bir sonraki sweep döngüsünde **yine orphan sayılmalıdır** (ör. kısa/geçici lock veya publish-önce-relock-sonra sıralaması). Bkz. §9 BAQ-1.

**Bağımlılık:** BR-A2-002, BR-A2-004, BR-A2-006 (L formülü sweep periyodunu S'yi içerir).

---

### BR-A2-006: Şemsiye kilit formülü — L ≥ M·W + Σbackoff + S + ε
**User Story:** US-A5 | **FR:** FR-A8 | **Öncelik:** Must

**Tanım:** JetStream tek redelivery otoritesidir (`ack-wait`=W, `maxDeliver`=M); engine sentinel kilidi (L) bunu kapsayan bir **şemsiyedir**, rakip bir saat değildir. Default: W=30s, M=4, S=120s, ε=60s, Σbackoff=1+2+4=7s → alt sınır **307s**; default **L=320s** (13s marj).

**Kanıt:** `[BA-VERIFIED]` `HandleExternalTaskCmd.java:89-91` (`validateWorkerViolation` — yalnız workerId eşitliği kontrol edilir; expiry kontrolü YOK) → engine kilidi redelivery saati DEĞİLDİR, JetStream'dir.

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Default parametreler (W=30,M=4,S=120,ε=60,Σbackoff=7) | L ≥ 307s zorunlu; default 320 |
| 2 | Topic-başına W override (uzun işli topic) | L de buna göre yeniden türetilmeli |
| 3 | Operatör L'yi elle formülün ALTINA yazarsa | `VAL_UMBRELLA_LOCK_TOO_SHORT` — **reject-startup (BAQ-3 kararı, 2026-07-14):** topic aktivasyonu ENGELLENİR (ERROR), bilinçli kaçış yalnız `allow-unsafe-lock-duration=true` flag'i ile mümkündür (+ kalıcı WARN her sweep/dispatch döngüsünde) |

**Sınır değerler:**
| Parametre | Min (kabul edilebilir) | Default | Geçersiz örnek |
|---|---|---|---|
| L | M·W+Σbackoff+S+ε (307s default'ta) | 320s | 300s (ilk yazımda MAJOR-B ile düzeltildi) |

**Karar (BAQ-3, 2026-07-14 — Levent onayı):** `VAL_UMBRELLA_LOCK_TOO_SHORT` davranışı netleşti — **default: reject-startup** (topic aktivasyonu engellenir, ERROR log). Bilinçli kaçış yolu: `allow-unsafe-lock-duration=true` config flag'i set edilirse config kabul edilir AMA her ilgili döngüde (dispatch/sweep) **kalıcı WARN** log'lanmaya devam eder — sessiz bir "bir kere uyar, sonra unut" davranışı YOKTUR. Bu, Phase 3/4'te config-validasyon katmanına (bootstrap-time) yansıtılacaktır.

**Bağımlılık:** BR-A2-005, BR-A2-007.

---

### BR-A2-007: Heartbeat yok — W·M sert tavan
**User Story:** US-A5 | **FR:** FR-A9 | **Öncelik:** Must

**Tanım:** `msg.inProgress()` ve engine `extendLock` **kullanılmaz**; W·M basamak-1'de sert bütçedir (D-H'ye ertelendi — **yeniden açılmaz**).

**Kanıt:** `[BA-VERIFIED]` `ExtendLockOnExternalTaskCmd.java:44-47` (`execute`: `EnsureUtil.ensureGreaterThanOrEqual(..., "Cannot extend a lock that expired", lockExpirationTime, now)` — süresi dolmuş kilit UZATILAMAZ, bu da zaten heartbeat'i basamak-1 worker kontratı olmadan anlamsız kılar).

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Uzun işli topic, W küçük seçilmiş | Worker `msg.inProgress()` KULLANAMAZ — W topic-başına artırılmalı |
| 2 | `extendLock` çağrısı denenirse (kilit dolmuşsa) | `BadUserRequestException` (kanıt yukarıda) |

**Bağımlılık:** BR-A2-006.

---

### BR-A2-008: Inbound completion-bridge
**User Story:** US-A4 | **FR:** FR-A7 | **Öncelik:** Must

**Tanım:** Reply mesajı `externalTaskService.complete(extTaskId, SENTINEL, vars)`'a bağlanır; business-error → `handleBpmnError`; transient → `handleFailure`. `complete` **başarılı olduktan sonra** reply ACK'lenir.

**Koşullar (bkz. Karar Matrisi 2 — tam tablo):**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Task bulundu + workerId eşit | `complete()` çalışır → ACK |
| 2 | Task bulunamadı (`NotFoundException`, `HandleExternalTaskCmd.java:48-50`) | Yakala + log WARN + ACK (idempotent yut — BR-A2-011) |
| 3 | workerId eşit değil (`BadUserRequestException`, `:52-53`) | **ASLA olmamalı** — invariant, escalate (BAQ-7) |
| 4 | Worker business-error döndü | `handleBpmnError(...)` |
| 5 | Complete çağrısı sırasında transient hata (DB down vb.) | `nak` — redelivery |

**Bağımlılık:** BR-A2-002 (kilit ön şart), BR-SUB-002 (custody-transfer ack).

---

### BR-A2-009: DLQ → incident bridge
**User Story:** US-A6 | **FR:** FR-A10, FR-A11 | **Öncelik:** Must

**Tanım:** `deliveryCount > M` → `dlq.jobs.<topic>` → incident-bridge → `handleFailure(extTaskId, SENTINEL, retries=0, retryDuration=0)` → Cockpit incident. `retries=0` task fetchable-predicate dışıdır, sweep asla dirtmez. **`retryDuration=0` sabit değeri BAQ-2 kararıdır (2026-07-14)** — bkz. BR-A2-010.

**Kanıt:** `[BA-VERIFIED]` `ExternalTaskEntity.java:443-448` (`setRetriesAndManageIncidents`: `areRetriesLeft() && retries<=0 → createIncident()`).

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | deliveryCount > M | DLQ + incident |
| 2 | İkinci kez aynı DLQ mesajı redeliver edilirse (bridge nak sonrası) | `setRetriesAndManageIncidents(0)` tekrar çağrılır ama `areRetriesLeft()` artık false → `createIncident()` **tekrar çağrılmaz** (doğal idempotency) |
| 3 | Task DLQ-bridge işlerken zaten bir başka yoldan complete edilmişse | `NotFoundException` → aynı idempotent-yut yolu (BR-A2-011) |

**Bağımlılık:** BR-SUB-001 (DLQ header preservation — korelasyon için şart), BR-SUB-004.

---

### BR-A2-010: Cockpit-retry revival path (BAQ-2 kararıyla netleşti)
**User Story:** US-A6 | **FR:** FR-A11 | **Öncelik:** Must

**Tanım:** Operatör Cockpit'ten retry verirse (`retries>0`), task **anında** yeniden fetchable olur ve sweep onu doğal olarak yeniden yayınlar — gecikme YOKTUR.

**Kanıt:** `[BA-VERIFIED]` `SetExternalTaskRetriesCmd.java:48-51` — `execute()` **yalnız** `externalTask.setRetriesAndManageIncidents(retries)` çağırır; `lockExpirationTime`'a dokunmaz.

**Karar (BAQ-2, 2026-07-14 — Levent onayı):** Bu fazda bulunan edge-case (DLQ→incident bridge'in `handleFailure(..., retries=0, retryDuration=X)` çağrısı `lockExpirationTime=now+X` set ettiğinden, Cockpit-retry X süresi dolmadan verilirse satır residual-lock nedeniyle gecikmeli fetchable oluyordu) **çözüldü**: DLQ→incident bridge'in `handleFailure` çağrısı artık **daima `retryDuration=0`** kullanır (bkz. BR-A2-009) → `lockExpirationTime = now` → fetchable-parite predikatı (`LOCK_EXP_TIME_ null OR ≤ now`) **operatör ne zaman retry verirse versin** anında sağlanır. Residual-lock gecikmesi riski ortadan kalkmıştır. Bu, Phase 4 LLD'de `handleFailure` çağrı sitesine sabit `retryDuration=0` parametresi olarak yansıtılacak bir implementasyon kısıtıdır.

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Cockpit retry verilir (herhangi bir zamanda) | `retryDuration=0` olduğundan `LOCK_EXP_TIME_` zaten `≤now` — satır anında fetchable, sweep bir sonraki S'de yakalar |
| 2 | (Tarihsel/reddedilen alternatif) `retryDuration>0` kullanılsaydı | Residual-lock gecikmesi olurdu — bu fazda BAQ-2 ile REDDEDİLDİ, `retryDuration=0` sabitlendi |

**Bağımlılık:** BR-A2-009, BR-A2-005.

---

### BR-A2-011: Geç-complete idempotency
**User Story:** US-A7 | **FR:** FR-A12 | **Öncelik:** Must

**Tanım:** L dolduktan sonra gelen reply yine başarılı complete olur (expiry kontrolü yok); ikinci (çift) complete "task yok" ile karşılaşır → yakalanır + ACK.

**Kanıt:** `[BA-VERIFIED]` `HandleExternalTaskCmd.java:89-91` (expiry kontrolsüz workerId eşitliği).

**Sınır değerler:**
| Girdi | Durum | Beklenen |
|---|---|---|
| Reply, L içinde | normal | complete başarılı |
| Reply, L sonrası ama re-lock/sweep araya girmemiş | expiry kontrolsüz | complete YİNE başarılı |
| İkinci (duplicate) reply, aynı task zaten complete edilmiş | task silinmiş | `NotFoundException` → yut+ACK |
| Duplicate, `duplicate_window` (2dk) İÇİNDE | stream dedup | Nats-Msg-Id ile zaten yutulur, complete-idempotency'e gelmez |
| Duplicate, `duplicate_window` DIŞINDA (L=320s > 2dk) | stream dedup çalışmaz | complete-idempotency (bu kural) yutar |

**Test senaryoları:**
| Senaryo | Girdi | Beklenen çıktı | Tür |
|---|---|---|---|
| Pencere-dışı çift | 2 reply, 400s ara | 1. complete, 2. yut+ACK | Edge case |
| workerId hep SENTINEL | — | Sahiplik asla el değiştirmez | Invariant |

**Bağımlılık:** BR-A2-008, BR-SUB-005 (dedup penceresi).

---

### BR-A2-012: Migrasyon guard — legacy poller dışlanması
**User Story:** US-A8 | **FR:** FR-A13 | **Öncelik:** Should (Q6: basamak-1'e dahil)

**Tanım:** A2 task'ı doğuştan sentinel-kilitli olduğundan native fetchable-predicate dışındadır; legacy `fetchAndLock` onu asla döndürmez. A2 olmayan (klasik) external task'lar etkilenmez.

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | A2-topic task, legacy poller çalışıyor | Poller onu görmez (lock dolu) |
| 2 | Klasik (A2-olmayan) task, aynı poller | Normal fetchAndLock ile alınır (davranış aynı) |
| 3 | Migration ortasında karışık topic seti | Behavior swap yalnız A2-topic'li aktivitelerde |

**Bağımlılık:** BR-A2-002.

---

### BR-A2-013: Sweep re-lock/re-publish sıralaması — sabit sıra, kabul edilen nadir risk (BAQ-1 kararıyla netleşti)
**User Story:** US-A3 | **FR:** FR-A5, FR-A6 | **Öncelik:** Must (kenar-durum netliği — BAQ-1)

**Tanım:** Bkz. BR-A2-005 edge-case notu ve state machine §2.1 guard notu. Bu kural ayrı numaralandırılmıştır çünkü Karar Matrisi 3'te bağımsız bir satır (re-lock başarılı, re-publish başarısız) olarak izlenmesi gerekir.

**Karar (BAQ-1, 2026-07-14 — Levent onayı):** Sıralama **SABİTTİR** — re-lock (DB yazısı) **önce**, JetStream re-publish **sonra**. İki adım arasında düğüm/broker çökmesi **kabul edilen nadir bir durumdur**; bedeli **≤ +L (320s) ek gecikme**dir (satır bir sonraki fetchable-parite döngüsüne kadar "taze kilitli" görünüp atlanır, ardından tekrar orphan sayılır — kalıcı kayıp DEĞİL, sınırlı gecikme). Adımların **atomikliği** (ör. iki-fazlı commit, publish-önce/relock-sonra sıralaması, veya kısa-geçici-lock deseni) bilinçli olarak **Phase 3/4 tasarım kararına** bırakılmıştır — bu faz yalnız üst-sınır garantisini (≤+L) ve riskin kabul edildiğini sabitler.

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Re-lock DB yazısı başarılı, JetStream publish başarılı | Normal (BR-A2-005 satır 1) |
| 2 | Re-lock DB yazısı başarılı, JetStream publish BAŞARISIZ (broker down) | Satır "taze kilitli" görünür ama teslim edilmedi — **kabul edilen risk**, bir sonraki fetchable-parite döngüsünde (≤L sonra) yine orphan sayılır; atomiklik mekanizması Phase 3/4'e bırakıldı |
| 3 | Re-lock DB yazısı BAŞARISIZ (DB down) | `SYS_SWEEP_RELOCK_FAILED` — sweep döngüsü bu satırı atlar, bir sonraki döngüde tekrar dener (satır durumu değişmediği için risk yok) |

**Bağımlılık:** BR-A2-005.

---

### BR-FLW-001: JavaDelegate → sendEvent (Flowable outbound)
**User Story:** US-B1 | **FR:** FR-B1 | **Öncelik:** Must

**Tanım:** `requestreply/NatsRequestReplyDelegate.java:19` (in-tx blocking) kaldırılır; outbound `NatsOutboundEventChannelAdapter.sendEvent(...)` ile motor-dışı yapılır; native push idiom korunur.

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Model delegate kullanıyor | Migrasyon rehberi ile `sendEvent`'e taşınır |
| 2 | Yeni model | Baştan `sendEvent` kullanır, delegate hiç görülmez |
| 3 | Fast-RPC istisnası talep edilirse | REDDEDİLDİ (05 §9) — yok |

**Bağımlılık:** BR-MIG-001.

---

### BR-FLW-002: JetStream ack+DLQ+dedup zorunluluğu
**User Story:** US-B2 | **FR:** FR-B2, FR-C6 | **Öncelik:** Must

**Tanım:** Basamak-1 kritik iş için core adapter'ın ack'siz/log-only yolu KULLANILMAZ; JetStream variant (`maxDeliver+1` DLQ tespiti + `Nats-Msg-Id`/correlation dedup) zorunludur.

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Basamak-1 kritik channel tanımlanır | JetStream adapter seçilmeli, core adapter değil |
| 2 | deliveryCount > maxDeliver | DLQ'ya yönlenir (`:133-146`) |
| 3 | İşleme sırasında exception | `nakWithBackoff` (2^(n-1)s, cap 30s) |
| 4 | Boş mesaj gövdesi | **BAQ-5 kararıyla (2026-07-14) contract-fix #5'e taşındı** — bkz. BR-SUB-007, artık WARN+DLQ |

**Bağımlılık:** BR-SUB-001, BR-SUB-002, BR-SUB-003, BR-SUB-007.

---

### BR-FLW-003: DLQ → failure-event bridge (default escalation)
**User Story:** US-B3 | **FR:** FR-B3 | **Öncelik:** Must

**Tanım:** DLQ mesajı aynı correlation key'leri koruyarak failure-event'e çevrilip `eventRegistry.eventReceived(...)`'a sokulur; happy-path ek DB maliyeti sıfır.

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Worker W·M bütçesini tüketir (kalıcı ölüm) | DLQ→failure-event, tespit gecikmesi=W·M, SLA beklenmez |
| 2 | Bekleyen instance escalation path'i var (event-based gateway/boundary/subprocess) | Correlate olur, model işler |
| 3 | Bekleyen subscription YOK (instance zaten resolve olmuş / correlation key kayıp) | `RES_FAILURE_EVENT_CORRELATION_MISS` — **WARN + metrik (BAQ-8 kararı, 2026-07-14):** tek olay benign yarış olabilir (instance başka yoldan zaten resolve olmuş); süreklilik/eşik aşımı ayrı bir alarm (`failure_event_correlation_miss` sayacı) tetikler |

**Karar (BAQ-8, 2026-07-14 — Levent onayı):** `RES_FAILURE_EVENT_CORRELATION_MISS` varsayılan seviyesi **WARN + metrik**tir (ERROR değil) — tek bir correlation-miss olayı çoğu zaman zararsız bir yarış durumudur (instance zaten başka bir yoldan sonlanmış olabilir). Ancak Micrometer `failure_event_correlation_miss` sayacı üzerinden **süreklilik/eşik-bazlı alarm** tanımlanmalıdır (ör. kısa pencerede tekrarlayan miss'ler → gerçek korelasyon-key kaybı/NFR-R6 riski sinyali) — bu eşik parametreleri Phase 3/4'te belirlenecektir.

**Bağımlılık:** BR-SUB-001 (korelasyon key'siz DLQ → bu köprü çalışamaz).

---

### BR-FLW-004: Opt-in boundary timer (wall-clock SLA)
**User Story:** US-B4 | **FR:** FR-B4 | **Öncelik:** Should (Q6 dahil)

**Tanım:** Yalnız gerçek deadline'ı olan modellerde opt-in boundary timer modellenir; timer-job satır maliyeti yalnız o modellerde ödenir.

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Model gerçek SLA'ya sahip | Boundary timer opt-in modellenir |
| 2 | Model SLA'sız (default) | Timer YOK, yalnız DLQ→failure-event (BR-FLW-003) |
| 3 | Timer-only default talep edilirse | REDDEDİLDİ — yeniden açılmaz |

**Bağımlılık:** BR-FLW-003.

---

### BR-FLW-005: Geç-sonuç politikası
**User Story:** US-B5 | **FR:** FR-B5 | **Öncelik:** Should (Q6 dahil)

**Tanım:** Escalation interrupting ise geç sonuç subscription bulamaz → ack+log+metric (drop); non-interrupting ise işlenir.

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Interrupting escalation zaten fırlamış, geç event gelir | Drop (ack+log+metric), `BUS_EVENT_CORRELATION_NOT_FOUND` |
| 2 | Non-interrupting escalation | Geç event yine işlenir |
| 3 | `eventReceived`'ın gerçek no-match davranışı | `[phase3'te doğrulanacak]` (D-D c) |

**Bağımlılık:** BR-FLW-003, BR-FLW-004.

---

### BR-SUB-001: DLQ header preservation (contract-fix #1)
**User Story:** US-C1 | **FR:** FR-C1 | **Öncelik:** Must

**Tanım:** `publishToDlq`, orijinal payload byte'larını VE orijinal header'ların tamamını kopyalar; ek meta header'lar eklenir (`X-Cadenzaflow-Dlq-Original-Subject`, `-Dlq-Delivery-Count`, `-Dlq-Reason`, `-Dlq-Timestamp`).

**Kanıt (mevcut açık, `[BA-VERIFIED]` bu fazda):** `JetStreamInboundEventChannelAdapter.java:210-236` (`publishToDlq`) — yalnız `msg.getData()` yayınlanıyor (satır 216-218, 227), header hiç okunmuyor/kopyalanmıyor. Aynı desen `JetStreamMessageCorrelationSubscriber.java:202-229` (satır 208-210 JetStream publish, 218-219 core-NATS fallback) — ikisi de yalnız `data` taşıyor.

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Mesaj DLQ'ya yönleniyor | Orijinal header'lar + 4 meta header eklenir |
| 2 | DLQ mesajından correlation key okunur | Bridge (US-A6/B3) correlate edebilir |
| 3 | Meta header `-Dlq-Reason` | Yalnız hata SINIFI/kodu taşır, payload/PII sızdırmaz (DP-6) |

**Bağımlılık:** yok (fix; BR-A2-009/BR-FLW-003 buna bağlı).

---

### BR-SUB-002: Custody-transfer ack — koşulsuz ack kaldırılır (contract-fix #2)
**User Story:** US-C2 | **FR:** FR-C2, FR-C6 | **Öncelik:** Must

**Tanım:** Ack yalnız kalıcılık el değiştirdikten sonra verilir. `dlqSubject==null` iken mesaj discard EDİLMEZ (nak edilir); DLQ publish başarısızsa nak edilir; dlq-of-dlq yok (bridge işleyemezse nak+alert, asla ack-drop).

**Kanıt (mevcut açık, `[BA-VERIFIED]` bu fazda):** `JetStreamInboundEventChannelAdapter.java:210-214` (`dlqSubject==null` → log WARN + `return`, ardından ÇAĞIRAN KOD satır 141-145'te `msg.ack()` çağırıyor — yani mesaj sessizce kaybediliyor); `:222-235` (JetStream VE core-NATS ikisi de başarısız → yalnız `log.error`, yine çağıran kod ack'liyor). Aynı desen cadenzaflow'da `JetStreamMessageCorrelationSubscriber.java:203-207` (`dlqSubject==null` → return, çağıran satır 127'de ack).

**Koşullar (bkz. Karar Matrisi 1 — tam tablo):**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Worker reply yayınladı | reply-PubAck-sonrası-ack |
| 2 | Engine-inbound complete/correlate başarılı | complete-sonrası-ack |
| 3 | DLQ publish başarılı | DLQ-PubAck-sonrası-ack (orijinal mesaj) |
| 4 | `dlqSubject==null` | **discard YOK** → nak (DÜZELTME) |
| 5 | DLQ publish (her iki yol) başarısız | nak + alert (DÜZELTME — mevcut kod ack'liyordu) |
| 6 | Bridge DLQ mesajını işleyemez | nak + alert, asla ack-drop (dlq-of-dlq yok) |

**Bağımlılık:** BR-SUB-006 (backoff deseni paylaşılır).

---

### BR-SUB-003: DLQ publish'te Nats-Msg-Id (contract-fix #3)
**User Story:** US-C3 | **FR:** FR-C3 | **Öncelik:** Must

**Tanım:** Her DLQ publish `Nats-Msg-Id = <orijinal-msg-id>.dlq` taşır — çökme-sonrası çift DLQ kaydına karşı.

**Kanıt (mevcut açık, `[BA-VERIFIED]` bu fazda):** `JetStreamInboundEventChannelAdapter.java:218` (`jetStream.publish(dlqSubject, data)` — 2. argüman yok, `Nats-Msg-Id` set edilmiyor); aynı `JetStreamMessageCorrelationSubscriber.java:210`.

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Aynı poison mesaj iki kez DLQ'ya yönlenir (crash-restart) | Stream dedup penceresi İÇİNDE tek kayıt |
| 2 | İki DLQ yönlenmesi arası `duplicate_window`'dan (2dk) uzun | Çift kayıt oluşur (dedup dışı, dürüst sınır) |

**Bağımlılık:** BR-SUB-001.

---

### BR-SUB-004: Tek ortak DLQ stream topolojisi — `jobs.*` namespace REZERVE (BAQ-4 kararıyla netleşti)
**User Story:** US-C4 | **FR:** FR-C4 | **Öncelik:** Should (Q6 dahil)

**Tanım:** Tüm DLQ trafiği tek `DLQ` stream'inde (`dlq.>`) toplanır, limits-based retention (default 14g, kiracı-bazlı override). Tüketiciler subject filtresiyle ayrışır: `dlq.jobs.>` → Camunda/CadenzaFlow incident-bridge; event-channel DLQ'ları → Flowable failure-event bridge. **`jobs.*` namespace'i A2'ye REZERVEDİR** (BAQ-4 kararı).

**Karar (BAQ-4, 2026-07-14 — Levent onayı):** `jobs.*` subject namespace'i **A2'ye (Camunda/CadenzaFlow) REZERVE edilmiştir**; Flowable Event Registry inbound channel'ları bu önekle ÇAKIŞAMAZ. Bu kural artık **aktif deployment-time validasyonla** zorunlu kılınır: bootstrap sırasında bir Flowable inbound channel subject'i `jobs.` önekiyle tanımlanmaya çalışılırsa `VAL_TOPIC_NAMESPACE_COLLISION` (ERROR) fırlatılır ve **deployment engellenir** (artık yalnızca dokümante bir kısıt değil, mekanik bir kapı).

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | A2 job DLQ'landı | `dlq.jobs.<topic>` → incident-bridge tüketir |
| 2 | Flowable event-channel DLQ'landı | `dlq.<event-subject>` → failure-event bridge tüketir |
| 3 | İdiom-başına ayrı stream talep edilirse | REDDEDİLDİ — yeniden açılmaz |
| 4 | Flowable bir inbound channel'ı `jobs.<x>` adlandırmaya çalışırsa | **`VAL_TOPIC_NAMESPACE_COLLISION` (ERROR) — deployment-time validasyon ENGELLER** (BAQ-4 kararıyla artık aktif, önceden yalnız tespit edilmeyen bir risk olarak dokümante edilmişti) |

**Bağımlılık:** BR-SUB-001.

---

### BR-SUB-005: WorkQueue stream + dedup penceresi
**User Story:** US-C5 | **FR:** FR-C5 | **Öncelik:** Must

**Tanım:** İş dağıtım stream'i WorkQueue tipindedir (her mesaj tek tüketici, nack→redeliver); `Nats-Msg-Id` dedup (A2: `externalTaskId`; Event Registry: correlation key); `duplicate_window` yapılandırılabilir (default 2dk).

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | `jobs.<type>` / event channel subject'i tanımlanır | WorkQueue stream'de |
| 2 | L (320s) > duplicate_window (2dk) | Pencere-dışı çiftler apply-zamanı idempotency (BR-A2-011/BR-FLW-005) ile yutulur — dokümante edilmeli |

**Bağımlılık:** yok (substrat temeli).

---

### BR-SUB-006: Trace header standardizasyonu (contract-fix #4)
**User Story:** US-C6 | **FR:** FR-C7 | **Öncelik:** Must

**Tanım:** Yazma tarafı yalnız `X-Cadenzaflow-Trace-Id` üretir; okuma tarafı iki adı da kabul eder (önce `X-Cadenzaflow-Trace-Id`, yoksa `X-Trace-Id`).

**Kanıt (mevcut açık, `[BA-VERIFIED]` bu fazda):** `JetStreamInboundEventChannelAdapter.java:119` (`NatsHeaderUtils.extractHeader(msg, "X-Trace-Id")` — yalnız eski adı okuyor); `BpmHeaders.java:12` (`TRACE_ID = "X-Cadenzaflow-Trace-Id"` — standart yazım adı).

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Mesaj `X-Cadenzaflow-Trace-Id` taşıyor | MDC'ye alınır |
| 2 | Mesaj yalnız eski `X-Trace-Id` taşıyor (geçiş dönemi üreticisi) | Fallback ile MDC'ye alınır |
| 3 | Yeni yazım kodu | Yalnız `X-Cadenzaflow-Trace-Id` üretir, `X-Trace-Id` YAZILMAZ |

**Bağımlılık:** yok (fix).

---

### BR-SUB-007: Boş mesaj gövdesi — contract-fix #5 (BAQ-5 kararıyla eklendi)
**User Story:** US-C2, US-B2 (kenar-durum genişletmesi — BR-A2-013'ün izlediği desenle aynı) | **FR:** FR-C2, FR-B2 | **Öncelik:** Must (BAQ-5 kararı, 2026-07-14)

**Tanım:** Boş mesaj gövdesi (`data==null || data.length==0`) artık **5. kontrat açığıdır**. Eski davranış (sessiz `log DEBUG + ACK`, hiçbir işlem/metrik yok) kalkar; yeni davranış: **WARN log + `metrics.dlqCount` artır + `publishToDlq` (header+Nats-Msg-Id korunarak) + ACK**. Ne bir job dispatch'i ne bir reply/event payload'ı meşru olarak boş olabilir — bu her zaman bir anomali sinyalidir (üretici hatası/serileştirme bug'ı).

**Karar (BAQ-5, 2026-07-14 — Levent onayı):** Kontrat-fix listesi **4'ten 5'e çıkar**. Bu düzeltme hem Flowable (`JetStreamInboundEventChannelAdapter.java:124-131`) hem A2/CadenzaFlow (`JetStreamMessageCorrelationSubscriber.java:107-114`) tarafında **aynı anda** uygulanır — ikisi de özdeş sessiz-ack desenini taşıyordu (bu fazda bizzat doğrulandı, iki dosyada da). `docs/06-external-task-over-jetstream.md §7` kontrat-fix listesi ayrıca güncellenmektedir (bu repo'nun sorumluluğu dışında, PO/Mimar tarafından işlenir).

**Kanıt:** `[BA-VERIFIED]` `JetStreamInboundEventChannelAdapter.java:124-131` (Flowable); `[BA-VERIFIED]` `JetStreamMessageCorrelationSubscriber.java:107-114` (A2/CadenzaFlow, `cadenzaflow-nats-channel/.../inbound/`) — ikisi de özdeş `if (data==null||data.length==0) { log.debug(...); msg.ack(); return; }` deseni.

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Mesaj gövdesi boş (job/reply/event, her iki motor idiomu) | WARN log + `metrics.dlqCount++` + `publishToDlq` (header+Nats-Msg-Id korunarak, BR-SUB-001/003 ile aynı kontrat) + ACK |
| 2 | Redelivery aynı boş body'yi üretirse (üretici hâlâ bozuk) | Aynı WARN+DLQ akışı tekrar çalışır — sonsuz redelivery YOK, DLQ'ya düşer |
| 3 | Mesaj gövdesi dolu (normal) | Etkilenmez — bu kural yalnız boş-body dalını değiştirir |

**Bağımlılık:** BR-SUB-001 (DLQ header preservation — bu akış da aynı `publishToDlq` yolunu kullanır), BR-SUB-002 (custody-transfer).

---

### BR-SUB-008: DLQ-bridge dayanıklılığı — nak-backoff + circuit-breaker (BAQ-6 kararıyla eklendi)
**User Story:** US-A6, US-B3 (DLQ-bridge davranışının genel-substrat genişletmesi) | **FR:** FR-A10, FR-B3 | **Öncelik:** Must (BAQ-6 kararı, 2026-07-14)

**Tanım:** DLQ-bridge (Camunda/CadenzaFlow incident-bridge; Flowable failure-event bridge) kendi işlemini (incident oluşturma / failure-event correlate) gerçekleştiremezse, dayanıklılık iki katmanlıdır: **(1)** standart `nakWithDelay` backoff (`2^(n-1)s`, cap 30s — mevcut ortak desen); **(2)** ardışık başarısızlık eşiği aşılırsa **circuit breaker** devreye girer (ERROR_HANDLING_GUIDELINE §4.2 eşikleri: 5 ardışık hata → OPEN; 30s açık süre → HALF_OPEN; 3 ardışık başarı → CLOSED).

**Karar (BAQ-6, 2026-07-14 — Levent onayı):** DLQ-bridge dayanıklılığı **nak-backoff + circuit-breaker** kombinasyonudur (ERROR_HANDLING_GUIDELINE §4.2 "external service calls için ZORUNLU" ilkesi burada DLQ-bridge→engine çağrılarına [incident/failure-event oluşturma] uygulanır). **CB OPEN iken DLQ mesajları KAYBOLMAZ** — `DLQ` stream'i kalıcıdır (limits-based, default 14g retention, BR-SUB-004), mesajlar stream'de bekler (nak edilmiş durumda, redelivery CB HALF_OPEN/CLOSED'a döndüğünde devam eder). Bu, `dlq-of-dlq YOK` ilkesiyle (hiçbir zaman ack-drop) tutarlıdır — CB'nin amacı bozuk bir downstream'e (Cockpit DB / Event Registry) karşı hot-loop'u önlemektir, mesaj kaybını önlemek değildir (o zaten stream kalıcılığıyla garanti altında).

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | DLQ-bridge işlemi ilk denemede/az sayıda denemede başarısız | Standart `nakWithDelay` backoff — CB henüz CLOSED |
| 2 | 5 ardışık başarısızlık (aynı downstream'e, örn. Cockpit DB down) | CB → **OPEN** — bu süre boyunca yeni denemeler hızlıca fail-fast edilir (downstream'e yeni istek gitmez), mesajlar nak'lı stream'de bekler |
| 3 | OPEN'dan 30s sonra | CB → **HALF_OPEN** — sınırlı sayıda deneme downstream'in iyileşip iyileşmediğini test eder |
| 4 | HALF_OPEN'da 3 ardışık başarı | CB → **CLOSED** — normal işleme döner, bekleyen DLQ mesajları redeliver edilip işlenir |
| 5 | HALF_OPEN'da başarısızlık | CB → tekrar **OPEN** (30s sayaç sıfırlanır) |

**Bağımlılık:** BR-A2-009, BR-FLW-003 (DLQ-bridge'in temel davranışı), `SYS_DLQ_BRIDGE_PROCESSING_FAILED` (EXCEPTION_CODES.md).

---

### BR-OBS-001: Normalize DB-roundtrip metriği — TEK sert kapı
**User Story:** US-D1 | **FR:** FR-D1 | **Öncelik:** Must

**Tanım:** Task-yaşamdöngüsü başına DB round-trip bileşenleri raporlanır (Task INSERT=1, Poll=0, fetchAndLock UPDATE=0, complete tx=1, sweep okuması≈~0). Bu metrik basamak-1 kapanışının **TEK** sert kabul kapısıdır (Q7).

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | A2-push modu ölçülür | poll+fetchAndLock=0, INSERT/complete artmıyor |
| 2 | Native-poll baseline ölçülür | Karşılaştırma referansı |
| 3 | `fetchAndLock` SQL'inin ayrı fingerprint verdiği | `[phase3'te doğrulanacak]` |

**Bağımlılık:** BR-A2-001, BR-A2-002, BR-A2-008, BR-OBS-003.

---

### BR-OBS-002: Destekleyici SLI katmanı (soft target)
**User Story:** US-D2 | **FR:** FR-D2 | **Öncelik:** Should (Q6 dahil)

**Tanım:** fetchAndLock QPS, lock-wait, HikariCP connection, dispatch p95, failure sayaçları izlenir; **hiçbiri sert kapı DEĞİLDİR** (Q7).

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Dispatch p95 > 200ms | Log/rapor, bench FAIL etmez |
| 2 | fetchAndLock QPS hot-path'te > 0 | Anomali sinyali (ama BR-OBS-001'in gölgesinde, ayrı sert kapı değil) |

**Bağımlılık:** BR-OBS-001.

---

### BR-OBS-003: Testcontainers bench — iki mod
**User Story:** US-D3 | **FR:** FR-D3 | **Öncelik:** Must

**Tanım:** Aynı senaryo native-poll ↔ A2-push modlarında koşar (`@Tag("bench")`, nightly/manuel); BR-OBS-001 metriğini iki mod için üretir.

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Docker/Testcontainers ortamı hazır | Bench koşar |
| 2 | Ortam hazır değil (CI'da Docker yok) | `SYS_BENCH_ENVIRONMENT_UNAVAILABLE` — build FAIL ETMEZ (nightly/manuel, ana CI'yı bloklamaz) |
| 3 | Bench sonucu BR-OBS-001 hedefini kaçırırsa | `BUS_BENCH_METRIC_REGRESSION` — sert kapı ihlali |

**Bağımlılık:** BR-OBS-001, BR-A2-001.

---

### BR-MIG-001: JavaDelegate outbound tam phase-out
**User Story:** US-E1 | **FR:** FR-E1 | **Öncelik:** Must

**Tanım:** Üç motorda tüm JavaDelegate outbound (senkron dahil, fast-RPC istisnası YOK) kaldırılır.

**Kanıt:** `NatsPublishDelegate.java:17`, `JetStreamPublishDelegate.java:17`, `NatsRequestReplyDelegate.java:19,56` (`connection.request(...)`, 30s in-tx blocking) — Camunda+CadenzaFlow; Flowable `requestreply/NatsRequestReplyDelegate.java:19`.

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Delegate kullanan model migrate edilir | A2 / Event Registry idiomuna taşınır |
| 2 | Fast-RPC istisnası talep edilirse | REDDEDİLDİ (05 §9) |

**Bağımlılık:** BR-A2-001, BR-FLW-001.

---

### BR-MIG-002: İdiom ayrımı netliği
**User Story:** US-E2 | **FR:** FR-E2 | **Öncelik:** Should (Q6 dahil)

**Tanım:** İş dağıtımı (A2/Event Registry) ↔ saf message-correlation (gerçek dış event) ayrımı dokümante edilir. `docs/04`'ün message-correlation idiomu iş dağıtımı için GEÇERSİZDİR (A2 supersede eder).

**Koşullar:**
| # | Koşul | Beklenen sonuç |
|---|---|---|
| 1 | Yeni model iş dağıtımı ihtiyacı | A2 / Event Registry kullanır |
| 2 | Yeni model gerçek dış event bekliyor | `correlateWithResult()` message-correlation kullanır |
| 3 | Dispatch için message-correlation seçilirse | Anti-pattern, tez ihlali — dokümantasyon bunu reddeder |

**Bağımlılık:** BR-A2-008.

---

## 4. Veri doğrulama kuralları

| Alan | Format | Aralık/bağımlılık | Cross-field |
|---|---|---|---|
| `Nats-Msg-Id` (job/reply) | string, boş olamaz | A2: `externalTaskId`; Event Registry: correlation key | Zorunlu — WorkQueue dedup için (BR-SUB-005) |
| `Nats-Msg-Id` (DLQ publish) | `<orijinal-msg-id>.dlq` | Yalnız DLQ publish yolunda | Orijinal id'ye bağımlı (BR-SUB-003) |
| `X-Cadenzaflow-Trace-Id` | string | Yazma zorunlu, okuma fallback | `X-Trace-Id` yalnız OKUNUR, hiç YAZILMAZ (BR-SUB-006) |
| `X-Cadenzaflow-Business-Key` | string, opsiyonel | Telco bağlamında MSISDN/abone-id olabilir | Masking kararı kiracıya ait (DP-8, normatif değil) |
| `L` (sentinel lockDuration) | ≥ M·W+Σbackoff+S+ε; altına düşerse **reject-startup** (BAQ-3 kararı, escape: `allow-unsafe-lock-duration=true` + kalıcı WARN) | Topic-başına override edilebilir | W/M/S/ε değişirse L yeniden türetilmeli |
| `W` (ack-wait) | > 0, topic p99 iş süresi + marj | Topic-başına override | L formülünü etkiler |
| `M` (maxDeliver) | ≥ 1, default 4 | — | Σbackoff = Σ 2^(n-1) (n=1..M-1) formülünü etkiler |
| `retryDuration` (handleFailure çağrısı, DLQ→incident bridge) | **sabit 0** (BAQ-2 kararı, 2026-07-14 — artık serbest parametre DEĞİL) | DLQ→incident bridge'in çağrı sitesine sabitlenir | `lockExpirationTime=now` → Cockpit-retry residual-lock gecikmesi olmaz |
| Mesaj gövdesi (job/reply/event) | boş OLAMAZ (business kuralı) | — | **BAQ-5 kararı (contract-fix #5):** boş body artık WARN+DLQ'ya yönlendirilir (BR-SUB-007), sessiz ack YOK |

---

## 5. Entegrasyon noktaları

### Entegrasyon: Motor-dışı polyglot Worker
**Tür:** NATS/JetStream (WorkQueue push/pull)
**Yön:** Bidirectional (job tüketir, reply üretir)

**Kontrat:**
- **Subject:** `jobs.<type>` (job) / `jobs.<type>.reply` (reply) — A2; event channel subject'leri — Event Registry.
- **Protokol:** NATS/JetStream 2.10+.
- **Auth:** NKey/JWT (production zorunlu) — `[phase3'te doğrulanacak]` (NFR-S3).
- **Timeout/ack-wait:** W (default 30s, topic-başına override).

**Veri eşleme:**
| Bizim alan | Worker alanı | Dönüşüm | Zorunlu |
|---|---|---|---|
| `externalTaskId` | job id | Direkt | Evet (A2) |
| `X-Cadenzaflow-Trace-Id` | trace context | Direkt (fallback: eski ad) | Hayır |
| Process değişkenleri | job/reply payload | Kiracı-tanımlı serileştirme | Evet |

**Hata yönetimi:**
| Worker hatası | Bizim aksiyonumuz | Business Exception |
|---|---|---|
| Worker crash (reply hiç gelmez) | JetStream redelivery (M'e kadar) | `SYS_WORKER_TRANSIENT_FAILURE` |
| Worker business-error reply | `handleBpmnError` | `BUS_WORKER_BUSINESS_ERROR` |
| Worker deliveryCount>M | DLQ + incident/failure-event | `BUS_JOB_DELIVERY_BUDGET_EXCEEDED` |
| Worker yanlış workerId ile complete dener (asla olmamalı) | CRITICAL + on-call page (BAQ-7) | `SYS_SENTINEL_WORKER_CONFLICT` |

**SLA:** Dispatch latency p95 ≤ 200ms (soft, Q7); worker'ın kendi SLA'sı kiracı-tanımlı (BR-FLW-004 opt-in timer).

---

## 6. Uyumluluk / veri koruma referansı

Bu belge PII sınıflandırmasını **tekrarlamaz** — tam envanter `docs/sentinel/phase1/DATA_CLASSIFICATION.md`'dedir. İş kuralı düzeyinde bağlayıcı noktalar:
- **DP-1/DP-2 (NFR-S1):** Payload/Business-Key değerleri hiçbir BR'nin log/metrik davranışında görünmez (bkz. BR-SUB-001 `-Dlq-Reason` notu).
- **DP-3 (NFR-S2):** DLQ retention 14g default + kiracı-bazlı config — BR-SUB-004'ün retention alanı.
- **DP-8 (normatif değil):** Business-Key masking kiracı kararı — hiçbir BR bunu zorunlu kılmaz.
- **KVKK/GDPR:** Worker güven sınırı dışına çıkan payload/Business-Key için `TENANT_PII_CHECKLIST_TEMPLATE.md` doldurulmadan production açılmaz (Q5 kararı) — bu, BR'lerin ÖNKOŞULUDUR, ayrı bir BR değildir.

---

## 7. Reddedilenler (kilitli, bu fazda yeniden açılmadı)

| Öğe | Durum | Bu BR kataloğuna etkisi |
|---|---|---|
| Hot central poller | REDDEDİLDİ | BR-A2-004'te "REDDEDİLDİ" olarak anıldı, kural olarak modellenmedi |
| Timer-only escalation | REDDEDİLDİ | BR-FLW-004'te anıldı |
| DLQ→ops-only escalation | REDDEDİLDİ | BR-FLW-003'ün zıttı, modellenmedi |
| Advisory-tabanlı DLQ tespiti | REDDEDİLDİ | BR-SUB-002 yalnız in-band `maxDeliver+1`'i modeller |
| Post-commit `lock()` / lazy kilit | REDDEDİLDİ | BR-A2-002 yalnız doğumda-kilit modelini kural yaptı |
| İdiom-başına ayrı DLQ stream | REDDEDİLDİ | BR-SUB-004 |
| Yalnız-mutlak-QPS / latency-öncelikli metrik | REDDEDİLDİ | BR-OBS-001/002 ayrımı |
| InProgress heartbeat (D-H) | ERTELENDİ | BR-A2-007 |
| gRPC ön kapısı (D-G) | ERTELENDİ | Bu belgede yok |

---

## 8. İzlenebilirlik özeti (US → BR → FR)

| US | BR | FR |
|---|---|---|
| US-A1 | BR-A2-001 | FR-A1 |
| US-A2 | BR-A2-002, BR-A2-003 | FR-A2, FR-A3 |
| US-A3 | BR-A2-004, BR-A2-005, BR-A2-013 | FR-A4, FR-A5, FR-A6 |
| US-A4 | BR-A2-008 | FR-A7 |
| US-A5 | BR-A2-006, BR-A2-007 | FR-A8, FR-A9 |
| US-A6 | BR-A2-009, BR-A2-010 | FR-A10, FR-A11 |
| US-A7 | BR-A2-011 | FR-A12 |
| US-A8 | BR-A2-012 | FR-A13 |
| US-B1 | BR-FLW-001 | FR-B1 |
| US-B2 | BR-FLW-002 | FR-B2, FR-C6 |
| US-B3 | BR-FLW-003 | FR-B3 |
| US-B4 | BR-FLW-004 | FR-B4 |
| US-B5 | BR-FLW-005 | FR-B5 |
| US-C1 | BR-SUB-001 | FR-C1 |
| US-C2 | BR-SUB-002, BR-SUB-007 (edge-case, BAQ-5) | FR-C2, FR-C6 |
| US-C3 | BR-SUB-003 | FR-C3 |
| US-C4 | BR-SUB-004 | FR-C4 |
| US-C5 | BR-SUB-005 | FR-C5 |
| US-C6 | BR-SUB-006 | FR-C7 |
| US-D1 | BR-OBS-001 | FR-D1 |
| US-D2 | BR-OBS-002 | FR-D2 |
| US-D3 | BR-OBS-003 | FR-D3 |
| US-E1 | BR-MIG-001 | FR-E1 |
| US-E2 | BR-MIG-002 | FR-E2 |
| US-A6, US-B3 | BR-SUB-008 (edge-case, BAQ-6 — DLQ-bridge dayanıklılığı, genel-substrat) | FR-A10, FR-B3 |

**Sonuç:** 24/24 US kapsandı (**0 boşluk**). Toplam **31 iş kuralı** (BR-A2-013, BR-SUB-007, BR-SUB-008 tek başlarına bir US'ye değil, mevcut US'lerin kenar-durum/genel-substrat genişletmesine karşılık gelir — BAQ-1/BAQ-5/BAQ-6 kararlarıyla 2026-07-14'te eklendi; ayrı satır olarak izlenmesi ilgili karar matrislerinde gereklidir).

---

## 9. BA-QUESTIONS — Karar Kaydı (Q→A, 2026-07-14)

Aşağıdaki 8 soru bu fazda kaynak koddan bizzat doğrulanan kanıtlarla ortaya çıktı; hiçbiri Phase 1'in reddettiği/erteldiği kararları yeniden açmadı. Levent tarafından **2026-07-14 tarihinde tamamı cevaplandı** (soru metinleri korunur, kararlar eklenir — Sentinel doküman revizyon disiplini). Kararların teslimatlara işlenişi §1-§8'de ilgili BR/matris/kod satırlarında görülebilir.

| # | Soru (BAQ-N) | Verilen karar (2026-07-14) | Bu fazda uygulanışı |
|---|---|---|---|
| **BAQ-1** | Sweep re-lock (DB yazısı) başarılı olup re-publish (JetStream) başarısız olursa satır bir sonraki ≤L (320s) süresince "taze kilitli" görünüp sweep'e görünmez hale geliyor (bkz. BR-A2-005/013, state machine §2.1). Re-lock/re-publish sırası (publish-önce mi, kısa-geçici-kilit mi) nasıl garanti altına alınmalı? | **Re-lock → publish sırası SABİT.** İki adım arası çökme **kabul edilen nadir durumdur** (bedel ≤ +L ek gecikme, kalıcı kayıp değil). Atomiklik mekanizması **Phase 3/4'e** bırakılır. | BR-A2-013 ve state machine §2.1 guard notu güncellendi; Karar Matrisi 3 satır 5 ve `SYS_SWEEP_REPUBLISH_FAILED` buna göre netleşti. |
| **BAQ-2** | DLQ→incident bridge'in `handleFailure(..., retries=0, retryDuration=X)` çağrısı `lockExpirationTime=now+X` set ediyor; Cockpit-retry bu alana dokunmuyor. `retryDuration` 0'a mı sabitlenmeli, yoksa kasıtlı bir tampon mu isteniyor? | **`retryDuration=0` SABİTLENDİ.** `lockExpirationTime=now` → Cockpit-retry ne zaman verilirse verilsin residual-lock gecikmesi olmadan akar. | BR-A2-009/010, flow 1.3, state machine §2.1, Karar Matrisi 3 satır 6, `EXCEPTION_CODES.md` §4 veri doğrulama tablosu güncellendi. |
| **BAQ-3** | US-A5 AC metni "validasyon/uyarı" diyor. Operatör L'yi formül alt sınırının altına yazarsa topic aktivasyonu reddedilsin mi (hard reject) yoksa yalnız WARN log mu? | **HARD-REJECT (reject-startup) default.** Bilinçli kaçış: `allow-unsafe-lock-duration=true` flag'i + kalıcı WARN (her döngüde, sessiz-unutma yok). | BR-A2-006, `VAL_UMBRELLA_LOCK_TOO_SHORT` (EXCEPTION_CODES.md), Ek Matris 6 (DECISION_MATRIX.md) güncellendi. |
| **BAQ-4** | `dlq.jobs.>` subject'i Camunda/CadenzaFlow incident-bridge'e sabit routing ile atanıyor; bir Flowable kiracısı inbound channel'ı `jobs.<x>` adlandırırsa DLQ mesajı yanlış bridge'e gider. `jobs.*` A2'ye REZERVE edilsin mi? | **REZERVE EDİLDİ + aktif bootstrap validasyonu.** `jobs.*` yalnız A2'ye aittir; Flowable channel'ı bu önekle tanımlanırsa deployment-time `VAL_TOPIC_NAMESPACE_COLLISION` (ERROR) deployment'ı ENGELLER. | BR-SUB-004, `VAL_TOPIC_NAMESPACE_COLLISION` (EXCEPTION_CODES.md), Ek Matris 5 satır 3 (DECISION_MATRIX.md) güncellendi. |
| **BAQ-5** | Mevcut kod boş-body mesajı sessizce ACK'liyor (DEBUG log, metrik yok). Bu "4 fix" listesine 5. madde olarak eklenmeli mi, yoksa bilinçli savunma-kodu mu? | **5. KONTRAT AÇIĞI.** Yeni davranış: WARN + DLQ'ya yönlendir + ACK (sessiz DEBUG+ack kalkar). Hem Flowable hem A2/CadenzaFlow tarafında aynı anda düzeltilir. `docs/06 §7` kontrat-fix listesi ayrı olarak (bu repo dışında) güncellenmektedir. | Yeni **BR-SUB-007** eklendi; flow 1.4/1.6, Karar Matrisi 1.B satır 2, `VAL_EMPTY_MESSAGE_BODY` (EXCEPTION_CODES.md) güncellendi. |
| **BAQ-6** | DLQ-bridge kendi işleyemezse nak+alert deniyor ama backoff/circuit-breaker politikası netleşmemişti (hot-loop riski). Standart consumer backoff mu paylaşılsın, ayrı politika mı? | **nak-backoff + circuit-breaker** (ERROR_HANDLING_GUIDELINE §4.2 eşikleri: 5 ardışık hata→OPEN, 30s→HALF_OPEN, 3 ardışık başarı→CLOSED). CB OPEN iken DLQ mesajları stream'de kalıcı bekler — kayıp YOK. | Yeni **BR-SUB-008** eklendi; `SYS_DLQ_BRIDGE_PROCESSING_FAILED` (EXCEPTION_CODES.md), Karar Matrisi 1.C satır 3 (DECISION_MATRIX.md) güncellendi. |
| **BAQ-7** | `BadUserRequestException` (workerId eşitsizliği invariant ihlali) ERROR seviyesinde loglanıp on-call'a page mi atmalı? | **CRITICAL + page.** Invariant ihlali, "BUS_=WARN" kuralının kapsamı DIŞINDA — anında insan müdahalesi gerekir. | BR-A2-003, flow 1.2, Karar Matrisi 2 satır 3, `SYS_SENTINEL_WORKER_CONFLICT` (EXCEPTION_CODES.md), §5 entegrasyon tablosu güncellendi. |
| **BAQ-8** | Flowable failure-event bridge'in `eventReceived()` çağrısı bekleyen subscription bulamazsa (NFR-R6 token-leak riski) WARN+metrik mi, ERROR+alert mi? | **WARN + metrik + eşik-alarmı.** Tek olay benign yarış olabilir; `failure_event_correlation_miss` sayacı üzerinden süreklilik/eşik-bazlı ayrı alarm tanımlanır. | BR-FLW-003, flow 1.5, Karar Matrisi 1.C satır 5 ve Matris 4 satır 3, `RES_FAILURE_EVENT_CORRELATION_MISS` (EXCEPTION_CODES.md) güncellendi. |

**Not:** BAQ-1…8 sorularının hiçbiri Phase 1'in kapsam-dışı/reddedilen kararlarını (hot-poll, timer-only, advisory-DLQ, heartbeat/D-H, gRPC/D-G) yeniden açmamıştır; hepsi mevcut US/FR'lerin içindeki kenar-durum/dayanıklılık netleştirmeleridir.

---

*Devamı: `DECISION_MATRIX.md` (4 ana + 2 destekleyici karar matrisi), `EXCEPTION_CODES.md` (exception-code kataloğu). BAQ-1…8 kararları her iki belgede de ilgili satırlara işlenmiştir.*
