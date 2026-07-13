# Strateji: External Task / Event-Driven Work Offload — JetStream Substratı, İki Engine İdiomu

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Tarih:** 2026-06-28
**Durum:** Stratejik tasarım / ADR seviyesi. **LLD değildir.** `05-db-offload-strategy.md` **basamak 1 (Dispatch push)**'in somutlaştırılması.
**Bağlam:** `05-db-offload-strategy.md` (omurga, §6.7 basamak 1 + P1/P2), `04-async-request-reply-design.md` (async desen), memory `sync-request-reply-holds-db-transaction`, `project-thesis-db-offload`.

---

## 0. Bu belge nasıl kullanılır (Sentinel girişi)

Bu doküman **basamak 1**'i (dispatch/external-task acquisition → NATS push) iki engine ailesi için ayrı ayrı somutlaştırır ve ortak substratı sabitler. Geliştirme **Sentinel** ile yürür:

- Camunda 7 / CadenzaFlow yolu (**A2 external-task-over-JetStream**) ve Flowable yolu (**Event Registry-over-JetStream**) **ayrı Sentinel pipeline**'larına girer; **ortak JetStream substratını** (§8) paylaşırlar.
- **Kilitli kararlar** §1 ve §4'tedir; **açık kararlar** §9'dadır (phase1/phase3 girdisi).
- Padding yok: her bölüm bir karara veya `file:line` kanıtına hizmet eder. Doğrulanmamış iddialar §10'da işaretli.

---

## 1. Karar özeti (kilitli)

1. **JavaDelegate tabanlı outbound tamamen phase-out edilir.** İş, motorun DB transaction'ı içinde koşmayacak (§3 kanıt).
2. **İki engine, iki idiom, tek substrat:**
   - **Camunda 7 + CadenzaFlow → A2:** native external-task semantiğini koru, `fetchAndLock` polling'ini **JetStream push** ile değiştir; job'ı JetStream WorkQueue taşır (§5).
   - **Flowable → Event Registry:** native push'lu Event Registry zaten doğru idiom; A2 retrofit edilmez. Mevcut channel adapter **basamak-1 olgunluğuna** çekilir (§6).
   - Her ikisi **ortak JetStream substratı + wire-contract** üzerinde buluşur (§8, docs/05 §7).
3. **Dürüst tavan:** Bu basamak **dispatch/polling'i** kaldırır; **token-move/correlate transaction** (P2) kalır — o ancak docs/05 basamak 6'da buharlaşır. "Sıfır DB lock" diye satılmaz.
4. **gRPC bu belgenin kapsamı dışında.** Worker doğrudan JetStream konuşur. gRPC, yalnızca worker-sınırı gerekçeleriyle (Zeebe ekosistem uyumu / broker'sız-kısıtlı worker) eklenecek **opsiyonel ön kapı**dır; ayrı belgeye ertelendi (§8 D-G).
5. **İdiom ayrımı (A2, `04-async-request-reply-design.md`'i supersede eder):** request-reply / **iş dağıtımı → A2** (external-task: lifecycle + Cockpit + `taskId`). `docs/04`'ün message-correlation idiom'u bu kullanım için **geçersiz.** Saf **message-correlation yalnız gerçek "olay bekleme"** için kalır (bizim dispatch etmediğimiz dış event / callback / business-event). Camunda/CadenzaFlow'da bu ayrım, Flowable'daki external-task ↔ Event-Registry ayrımının aynısıdır. Inbound subscriber'ın `correlateWithResult()` yapısı atılmaz → A2 completion-bridge'e (`externalTaskService.complete`) evrilir (§5.2).

---

## 2. Neden bu belge

Mevcut entegrasyon iki yarımdan oluşuyor:
- **Outbound:** service-task `JavaDelegate` (motor içinde, in-transaction).
- **Inbound:** message-correlation subscriber (motor dışından gelen reply'ı token'a bağlar).

Tez (docs/05 §1) "yüksek-hacimli işin DB transaction'ına yük bindirmesini" hedef alıyor. Outbound delegate tam da bunu ihlal ediyor (§3). Bu belge outbound'u **decoupled, push-tabanlı, motor-dışı** bir modele taşır; inbound'un büyük kısmı yeniden kullanılır.

---

## 3. Phase-out: JavaDelegate neden gidiyor (kanıt)

Camunda outbound delegate'lerinin üçü de `implements JavaDelegate` → motorun `CommandContext` = DB transaction'ı içinde koşar:

- `camunda-nats-channel/.../outbound/NatsPublishDelegate.java:17`
- `camunda-nats-channel/.../outbound/JetStreamPublishDelegate.java:17`
- `camunda-nats-channel/.../outbound/NatsRequestReplyDelegate.java:19`

En kötü suçlu — senkron blocking I/O **transaction içinde**:

```java
// NatsRequestReplyDelegate.java:56  (timeout default 30s)
Message reply = connection.request(subjectVal, data, timeoutVal);
```

Bu satır, motorun token'ı ilerleten transaction'ı içinde bir ağ round-trip'i (30s'ye kadar) boyunca **DB transaction + execution optimistic lock'unu açık tutar** (memory `sync-request-reply-holds-db-transaction`). `NatsPublishDelegate`/`JetStreamPublishDelegate` bloklamaz ama yine in-tx'tir (dual-write/D2 riski).

Flowable'da karşılığı: `flowable-nats-channel/.../requestreply/NatsRequestReplyDelegate.java:19` (aynı in-tx blocking deseni) — o da phase-out kapsamında.

**Karar:** üç delegate (× ilgili motorlar) çıkar; outbound = motor-dışı push (§5/§6).

---

## 4. İki idiom, tek substrat

| | **A2 — External task (Camunda7/CadenzaFlow)** | **Event Registry (Flowable)** |
|---|---|---|
| Zihinsel model | **İş dağıtımı** ("şu işi biri yapsın, raporlasın") | **Olay mesajlaşması** ("event yayınla/bekle", correlate) |
| Birim | `ACT_RU_EXT_TASK` (kimlikli görev) | event + correlation key |
| Teslimat | poll (native) → **A2 ile push** | **native push** (channel adapter, queue-group) |
| "Tek worker alır" | lock / JetStream WorkQueue claim | queue-group / correlation |
| İş yaşam döngüsü | lock + retry-count + `handleFailure` + BPMN-error + incident (motor yönetir) | yok — hata messaging seviyesinde (nack/DLQ) + modellenen boundary-timer |
| Wait-state DB izi | ext-task entity | event subscription (`ACT_RU_EVENT_SUBSCR`) |
| token-move tx (P2) | `complete()` = kısa tx **kalır** | correlate-continue = kısa tx **kalır** |
| Fan-out / event-start | yok (mid-process iş) | **var** (1 event → N correlation, process start) |
| Cross-engine taşınabilirlik | Camunda7 ↔ CadenzaFlow birebir | Flowable'a özgü idiom |
| Bu repoda | henüz yok (yazılacak) | **zaten implemente** (inbound/outbound + JetStream) |

**Çekirdek tespit:** Her iki idiom da aynı hedefe (push not poll) ve aynı tavana (token-move tx kalır, P2) varır. Fark: Camunda'da push'u *biz inşa ederiz* (ext-task entity ile güreşerek); Flowable'da push *modelin doğasında* gelir.

---

## 5. Camunda 7 / CadenzaFlow — A2: job-push external task over JetStream

### 5.1 Hedef ve kapsam
Native external-task **semantiğini koru** (BPMN `camunda:type="external"`, `complete`/`handleFailure`/BPMN-error, Cockpit görünürlüğü) — sadece **fetchAndLock polling'ini JetStream push ile değiştir**. Job payload'ı JetStream taşır; WorkQueue claim'i `SELECT FOR UPDATE`'in yerine geçer. Camunda7 ↔ CadenzaFlow birebir taşınır.

### 5.2 Mimari

```
[Engine node] token → ext-task row (ACT_RU_EXT_TASK) → COMMIT
     │  ① post-commit TransactionListener (aynı node, tx DIŞI, elindeki veriyle → DB sorgusu YOK, yarış YOK)
     │  ② soğuk orphan-sweep (seyrek · read-only · FOR-UPDATE'siz · tek node) — yalnız ①'in kaçırdığı çökme-orphan'ları
     ▼
 JetStream  jobs.<topic>   (WorkQueue stream, Nats-Msg-Id = externalTaskId → dedup)
     │  durable consumer (push/pull), queue-group LB
     ▼
 [Worker]  (motor-dışı, polyglot)  → iş → sonuç → jobs.<topic>.reply
     │
     ▼
[Inbound bridge] → externalTaskService.complete(extTaskId, workerId, vars)   ← KISA token-move tx
     hata → handleFailure / handleBpmnError ;  JetStream nack → maxDeliver → DLQ
```

**İnbound yarısı neredeyse hazır:** mevcut `camunda-nats-channel/.../inbound/NatsMessageCorrelationSubscriber.java:96-104` deseni (`createMessageCorrelation(...).correlateWithResult()`). Tek fark, çağrının `externalTaskService.complete(...)` olması. Ağırlıklı yeni kod = **outbound push-bridge**.

### 5.3 Outbox ilkesi + tetikleme (D-A çözüldü: hot-poll REDDEDİLDİ)
`ACT_RU_EXT_TASK` tablosu **transactional outbox**'tur — tek DB yazımı = doğruluk kaynağı; push onun idempotent türevidir → **dual-write yok** (docs/05 D2 doğal çözülür).

**Tetikleme = post-commit `TransactionListener` + soğuk orphan-sweep.** Task'ı oluşturan node, commit'ten *sonra* (tx dışı) o task'ı **elindeki veriyle** yayınlar — happy-path'te **DB sorgusu yok, cross-node yarış yok** (herkes yalnız kendi yarattığını yayınlar). Fast-path at-most-once; kaçan çökme-orphan'ları **seyrek · read-only · FOR-UPDATE'siz** bir sweep (tek node/leader) toplar, `Nats-Msg-Id` dedup çift-yayını yutar → net **at-least-once**.

**REDDEDİLEN — (a) merkezi hot poller (2026-07-02):** N engine-node her biri poller çalıştırırsa aynı `ACT_RU_EXT_TASK`'ta `fetchAndLock` (SELECT FOR UPDATE) için **yarışır** → contention worker'dan engine-poller'a *taşınır*, kalkmaz (P1). Contention **sıcak poll döngüsündedir**; post-commit listener o döngüyü tümden kaldırır, geriye yalnız soğuk-seyrek okuma kalır — basamak-1'in DB kazancı budur.

### 5.4 Doğruluk tehlikeleri (peşinen işaretli)
- **İki redelivery saati:** JetStream `ack-wait`/`maxDeliver` ile external-task `lockDuration` **hizalanmalı**; yoksa motor task'ı yeniden fetchable yaparken JetStream da redeliver eder → çift işleme. Hizalama + ack disiplini şart.
- **Lock sahipliği:** `complete(taskId, workerId)` task'ın o `workerId`'ye kilitli olmasını ister. Worker `fetchAndLock` yapmadığı için task **oluşturulurken/post-commit** bir **sentinel workerId**'ye kilitlenir (poller yok) ve bu id payload'da taşınır; complete o workerId ile yapılır.
- **Idempotency:** inbound `complete` idempotent olmalı (zaten-complete task → yakala + ack). Doğal idempotency anahtarı `externalTaskId`.

### 5.5 Ne kalkar / ne kalır
| | Native external task | A2 (JetStream push) |
|---|---|---|
| Token park (wait-state) | ✅ commit, lock yok | ✅ aynı |
| **fetchAndLock poll/lock storm** | N worker × `SELECT FOR UPDATE` | **kalkar** — ne worker ne poller poll'ü; happy-path'te DB sorgusu yok (post-commit listener) |
| `ACT_RU_EXT_TASK` satırı | 1 INSERT/task | **kalır** (outbox olarak kullanılıyor) |
| `complete` = token-move tx | kısa tx + optimistic lock | **kalır** (P2 → basamak 6) |

---

## 6. Flowable — Event Registry over JetStream

### 6.1 Native push (kanıt)
Flowable entegrasyonu Event Registry channel adapter'larıdır — **push by design, polling yok, external-task entity yok:**

- `flowable-nats-channel/.../NatsInboundEventChannelAdapter.java:49` → `dispatcher.subscribe(subject, queueGroup, this::handleMessage)` (push + queue-group LB)
- `:88` → `eventRegistry.eventReceived(inboundChannelModel, event)` (Event Registry'ye devir → correlation)
- `NatsOutboundEventChannelAdapter.java:29` → `sendEvent(rawEvent, headerMap)` (outbound publish)
- `NatsChannelDefinitionProcessor.java:22` → `implements ChannelModelProcessor` (inbound/outbound kayıt)
- JetStream varyantları: `jetstream/JetStreamInboundEventChannelAdapter.java:152` (`eventReceived`), `jetstream/JetStreamOutboundEventChannelAdapter.java:36` (`sendEvent`)

Yani basamak 1'in büyük kısmı (poll→push) Flowable'da **zaten yapılmış.**

### 6.2 Yapılacak: basamak-1 olgunluğu
- **Delegate phase-out:** `requestreply/NatsRequestReplyDelegate.java:19` (in-tx blocking) çıkar; outbound = `sendEvent` (motor-dışı tüketici işi yapar, sonucu inbound event olarak correlate eder).
- **JetStream sağlamlığı:** core adapter (`:88`) ack'siz, hata'da sadece log'luyor → exactly-once-ish için JetStream variant + **ack + DLQ + dedup** (`Nats-Msg-Id`/correlation idempotency).
- **Escalation:** correlate edilmeyen event → motor bekler; "garanti yapılsın ya da escalate" gereken işlerde **boundary timer** modellenir (Event Registry job-lifecycle vermez).

### 6.3 Ne kalkar / ne kalır
- **Kalkar:** dispatch polling (zaten yok), in-tx blocking delegate.
- **Kalır:** event subscription wait-state + correlate-continue token-move tx (P2 paritesi).

---

## 7. Ortak JetStream substratı (her iki idiom — docs/05 §7)

- **Subject şeması:** `jobs.<type>` / event channel subject'leri; `*.reply` / inbound channel.
- **Stream tipi:** iş dağıtımı için **WorkQueue** (her mesaj tek tüketiciye, nack→redeliver).
- **Dedup:** `Nats-Msg-Id` (A2'de `externalTaskId`; Event Registry'de correlation key) + apply-zamanı idempotency (docs/05 P3/§10).
- **DLQ:** `dlq.<subject>`, `maxDeliver`, `nakWithDelay`.
- **Header'lar:** mevcut `BpmHeaders` (`X-Cadenzaflow-Trace-Id`, `-Business-Key`, `-Idempotency-Key`, async: `-Correlation-Id`, `-Reply-Subject`).
- **Kısıt:** iki idiom da aynı teli yayar/tüketir → worker ekosistemi paylaşılır (docs/05 P5).

---

## 8. Açık kararlar (Sentinel phase1/phase3)

- **D-A — A2 outbound tetikleme:** ✅ **ÇÖZÜLDÜ (2026-07-02)** = post-commit `TransactionListener` (oluşturan node, tx dışı, **sorgusuz**) + soğuk read-only orphan-sweep + `Nats-Msg-Id` dedup. Hot central poller **reddedildi** (N-node poller `fetchAndLock` contention'ı = P1 ihlali). (§5.3)
- **D-B — lockDuration ↔ ack-wait hizalaması:** somut değerler + redelivery politikası. (§5.4) — **sıradaki açık karar.**
- **D-C — sentinel-lock modeli:** task oluşturulurken/post-commit sentinel workerId ile kilitlenir (poller yok); `complete` o workerId ile; complete-path lock doğrulaması phase3'te. (§5.4)
- **D-D — Flowable escalation:** boundary-timer pattern'i mi, başka bir SLA mekanizması mı? (§6.2)
- **D-E — DLQ/ack semantiği:** iki idiom için ortak mı, idiom-özel mi? (§7)
- **D-F — başarı metriği:** poll-storm azalması nasıl ölçülür (DB QPS / lock-wait)? Baseline + hedef.
- **D-G — gRPC ön kapısı:** gerekli mi (Zeebe-uyum / broker'sız worker), ne zaman? Ayrı belge. (§1.4)

---

## 9. Doğrulama notları (kanıt-temelli — phase3 girdisi)

- **External-task subsystem'in CadenzaFlow fork'unda varlığı** → Camunda7 paritesi (ACT_RU_EXT_TASK, `externalTaskService`) `~/Workspaces/cadenzaflow`'da doğrulanacak (assert değil, doğrula).
- **Flowable external-worker job vs Event Registry** → bu belge Event Registry yolunu seçiyor; external-worker job modeli kapsam dışı, gerekirse ayrı brief.
- **`complete()` lock zorunluluğu** → Camunda engine'de `complete` task'ın workerId-lock'unu ister; sentinel-lock şeması (D-C) buna göre doğrulanacak.
- **Mevcut kodda external task yok** → `camunda/cadenzaflow` kaynaklarında `ExternalTask`/`fetchAndLock`/`camunda:type="external"` sıfır eşleşme (2026-06-28); A2 tamamen yeni kod.
- **token-move tx kalır** → zaten kanıtlı (P2; memory `sync-request-reply-holds-db-transaction`).
