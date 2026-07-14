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
[Engine node] token → ext-task row (ACT_RU_EXT_TASK, doğumda sentinel-kilitli — D-C) → COMMIT
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

### 5.4 İki redelivery saati (D-B çözüldü: şemsiye kilit — JetStream tek otorite)

**Kanıt önce** (fork: `~/Workspaces/cadenzaflow/cadenzaflow-bpm-platform/engine`, hepsi upstream-Camunda7 davranışı — **D-B fork değişikliği gerektirmez**, tümü public API):
- `HandleExternalTaskCmd.java:89-91` — `complete`/`handleFailure` YALNIZ `workerId` eşitliğini kontrol eder, **lock expiry'yi kontrol ETMEZ.** Süresi dolmuş kilitle geç gelen complete, araya farklı workerId'li kilit girmediyse başarılıdır.
- `ExternalTask.xml:220-222` — fetchable = `LOCK_EXP_TIME_ null|<=now` **AND** `RETRIES_ null|>0`. `retries=0` task hiçbir fetch'e görünmez (incident bölgesi).
- `LockExternalTaskCmd.java:50-61` — kilit ihlali yalnız *farklı worker + süresi dolmamış kilit* kombinasyonunda; aynı sentinel workerId ile re-lock her zaman geçer.
- `ExternalTaskEntity.java:419,443-446` — `failed()` her çağrıda `lockExpirationTime = now + retryDuration` yazar; `retries<=0` → incident yaratır.

**Karar (2026-07-13) — saat hiyerarşisi:** A2'de poller olmadığı için engine kilidi redelivery saati DEĞİLDİR; tek redelivery otoritesi **JetStream**'dir (`ack-wait`=W, `maxDeliver`=M). Engine kilidi iki işe iner: (a) sahiplik işareti (sentinel workerId), (b) sweep'in orphan eşiği. Hizalama kuralı eşitlik değil **şemsiye**:

```
L (sentinel lockDuration) = W·M (JetStream teslimat bütçesi) + S (sweep periyodu) + ε (complete-yolu payı)
```

**Default değerler** (topic-başına override; W=30s mevcut adapter paritesi — `JetStreamMessageCorrelationSubscriber.java:57`):

| Parametre | Default | Not |
|---|---|---|
| W — `ack-wait` | 30s | topic'in p99 iş süresi + marj; uzun işli topic büyük W seçer |
| M — `maxDeliver` | 4 (=3 retry) | mevcut adapter deseni: `maxDeliver+1` → DLQ (`:58,117`) |
| S — sweep periyodu | 120s | soğuk, read-only (§5.3) |
| ε — pay | 60s | reply/complete işleme payı |
| **L — sentinel lock** | **300s (5dk)** | = 30·4 + 120 + 60 |

*(D-E rafinesi 2026-07-14: worker `nakWithDelay` kullanıyorsa teslimat bütçesi backoff toplamı kadar uzar → şemsiye koşulu genel haliyle **L ≥ M·W + Σbackoff + S + ε**. Defaults'ta Σbackoff = 1+2+4 = 7s → L=300s rahat tutar.)*

**Redelivery politikası:**
1. **Happy path:** worker işi bitirir → `jobs.<topic>.reply`'a yayınlar → **sonra** job mesajını ACK'ler (reply-önce-ack = at-least-once). Inbound bridge reply'ı alır → `complete(extTaskId, sentinelWorkerId)` → complete **başarılıysa** reply mesajını ACK'ler.
2. **Transient hata:** worker NAK(delay) ya da ack-wait dolmasına bırakır → redelivery (bütçe: toplam M teslimat).
3. **Bütçe bitti:** deliveryCount > M → DLQ subject'i + bridge `handleFailure(..., retries=0)` → **incident (Cockpit görünürlüğü)**. `retries=0` task fetchable-predicate dışı → sweep onu ASLA yeniden yayınlamaz (`ExternalTask.xml:222`); operatör Cockpit'ten retry verirse task yeniden fetchable olur → sweep doğal olarak yeniden yayınlar (Cockpit-retry JetStream'e geri akar).
4. **Geç complete:** L dolduktan sonra gelen reply yine başarılıdır (kanıt: expiry kontrolü yok) — tek sentinel workerId olduğundan sahiplik asla el değiştirmez. Çift işlenen işin ikinci complete'i "task yok" → yakala + ACK (**idempotency**: anahtar `externalTaskId`).
5. **Heartbeat YOK (basamak-1):** W·M sert tavandır. `msg.inProgress()` (JetStream'in DB-yazısız extendLock muadili) basamak-1 worker kontratında yok → D-H'ye ertelendi. Engine `extendLock` da kullanılmaz (DB yazısı üretir + süresi dolmuş kilidi uzatamaz — `ExtendLockOnExternalTaskCmd.java:46-47`).

**Sweep kriteri = fetchable-parite:** sweep, engine'in fetchable predicate'inin (`ExternalTask.xml:220-222`) birebir aynısını sorgular (lock süresi dolmuş/yok **ve** retries≠0, A2 topic'leri) → "native bir poller neyi alabilirse onu ve yalnız onu" yeniden yayınlar (yayın öncesi sentinel re-lock — aynı workerId, her zaman geçer). DLQ'lu task dirilmez; tamamlanan task satırı zaten silinmiştir.

**Dürüst sınır:** `Nats-Msg-Id` dedup penceresi sonludur (stream `duplicate_window`, default 2dk). L (5dk) sonrası sweep re-publish'i pencere dışıdır → çifti stream değil, **inbound complete-idempotency** (madde 4) yutar. At-least-once'ın bedeli budur; vanilla external-task'ta da aynıdır (lock-expiry sonrası re-fetch).

**Lock sahipliği (D-C çözüldü 2026-07-13: doğumda in-tx kilit):**

Kanıt zinciri: `complete` kilitsiz task'ta **başarısız olur** (workerId null → eşitlik ihlali, `HandleExternalTaskCmd.java:89-91`) → sentinel kilit complete-yolunun ön şartıdır. `createAndInsert` task'ı **kilitsiz doğurur ve entity'yi döndürür** (`ExternalTaskEntity.java:568-588`); entity flush edilmeden `lock(SENTINEL, L)` çağrılırsa (`:472-474`) kilit alanları **aynı INSERT'e biner → ek DB yazısı SIFIR.**

**Mekanizma — custom `ExternalTaskActivityBehavior` (plugin, fork değişikliği YOK):**
- `BpmnParse.java:2564` behavior'ın takıldığı tek nokta; `BpmnParseListener` (`preParseListeners`, `ProcessEngineConfigurationImpl.java:687,2189`) A2-topic'li aktivitelerde behavior'ı swap eder.
- Override edilen `execute()`: `createAndInsert(...)` → aynı tx'te `task.lock(SENTINEL, L)` → `TransactionContext.addTransactionListener(COMMITTED, publish(task))` (`TransactionContext.java:49`, `TransactionState.java:25`) — **D-A'nın "hangi task bu tx'te yaratıldı" kancası da aynı noktada bedavaya çözülür.**
- Dürüst not: impl-sınıf bağımlılığı VAR (`ExternalTaskEntity`, `TransactionContext`) — standart Camunda plugin deseni, ama upgrade'lerde izlenecek yüzey.

**Sentinel workerId = küme-geneli TEK sabit** (örn. `a2-jetstream-bridge`): reply'ı queue-group'tan *herhangi bir* bridge node'u tüketebilir; complete birebir workerId eşitliği istediğinden node-başına id complete'i kırar. Id ayrıca payload'da taşınır (audit).

**Yan kazançlar:** (a) task doğuştan kilitli → migration döneminde legacy poller A2 task'ını asla fetch edemez (fetchable-predicate dışı); (b) sweep tek kriterle çalışır (fetchable-parite, §D-B) — hiç-yayınlanmamış orphan da aynı yoldan ≤ L+S (~7dk) içinde toplanır, re-publish öncesi re-lock ile L tazelenir (aynı-workerId re-lock her zaman serbest, `LockExternalTaskCmd.java:50-61`).

**REDDEDİLEN:** (2) post-commit `lock()` çağrısı — hot-path'te +1 UPDATE/task + "kilitli-ama-yayınlanmamış" crash penceresi için ikinci sweep kriteri gerektirir; (3) complete-önü lazy kilit — in-flight task kilitsiz gezer → migration guard kaybolur, sweep in-flight/orphan ayıramaz.

### 5.5 Ne kalkar / ne kalır
| | Native external task | A2 (JetStream push) |
|---|---|---|
| Token park (wait-state) | ✅ commit, lock yok | ✅ aynı |
| **fetchAndLock poll/lock storm** | N worker × `SELECT FOR UPDATE` | **kalkar** — ne worker ne poller poll'ü; happy-path'te DB sorgusu yok (post-commit listener) |
| `ACT_RU_EXT_TASK` satırı | 1 INSERT/task | **kalır** (outbox olarak kullanılıyor) |
| `complete` = token-move tx | kısa tx + optimistic lock | **kalır** (P2 → basamak 6) |

### 5.6 Başarı metriği (D-F çözüldü 2026-07-14: normalize metrik + SLI katmanı)

**Birincil — task-yaşamdöngüsü başına DB round-trip** (donanım-bağımsız, §5.5 iddiasını doğrudan kanıtlar):

| Bileşen | Baseline (native poll) | A2 hedef |
|---|---|---|
| Task INSERT | 1 | 1 (sentinel kilit dahil — D-C, aynı INSERT) |
| Poll sorguları (amortize) | N_worker × f_poll ÷ throughput | **0** |
| `fetchAndLock` UPDATE | 1 | **0** (kilit doğumda) |
| `complete` token-move tx | 1 | 1 (P2 — basamak 6'ya kadar, dürüst tavan) |
| Sweep okuması (amortize) | — | ≤ 1 read / S(120s) / cluster ≈ ~0 |

Ölçüm: `pg_stat_statements` sorgu-parmak-izi sayaçları (fetchAndLock SQL'i ayrı fingerprint verir → poll bileşeni izole sayılır) ya da datasource-proxy sayacı.

**Destekleyici SLI'lar (ops gerçeği):**
- `fetchAndLock` QPS: hot-path **0/s**; yalnız sweep ≤ 1 FOR-UPDATE'siz read / 120s / cluster.
- `ACT_RU_EXT_TASK` lock-wait: ~0 (`pg_locks` / `innodb_row_lock_waits`).
- HikariCP aktif connection (aynı yükte): düşmeli — tezin connection-tutma ayağı.
- Dispatch latency (commit → worker deliver): **p95 ≤ 200ms**; baseline'da yapısal alt sınır ≈ poll-aralığı/2.
- Failure-path: mevcut `NatsChannelMetrics` sayaçları (dlq/nak/ack + processingTimer) + sweep-republish sayacı + en-yaşlı-orphan yaşı.

**Metodoloji — Testcontainers yük-bench modülü (basamak-1 teslimatına DAHİL):** PG + engine + NATS + N simüle worker; **aynı senaryo iki modda** koşar (native-poll baseline ↔ A2-push), `@Tag("bench")`, CI'da nightly/manuel. **Kabul kriteri:** birincil metrikte poll + fetchAndLock bileşenleri **0**, INSERT/complete bileşenleri **artmıyor**; destekleyici SLI'lar tablo hedeflerinde.

**REDDEDİLEN:** yalnız-mutlak-QPS (ortam-bağımlı, karşılaştırılamaz); latency-öncelikli (tezi dolaylı ölçer, poll-aralığı seçimiyle şişirilebilir); bench'siz staging ölçümü (baseline kontrolsüz → kapanış kriteri muğlak).

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
- **Escalation (D-D çözüldü 2026-07-13: katmanlı):** iki başarısızlık sınıfı, iki mekanizma — tek mekanizma ikisini de iyi yakalayamaz:
  - **Default — DLQ→failure-event bridge (deterministik worker ölümü):** worker'ın jobs-consumer'ında delivery bütçesi (W·M) bitince mesaj DLQ'ya düşer; **DLQ-bridge** mesajı **aynı correlation key'lerle** (BpmHeaders zaten taşıyor) bir *failure-event*'e çevirip Event Registry'ye geri sokar → bekleyen instance'a correlate olur → model escalation path'ini işler (event-based gateway / event-registry boundary event / event subprocess — mevcut yakalama biçimleri phase3'te doğrulanacak). Tespit gecikmesi = W·M (**SLA beklenmez**); happy-path ek DB maliyeti **SIFIR**. Mevcut DLQ tesisatının üstüne kurulur (`JetStreamInboundEventChannelAdapter.java:75-77` maxDeliver+1 consumer, `:133-146` DLQ routing + metrik).
  - **Opt-in — boundary timer (wall-clock iş SLA'sı):** worker canlı ama iş deadline'ı aşıyorsa bunu yalnız saat yakalar → gerçek deadline'ı olan modellerde BPMN boundary timer modellenir; timer-job satır maliyeti (instance-başına INSERT+DELETE — phase3'te doğrulanacak) yalnız o modellerde ödenir.
  - **Geç-sonuç politikası:** escalation interrupting ise sonradan gelen sonuç correlate edecek subscription bulamaz → **ack + log + metric** (drop); non-interrupting modellenirse geç sonuç yine işlenir — model kararı.
  - **REDDEDİLEN:** (2) timer-only — tespit gecikmesi = SLA (anında ölen worker'ı bile saat dolunca fark eder), her instance happy-path'te timer-job yazısı öder, JetStream'in kesin DLQ sinyali ziyan olur; (3) DLQ→ops-only — token sonsuz beklemede kalır (leak), Flowable'da ona işaret eden incident muadili de yok.
  - **A2 simetrisi (D-E ön-notu):** substrat sinyali ortak (`maxDeliver` → DLQ); DLQ-*sonrası* idiom-özel: Camunda → `handleFailure(retries=0)` → incident (Cockpit), Flowable → failure-event → BPMN escalation.

### 6.3 Ne kalkar / ne kalır
- **Kalkar:** dispatch polling (zaten yok), in-tx blocking delegate.
- **Kalır:** event subscription wait-state + correlate-continue token-move tx (P2 paritesi).

---

## 7. Ortak JetStream substratı (her iki idiom — docs/05 §7)

- **Subject şeması:** `jobs.<type>` / event channel subject'leri; `*.reply` / inbound channel.
- **Stream tipi:** iş dağıtımı için **WorkQueue** (her mesaj tek tüketiciye, nack→redeliver).
- **Dedup:** `Nats-Msg-Id` (A2'de `externalTaskId`; Event Registry'de correlation key) + apply-zamanı idempotency (docs/05 P3/§10). Dikkat: stream dedup penceresi sonlu (`duplicate_window`, default 2dk) — pencere-dışı çiftleri complete/correlate-idempotency yutar (§5.4).
- **DLQ / ack semantiği (D-E çözüldü 2026-07-14 — ortak kontrat, idiom-özel post-DLQ):**
  - **Custody-transfer ilkesi (ortak ack disiplini):** ack, yalnız kalıcılık el değiştirdikten sonra — worker: reply-PubAck-sonrası-ack (business error da bir reply'dır: error-reply → ack); engine-inbound: complete/correlate dönüşü-sonrası-ack; DLQ yolu: **DLQ-PubAck-sonrası-ack**. Transient hata → `nakWithDelay` (üstel backoff `2^(n-1)`s, cap 30s — mevcut iki adapter'ın ortak deseni).
  - **Tespit (ortak, in-band):** `maxDeliver+1` deseni — consumer `deliveryCount > M` görünce DLQ'ya kendisi yönlendirir (iki adapter'da çalışır durumda). **REDDEDİLEN:** `MAX_DELIVERIES` advisory-tabanlı tespit — advisory best-effort core-NATS yayınıdır, kaçarsa poison mesaj sessizce sıkışır + ayrı consumer bileşeni gerektirir.
  - **DLQ mesaj şeması (wire-contract):** subject `dlq.<orijinal-subject>`; payload byte-aynen; **orijinal header'lar AYNEN kopyalanır** + meta header'lar: `X-Cadenzaflow-Dlq-Original-Subject`, `-Dlq-Delivery-Count`, `-Dlq-Reason`, `-Dlq-Timestamp`; `Nats-Msg-Id = <orijinal-msg-id>.dlq` (crash-sonrası çift-yayın dedup'u).
  - **Topoloji:** TEK ortak `DLQ` stream (`dlq.>`), retention limits-based (default 14 gün), WorkQueue DEĞİL; tüketiciler subject filtresiyle ayrışır: `dlq.jobs.>` → Camunda incident-bridge, event-channel DLQ'ları → Flowable failure-event bridge; ops tek yüzeyden izler. Idiom-başına ayrı stream **reddedildi** (2× provisioning/izleme; tek wire-contract sadeliğinden sapma).
  - **Post-DLQ idiom-özel (D-D):** Camunda → `handleFailure(retries=0)` → incident; Flowable → failure-event.
  - **dlq-of-dlq YOK:** bridge DLQ mesajını işleyemezse nak + alert; asla ack-drop.
  - ⚠️ **Mevcut adapter'larda kontrata aykırı üç açık (basamak-1 fix listesi):** (1) `publishToDlq` header kopyalamıyor — yalnız `msg.getData()` (flowable `JetStreamInboundEventChannelAdapter.java:218,227` / cadenzaflow `JetStreamMessageCorrelationSubscriber.java:210,219`) → correlation key'siz DLQ mesajından failure-event bridge correlate EDEMEZ; (2) DLQ-publish başarısızlığı yutulup koşulsuz `msg.ack()` yapılıyor (flowable `:141-145` / cadenzaflow `:123-127`), `dlqSubject==null` iken discard → **sessiz poison-mesaj kaybı** (custody-transfer ihlali); (3) DLQ publish'te `Nats-Msg-Id` yok → çift DLQ kaydı riski.
- **Header'lar:** mevcut `BpmHeaders` (`X-Cadenzaflow-Trace-Id`, `-Business-Key`, `-Idempotency-Key`, async: `-Correlation-Id`, `-Reply-Subject`).
- **Kısıt:** iki idiom da aynı teli yayar/tüketir → worker ekosistemi paylaşılır (docs/05 P5).

---

## 8. Açık kararlar (Sentinel phase1/phase3)

- **D-A — A2 outbound tetikleme:** ✅ **ÇÖZÜLDÜ (2026-07-02)** = post-commit `TransactionListener` (oluşturan node, tx dışı, **sorgusuz**) + soğuk read-only orphan-sweep + `Nats-Msg-Id` dedup. Hot central poller **reddedildi** (N-node poller `fetchAndLock` contention'ı = P1 ihlali). (§5.3)
- **D-B — lockDuration ↔ ack-wait hizalaması:** ✅ **ÇÖZÜLDÜ (2026-07-13)** = **şemsiye kilit**: JetStream tek redelivery otoritesi; `L = W·M + S + ε` (default 30s·4 + 120s + 60s = **5dk**); DLQ → `handleFailure(retries=0)` → incident; sweep kriteri = fetchable-parite; heartbeat yok (→ D-H). Kilit kanıt: complete lock-expiry kontrol etmez (`HandleExternalTaskCmd.java:89-91`) → engine kilidi redelivery saati değil, şemsiye. Fork değişikliği gerektirmez. (§5.4)
- **D-C — sentinel-lock modeli:** ✅ **ÇÖZÜLDÜ (2026-07-13)** = **doğumda in-tx kilit**: custom `ExternalTaskActivityBehavior` (BpmnParseListener swap) `createAndInsert` + `lock(SENTINEL, L)` aynı tx → kilit aynı INSERT'e biner (sıfır ek yazı); aynı noktada COMMITTED TransactionListener = D-A publish kancası. Sentinel workerId küme-geneli tek sabit. Post-commit `lock()` ve lazy kilit **reddedildi**. Fork değişikliği yok; impl-sınıf bağımlılığı var. (§5.4)
- **D-D — Flowable escalation:** ✅ **ÇÖZÜLDÜ (2026-07-13)** = **katmanlı**: default DLQ→failure-event bridge (deterministik worker ölümü, tespit=W·M, happy-path ek DB maliyeti sıfır, aynı correlation key'ler) + opt-in boundary timer (yalnız gerçek wall-clock deadline'lı modeller). Timer-only ve DLQ→ops-only **reddedildi**. (§6.2)
- **D-E — DLQ/ack semantiği:** ✅ **ÇÖZÜLDÜ (2026-07-14)** = **ortak kontrat + idiom-özel post-DLQ**: custody-transfer ack ilkesi (ack yalnız kalıcılık el değiştirince); in-band `maxDeliver+1` tespiti (advisory reddedildi); DLQ şeması = orijinal payload+header'lar aynen + `X-*-Dlq-*` meta + `Nats-Msg-Id=<orijinal>.dlq`; TEK `DLQ` stream (`dlq.>`, limits, 14g); dlq-of-dlq yok. Mevcut adapter'larda 3 kontrat açığı fix-listesine alındı (header kaybı, koşulsuz ack, dedup yok). D-B şemsiyesi rafine: `L ≥ M·W + Σbackoff + S + ε`. (§7)
- **D-F — başarı metriği:** ✅ **ÇÖZÜLDÜ (2026-07-14)** = **normalize birincil metrik** (task başına DB round-trip; poll + fetchAndLock bileşenleri → 0, INSERT/complete artmaz) + **destekleyici SLI katmanı** (fetchAndLock QPS=0, lock-wait~0, HikariCP↓, dispatch p95≤200ms, failure sayaçları). Baseline+hedef **Testcontainers yük-bench** modülüyle üretilir (aynı senaryo, native-poll ↔ A2-push, `@Tag("bench")`, basamak-1 teslimatına dahil). (§5.6)
- **D-G — gRPC ön kapısı:** gerekli mi (Zeebe-uyum / broker'sız worker), ne zaman? Ayrı belge. (§1.4)
- **D-H — InProgress heartbeat (basamak-1 SONRASI):** uzun işli topic'lerde küçük W + `msg.inProgress()` (DB-yazısız ack-wait uzatma; deliveryCount artırmaz). Kontrata "toplam heartbeat süresi ≤ L−ε" sınırı gerekir. Basamak-1 için **reddedildi** (2026-07-13, sabit W·M bütçesi tercih edildi — basit kontrat, statik L). (§5.4)

> **Durum (2026-07-14):** D-A…D-F **tamamı çözüldü**; D-G/D-H bilinçli ertelendi. Bu belge **Sentinel phase1 girdisi olarak HAZIR.** Basamak-1 kod kapsamı: custom activity behavior + post-commit publisher (D-A/D-C), sweep (D-B), inbound completion-bridge, DLQ bridge'leri (D-D/D-E), §7 kontrat-fix listesi (3 açık), bench modülü (D-F).

---

## 9. Doğrulama notları (kanıt-temelli — phase3 girdisi)

- **External-task subsystem'in CadenzaFlow fork'unda varlığı** → ✅ **DOĞRULANDI (2026-07-13):** `org.cadenzaflow.bpm.engine.impl.cmd.{Handle,Complete,Lock,ExtendLockOn}ExternalTaskCmd` + `mapping/entity/ExternalTask.xml` fork'ta mevcut, upstream-Camunda7 davranışıyla (paket adı dışında değişiklik yok).
- **Flowable external-worker job vs Event Registry** → bu belge Event Registry yolunu seçiyor; external-worker job modeli kapsam dışı, gerekirse ayrı brief.
- **`complete()` lock zorunluluğu** → ✅ **DOĞRULANDI (2026-07-13):** yalnız workerId eşitliği kontrol edilir (`HandleExternalTaskCmd.java:89-91`), lock-expiry kontrolü YOK. Fetchable predicate: `ExternalTask.xml:220-222`; re-lock kuralı: `LockExternalTaskCmd.java:50-61`; `failed()`'in saat-bağlaması: `ExternalTaskEntity.java:419,443-446`. → D-B/D-C bu davranışlar üzerine kuruldu, fork değişikliği gerektirmez.
- **JetStream tarafı (NATS docs'tan phase3'te doğrulanacak):** stream `duplicate_window` default 2dk; `msg.inProgress()` ack-wait'i sıfırlar ve deliveryCount'u ARTIRMAZ; maxDeliver aşımında `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES` advisory'si yayınlanır (DLQ köprüsü için alternatif tetik).
- **D-D Flowable tarafı (phase3'te doğrulanacak — lokal Flowable engine kaynağı yok):** (a) Event Registry'nin hangi yakalama biçimlerini desteklediği (event-registry boundary event / event subprocess ve sürüm eşiği); (b) boundary timer'ın instance-başına `ACT_RU_TIMER_JOB` INSERT+DELETE maliyeti; (c) `eventReceived`'ın eşleşmeyen (geç) event'teki davranışı (sessiz drop mu, hata mı) → geç-sonuç politikasının dayanağı. Repo tarafı kanıtlı: DLQ tesisatı `JetStreamInboundEventChannelAdapter.java:75-77,133-146`.
- **D-F altyapı tespiti (2026-07-14):** Micrometer tabanı mevcut (`nats-core/.../metrics/NatsChannelMetrics.java`, adapter'lar dlq/ack/nak/processingTimer yayınlıyor); dört modülde Testcontainers entegrasyon testi var → bench modülü mevcut altyapı üstüne kurulur. Phase3 doğrulaması: `pg_stat_statements`'ın fetchAndLock SQL'ini ayrı fingerprint olarak verdiği (parametrizasyon farkları tek fingerprint'te toplanmayabilir).
- **D-E kanıtları** → ✅ **DOĞRULANDI (2026-07-14):** iki adapter'da özdeş DLQ/backoff deseni (`MAX_BACKOFF=30s`, `calculateBackoff=2^(n-1)`s — flowable `:32,207` / cadenzaflow `:32,199`); üç kontrat açığı file:line ile §7'de işaretli. JetStream tarafı phase3 doğrulaması: core-NATS fallback publish'inin stream'e yakalanma koşulu (DLQ stream subject'e bound ise core publish de yakalanır — PubAck'siz) ve `Nats-Msg-Id` dedup'unun core publish'te ÇALIŞMADIĞI varsayımı.
- **D-C kanca noktaları** → ✅ **DOĞRULANDI (2026-07-13):** `createAndInsert` kilitsiz doğurur + entity döndürür (`ExternalTaskEntity.java:568-588`); behavior tek noktada takılır (`BpmnParse.java:2564`); `preParseListeners` extension mevcut (`ProcessEngineConfigurationImpl.java:687,2189,3469`); `TransactionContext.addTransactionListener` + `TransactionState.COMMITTED` command içinden erişilebilir (`TransactionContext.java:49`, `TransactionState.java:25`). Phase3'te ek doğrulama: custom behavior'da `lock()`'un flush-öncesi çağrısının tek INSERT ürettiği (ikinci UPDATE çıkmadığı) entegrasyon testiyle kanıtlanacak.
- **Mevcut kodda external task yok** → `camunda/cadenzaflow` kaynaklarında `ExternalTask`/`fetchAndLock`/`camunda:type="external"` sıfır eşleşme (2026-06-28); A2 tamamen yeni kod.
- **token-move tx kalır** → zaten kanıtlı (P2; memory `sync-request-reply-holds-db-transaction`).
