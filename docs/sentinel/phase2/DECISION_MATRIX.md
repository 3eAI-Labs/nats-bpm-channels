# DECISION MATRIX — Basamak-1: External Task / Event-Driven Work Offload over JetStream

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Sentinel fazı:** Phase 2 — Business Analyst
**İlgili:** `BUSINESS_LOGIC.md` (BR-XXX kataloğu, süreç akışları, durum makineleri), `EXCEPTION_CODES.md`
**Tarih:** 2026-07-14

> BA Guideline §2.2 gereği "if-this-then-that" mantığı burada tablo olarak sabitlenir — düz metin YASAK. Her satır bir BR/FR/US'ye bağlıdır. Kanıtlar `[BA-VERIFIED]` (bu fazda kaynak koddan bizzat okundu) ya da `[phase1-verified]` (Phase 1'de doğrulandı, bu fazda yeniden kullanıldı) etiketlidir. BA-QUESTION referansları (`BAQ-N`) `BUSINESS_LOGIC.md §9`'a bağlıdır.

---

## Matris 1 — Mesaj-yaşamdöngüsü: teslim → işlem → (ack | nak-backoff | DLQ)

Custody-transfer ilkesinin durum-geçiş matrisi. **Üç rol ayrı satır gruplarında** ele alınır çünkü her birinin custody-transfer anı farklıdır (BUSINESS_LOGIC.md §1.6/§2.2).

### 1.A — Worker consumer (`jobs.<topic>` job-side; wire-contract ile yönetilir, worker impl bu repo dışı)

| # | Girdi durumu | Guard koşulu | Aksiyon | Custody-transfer anı | BR/FR | Kanıt |
|---|---|---|---|---|---|---|
| 1 | Job teslim edildi, worker işi bitirdi | İş başarılı | `jobs.<topic>.reply`'a yayınla → **sonra** job mesajını ACK'le | Reply-PubAck-sonrası-ack | BR-SUB-002 / FR-C2 | IR-5 (`SRS.md §5`) |
| 2 | Job teslim edildi, worker business-error tespit etti | BPMN error senaryosu | Error-reply yayınla (bu da bir reply'dır) → ACK | Error-reply-PubAck-sonrası-ack | BR-A2-008 / FR-A7 | `06 §7` custody-transfer ilkesi |
| 3 | Job işlenirken transient hata (I/O, timeout, uncaught exception) | Reply üretilemedi | `nakWithDelay(2^(n-1)s, cap 30s)` | Custody transfer OLMADI — mesaj WorkQueue'da kalır | BR-SUB-002 / FR-C6 | `[BA-VERIFIED]` `JetStreamInboundEventChannelAdapter.java:189-208` (ortak backoff deseni, worker-tarafı analogu) |
| 4 | Worker crash (ack-wait boyunca hiç yanıt yok) | — | JetStream kendiliğinden redeliver eder (worker müdahalesi yok) | Custody transfer OLMADI | BR-A2-006 / FR-A8 | Şemsiye kilit ilkesi |
| 5 | deliveryCount > M (job-side bütçe bitti) | in-band tespit (worker'ın kendi consumer'ı) | Job'ı `dlq.jobs.<topic>`'a yönlendir → orijinal job mesajını ACK'le | DLQ-PubAck-sonrası-ack | BR-A2-009 / FR-A10 | `06 §7` D-E |

### 1.B — Engine-inbound consumer (A2 completion-bridge; Flowable `JetStreamInboundEventChannelAdapter`)

| # | Girdi durumu | Guard koşulu | Aksiyon | Custody-transfer anı | BR/FR | Kanıt |
|---|---|---|---|---|---|---|
| 1 | Reply/event teslim edildi, data boş değil | `deliveryCount ≤ maxDeliver` | İşle: `complete()` (A2) / `eventReceived()` (Flowable) | — (aşağıdaki alt-satırlara bağlı) | BR-A2-008, BR-FLW-002 | `[BA-VERIFIED]` `JetStreamInboundEventChannelAdapter.java:149-160` |
| 1a | ...işlem başarılı | complete/correlate döndü | ACK | Complete/correlate-dönüşü-sonrası-ack | BR-A2-008 / FR-A7 | `06 §7` |
| 1b | ...işlem exception fırlattı (genel) | herhangi bir `Exception` | `metrics.nakCount++` → `nakWithBackoff` | Custody transfer OLMADI | BR-SUB-002 / FR-C6 | `[BA-VERIFIED]` `:165-172` |
| 2 | Data boş (`data==null \|\| length==0`) | — | log DEBUG + **ACK** (BAQ-5 — mevcut davranış, fix listesi dışı) | Vacuous — hiçbir yan-etki üretilmedi ama custody devredilmiş sayılıyor | BR-FLW-002 (edge-case) | `[BA-VERIFIED]` `:124-131` |
| 3 | `deliveryCount > maxDeliver` | in-band tespit | `publishToDlq(msg)` → orijinal mesajı ACK'le | DLQ-PubAck-sonrası-ack (§ Matris 1.C ile devam) | BR-A2-009, BR-FLW-003 / FR-A10, FR-B3 | `[BA-VERIFIED]` `:133-146` |
| 4 | DLQ publish (JetStream) başarılı | `dlqSubject != null` | Orijinal mesajı ACK'le | DLQ-PubAck-sonrası-ack | BR-SUB-002 / FR-C2 | `[BA-VERIFIED]` `:210,217-221` |
| 5 | DLQ publish (JetStream) başarısız, core-NATS fallback başarılı | — | Orijinal mesajı ACK'le | Fallback-PubAck-sonrası-ack | BR-SUB-002 / FR-C2 | `[BA-VERIFIED]` `:222-230` |
| 6 | `dlqSubject == null` | config eksik | **DÜZELTME:** discard YOK → nak | Custody transfer OLMADI (mevcut kod: discard+ack — DÜZELTİLİR) | BR-SUB-002 / FR-C2 | `[BA-VERIFIED]` `:210-214` (mevcut açık) |
| 7 | DLQ publish (her iki yol) başarısız | JetStream VE core-NATS ikisi de fail | **DÜZELTME:** nak + alert | Custody transfer OLMADI (mevcut kod: log-only+ack — DÜZELTİLİR) | BR-SUB-002 / FR-C2 | `[BA-VERIFIED]` `:222-235` (mevcut açık) |

### 1.C — DLQ-bridge (Camunda/CadenzaFlow incident-bridge; Flowable failure-event bridge)

| # | Girdi durumu | Guard koşulu | Aksiyon | Custody-transfer anı | BR/FR | Kanıt |
|---|---|---|---|---|---|---|
| 1 | DLQ mesajı tüketildi, header'lar korunmuş (US-C1 fix uygulandı) | correlation key okunabilir | Camunda: `handleFailure(retries=0)` → incident / Flowable: failure-event → `eventReceived` | İncident-oluşturma/correlate-sonrası-ack | BR-A2-009, BR-FLW-003 | BR-SUB-001 |
| 2 | ...işlem başarılı | — | ACK | İncident-oluşturma/correlate-sonrası-ack | aynı | — |
| 3 | ...işlem sırasında exception (DB down, event registry down) | — | **nak + alert** (asla ack-drop, dlq-of-dlq yok) | Custody transfer OLMADI | `06 §7` "dlq-of-dlq YOK" | `[phase1-verified]` |
| 4 | ...redeliver edilen AYNI DLQ mesajı ikinci kez işlenir | `handleFailure(retries=0)` tekrar çağrılır | `setRetriesAndManageIncidents(0)`: `areRetriesLeft()` artık false → **duplicate incident OLUŞMAZ** | ACK (doğal idempotency) | BR-A2-009 | `[BA-VERIFIED]` `ExternalTaskEntity.java:443-448` |
| 5 | Flowable: `eventReceived` bekleyen subscription bulamadı | correlation miss (instance zaten resolve / key kayıp) | Log + metrik; ciddiyet **BAQ-8**'e bağlı (WARN+metrik vs ERROR+alert) | Ack edilir (mesajın kendisi işlendi, iş sonucu ayrı konu) | BR-FLW-003 | US-B3 AC#4, `[phase3'te doğrulanacak]` |

---

## Matris 2 — Complete yolu (A2)

Reply geldi → 4 olası çıktı (US-A4/A7 kabul kriterleri).

| # | Girdi | Guard koşulu | Aksiyon | ACK/NAK | Idempotency etkisi | BR/FR | Kanıt |
|---|---|---|---|---|---|---|---|
| 1 | Reply işlendi, `complete(extTaskId, SENTINEL, vars)` çağrıldı | Task bulundu (`findExternalTaskById != null`) VE `workerId == SENTINEL` | `complete()` çalışır → token ilerler | ACK (complete-sonrası) | Normal, tek-seferlik yürütme | BR-A2-008 / FR-A7 | `[BA-VERIFIED]` `HandleExternalTaskCmd.java:44-68` |
| 2 | Reply işlendi ama task **bulunamadı** | `findExternalTaskById == null` → `NotFoundException` (`ensureNotNull`) | Yakala + log WARN | **ACK** (idempotent yut) | Bu, US-A7'nin idempotency mekanizmasıdır — duplicate/geç reply sessizce yutulur | BR-A2-011 / FR-A12 | `[BA-VERIFIED]` `HandleExternalTaskCmd.java:48-50` |
| 3 | Reply işlendi, task bulundu ama **workerId eşit değil** | `validateWorkerViolation==true` → `BadUserRequestException` | **ASLA olmamalı — invariant.** Log ERROR + alert (BAQ-7) | **ACK edilmez** (nak veya manuel müdahale — ciddiyet BAQ-7'ye bağlı) | Otomatik retry YAPILMAZ — invariant ihlali insan incelemesi gerektirir | (yeni, invariant) | `[BA-VERIFIED]` `HandleExternalTaskCmd.java:52-53` |
| 4 | `complete()` çağrısı sırasında transient hata (DB bağlantısı koptu vb.) | Task bulundu + workerId eşit AMA DB write fail | NAK (redelivery) | NAK — bir sonraki redelivery'de tekrar denenir | At-least-once — worker'ın reply'ı idempotent tekrar işlenebilir olmalı (aynı task, aynı complete çağrısı) | BR-A2-008 (transient dal) | `06 §7` custody-transfer genel ilkesi |

**Not — satır 2 vs 3 ayrımı (BUSINESS_LOGIC.md §1.2'de vurgulandı):** Bu iki çıktı **FARKLI exception tipleriyle** (`NotFoundException` vs `BadUserRequestException`) ayırt edilebilir — bridge implementasyonu iki ayrı `catch` bloğu ile bunları karıştırmadan yönlendirebilir. Satır 2 **beklenen/benign** (WARN, iş akışı normal), satır 3 **hiç beklenmeyen** (ERROR, sistem/config sağlığı sorunu) — ERROR_HANDLING_GUIDELINE §6 "Business rule violations WARN, unexpected errors ERROR" ayrımı burada birebir uygulanır: satır 2 bir iş kuralı sonucu değildir (motor davranışı — expiry kontrolsüz complete zaten BR-A2-011'in TASARLANMIŞ sonucu), satır 3 gerçek bir beklenmeyen durumdur.

---

## Matris 3 — Sweep kararı

Her satır (ACT_RU_EXT_TASK / A2-topic adayı), fetchable-parite predicate'ine göre değerlendirilir.

| # | Guard koşulu (predicate bileşenleri) | Aksiyon | BR/FR | Kanıt |
|---|---|---|---|---|
| 1 | `LOCK_EXP_TIME_ null OR ≤now` **AND** `RETRIES_ null OR >0` **AND** `SUSPENSION_STATE_ null OR =1` **AND** A2-topic | **Re-lock(SENTINEL,L) + re-publish** | BR-A2-005 / FR-A5, FR-A6 | `[BA-VERIFIED]` `ExternalTask.xml` fetchable WHERE bloğu; `[BA-VERIFIED]` `LockExternalTaskCmd.java:50-61` (aynı-workerId re-lock her zaman geçer) |
| 2 | `RETRIES_ = 0` (diğer koşullar ne olursa olsun) | **Atla** — asla yeniden yayınlama (DLQ'lanmış/incident bölgesi) | BR-A2-009 / FR-A11 | `[BA-VERIFIED]` `ExternalTask.xml` (`RETRIES_ null OR >0` AND'i tek başına satırı dışlar) |
| 3 | `SUSPENSION_STATE_ ≠ 1` (process/instance askıda) | **Atla** — resume bekle | BR-A2-005 / FR-A5 | Aynı predicate, üçüncü AND bileşeni |
| 4 | `LOCK_EXP_TIME_ > now` (kilit hâlâ taze — in-flight) | **Atla** — orphan değil, normal yürüyor | BR-A2-005 / FR-A5 | Aynı predicate, birinci AND bileşeni |
| 5 | **(edge-case, BAQ-1)** Kilit taze GÖRÜNÜYOR ama bir ÖNCEKİ sweep döngüsünde re-lock başarılı + re-publish BAŞARISIZ olmuştu | Bu döngüde de **Atla** (satır 4 ile aynı guard, ama mesaj hiçbir zaman teslim edilmedi) — **iş kuralı ihlali riski**: satır fiilen orphan ama predicate'e göre "taze" | BR-A2-013 (yeni) | Bu fazda türetildi — bkz. BUSINESS_LOGIC.md BR-A2-013, state machine §2.1 guard notu |
| 6 | **(edge-case, BAQ-2)** `RETRIES_>0` (Cockpit retry verildi) AMA `LOCK_EXP_TIME_` hâlâ DLQ-anındaki `now+retryDuration` değerinde (gelecekte) | Satır 4'ün guard'ına düşer → **Atla**, ta ki `retryDuration` süresi geçene kadar | BR-A2-010 (edge-case) | `[BA-VERIFIED]` `SetExternalTaskRetriesCmd.java:48-51` (lockExpirationTime'a dokunmaz) + `ExternalTaskEntity.java:402-419` (`failed()`'in lockExpirationTime ataması) |

**Sweep'in kendisi başarısız olursa (DB read/write hatası):** ayrı bir dal — bkz. `EXCEPTION_CODES.md` `SYS_SWEEP_QUERY_FAILED` / `SYS_SWEEP_RELOCK_FAILED` / `SYS_SWEEP_REPUBLISH_FAILED`.

---

## Matris 4 — Escalation (Flowable)

| # | Girdi/durum | Guard koşulu | Aksiyon | BR/FR | Kanıt |
|---|---|---|---|---|---|
| 1 | Worker jobs-consumer deliveryCount > M (kalıcı worker ölümü) | Default (opt-in timer modellenmemiş de olsa HER ZAMAN çalışır) | **DLQ→failure-event bridge**: aynı correlation key'lerle failure-event oluştur → `eventReceived` | BR-FLW-003 / FR-B3 | `06 §6.2` D-D |
| 2 | ...failure-event bekleyen subscription bulur | Model escalation biçimi: event-based gateway / event-registry boundary event / event subprocess | Model escalation path'ini işler (token leak yok) | BR-FLW-003 | `[phase3'te doğrulanacak]` (D-D a — hangi yakalama biçimleri desteklenir) |
| 3 | ...failure-event bekleyen subscription BULAMAZ | Instance zaten resolve olmuş / correlation key kaybı | `RES_FAILURE_EVENT_CORRELATION_MISS` — ciddiyet BAQ-8'e bağlı | BR-FLW-003 | NFR-R6 (token-leak yasağı) |
| 4 | Model gerçek wall-clock SLA'sına sahip | **Opt-in** boundary timer modellenmiş | Deadline aşılırsa timer tetiklenir — **worker canlı olsa BİLE** (DLQ'dan bağımsız) | BR-FLW-004 / FR-B4 | `06 §6.2` D-D opt-in |
| 5 | Model SLA'sız (default, timer modellenmemiş) | — | Yalnız satır 1-3 (DLQ→failure-event) geçerli; timer YOK | BR-FLW-004 | REDDEDİLDİ: timer-only default |
| 6 | Escalation (DLQ→failure-event VEYA timer) zaten fırlamış — **interrupting** | Geç sonuç (worker'ın gecikmiş reply'ı) sonradan gelir | Subscription artık YOK → **ACK + log + metric (drop)** | BR-FLW-005 / FR-B5 | US-B5 AC#1 |
| 7 | Escalation **non-interrupting** modellenmiş | Geç sonuç sonradan gelir | Subscription hâlâ var → **işlenir** (model kararı) | BR-FLW-005 / FR-B5 | US-B5 AC#2 |
| 8 | `eventReceived`'ın no-match durumunda gerçek davranışı (sessiz drop mu, hata mı) | — | `[phase3'te doğrulanacak]` (D-D c) — satır 6/7'nin altyapı-düzeyi doğrulaması henüz yok | BR-FLW-005 | US-B5 AC#3 |

**A2 simetrisi (referans, D-E ön-notu):** Substrat sinyali ortak (`maxDeliver` → DLQ); DLQ-*sonrası* idiom-özeldir — Camunda/CadenzaFlow → `handleFailure(retries=0)` → incident (Matris 3, satır 1'in DLQ-tetiklenen dalı); Flowable → failure-event (bu matris, satır 1).

---

## Ek Matris 5 — DLQ topoloji routing (US-C4, destekleyici)

| # | DLQ subject deseni | Tüketici | Guard | BR/FR |
|---|---|---|---|---|
| 1 | `dlq.jobs.<topic>` | Camunda/CadenzaFlow incident-bridge | Subject `jobs.` öneki ile başlıyor | BR-SUB-004 / FR-C4 |
| 2 | `dlq.<event-channel-subject>` (jobs. önekiyle BAŞLAMAYAN) | Flowable failure-event bridge | Subject filtre-dışı kalan her şey | BR-SUB-004 / FR-C4 |
| 3 | **(BAQ-4)** Flowable inbound channel `jobs.<x>` adlandırılmış | Belirsiz — subject-prefix routing'e göre YANLIŞLIKLA incident-bridge'e gider | Namespace çakışması — çözüm BAQ-4'e bağlı | BR-SUB-004 (edge-case) |

---

## Ek Matris 6 — Şemsiye kilit parametre doğrulama (US-A5, destekleyici)

| # | Girdi (operatör config) | Guard koşulu | Aksiyon | BR/FR |
|---|---|---|---|---|
| 1 | Topic-özel `L`, `W`, `M`, `S`, `ε` girilmiş | `L ≥ M·W + Σbackoff + S + ε` sağlanıyor | Kabul, config aktive edilir | BR-A2-006 / FR-A8 |
| 2 | Topic-özel `L` girilmiş, formülün ALTINDA | `L < M·W + Σbackoff + S + ε` | `VAL_UMBRELLA_LOCK_TOO_SHORT` — **davranış BAQ-3'e bağlı**: (a) reject-startup (topic aktive edilmez) veya (b) warn-only (config kabul edilir, log WARN) | BR-A2-006 (edge-case) |
| 3 | `W`/`M`/`S`/`ε` değişmiş ama `L` default'tan türetilmemiş (elle sabit bırakılmış) | Tutarsızlık riski | Aynı VAL_ kodu — yeniden hesaplama önerisi log'lanmalı | BR-A2-006 |
| 4 | Hiçbir override yok | Default'lar (W=30,M=4,S=120,ε=60,Σbackoff=7,L=320) | Kabul | BR-A2-006 |

---

## İzlenebilirlik özeti (Matris → BR → FR → US)

| Matris | Kapsadığı BR | Kapsadığı FR | Kapsadığı US |
|---|---|---|---|
| Matris 1 (mesaj-yaşamdöngüsü, 3 alt-tablo) | BR-SUB-002, BR-A2-008, BR-A2-009, BR-FLW-002, BR-FLW-003 | FR-C2, FR-C6, FR-A7, FR-A10, FR-B2, FR-B3 | US-A4, US-A6, US-B2, US-B3, US-C2 |
| Matris 2 (complete yolu) | BR-A2-008, BR-A2-011 | FR-A7, FR-A12 | US-A4, US-A7 |
| Matris 3 (sweep) | BR-A2-005, BR-A2-009, BR-A2-010, BR-A2-013 | FR-A5, FR-A6, FR-A10, FR-A11 | US-A3, US-A5, US-A6 |
| Matris 4 (escalation) | BR-FLW-003, BR-FLW-004, BR-FLW-005 | FR-B3, FR-B4, FR-B5 | US-B3, US-B4, US-B5 |
| Ek Matris 5 (DLQ topoloji) | BR-SUB-004 | FR-C4 | US-C4 |
| Ek Matris 6 (L-doğrulama) | BR-A2-006 | FR-A8 | US-A5 |

**Toplam:** 6 karar matrisi (4 birincil + 2 destekleyici), toplam **44 karar satırı** (Matris 1: 1.A=5 + 1.B=9 + 1.C=5 → 19 satır; Matris 2: 4; Matris 3: 6; Matris 4: 8; Ek Matris 5: 3; Ek Matris 6: 4 → 19+4+6+8+3+4=44).

---

*İlgili: `BUSINESS_LOGIC.md` (BR-XXX tanımları, süreç akışları), `EXCEPTION_CODES.md` (bu matrislerdeki her "aksiyon" hücresinin exception-code karşılığı). BA-QUESTIONS: `BUSINESS_LOGIC.md §9` (BAQ-1…8).*
