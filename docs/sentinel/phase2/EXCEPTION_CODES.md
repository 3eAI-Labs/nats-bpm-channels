# EXCEPTION CODES — Basamak-1: External Task / Event-Driven Work Offload over JetStream

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Sentinel fazı:** Phase 2 — Business Analyst
**İlgili:** `BUSINESS_LOGIC.md` (BR-XXX), `DECISION_MATRIX.md`, `ERROR_HANDLING_GUIDELINE.md` (kategori taksonomisi)
**Tarih:** 2026-07-14

> Bu katalog `ERROR_HANDLING_GUIDELINE.md §1.1` kategori formatını (`{CATEGORY}_{DESCRIPTION}`) kullanır: `VAL_`, `BUS_`, `RES_`, `SYS_`, `EXT_`. Bu sistem bir **HTTP API değil, asenkron mesajlaşma katmanıdır** — bu yüzden "HTTP Status" kolonu yerine **"Statü"** (ack/nak/DLQ/incident/failure-event/log-only/build-fail vb.) kullanılır; kategori semantiği (VAL_=girdi hatası, BUS_=iş kuralı, RES_=varlık-durumu, SYS_=sistem arızası, EXT_=harici bağımlılık) korunur. Guideline §6 ilkesi uygulanır: **BUS_ kodları WARN'dır** (beklenen davranış), **SYS_ kodları ERROR'dır** (mühendislik dikkati gerektirir). Her kod `[BA-VERIFIED]` veya `[phase1-verified]` kanıtlıdır; kaynak koddan doğrulanamayan davranış `[phase3'te doğrulanacak]` etiketlidir.

**Kaynak (source) sözlüğü:** İstenen dört kaynak — **Worker** (motor-dışı, wire-contract ile yönetilir), **Bridge** (engine-inbound completion-bridge + DLQ-bridge/incident-bridge/failure-event-bridge), **Sweep** (soğuk orphan-sweep), **Bench** (Testcontainers yük-bench). Tam FR/NFR kapsaması için iki ek kaynak tanımlandı: **Publisher** (post-commit `TransactionListener`) ve **Config/Bootstrap** (deployment-time doğrulama) — bunlar dört canonical kaynağın doğal uzantısıdır (worker/bridge/sweep/bench mesaj-işleme anına özgüdür; publisher ve config farklı yaşam-döngüsü anlarına aittir).

---

## 1. Worker consumer kaynaklı

| Kod | Statü | Tetikleyen koşul | Davranış | Idempotency etkisi | Log seviyesi | BR/FR/US | Kanıt |
|---|---|---|---|---|---|---|---|
| `BUS_WORKER_BUSINESS_ERROR` | error-reply → ack | Worker BPMN business error tespit etti | `handleBpmnError(extTaskId, SENTINEL, errorCode,...)` çağrılır (native lifecycle) | Redelivery'de aynı error-reply yeniden işlenirse task muhtemelen zaten resolve olmuştur → `RES_EXTERNAL_TASK_NOT_FOUND` yoluna düşer | WARN | BR-A2-008 / FR-A7 / US-A4 | `06 §5.4` madde 1 |
| `SYS_WORKER_TRANSIENT_FAILURE` | nak-backoff | Worker işlerken I/O/timeout/uncaught exception — reply üretilemedi | JetStream kendiliğinden redeliver (worker nak veya ack-wait dolması) | At-least-once — worker aynı job'ı yeniden idempotent işlemeli | WARN (redelivery bütçesi dahilinde) / ERROR (M'e yaklaşırken) | BR-A2-001, BR-FLW-002 / FR-A1, FR-B2 / US-A1, US-B2 | `06 §5.4` madde 2 |
| `BUS_JOB_DELIVERY_BUDGET_EXCEEDED` | DLQ-route | `deliveryCount > M` — job-side redelivery bütçesi tükendi (worker'ın kendi consumer'ı in-band tespit eder) | Job'ı `dlq.jobs.<topic>`'a yönlendir (header+Nats-Msg-Id korunarak) → orijinal job ACK'lenir | Tek-seferlik geçiş (DLQ'ya bir kez düşer, dedup `Nats-Msg-Id=<id>.dlq` çakışmayı önler) | WARN | BR-A2-009 / FR-A10 / US-A6 | `06 §5.4` madde 3, `§7` D-E |

---

## 2. Engine-inbound consumer kaynaklı (A2 completion-bridge; Flowable `JetStreamInboundEventChannelAdapter`)

| Kod | Statü | Tetikleyen koşul | Davranış | Idempotency etkisi | Log seviyesi | BR/FR/US | Kanıt |
|---|---|---|---|---|---|---|---|
| `VAL_EMPTY_MESSAGE_BODY` | ack (mevcut) | `msg.getData()==null \|\| length==0` | Mevcut kod: log DEBUG + ACK, hiçbir işlem yapılmaz | Vacuous — hiçbir yan-etki yok; redelivery'nin faydası yok (aynı boş body) | DEBUG (mevcut) — **BAQ-5: WARN+metrik'e yükseltilmeli mi?** | BR-FLW-002 (edge-case) / FR-B2 / US-B2 | `[BA-VERIFIED]` `JetStreamInboundEventChannelAdapter.java:124-131` |
| `BUS_REPLY_DELIVERY_BUDGET_EXCEEDED` | DLQ-route | `deliveryCount > maxDeliver` reply/event subject'inde (engine-inbound consumer'ın kendi in-band tespiti) | `publishToDlq(msg)` → orijinal mesaj ACK'lenir | Tek-seferlik geçiş | WARN | BR-A2-009, BR-FLW-003 / FR-A10, FR-B3 / US-A6, US-B3 | `[BA-VERIFIED]` `JetStreamInboundEventChannelAdapter.java:133-146` |
| `SYS_DLQ_PUBLISH_FAILED` | nak+alert (DÜZELTME) | JetStream DLQ publish VE core-NATS fallback ikisi de başarısız; VEYA `dlqSubject==null` | **Mevcut kod:** discard/log-only + koşulsuz ack (custody-transfer ihlali — US-C2 fix hedefi). **Düzeltilmiş davranış:** nak + alert, asla ack-drop | Nak → redelivery → aynı hata tekrarlanırsa sonsuz döngü riski (BAQ-6: backoff/circuit-breaker politikası netleşmeli) | ERROR | BR-SUB-002 / FR-C2 / US-C2 | `[BA-VERIFIED]` `:210-214,222-235` (mevcut açık) |
| `RES_EXTERNAL_TASK_NOT_FOUND` | ack (idempotent yut) | `complete()`/`handleFailure()` çağrısı sırasında `findExternalTaskById()==null` → `NotFoundException` | Yakala + log WARN + ACK — **bu, US-A7'nin tasarlanmış idempotency mekanizmasıdır**, hata değil beklenen davranıştır | Duplicate/geç reply'ın (dedup penceresi dışı) sessizce yutulma yoludur | WARN | BR-A2-011 / FR-A12 / US-A7 | `[BA-VERIFIED]` `HandleExternalTaskCmd.java:48-50` |
| `SYS_SENTINEL_WORKER_CONFLICT` | **escalate — ack YOK** | `complete()`/`handleFailure()` çağrısı `validateWorkerViolation()==true` → `BadUserRequestException` | **ASLA olmamalı — invariant.** SENTINEL küme-geneli tek sabit olduğundan bu yalnız config drift / manuel müdahale / migration-guard bozulması ile oluşabilir. Otomatik ack/nak YERİNE insan incelemesi tetiklenmeli (BAQ-7: paging politikası) | N/A — retry edilmemeli, kök neden düzeltilmeden tekrar tetiklenir | **ERROR + alert** (guideline §6: BUS_ değil, gerçek invariant ihlali) | (yeni, BR-A2-003 invariant) / FR-A3 / US-A2 | `[BA-VERIFIED]` `HandleExternalTaskCmd.java:52-53` |
| `BUS_EVENT_CORRELATION_NOT_FOUND` | ack+log+metric (drop) | Flowable: `eventReceived()` bekleyen subscription bulamaz — ya (a) interrupting escalation zaten fırlamış (geç sonuç, US-B5) ya da (b) aynı event'in daha önceki bir teslimi zaten correlate olmuş (redelivered duplicate). Adapter perspektifinden iki durum **teknik olarak ayırt edilemez** (aynı "no match" çıktısı) — her ikisi de aynı ack+log+metric aksiyonuna varır | Bu, Flowable'ın US-A7-paralel idempotency/late-result mekanizmasıdır | WARN | BR-FLW-005 / FR-B5 / US-B5 | US-B5 AC#1; `[phase3'te doğrulanacak]` `eventReceived` tam davranışı (D-D c) |

---

## 3. DLQ-bridge kaynaklı (Camunda/CadenzaFlow incident-bridge; Flowable failure-event bridge)

| Kod | Statü | Tetikleyen koşul | Davranış | Idempotency etkisi | Log seviyesi | BR/FR/US | Kanıt |
|---|---|---|---|---|---|---|---|
| `RES_FAILURE_EVENT_CORRELATION_MISS` | log+metrik (ciddiyet BAQ-8) | Flowable failure-event bridge, DLQ mesajını failure-event'e çevirip `eventReceived()` çağırır ama bekleyen subscription YOK | Bu, escalation zincirinin **son savunma hattıdır** (NFR-R6 token-leak yasağı). Davranış: WARN+metrik (masum varsayım) VEYA ERROR+alert (leak-riski varsayımı) — **BAQ-8 onayı bekleniyor**, bu belgede temkinli taraf (ERROR+alert) önerilir | Mesajın kendisi ack'lenir (işlendi); asıl risk iş sonucunun kaybolmasıdır, mesaj kaybı değil | ERROR (önerilen, BAQ-8 onayına kadar) | BR-FLW-003 / FR-B3 / US-B3 | NFR-R6; `[phase3'te doğrulanacak]` (D-D c) |
| `SYS_DLQ_BRIDGE_PROCESSING_FAILED` | nak+alert | DLQ-bridge incident/failure-event oluştururken kendisi exception fırlatır (DB down, event-registry down) | nak + alert — **asla ack-drop** (dlq-of-dlq yok, `06 §7`) | Redeliver edilen DLQ mesajının tekrar işlenmesi, `setRetriesAndManageIncidents` sayesinde doğal idempotenttir (bkz. aşağıdaki not) | ERROR | BR-A2-009, BR-FLW-003 / FR-A10, FR-B3 / US-A6, US-B3 | `06 §7` "dlq-of-dlq YOK" |
| `BUS_INCIDENT_ALREADY_CREATED` *(bilgilendirici, hata değil)* | ack (no-op) | Aynı DLQ mesajı redeliver edilip `handleFailure(retries=0)` ikinci kez çağrılır | `setRetriesAndManageIncidents(0)`: `areRetriesLeft()` artık `false` (ilk çağrıda zaten 0'landı) → `createIncident()` **tekrar çağrılmaz** — duplicate incident oluşmaz | Doğal idempotency — kod-kanıtlı | DEBUG/INFO | BR-A2-009 / FR-A10 / US-A6 | `[BA-VERIFIED]` `ExternalTaskEntity.java:443-448` |

---

## 4. Sweep kaynaklı

| Kod | Statü | Tetikleyen koşul | Davranış | Idempotency etkisi | Log seviyesi | BR/FR/US | Kanıt |
|---|---|---|---|---|---|---|---|
| `SYS_SWEEP_QUERY_FAILED` | log-only, döngü atlanır | Sweep'in fetchable-parite sorgusu DB hatasıyla başarısız olur | Log ERROR; bir sonraki S(120s) döngüsünde tekrar denenir | Etkilenen satırlar bir döngü gecikmeyle işlenir — orphan yaşı ≤ L+2S'e çıkabilir (NFR-R3 sınırının hafif esnemesi) | ERROR | BR-A2-005 / FR-A5 / US-A3 | `06 §5.4` sweep tanımı |
| `SYS_SWEEP_RELOCK_FAILED` | log-only, satır atlanır | Sweep, fetchable satırı bulduktan sonra `lock(SENTINEL,L)` DB yazısı başarısız olur (DB down, deadlock) | Log ERROR; satır bu döngüde re-publish edilmez — bir sonraki döngüde tekrar fetchable-parite'e girer (guard hâlâ sağlanıyor çünkü lock hiç güncellenmedi) | Zararsız — satır durumu değişmedi, yalnız gecikme | ERROR | BR-A2-005 / FR-A5 / US-A3 | Aynı akışın türetilmiş dalı |
| `SYS_SWEEP_REPUBLISH_FAILED` | **kritik edge-case (BAQ-1)** | Sweep, re-lock DB yazısını BAŞARIYLA yaptıktan SONRA JetStream re-publish başarısız olur (broker down) | Satır artık `LOCK_EXP_TIME_=now+L` ile "taze kilitli" görünür ama HİÇBİR YERE teslim edilmedi — sonraki sweep döngüleri (≤L≈320s) bu satırı "in-flight" sanıp atlar (Karar Matrisi 3, satır 5) | **Potansiyel görev kaybı** (fiili olarak L süresince) — mekanizma güvence altına alınmalı (BAQ-1, Phase 3/4 tasarım kararı) | ERROR + öneri: en-yaşlı-orphan-yaşı metriğine (US-D2) yansıtılmalı | BR-A2-013 (yeni) / FR-A5, FR-A6 / US-A3 | Bu fazda türetildi — bkz. BUSINESS_LOGIC.md BR-A2-013 |
| `BUS_TASK_RETRIES_EXHAUSTED` *(beklenen durum, hata değil)* | atla (no-op) | Sweep, `RETRIES_=0` satırıyla karşılaşır | Asla dokunmaz — DLQ'lanmış/incident bölgesi, yalnız Cockpit-retry ile geri döner | Read-only, yan etkisi yok | DEBUG (rutin) | BR-A2-009 / FR-A11 / US-A6 | `[BA-VERIFIED]` `ExternalTask.xml` fetchable predicate |
| `RES_TASK_SUSPENDED` *(beklenen durum, hata değil)* | atla (no-op) | Sweep, `SUSPENSION_STATE_ ≠ 1` satırıyla karşılaşır | Asla dokunmaz — resume bekler | Read-only | DEBUG (rutin) | BR-A2-005 / FR-A5 / US-A3 | `[BA-VERIFIED]` `ExternalTask.xml` fetchable predicate |

---

## 5. Bench kaynaklı (Testcontainers yük-bench modülü)

| Kod | Statü | Tetikleyen koşul | Davranış | Idempotency etkisi | Log seviyesi | BR/FR/US | Kanıt |
|---|---|---|---|---|---|---|---|
| `SYS_BENCH_ENVIRONMENT_UNAVAILABLE` | build-warn-only (FAIL etmez) | Testcontainers PG/engine/NATS'ı ayağa kaldıramaz (örn. CI'da Docker daemon yok) | Bench koşusu inconclusive işaretlenir; **ana CI build'i bloklamaz** (`@Tag("bench")`, nightly/manuel) | N/A (test-zamanı) | WARN | BR-OBS-003 / FR-D3 / US-D3 | `06 §5.6` metodoloji |
| `BUS_BENCH_METRIC_REGRESSION` | **build-fail (sert kapı)** | Normalize DB-roundtrip metriği (US-D1) hedefi kaçırır: A2 modunda poll/fetchAndLock bileşenleri > 0, VEYA INSERT/complete bileşenleri baseline'a göre artmış | Bench raporu FAIL; **basamak-1 kapanışının TEK sert kabul kapısı** (Q7 kararı) | N/A (test-zamanı) | ERROR (bench raporunda) | BR-OBS-001 / FR-D1 / US-D1 | `06 §5.6` D-F, Q7 |
| `SYS_BENCH_SLI_DRIFT` | build-warn-only (FAIL etmez) | Destekleyici SLI hedefi kaçırılır (örn. dispatch p95 > 200ms, lock-wait > 0, HikariCP düşmüyor) | Rapor edilir, regresyon olarak işaretlenir; **kapanışı bloklamaz** (Q7: SLI izlenen hedef, sert kapı değil) | N/A | WARN (bench raporunda) | BR-OBS-002 / FR-D2 / US-D2 | `06 §5.6` Q7 netleştirmesi |

---

## 6. Config/Bootstrap kaynaklı (deployment-time doğrulama — dört canonical kaynağın uzantısı)

| Kod | Statü | Tetikleyen koşul | Davranış | Idempotency etkisi | Log seviyesi | BR/FR/US | Kanıt |
|---|---|---|---|---|---|---|---|
| `VAL_UMBRELLA_LOCK_TOO_SHORT` | **davranış BAQ-3'e bağlı** | Topic-özel `L` override'ı, `L ≥ M·W+Σbackoff+S+ε` şemsiye koşulunu ihlal eder | (a) reject-startup: topic aktivasyonu engellenir, VEYA (b) warn-only: config kabul edilir + log WARN — **US-A5 AC metni "validasyon/uyarı" diyerek ikisini de açık bırakıyor, BAQ-3 onayı gerekli** | N/A (deployment-zamanı) | ERROR (reject ise) / WARN (warn-only ise) | BR-A2-006 / FR-A8 / US-A5 | `06 §5.4` D-B formülü |
| `VAL_TOPIC_NAMESPACE_COLLISION` | **davranış BAQ-4'e bağlı** | Flowable inbound channel subject'i `jobs.*` önekiyle çakışır (US-C4 DLQ routing'i `dlq.jobs.>`'u sabit olarak incident-bridge'e yönlendiriyor) | Şu an **tespit edilmiyor** (dokümante bir kısıt yok) — DLQ mesajı yanlış bridge'e gider. BAQ-4 onayına kadar önerilen: deployment-time bir subject-namespace validasyonu eklenmeli | N/A (deployment-zamanı) | ERROR (önerilen) | BR-SUB-004 (edge-case) / FR-C4 / US-C4 | Bu fazda türetildi — bkz. DECISION_MATRIX.md Ek Matris 5, satır 3 |

---

## 7. Publisher kaynaklı (post-commit `TransactionListener` — dört canonical kaynağın uzantısı)

| Kod | Statü | Tetikleyen koşul | Davranış | Idempotency etkisi | Log seviyesi | BR/FR/US | Kanıt |
|---|---|---|---|---|---|---|---|
| `EXT_JETSTREAM_PUBLISH_UNAVAILABLE` | log-only (tasarım gereği tolere edilir) | Post-commit listener'ın ilk (fast-path) publish çağrısı, JetStream/broker o an ulaşılamaz olduğu için başarısız olur | Log WARN — **bu hata özel bir aksiyon GEREKTİRMEZ**, çünkü D-A tasarımı zaten bu senaryoyu soğuk sweep ile karşılamak üzere kurulmuştur (at-most-once fast path + at-least-once sweep net garanti) | Orphan, sweep tarafından ≤ L+S (~7dk, NFR-R3) içinde toplanır | WARN | BR-A2-004 / FR-A4 / US-A3 | `06 §5.3` D-A ("kaçan çökme-orphan'ları... sweep toplar") |

---

## 8. Kategori özeti

| Kategori | Kod sayısı | Anlamı (guideline §1.1) |
|---|---|---|
| `VAL_` | 3 | Girdi/config biçim hatası (boş body, L-floor, namespace çakışması) |
| `BUS_` | 7 | İş kuralı sonucu — **beklenen davranış**, çoğunlukla WARN (worker-business-error, job/reply-delivery-budget-exceeded ×2, event-correlation-not-found, incident-already-created, task-retries-exhausted, bench-metric-regression) |
| `RES_` | 3 | Varlık-durum sorunu (task-not-found, task-suspended, failure-event-correlation-miss) |
| `SYS_` | 9 | Sistem/altyapı arızası — çoğunlukla ERROR (worker-transient-failure, dlq-publish-failed, sentinel-conflict, dlq-bridge-processing-failed, sweep-query/relock/republish-failed ×3, bench-environment-unavailable, bench-sli-drift) — *not: SYS_WORKER_TRANSIENT_FAILURE (redelivery bütçesi dahilinde), SYS_BENCH_ENVIRONMENT_UNAVAILABLE ve SYS_BENCH_SLI_DRIFT WARN'dır, kategori SYS_ olsa da build'i bloklamaz* |
| `EXT_` | 1 | Harici bağımlılık (broker) geçici kullanılamazlığı — tasarım gereği tolere edilir |

**Toplam: 23 exception/durum kodu** (7 kaynak grubu: Worker=3, Engine-inbound=6, DLQ-bridge=3, Sweep=5, Bench=3, Config/Bootstrap=2, Publisher=1 → 3+6+3+5+3+2+1=23). Bunlardan 3 kodu (`BUS_INCIDENT_ALREADY_CREATED`, `BUS_TASK_RETRIES_EXHAUSTED`, `RES_TASK_SUSPENDED`) tabloda açıkça **"beklenen durum, hata değil"** olarak etiketlenmiştir (ERROR_HANDLING_GUIDELINE §6 ilkesi: "Errors are not exceptions to normal flow — they ARE normal flow").

---

## 9. İzlenebilirlik özeti (Kod → BR → FR → US)

| Kod | BR | FR | US |
|---|---|---|---|
| BUS_WORKER_BUSINESS_ERROR | BR-A2-008 | FR-A7 | US-A4 |
| SYS_WORKER_TRANSIENT_FAILURE | BR-A2-001, BR-FLW-002 | FR-A1, FR-B2 | US-A1, US-B2 |
| BUS_JOB_DELIVERY_BUDGET_EXCEEDED | BR-A2-009 | FR-A10 | US-A6 |
| VAL_EMPTY_MESSAGE_BODY | BR-FLW-002 | FR-B2 | US-B2 |
| BUS_REPLY_DELIVERY_BUDGET_EXCEEDED | BR-A2-009, BR-FLW-003 | FR-A10, FR-B3 | US-A6, US-B3 |
| SYS_DLQ_PUBLISH_FAILED | BR-SUB-002 | FR-C2 | US-C2 |
| RES_EXTERNAL_TASK_NOT_FOUND | BR-A2-011 | FR-A12 | US-A7 |
| SYS_SENTINEL_WORKER_CONFLICT | BR-A2-003 | FR-A3 | US-A2 |
| BUS_EVENT_CORRELATION_NOT_FOUND | BR-FLW-005 | FR-B5 | US-B5 |
| RES_FAILURE_EVENT_CORRELATION_MISS | BR-FLW-003 | FR-B3 | US-B3 |
| SYS_DLQ_BRIDGE_PROCESSING_FAILED | BR-A2-009, BR-FLW-003 | FR-A10, FR-B3 | US-A6, US-B3 |
| BUS_INCIDENT_ALREADY_CREATED | BR-A2-009 | FR-A10 | US-A6 |
| SYS_SWEEP_QUERY_FAILED | BR-A2-005 | FR-A5 | US-A3 |
| SYS_SWEEP_RELOCK_FAILED | BR-A2-005 | FR-A5 | US-A3 |
| SYS_SWEEP_REPUBLISH_FAILED | BR-A2-013 | FR-A5, FR-A6 | US-A3 |
| BUS_TASK_RETRIES_EXHAUSTED | BR-A2-009 | FR-A11 | US-A6 |
| RES_TASK_SUSPENDED | BR-A2-005 | FR-A5 | US-A3 |
| SYS_BENCH_ENVIRONMENT_UNAVAILABLE | BR-OBS-003 | FR-D3 | US-D3 |
| BUS_BENCH_METRIC_REGRESSION | BR-OBS-001 | FR-D1 | US-D1 |
| SYS_BENCH_SLI_DRIFT | BR-OBS-002 | FR-D2 | US-D2 |
| VAL_UMBRELLA_LOCK_TOO_SHORT | BR-A2-006 | FR-A8 | US-A5 |
| VAL_TOPIC_NAMESPACE_COLLISION | BR-SUB-004 | FR-C4 | US-C4 |
| EXT_JETSTREAM_PUBLISH_UNAVAILABLE | BR-A2-004 | FR-A4 | US-A3 |

**Sonuç:** 23/23 kod, sıfır istisna dışı (traceless) kod yok — hepsi bir BR üzerinden bir US'ye bağlanır.

---

## 10. BA-QUESTIONS referansı

Bu katalogdaki 5 kodun davranışı doğrudan `BUSINESS_LOGIC.md §9`'daki BA-QUESTIONS'a bağlıdır:
- `SYS_DLQ_BRIDGE_PROCESSING_FAILED` → **BAQ-6** (backoff/circuit-breaker politikası)
- `SYS_SENTINEL_WORKER_CONFLICT` → **BAQ-7** (paging ciddiyeti)
- `RES_FAILURE_EVENT_CORRELATION_MISS` → **BAQ-8** (WARN vs ERROR)
- `VAL_UMBRELLA_LOCK_TOO_SHORT` → **BAQ-3** (reject vs warn)
- `VAL_TOPIC_NAMESPACE_COLLISION` → **BAQ-4** (namespace rezervasyonu)
- `VAL_EMPTY_MESSAGE_BODY` → **BAQ-5** (5. contract-fix mi, bilinçli savunma-kodu mu)
- `SYS_SWEEP_REPUBLISH_FAILED` → **BAQ-1** (re-lock/re-publish sıralama garantisi)

Tam soru metinleri ve gerekçeleri: `BUSINESS_LOGIC.md §9`.

---

*İlgili: `BUSINESS_LOGIC.md` (BR-XXX kataloğu), `DECISION_MATRIX.md` (bu kodların üretildiği karar noktaları).*
