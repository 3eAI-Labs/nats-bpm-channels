# API / Wire-Contract — Anlatı
## Basamak-1: External Task / Event-Driven Work Offload over JetStream

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Sentinel fazı:** Phase 3 — Architect
**Tarih:** 2026-07-14
**Durum:** İnceleme bekliyor

> **Makine-okunur sözleşme tek yerdedir:** [`api/asyncapi.yaml`](api/asyncapi.yaml) (AsyncAPI 3.1.0, `asyncapi validate` ile **0 error / 0 governance issue**). Bu belge o spec'in **anlatısıdır** — subject/mesaj/şema tanımlarını **satır-içi TEKRARLAMAZ**, spec'e referans verir (ADR-0006). Burada yalnızca AsyncAPI standart binding'i **olmayan** JetStream stream/consumer konfigürasyonu (config tabloları) normatif olarak elaborate edilir. İzlenebilirlik: IR-1…IR-6 (`SRS.md §5`), BR-SUB-001…008, docs/06 §7, docs/05 §7.

---

## 1. Subject şeması (IR-1)

Kanonik subject'ler ve sahiplikleri (tam tanım: asyncapi `channels`):

| Subject deseni | Kanal (asyncapi) | Yön | Sahip idiom | Not |
|---|---|---|---|---|
| `jobs.<topic>` | `a2JobDispatch` | engine → worker | **A2** (REZERVE) | WorkQueue; `Nats-Msg-Id=externalTaskId` |
| `jobs.<topic>.reply` | `a2JobReply` | worker → engine-inbound | **A2** (REZERVE) | complete/handleFailure/handleBpmnError |
| `<eventChannelSubject>` | `flowableEventChannel` | çift yön | Event Registry | `jobs.*` önekiyle ÇAKIŞAMAZ |
| `dlq.<orijinal-subject>` | `deadLetter` | consumer → DLQ-bridge | ortak | TEK `dlq.>` stream |

### 1.1 `jobs.*` namespace rezervasyonu (BAQ-4 · BR-SUB-004 · VAL_TOPIC_NAMESPACE_COLLISION)

`jobs.*` **ve** `dlq.jobs.*` önekleri **A2'ye (Camunda/CadenzaFlow) REZERVEDİR**. Bir Flowable Event Registry inbound channel'ı bu önekle subject tanımlamaya çalışırsa **bootstrap-time** validasyon `VAL_TOPIC_NAMESPACE_COLLISION` (ERROR) fırlatır ve **deployment ENGELLENİR** (aktif mekanik kapı; artık yalnız dokümante bir kısıt değil). Gerekçe: `dlq.jobs.>` sabit routing ile incident-bridge'e gider (§2.3); çakışma yanlış-bridge DLQ yönlendirmesi üretir → yapısal olarak imkânsız kılınır.

---

## 2. Stream & consumer konfigürasyonu (IR-6 — AsyncAPI-dışı, normatif)

> Bu tablolar `api/asyncapi.yaml`'daki `x-jetstream-*` uzantı alanlarının **normatif elaborasyonudur**; AsyncAPI standart binding'i JetStream stream/consumer semantiğini taşımadığı için burada tam tanımlanır (ADR-0006).

### 2.1 İş dağıtımı stream'leri (jobs / reply / event channels)

| Alan | Değer | Kaynak |
|---|---|---|
| Retention | **WorkQueue** (her mesaj tek tüketiciye, ack'te silinir) | FR-C5/BR-SUB-005 |
| Storage | File | mevcut `JetStreamStreamManager.java:30-32` deseni (Limits→WorkQueue'e ayarlanır) |
| `ack-wait` (W) | 30s default, topic-başına override | ADR-0001 |
| `maxDeliver` (M) | 4 (consumer `maxDeliver+1`=5 → in-band DLQ tespiti) | mevcut `:75-77` / `:58` |
| `duplicate_window` | 120s (yapılandırılabilir; default NATS 2dk) | §11 HLD ✅ |
| Dedup header | `Nats-Msg-Id` = `externalTaskId` (A2) / correlation key (Event Registry) | IR-3 |
| Tüketici teslimi | push, **queue-group** (tek-worker-alır) | US-A1/US-C5 |

### 2.2 DLQ stream (tek ortak `dlq.>`)

| Alan | Değer | Kaynak |
|---|---|---|
| Subject | `dlq.>` (TEK stream, idiom-başına ayrı stream REDDEDİLDİ) | FR-C4/BR-SUB-004 |
| Retention | **Limits-based** (WorkQueue DEĞİL) | FR-C4 |
| Saklama | default **14 gün**, **kiracı-bazlı override** (PII kiracı kısaltmalı/erişimi kısıtlamalı) | Q3/DP-3/NFR-S2 |
| Dedup header | `Nats-Msg-Id` = `<orijinal-msg-id>.dlq` | BR-SUB-003 |
| Routing (tüketici filtresi) | `dlq.jobs.>` → incident-bridge; diğer `dlq.>` → failure-event bridge | §2.3 / Ek Matris 5 |

### 2.3 DLQ routing tablosu (Ek Matris 5)

| DLQ subject deseni | Tüketici | Post-DLQ aksiyon |
|---|---|---|
| `dlq.jobs.<topic>` | Camunda/CadenzaFlow **incident-bridge** | `handleFailure(retries=0, retryDuration=0)` → Cockpit incident (BAQ-2) |
| `dlq.<event-subject>` (jobs. önekSİZ) | Flowable **failure-event bridge** | aynı correlation key'lerle `eventReceived` failure-event |

Her iki bridge: **nak-backoff + circuit-breaker** (ADR-0004, BR-SUB-008); CB OPEN iken mesaj kalıcı stream'de bekler (kayıp YOK); **dlq-of-dlq YOK**.

---

## 3. Header sözleşmesi (IR-2 — tam şema: asyncapi `components/schemas`)

Header **adları ve sınıfları** (değer şemaları asyncapi'de `BpmHeadersWithDedup`/`ReplyHeaders`/`DlqHeaders`):

| Header | Zorunluluk | Sınıf (DATA_CLASSIFICATION) | Not |
|---|---|---|---|
| `Nats-Msg-Id` | job/reply/event: zorunlu | INTERNAL/PSEUDONYMOUS | dedup; DLQ'da `<orijinal>.dlq` |
| `X-Cadenzaflow-Trace-Id` | opsiyonel | PSEUDONYMOUS | yazma tek ad; okuma `X-Trace-Id` fallback (FR-C7) |
| `X-Cadenzaflow-Business-Key` | opsiyonel | **CONFIDENTIAL/koşullu PII** | telco'da MSISDN olabilir; masking kiracı kararı (DP-8) |
| `X-Cadenzaflow-Idempotency-Key` | opsiyonel | PSEUDONYMOUS | apply-zamanı idempotency |
| `X-Cadenzaflow-Correlation-Id` | async | PSEUDONYMOUS | reply eşleştirme (echo). **Yeni kontrat header'ı** — `BpmHeaders.java`'da HENÜZ yok (yalnız 3 sabit `:12-14`); sabit phase5'te |
| `X-Cadenzaflow-Reply-Subject` | async | INTERNAL | reply routing. **Yeni kontrat header'ı** — kod phase5 (asyncapi `ReplyHeaders` ile hizalı) |
| `X-Cadenzaflow-Dlq-Original-Subject` | DLQ | PUBLIC | routing meta |
| `X-Cadenzaflow-Dlq-Delivery-Count` | DLQ | INTERNAL | ops meta |
| `X-Cadenzaflow-Dlq-Reason` | DLQ | INTERNAL | yalnız hata sınıfı/kod — **PII sızdırMAZ** (DP-6) |
| `X-Cadenzaflow-Dlq-Timestamp` | DLQ | PUBLIC | zaman damgası |

**Trace-header geçiş kuralı (FR-C7 · BR-SUB-006):** yazma tarafı **yalnız** `X-Cadenzaflow-Trace-Id` (`BpmHeaders.java:12`) üretir; okuma tarafı önce `X-Cadenzaflow-Trace-Id`, yoksa `X-Trace-Id` (fallback — eski üreticilerle geriye uyum; mevcut okuma `JetStreamInboundEventChannelAdapter.java:119`).

---

## 4. Ack / custody-transfer semantiği (IR-4/IR-5 · BR-SUB-002)

**Custody-transfer:** ack yalnız kalıcılık el değiştirdikten sonra. Rol-başına ack anı (tam matris: `DECISION_MATRIX.md` Matris 1):

| Rol | Custody-transfer anı |
|---|---|
| Worker (jobs consumer) | reply-PubAck-sonrası-ack (business-error de reply → error-reply → ack) |
| Engine-inbound (reply/event consumer) | complete/correlate-dönüşü-sonrası-ack |
| DLQ yolu | **DLQ-PubAck-sonrası-ack**; publish fail (her iki yol) → **nak** (asla ack-drop) |

- Transient hata → `nakWithDelay` (üstel `2^(n-1)`s, cap 30s — mevcut ortak desen `:204-208`).
- `deliveryCount > M` → in-band DLQ (advisory REDDEDİLDİ, §11 HLD).
- `dlqSubject==null` → **nak** (discard DEĞİL — kontrat-fix #2).

---

## 5. Payload sözleşmeleri (tam şema: asyncapi `components/schemas`)

| Mesaj (asyncapi) | Payload şeması | Tetiklediği engine aksiyonu |
|---|---|---|
| `JobRequest` | `OpaqueBusinessPayload` (kiracı-tanımlı, RESTRICTED/PII) | worker işi |
| `JobSuccessReply` | `OpaqueBusinessPayload` | `complete(extTaskId, SENTINEL, vars)` |
| `JobBpmnErrorReply` | `BpmnErrorPayload` (`errorCode` zorunlu) | `handleBpmnError(...)` |
| `JobTransientFailureReply` | `TransientFailurePayload` (`errorMessage` zorunlu) | `handleFailure(...)` |
| `EventMessage` | `OpaqueBusinessPayload` | `eventReceived(...)` correlate |
| `DeadLetterMessage` | `OpaqueBusinessPayload` (byte-aynen kopya) + `DlqHeaders` | DLQ-bridge (incident / failure-event) |

**Failure-event payload (BR-FLW-003):** DLQ mesajının **orijinal payload'ı + orijinal correlation header'ları** korunur (kontrat-fix #1); failure-event bridge bunları aynen `eventReceived`'a taşır → bekleyen instance correlate olur. Ayrı bir "failure-event" şeması YOKTUR — failure-event = orijinal event mesajının DLQ-meta ile zenginleştirilmiş biçimi; bridge onu Event Registry'ye failure-semantiğiyle (model escalation path'i) sokar.

**Boş-body kuralı (BR-SUB-007):** hiçbir job/reply/event meşru olarak boş olamaz → boş body `VAL_EMPTY_MESSAGE_BODY` = WARN + DLQ (sessiz ack YOK).

---

## 6. Sözleşme değişmezleri (invariants)

- **NFR-M3:** iki idiom (A2 + Event Registry) **aynı** wire-contract'ı yayar/tüketir → worker ekosistemi paylaşılır.
- **NFR-M4:** wire-contract, basamak-6 native engine (Track A) tarafından **dışarıya aynı** verilebilecek biçimde tanımlı → worker'lar değişmeden bağlanabilir (docs/05 §7). Bu, `api/asyncapi.yaml`'ın Track A'ya devredilebilir tek artefakt olmasını gerektirir (ADR-0006).
- **At-least-once + idempotent** (NFR-R1): dedup penceresi sonlu (`duplicate_window`); pencere-dışı çift complete/correlate-idempotency ile yutulur.

---

## 7. Referanslar
- Makine-okunur sözleşme: [`api/asyncapi.yaml`](api/asyncapi.yaml)
- ADR-0006 (contract-first), ADR-0004 (CB), ADR-0001 (W/M/S/ε/L)
- `SRS.md §5` (IR-1…6), `DECISION_MATRIX.md` (Matris 1, Ek Matris 5/6), `EXCEPTION_CODES.md`
- `DATA_CLASSIFICATION.md` (header/payload sınıfları), `docs/06 §7`, `docs/05 §7`
