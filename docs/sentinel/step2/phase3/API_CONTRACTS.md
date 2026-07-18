# API / Wire-Contract — Anlatı
## Basamak-2: History Offload (ACT_HI → NATS → async query-store)

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Sentinel fazı:** Phase 3 — Architect
**Tarih:** 2026-07-17
**Durum:** İnceleme bekliyor

> **Makine-okunur sözleşmeler tek yerdedir:** [`api/asyncapi.yaml`](api/asyncapi.yaml) (AsyncAPI 3.1.0 — history teli; `asyncapi validate` ile **0 error / 0 governance issue**) ve [`api/openapi.yaml`](api/openapi.yaml) (OpenAPI 3.0.3 — read-only history sorgu-API; `redocly lint` **temiz**). Bu belge o spec'lerin **anlatısıdır** — subject/mesaj/şema/uç-nokta tanımlarını **satır-içi TEKRARLAMAZ**, spec'lere referans verir (ADR-0013/ADR-0014; phase-review: inline spec = 🔴). Burada yalnızca AsyncAPI/OpenAPI standart binding'i **olmayan** JetStream stream/consumer konfigürasyonu (config tabloları) normatif olarak elaborate edilir. İzlenebilirlik: IR-1…IR-7 (`SRS.md §5`), BR-HDL/BR-REL/BR-QRY, docs/07 §1 (D-E).

---

## 1. Subject şeması (IR-1)

Kanonik subject'ler ve sahiplikleri (tam tanım: asyncapi `channels`):

| Subject deseni | Kanal (asyncapi) | Yön | Sahip | Not |
|---|---|---|---|---|
| `history.<engineId>.<class>.<processInstanceId>` | `historyEvent` | engine → projeksiyon | relay (audit-kritik) + post-commit (bulk) | Limits stream; instance-anahtarlı sıra; `Nats-Msg-Id=<historyEventId>:<eventType>` |
| `dlq.history.<orijinal-subject>` | `historyDeadLetter` | consumer/relay → DLQ | ortak | AYRI `dlq.history.>` stream (CQ-6); header-korumalı byte-ayna |

### 1.1 `history.*` / `dlq.history.*` namespace izolasyonu (ADR-0019 · DP-13)

`history.>` ve `dlq.history.>` ayrı JetStream stream'lerinde yaşar (CQ-6 — iş-dağıtımı `dlq.>` stream'inden bağımsız). Subject-level authz (ADR-0019): relay/post-commit publisher yalnız `history.>`'e publish; projeksiyon consumer + reconciliation/replay yalnız `history.>`'e subscribe; `dlq.history.>` inceleme yalnız yetkili ops hesabına (`RES_HISTORY_DLQ_ACCESS_DENIED`). Provisioning eksikse `VAL_HISTORY_STREAM_PROVISIONING_MISSING` (BR-DBT-002).

---

## 2. Stream & consumer konfigürasyonu (IR-7 — AsyncAPI-dışı, normatif)

> Bu tablolar `api/asyncapi.yaml`'daki `x-jetstream` uzantı alanlarının **normatif elaborasyonudur**; AsyncAPI standart binding'i JetStream stream/consumer semantiğini taşımadığı için burada tam tanımlanır (ADR-0013).

### 2.1 History stream (`HISTORY`)

| Alan | Değer | Kaynak |
|---|---|---|
| Retention | **Limits-based** (WorkQueue DEĞİL — projeksiyon consumer + reconciliation/replay okur) | IR-7 · ADR-0013 |
| Storage | File | basamak-1 `JetStreamStreamManager` deseni (Limits) |
| Retention penceresi | konfigürable, default **7 gün** (kısa transit-buffer; kalıcı kayıt projeksiyon store) | ADR-0019 · DP-13 |
| `ack-wait` | 30s default | basamak-1 deseni |
| `maxDeliver` | 4 (consumer `maxDeliver+1`=5 → in-band DLQ tespiti) | basamak-1 deseni |
| `duplicate_window` | 120s (default NATS 2dk) | basamak-1 §11 ✅ |
| Dedup header | `Nats-Msg-Id = <historyEventId>:<eventType>` | IR-3 · BR-HDL-006 |
| Partitioning | deterministik subject-mapped (processInstanceId token hash) — **ARCH-Q3** | ADR-0011 |

### 2.2 History DLQ stream (`DLQ_HISTORY`, ayrı — CQ-6)

| Alan | Değer | Kaynak |
|---|---|---|
| Subject | `dlq.history.>` (AYRI stream) | IR-1 · BR-REL-004 · ADR-0019 |
| Retention | **Limits-based** | IR-7 |
| Saklama | default **14 gün**, **kiracı-bazlı override** (PII kiracı kısaltmalı/erişimi kısıtlamalı) | DP-13 · NFR-S4 |
| Dedup header | `Nats-Msg-Id = <orijinal-msg-id>.dlq` | BR-REL-005 |
| Tüketim | ops/inceleme (yetkili); DLQ-bridge CB korumalı (ADR-0004) | ADR-0019/0004 |

---

## 3. Header sözleşmesi (IR-2 — tam şema: asyncapi `components/schemas`)

Header **adları ve sınıfları** (değer şemaları asyncapi'de `HistoryHeaders`/`HistoryDlqHeaders`):

| Header | Zorunluluk | Sınıf (DATA_CLASSIFICATION) | Not |
|---|---|---|---|
| `Nats-Msg-Id` | zorunlu | INTERNAL | dedup = `<historyEventId>:<eventType>`; DLQ'da `<orijinal>.dlq` |
| `X-Cadenzaflow-History-Engine-Id` | zorunlu (event) | INTERNAL | motor örneği |
| `X-Cadenzaflow-History-Class` | zorunlu | PUBLIC | ACT_HI sınıfı (routing/metrik tag) |
| `X-Cadenzaflow-History-Event-Type` | zorunlu (event) | INTERNAL | dedup parçası |
| `X-Cadenzaflow-History-Event-Id` | zorunlu | INTERNAL | dedup parçası |
| `X-Cadenzaflow-History-Process-Instance-Id` | zorunlu | PSEUDONYMOUS | partition/sıra anahtarı |
| `X-Cadenzaflow-Trace-Id` | opsiyonel | PSEUDONYMOUS | devralınan; okuma `X-Trace-Id` fallback |
| `X-Cadenzaflow-Business-Key` | opsiyonel | **CONFIDENTIAL/koşullu PII** | subject'e GÖMÜLMEZ (DP-2); masking kiracı (DP-8) |
| `X-Cadenzaflow-Idempotency-Key` | opsiyonel | PSEUDONYMOUS | devralınan |
| `X-Cadenzaflow-Dlq-Original-Subject` | DLQ | PUBLIC | routing meta |
| `X-Cadenzaflow-Dlq-Delivery-Count` | DLQ | INTERNAL | ops meta |
| `X-Cadenzaflow-Dlq-Reason` | DLQ | INTERNAL | yalnız hata sınıfı/kod — PII SIZDIRMAZ (DP-6) |
| `X-Cadenzaflow-Dlq-Timestamp` | DLQ | PUBLIC | zaman damgası |

---

## 4. Ack / custody-transfer semantiği (IR-4/IR-5 · BR-REL-001/002/005)

**Custody-transfer:** ack yalnız kalıcılık el değiştirdikten sonra. Rol-başına ack anı (tam matris: `DECISION_MATRIX.md` Matris 5):

| Rol | Custody-transfer anı |
|---|---|
| Relay (outbox → NATS) | **PubAck-sonrası** outbox satırı silinir; PubAck alınamazsa retry (satır SİLİNMEZ) |
| Post-commit publisher (bulk) | at-most-once — publish sonrası (kayıp reconciliation'da görünür) |
| Projeksiyon consumer | merge-upsert (veya bilinçli no-op) sonrası ack; transient → `nakWithDelay` |
| History-DLQ yolu | **DLQ-PubAck-sonrası-ack**; publish fail → **nak** (asla ack-drop) |

- `deliveryCount > M` → in-band history-DLQ (advisory REDDEDİLDİ — basamak-1 §11).
- Merge-upsert stale-event (stream-sequence ≤ mevcut) → no-op (`BUS_PROJECTION_STALE_EVENT_DISCARDED`) ama custody yine transfer olur (ack).

---

## 5. Payload sözleşmeleri (tam şema: asyncapi/openapi `components/schemas`)

**History teli (asyncapi):**

| Mesaj (asyncapi) | Payload şeması | Tetiklediği aksiyon |
|---|---|---|
| `HistoryEventMessage` | `HistoryEventPayload` (kimlik alanları + opak `payload`) | projeksiyon merge-upsert |
| `HistoryDeadLetterMessage` | `OpaqueHistoryPayload` (byte-aynen) + `HistoryDlqHeaders` | history-DLQ (ops inceleme) |

- **Byte-array HISTORY payload taşıma (ARCH-Q1):** `HistoryEventPayload.payload` opak string modellenir; inline byte vs referans nihai kararı ARCH-Q1 (öneri: referans) — **kontrat şekli her halde sabit** (ADR-0010/0013).
- **Merge-upsert versiyon alanı (BA-Q1):** `streamSequence` JetStream tarafından atanır, consumer doldurur; tie-break/versiyon otoritesi (ADR-0012).

**Sorgu-API (openapi — read-only, çekirdek-4):**

| Uç-nokta (openapi `operationId`) | Çekirdek-4 deseni | Yanıt şeması |
|---|---|---|
| `getProcessInstanceHistory` | (1) processInstanceId → tam geçmiş | `ProcessInstanceHistory` |
| `listProcessInstanceHistory` | (2) businessKey / (3) zaman-aralığı+definition → liste | `ProcessInstanceHistory[]` (sayfalamalı) |
| `listActivityHistory` / `listTaskHistory` / `listVariableHistory` | (4) instance → activity/task/variable geçmişi | ilgili şema[] (sayfalamalı, PII-maskeli) |

- **Standart yanıt zarfı:** `{success, message, code, data, meta{page,size,total,traceId}}` (ARCHITECT_GUIDELINE §4). Kod alanı EXCEPTION_CODES taksonomisi (`VAL_QUERY_UNSUPPORTED_PATTERN`, `AUTH_QUERY_ACCESS_DENIED`, `BUS_QUERY_PII_MASKED`).
- **PII maskeleme:** RESTRICTED/PII alanlar (variable değeri, operatör kimliği, serbest metin) role-bazlı maskeli (DP-15); log'a PII değeri YAZILMAZ (DP-1).

---

## 6. Sözleşme değişmezleri (invariants)

- **NFR-M3:** iki yayın yolu (relay + post-commit) **aynı** history wire-contract'ı üretir → consumer yol-agnostik.
- **NFR-M5:** wire-contract, Flowable basamak-2b tarafından **aynı** biçimde tüketilebilecek şekilde tanımlı (yalnız adapter işi; D-G).
- **At-least-once + idempotent (NFR-R6):** dedup penceresi sonlu (`duplicate_window`); pencere-dışı çift merge-upsert (stream-sequence versiyon) ile yutulur (BA-Q1).
- **Custody-transfer (NFR-R3):** hiçbir history mesajı kalıcılık el değiştirmeden ACK'lenmez; sessiz kayıp yok.
- **Ayrı-stream (CQ-6):** history ve DLQ iş-dağıtımı stream'lerinden bağımsız.

---

## 7. Referanslar
- Makine-okunur sözleşmeler: [`api/asyncapi.yaml`](api/asyncapi.yaml), [`api/openapi.yaml`](api/openapi.yaml)
- ADR-0013 (history wire-contract), ADR-0014 (sorgu-API), ADR-0019 (stream retention + subject authz), ADR-0010/0012 (yayın yolları + merge-upsert), ADR-0004 (CB — DLQ-bridge)
- `SRS.md §5` (IR-1…7), `DECISION_MATRIX.md` (Matris 4/5), `EXCEPTION_CODES.md`
- `DATA_CLASSIFICATION.md` (header/payload sınıfları), `docs/07 §1` (D-E)
