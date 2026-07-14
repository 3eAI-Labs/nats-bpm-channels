# 04.1 — 5 Kontrat-Fix: Somut Değişiklik Noktaları (file:line)

**Kaynak:** HLD §2.7, ADR-0006/0007; `BUSINESS_LOGIC.md` BR-SUB-001/002/003/006/007; `EXCEPTION_CODES.md` §2-3.
Bu tablo, phase2/3'ün tespit ettiği **mevcut açıkların** her üç dosyadaki (flowable, camunda, cadenzaflow) tam `file:line` konumunu ve LLD'nin öngördüğü **somut değişikliği** eşler. Üç dosya ADR-0007 §2 gereği ayna olduğundan satırlar neredeyse özdeştir (bu fazda `diff`/okuma ile teyit edildi).

---

## Fix#1 — DLQ header preservation (BR-SUB-001, FR-C1, US-C1)

| Dosya | Mevcut açık (file:line, bu fazda okundu) | Değişiklik |
|---|---|---|
| `flowable-nats-channel/.../jetstream/JetStreamInboundEventChannelAdapter.java` | `publishToDlq(Message msg)` metodu `:210-237`; yalnız `byte[] data = msg.getData();` (`:216`) yayınlanıyor, header hiç okunmuyor/kopyalanmıyor (`:218` `jetStream.publish(dlqSubject, data)` — 2-arg overload, header yok) | `publishToDlq` private metodu **SİLİNİR**; çağrı sitesi (`:141`, `:146`) `dlqPublisher.publish(msg, dlqSubject, DlqReason.DELIVERY_BUDGET_EXCEEDED, subject, channelKey)` (`nats-core`, `03_classes/1_nats_core_common.md` §2.3) ile değişir |
| `camunda-nats-channel/.../inbound/JetStreamMessageCorrelationSubscriber.java` | `publishToDlq(Message msg)` `:204-231`; `:212` `jetStream.publish(config.getDlqSubject(), data)` | Aynı devir — `A2CompletionBridge.routeToDlqAndDecide(...)` (`03_classes/2_camunda_a2.md` §4) |
| `cadenzaflow-nats-channel/.../inbound/JetStreamMessageCorrelationSubscriber.java` | `publishToDlq(Message msg)` `:202-229`; `:210` `jetStream.publish(...)` | Ayna — aynı devir |

**Sonuç:** DLQ mesajı artık orijinal header'ların **tamamını** (`Headers` çok-değerli map iterasyonu, `DlqPublisher.copyOriginalHeadersVerbatim`) + 4 meta header (`DlqHeaders.ORIGINAL_SUBJECT/DELIVERY_COUNT/REASON/TIMESTAMP`) taşır.

---

## Fix#2 — Custody-transfer ack (BR-SUB-002, FR-C2/C6, US-C2)

| Dosya | Mevcut açık (file:line) | Değişiklik |
|---|---|---|
| `flowable-nats-channel/.../jetstream/JetStreamInboundEventChannelAdapter.java` | `:141-145` (`publishToDlq(msg); ... msg.ack();` — publish sonucu KONTROL EDİLMEDEN koşulsuz ack); `:211-214` (`dlqSubject==null` → `log.warn` + `return`, çağıran yine ack'liyor); `:222-235` (her iki publish de başarısız → yalnız `log.error`, yine ack) | `msg.ack()` çağrısı `DlqPublishOutcome`'a **koşullu** hale gelir (`03_classes/1_nats_core_common.md` §2.5 switch bloğu): `PUBLISHED_*` → ack; `FAILED_*` → `msg.nakWithDelay(...)` |
| `camunda-nats-channel/.../inbound/JetStreamMessageCorrelationSubscriber.java` (→ `A2CompletionBridge`) | `:127-129` (dlq sonrası koşulsuz ack); `:203-207` (`dlqSubject==null` → return, ack); `:214-228` (both-fail → ack) | Aynı switch deseni, `A2CompletionBridge.routeToDlqAndDecide` |
| cadenzaflow ayna | `:123-127`, `:203-207` (ADR-0003/0007 dokümanlarında da anılan satırlar) | Aynı devir |

**Sonuç:** `dlqSubject==null` → **discard yok, nak**; DLQ-publish tam-başarısızlık → **nak+alert**, asla ack-drop (`dlq-of-dlq YOK`).

---

## Fix#3 — DLQ publish'te `Nats-Msg-Id` (BR-SUB-003, FR-C3, US-C3)

| Dosya | Mevcut açık (file:line) | Değişiklik |
|---|---|---|
| flowable | `:218` (`jetStream.publish(dlqSubject, data)` — `Nats-Msg-Id` YOK) | `DlqPublisher.publish(...)` içinde `dlqHeaders.put(NATS_MSG_ID_HEADER, originalMsgId + ".dlq")` (§2.3) |
| camunda | `:212` | Aynı |
| cadenzaflow | `:210` | Aynı |

**Sonuç:** çökme-sonrası çift DLQ kaydı, stream `duplicate_window` (120s) içinde dedup'lanır.

---

## Fix#4 — Trace-header standardizasyonu (BR-SUB-006, FR-C7, US-C6)

| Dosya | Mevcut açık (file:line) | Değişiklik |
|---|---|---|
| flowable | `handleMessage(...)` `:119` — `NatsHeaderUtils.extractHeader(msg, "X-Trace-Id")` (yalnız eski ad okunuyor) | `BpmHeaders.extractTraceIdWithFallback(msg)` (`nats-core §1.1`) — önce `X-Cadenzaflow-Trace-Id`, yoksa `X-Trace-Id` |
| camunda | `handleMessage(...)` `:102` — aynı desen | Aynı değişiklik |
| cadenzaflow | `handleMessage(...)` `:102` — aynı desen | Aynı değişiklik |
| **Yazma tarafı** (`BpmHeaders.java:12`) | zaten yalnız `X-Cadenzaflow-Trace-Id` üretiyor — DEĞİŞMEZ | — (yazma tarafı zaten kontrata uygun; yalnız okuma tarafı düzeltilir) |

**Sonuç:** yeni üreticiler yalnız standart adı yazar; geçiş döneminde eski üreticilerle geriye uyum okuma-fallback ile korunur.

---

## Fix#5 — Boş mesaj gövdesi (BR-SUB-007, FR-C2/B2, US-C2/B2 — BAQ-5 ile eklendi)

| Dosya | Mevcut açık (file:line) | Değişiklik |
|---|---|---|
| flowable | `:124-131` — `if (data==null\|\|data.length==0) { log.debug(...); msg.ack(); return; }` (sessiz DEBUG+ack) | `log.warn(...)` + `dlqPublisher.publish(msg, dlqSubject, DlqReason.EMPTY_MESSAGE_BODY, ...)` + outcome'a göre ack/nak (Fix#2 ile aynı switch) |
| camunda | `:108-114` — aynı desen | Aynı değişiklik (`A2CompletionBridge`) |
| cadenzaflow | `:108-114` — aynı desen (bu fazda bizzat okundu, `handleMessage` `:107-114`) | Aynı değişiklik |

**Sonuç:** boş-body artık gözlemlenebilir (WARN + `metrics.dlqCount`) ve DLQ'ya yönlendirilir — sessiz kayıp yok; redelivery aynı boş body'yi üretirse sonsuz redelivery değil, DLQ'ya düşer (aynı akış tekrar çalışır).

---

## Özet tablo — kontrat-fix → sınıf → BR/FR/US

| # | Fix | Ortak uygulanan sınıf | BR | FR | US |
|---|---|---|---|---|---|
| 1 | Header preservation | `DlqPublisher` (nats-core) | BR-SUB-001 | FR-C1 | US-C1 |
| 2 | Custody-transfer ack | `DlqPublisher` + her caller'ın switch bloğu | BR-SUB-002 | FR-C2/C6 | US-C2 |
| 3 | DLQ dedup id | `DlqPublisher` | BR-SUB-003 | FR-C3 | US-C3 |
| 4 | Trace-header fallback | `BpmHeaders.extractTraceIdWithFallback` (nats-core) | BR-SUB-006 | FR-C7 | US-C6 |
| 5 | Boş-body → DLQ | `DlqPublisher` + her caller | BR-SUB-007 | FR-C2/B2 | US-C2/B2 |

**Not (ADR-0006):** bu tablo `api/asyncapi.yaml`'ı **tekrarlamaz** — wire-contract (subject/mesaj/şema) tek kaynak orada yaşar; burada yalnız **mevcut Java kodundaki** değişiklik noktaları normatiftir.
