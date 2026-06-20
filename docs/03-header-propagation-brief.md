# Brief: Header propagation in cadenzaflow-nats-channel delegates

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Module:** `cadenzaflow-nats-channel`
**Spec kaynağı:** `~/Workspaces/cadenzaflow/docs/connector-architecture.md` v1.5, §4.1 ve §9 #6
**Brief oluşturuldu:** 2026-05-14

---

## Context

CadenzaFlow connector spec §4.1 üç **mandatory header** belirler ve §9 #6 worker'ların unknown `X-Cadenzaflow-*` header'larını transparent propagate etmesini şart koşar:

- `X-Cadenzaflow-Trace-Id` (her mesajda)
- `X-Cadenzaflow-Business-Key` (her mesajda)
- `X-Cadenzaflow-Idempotency-Key` (her mesajda)

Mevcut üç delegate hiçbiri header eklemiyor — spec ihlali. Bu brief implementation gap'i kapatıyor.

## Scope

İlk turda **yalnızca** `cadenzaflow-nats-channel` modülü kapsanır. Aynı pattern `camunda-nats-channel` ve `flowable-nats-channel`'a sonraki turda uygulanacak (ayrı brief).

## Files

### Yeni dosyalar

- **`nats-core/src/main/java/com/threeai/nats/core/headers/CadenzaflowHeaders.java`** — helper, `DelegateExecution`'dan mandatory header'ları üretir, `io.nats.client.impl.Headers` döndürür
- **`nats-core/src/test/java/com/threeai/nats/core/headers/CadenzaflowHeadersTest.java`** — helper'ın tüm üretim path'lerini test eder

### Değişecek dosyalar

- `cadenzaflow-nats-channel/src/main/java/com/threeai/nats/cadenzaflow/outbound/NatsRequestReplyDelegate.java`
- `cadenzaflow-nats-channel/src/main/java/com/threeai/nats/cadenzaflow/outbound/NatsPublishDelegate.java`
- `cadenzaflow-nats-channel/src/main/java/com/threeai/nats/cadenzaflow/outbound/JetStreamPublishDelegate.java`
- Üçünün test sınıfları (`*DelegateTest.java`)

## Header üretim mantığı (helper içinde)

| Header | Source |
|---|---|
| `X-Cadenzaflow-Trace-Id` | Önce `execution.getVariable("traceId")` (String); null/blank ise `UUID.randomUUID().toString()` üretilir |
| `X-Cadenzaflow-Business-Key` | `execution.getProcessBusinessKey()`; null ise header **eklenmez** (pragmatic — spec "mandatory" diyor ama business key olmayabilir; null durumda boş string yerine atlama) |
| `X-Cadenzaflow-Idempotency-Key` | BPMN field expression `idempotencyKey` varsa o kullanılır; yoksa deterministic default `<processInstanceId>:<activityInstanceId>` (retry'da aynı kalır, cloud queue native dedup için fit) |

Helper imza önerisi:

```java
public static Headers from(DelegateExecution execution, String explicitIdempotencyKey) { ... }
```

`explicitIdempotencyKey` null ise default deterministic üretim devreye girer.

## Changes per delegate

### `NatsRequestReplyDelegate.java`

Mevcut satır 56:

```java
Message reply = connection.request(subjectVal, data, timeoutVal);
```

Yeni:

```java
NatsMessage msg = NatsMessage.builder()
        .subject(subjectVal)
        .data(data)
        .headers(CadenzaflowHeaders.from(execution, idempotencyKeyVal))
        .build();
Message reply = connection.request(msg, timeoutVal);
```

Yeni field: `private Expression idempotencyKey;` + setter. Opsiyonel — null ise helper default üretir.

### `NatsPublishDelegate.java`, `JetStreamPublishDelegate.java`

Builder zincirine `.headers(CadenzaflowHeaders.from(execution, idempotencyKeyVal))` eklenir. Aynı `Expression idempotencyKey` field eklemesi.

## Test gereksinimleri

Her delegate test sınıfında yeni assertion'lar:

1. **Header presence** — yayınlanan `NatsMessage`'da üç header'ın varlığı (Business-Key opsiyonel)
2. **Trace-Id fallback** — variable yokken UUID üretildiği
3. **Trace-Id variable use** — variable varsa onun kullanıldığı
4. **Business-Key null behavior** — process business key null ise header'ın eklenmediği
5. **Idempotency-Key default** — BPMN field yoksa `processInstanceId:activityInstanceId` formatında
6. **Idempotency-Key override** — BPMN field varsa onun kullanıldığı

Yeni `CadenzaflowHeadersTest` mock `DelegateExecution` ile yukarıdaki tüm yolları kapsar.

Mevcut test'lerin geri kalan davranışı (subject, payload, exception) **aynı kalır** — sadece header assertion eklemesi.

## Build + verify

Repo Java 21 hedefli (`pom.xml` `<java.version>21</java.version>`).

```bash
cd ~/Workspaces/3eai-labs/nats-bpm-channels
mvn test -pl cadenzaflow-nats-channel,nats-core -am
```

Sistemde Java 21+ varsa native, system default Java 25 ise compiler `source/target=21` ile uyumlu çalışır.

Testcontainers entegrasyon testleri varsa (`NatsRequestReplyDelegateTest` içinde olabilir) header'ı gerçek NATS broker üzerinden round-trip ile de doğrula.

## Acceptance criteria

1. Üç delegate de NATS mesajını mandatory header'larla yayınlıyor
2. `mvn test -pl cadenzaflow-nats-channel,nats-core -am` temiz geçiyor
3. Test coverage tüm header üretim path'lerini kapsıyor
4. Header isimleri spec §4.1 ile birebir (`X-Cadenzaflow-Trace-Id` vb. — büyük/küçük harf doğru)

## Commit / push disiplini

Kullanıcı review etmeden push yok. Commit hazırlandıktan sonra kullanıcı doğrular.

## Out of scope

- `camunda-nats-channel` ve `flowable-nats-channel` — ayrı brief
- Reply path incoming header'larının MDC'ye taşınması (logging için)
- OTel / Micrometer Tracing context entegrasyonu (Trace-Id source genişlemesi) — gelecek iş
- Engine-side idempotency-key persistence (Gemini madde #5; engine tarafı, adapter tarafı değil) — gelecek iş
