# Tasarım: Async Request-Reply (transaction-dışı worker delegasyonu)

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Kapsam (karar):** `cadenzaflow-nats-channel` + `camunda-nats-channel` birlikte (ikisi aynı `RuntimeService` API'sini paylaşır). Flowable ayrı brief.
**Tarih:** 2026-06-19
**Bağlam:** Projenin tezi — işi engine DB transaction'ının dışında yürütmek (bkz `01-vision-roadmap.md`). Mevcut senkron `NatsRequestReplyDelegate` bu tezi ihlal ediyor: `connection.request()` engine command/transaction'ı içinde bloke olur, dış bekleme boyunca DB connection tutar (kanıt: engine `AbstractTransactionInterceptor` doBegin→execute→doCommit). Bu tasarım, request-reply'ı bloke-etmeyen, transaction-tutmayan bir desene çevirir.

---

## 1. Hedef

Bir BPMN service task'ı external worker'a iş devredip cevabını alırken **hiçbir DB transaction veya connection'ı bekleme boyunca açık tutmamak**. Engine sadece iki kısa, ayrı transaction görür: (1) request gönderimi + wait-state'e geçiş, (2) reply geldiğinde token'ın ilerletilmesi. Worker ne kadar yavaş olursa olsun arada engine kaynağı tutulmaz.

**Senkron delegate kaldırılmaz** — ms-cevaplı, düşük-eşzamanlı RPC için meşru ve daha basit kalır. Async desen **opt-in** yeni bir yol olarak eklenir. İki desen yan yana yaşar; BPMN modelleyici hangisinin uygun olduğunu seçer.

---

## 2. Akış

```
  [Send Task]                         [Message Catch Event]            (devam)
  asyncRequestDelegate                 messageName=natsReply              │
       │  1. correlationId üret (default = idempotency-key)               │
       │  2. execution-local var: natsCorrelationId = <id>                │
       │  3. NATS publish: subject=task.x                                 │
       │     headers: Correlation-Id, Reply-Subject, Idempotency, Trace   │
       │  4. delegate DÖNER  ───────────►  süreç wait-state'e girer        │
       │                                   ⇣ TRANSACTION COMMIT           │
       ▼                                   ⇣ DB connection SERBEST         │
   (engine bekler — kaynak tutmaz)                                        │
                                                                          │
   Worker (herhangi bir dil):                                            │
     - request'i işler                                                   │
     - reply'ı Reply-Subject'e publish eder                              │
     - Correlation-Id header'ını AYNEN echo eder                         │
                                                                          │
   [Reply Subscriber] (JetStream durable, ayrı transaction):            │
     - reply'dan correlationId'yi okur                                   │
     - createMessageCorrelation("natsReply")                            │
         .localVariableEquals("natsCorrelationId", correlationId)        │
         .setVariable(resultVar, replyBody)                              │
         .correlateWithResult()                                          │
     - eşleşme bulunca → token ilerler ──────────────────────────────────┘
     - eşleşme YOKSA (reply commit'ten önce geldi) → NAK → redelivery
```

**Timeout artık delegate parametresi değil:** Catch event'e bir **boundary timer** eklenir. Reply zamanında gelmezse timer ateşler, hata yoluna gider — engine-native, hiçbir thread/connection tutmaz. Senkron delegate'in `timeout` alanının yerini bu alır.

---

## 3. Correlation mekanizması (kritik)

Reply, **doğru bekleyen execution'a** yönlenmeli — sadece messageName + businessKey yetmez (bir instance'ta paralel birden çok request olabilir; businessKey benzersiz değildir). Çözüm: **request başına benzersiz, wait boyunca stabil bir correlation id.**

- **Kaynak:** Eklediğimiz idempotency-key (`CadenzaflowHeaderBinder`, default `processInstanceId:activityInstanceId`, veya BPMN explicit). Bir activity instance için benzersiz ve stabil → doğal correlation id. Ayrı bir `X-Cadenzaflow-Correlation-Id` header'ı kullanılır ama default'u idempotency-key değeridir.
- **Eşleşme:** Send delegate correlation id'yi **execution-local variable** (`natsCorrelationId`) olarak yazar. Reply subscriber `localVariableEquals("natsCorrelationId", id)` ile **tam o execution'a** korele eder. `processInstanceBusinessKey` değil — execution-precise.
- **Worker sözleşmesi:** Worker `X-Cadenzaflow-Correlation-Id` header'ını cevabında **birebir echo** etmek zorunda. Bu, polyglot worker'lar için tek ek yük (mevcut request-reply'da da reply subject echo'su vardı; benzer disiplin).

---

## 4. Reply transport: JetStream durable default, Core NATS opt-in (karar)

**Varsayılan = JetStream durable consumer.** İki yarış/dayanıklılık durumunu çözer:

1. **Reply, send transaction'ı commit olmadan önce gelir** (hızlı worker). Subscriber korele etmeye çalışır, bekleyen execution henüz DB'de yoktur → eşleşme sıfır. JetStream'de: eşleşme sıfırsa **NAK** (delay ile), mesaj redeliver edilir; send commit olduktan sonraki teslimde korele olur. Mevcut `JetStreamMessageCorrelationSubscriber` ack/nack/DLQ + `nakWithDelay` altyapısına zaten sahip — genişletilir.
2. **Subscriber kısa süre düşük** → JetStream reply'ı saklar, geri gelince teslim eder.

`maxDeliver` aşılırsa reply DLQ'ya gider (boundary timer zaten süreci kurtarır). Tezle uyumlu: dayanıklılık **NATS'ta**, engine DB'sinde değil.

**Core NATS opt-in:** `replies[].jetstream=false` ile Core NATS reply yolu da sunulur — **best-effort / fast senaryolar** için. Kısıt config doküman ve loglarında AÇIKÇA belirtilir: erken-reply yarışında reply **kaybolur** (redelivery yok), subscriber düşükken gelen reply kaybolur. Bu durumda kurtarma yalnızca boundary timer'dır (süreç timeout dalına gider, sessiz kayıp değil — ama reply israf olur). Yüksek-değerli/uzun-worker senaryolarında kullanılmamalı. Mevcut `NatsMessageCorrelationSubscriber` (Core) bu yol için temel.

---

## 5. Idempotency / at-least-once

JetStream at-least-once → aynı reply iki kez teslim edilebilir. Davranış: ilk korelasyon token'ı ilerletir; ikinci teslimde `localVariableEquals` ile eşleşen bekleyen execution **kalmamıştır** (zaten ilerledi) → korelasyon sıfır eşleşme. Bunu **transient değil, terminal** ayırt etmek gerekir:
- "Henüz bekleyen yok" (reply erken geldi) → **NAK**, redelivery.
- "Artık bekleyen yok" (zaten korele oldu / instance bitti) → **ACK**, no-op.

Bu ikisini ayırmak için: korelasyon sıfır eşleşince, kısa bir grace/retry penceresi (maxDeliver + backoff) sonunda hâlâ eşleşmezse ACK + DLQ/log. Pratik kural: NAK ile birkaç kez dene; tükenince ACK + warn (duplicate veya orphan reply). Açık karar noktası — bkz §9.

---

## 6. Yeni / değişen bileşenler

| Bileşen | Tür | İş |
|---|---|---|
| `AsyncRequestReplyDelegate` | YENİ (outbound) | correlationId üret + local var yaz + headers ile publish (Core veya JetStream) + DÖN (bloke olmaz). `subject`, `replySubject`, `correlationVariable` (default `natsCorrelationId`), `payloadVariable`, opsiyonel `idempotencyKey` field'ları |
| `ReplyCorrelationSubscriber` | YENİ (inbound) | reply subject'i (JetStream durable) dinler; correlationId header → `localVariableEquals` korelasyon; resultVar set; erken reply → NAK. Mevcut `JetStreamMessageCorrelationSubscriber`'dan türetilebilir/ortak taban |
| `ReplyCorrelationConfig` | YENİ (config) | `spring.nats.cadenzaflow.replies[]`: replySubject, messageName, correlationHeader, resultVariable, durableName, maxDeliver, dlqSubject, stream |
| `NatsSubscriptionRegistrar` | DEĞİŞİR | reply subscriber'larını da kaydet (mevcut subscription döngüsüne paralel) |
| `BpmHeaders` / `CadenzaflowHeaderBinder` | DEĞİŞİR | `X-Cadenzaflow-Correlation-Id` + `X-Cadenzaflow-Reply-Subject` header'larını ekle (correlation id default = idempotency-key değeri) |
| Senkron `NatsRequestReplyDelegate` | KALIR | fast-RPC için; README'de "yalnızca ms-cevaplı/düşük-eşzamanlı" diye kısıt belgelenir |

---

## 7. BPMN modeli

```xml
<!-- 1. Gönderim: bloke etmeyen, transaction tutmayan -->
<serviceTask id="sendSms" cadenzaflow:delegateExpression="${asyncRequestReply}">
  <extensionElements>
    <cadenzaflow:field name="subject" stringValue="task.send-sms" />
    <cadenzaflow:field name="replySubject" stringValue="cadenzaflow.replies.sms" />
    <cadenzaflow:field name="payloadVariable" stringValue="smsPayload" />
    <cadenzaflow:field name="resultVariable" stringValue="smsResult" />
  </extensionElements>
</serviceTask>

<!-- 2. Bekleme: engine-native wait-state, kaynak tutmaz -->
<intermediateCatchEvent id="awaitSms">
  <messageEventDefinition messageRef="natsReply" />
</intermediateCatchEvent>

<!-- 3. Timeout: boundary timer, thread/connection yok -->
<boundaryEvent id="smsTimeout" attachedToRef="awaitSms">
  <timerEventDefinition><timeDuration>PT30S</timeDuration></timerEventDefinition>
</boundaryEvent>
```

Modelleyici tek bir sub-process şablonuyla bunu paketleyebilir (call activity / template). İleride bir "async-request-reply" BPMN şablonu yayınlanabilir (modeler için).

---

## 8. Engine portability

- **CadenzaFlow + Camunda 7:** Aynı `RuntimeService.createMessageCorrelation().localVariableEquals()` API'si — ortak desen, modül başına ince binder.
- **Flowable:** Message correlation modeli farklı (Event Registry channel + `RuntimeService` API'si ayrışır). Flowable için ayrı eşleme gerekir (muhtemelen receive-task + `messageEventReceived` veya event registry inbound channel). Ayrı brief.
- Header üretimi `BpmHeaders` (core, engine-neutral) + engine binder ayrımını korur — mevcut header işiyle tutarlı.

---

## 9. Kararlar

1. **Reply transport:** ✅ JetStream durable **default** + Core NATS **opt-in** (`replies[].jetstream=false`, kısıtlar §4'te belgeli). KARAR.
2. **Correlation id kaynağı:** ✅ Idempotency-key yeniden kullanılır (`X-Cadenzaflow-Correlation-Id` default = idempotency-key değeri; dedup ve reply-routing aynı değer). KARAR.
3. **Kapsam:** ✅ `cadenzaflow-nats-channel` + `camunda-nats-channel` birlikte. Flowable ayrı brief. KARAR.
4. **Orphan/duplicate reply politikası (varsayılan kabul):** JetStream'de eşleşme sıfırsa `nakWithDelay` (örn. 1s exponential) ile `maxDeliver` (default 5) kez dene; tükenince **DLQ + warn** (`dlqSubject`, default `dlq.{replySubject}`). Bu, "erken reply" (redelivery'de eşleşir) ile "orphan/duplicate" (hiç eşleşmez → DLQ) durumlarını tek mekanizmayla ayırır — ayrı grace penceresi gerekmez. Süreç tarafı boundary timer ile zaten korunur.

---

## 10. Out of scope (bu tur)

- Flowable async eşlemesi (ayrı brief)
- Senkron delegate'in kaldırılması (kalıyor, kısıt belgeleniyor)
- BPMN modeler template yayını
- OTel/tracing context'inin reply path'ine taşınması
