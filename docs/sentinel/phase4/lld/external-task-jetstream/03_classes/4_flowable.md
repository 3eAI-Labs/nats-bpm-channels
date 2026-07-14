# 03.4 — `flowable-nats-channel`: Event Registry Olgunluğu + FailureEventBridge

**Modül:** `flowable-nats-channel`, paket `org.flowable.eventregistry.spring.nats.*`.
**Kaynak:** HLD §2.6 (FailureEventBridge), §2.8 (outbound/inbound olgunluk + delegate phase-out), ADR-0004 (CB), ADR-0007 §3.

---

## 1. Mevcut outbound/inbound — değişmeyen + değişen

### 1.1 Değişmeyen (native push zaten doğru idiom, HLD §2.8)

- `NatsOutboundEventChannelAdapter.sendEvent(...)` (core, `:29`) ve JetStream varyantı `JetStreamOutboundEventChannelAdapter.sendEvent(...)` (`:36`) **değişmez** — zaten motor-dışı, zaten `sendEvent` idiomunda (FR-B1).
- `NatsChannelDefinitionProcessor` kayıt akışı (`registerJetStreamInbound`/`registerJetStreamOutbound`, `:103-147`) **değişmez** — yalnız §2'deki `findBySubject` eklentisi + `validateSubject` genişlemesi (§4).

### 1.2 Değişen: `JetStreamInboundEventChannelAdapter` (FR-B2, contract-fix'ler)

`handleMessage(Message msg)` (`:118-176`) **`DlqPublisher`'a devreder** — kendi `publishToDlq` private metodunu (`:210-237`) **siler**. Tam değişiklik noktaları: `04_interfaces/1_contract_fixes.md`.

Trace-header okuma (`:119`, `NatsHeaderUtils.extractHeader(msg, "X-Trace-Id")`) → `BpmHeaders.extractTraceIdWithFallback(msg)` (`nats-core §1.1`) ile **değişir** (Fix#4).

---

## 2. `FailureEventBridge` (HLD §2.6 · BR-FLW-003/005 · FR-B3/B5 · US-B3/B5 · ADR-0004)

**Yeni sınıf**, paket `org.flowable.eventregistry.spring.nats.escalation` (isimlendirme notu: `02_package_structure.md` §2 — "A2FailureEventBridge" DEĞİL, motor idiomu Flowable'da A2 yok).

```java
package org.flowable.eventregistry.spring.nats.escalation;

public class FailureEventBridge {

    private final EventRegistry eventRegistry;
    private final ChannelModelLookup channelModelLookup;   // §2.2 — subject -> InboundChannelModel
    private final CircuitBreaker circuitBreaker;            // "cb-failure-event-bridge-flowable" (ADR-0004)
    private final NatsChannelMetrics metrics;

    /** dlq.> tüketen consumer'ın işleyicisi — yalnız dlq.jobs.> DIŞINDAKİ subject'ler bu bridge'e yönlenir
        (routing: NatsChannelDefinitionProcessor'ın consumer filter'ı, bkz. 08_config.md §3). */
    void handleDlqMessage(Message dlqMsg) {
        String originalSubject = NatsHeaderUtils.extractHeader(dlqMsg, DlqHeaders.ORIGINAL_SUBJECT);   // Fix#1 sayesinde mevcut
        Optional<InboundChannelModel> model = channelModelLookup.findBySubject(originalSubject);
        if (model.isEmpty()) {
            log.error("FailureEventBridge: no inbound channel model registered for original subject — " +
                    "cannot reconstruct failure-event, routing back to DLQ is not possible (dlq-of-dlq YOK)",
                    kv("original_subject", originalSubject));
            dlqMsg.nakWithDelay(backoffFor(deliveryCountOf(dlqMsg)));   // SYS_DLQ_BRIDGE_PROCESSING_FAILED
            return;
        }
        try {
            circuitBreaker.executeCallable(() -> {
                // BR-FLW-003: aynı correlation key'lerle (BpmHeaders zaten DLQ'da AYNEN, Fix#1) failure-event.
                NatsInboundEvent failureEvent = new NatsInboundEvent(dlqMsg);   // orijinal payload+header'lar byte-aynen
                eventRegistry.eventReceived(model.get(), failureEvent);
                return null;
            });
            metrics.ackCount(dlqMsg.getSubject(), model.get().getKey()).increment();
            dlqMsg.ack();   // correlate-sonrası-ack

        } catch (CallNotPermittedException cbOpen) {
            dlqMsg.nakWithDelay(backoffFor(deliveryCountOf(dlqMsg)));   // CB OPEN — fail-fast

        } catch (NoMatchingSubscriptionException noMatch) {
            // Bekleyen subscription yok — BAQ-8: WARN + metrik (ERROR DEĞİL), tek olay benign yarış olabilir.
            log.warn("FailureEventBridge: no waiting subscription for failure-event — " +
                    "instance likely already resolved via another path",
                    kv("original_subject", originalSubject));
            metrics.failureEventCorrelationMissCount(model.get().getKey()).increment();   // RES_FAILURE_EVENT_CORRELATION_MISS
            dlqMsg.ack();   // mesajın kendisi işlendi — risk iş-sonucu kaybı, mesaj kaybı DEĞİL

        } catch (Exception downstreamFailure) {
            log.error("FailureEventBridge processing failed", downstreamFailure);   // SYS_DLQ_BRIDGE_PROCESSING_FAILED
            dlqMsg.nakWithDelay(backoffFor(deliveryCountOf(dlqMsg)));
        }
    }
}
```

**`NoMatchingSubscriptionException` notu (⏭ phase4/5 doğrulaması, HLD §11 kalem 6c):** Flowable `EventRegistry.eventReceived(...)`'ın eşleşmeyen event'te **gerçekten** bir exception fırlatıp fırlatmadığı (yoksa sessizce no-op mu döndüğü) resmi dokümanda **net değildir** — bu LLD, davranışı **her iki ihtimali de** ele alacak şekilde tasarlanmıştır: eğer `eventReceived` sessizce döner (exception yok), `FailureEventBridge` bunu **başarı** olarak yorumlar ve **yanlışlıkla ack+"başarılı" loglar** — bu durumda `RES_FAILURE_EVENT_CORRELATION_MISS` **hiç tetiklenmez** ve BAQ-8'in öngördüğü WARN+metrik **kaçırılır**. Bu, `TEST_SPECIFICATIONS.md` (d)'nin gerekçesidir: davranış test'le doğrulanana kadar, kod **her iki dalı da** ele alacak şekilde yazılmalı (örn. Flowable'ın gerçek davranışı `boolean`/sonuç nesnesi dönüyorsa ona göre dallan; exception atıyorsa yukarıdaki `catch` bloğu kalır). **Bu belirsizlik LLD-QUESTIONS'a taşınmıştır** (dosya sonu) — kodlanacak kesin dal Phase 5'te test sonucuna göre sabitlenir.

### 2.1 Geç-sonuç politikası (BR-FLW-005, US-B5)

`FailureEventBridge` kendisi geç-sonuç politikasını **uygulamaz** — bu, Event Registry'nin **kendi** interrupting/non-interrupting semantiğinin doğal sonucudur: escalation zaten fırlamışsa (interrupting), orijinal event-subscription artık yok → worker'ın gecikmiş orijinal-yol sonucu (DLQ'dan BAĞIMSIZ, ayrı bir inbound event) `eventReceived`'a geldiğinde subscription bulunamaz → **aynı** `RES_FAILURE_EVENT_CORRELATION_MISS`/no-match yolu (bu sınıfın DEĞİL, `JetStreamInboundEventChannelAdapter`'ın normal-yol işleyicisinin) devreye girer. Non-interrupting ise subscription hâlâ var → normal işlenir. **Bu LLD'nin FailureEventBridge'e eklediği bir davranış YOK** — mevcut `eventReceived` semantiğinin (BR-FLW-005) doğal sonucu, ayrıca kodlanmaz.

---

## 2.2 `ChannelModelLookup` (yeni yardımcı — subject→model)

`NatsChannelDefinitionProcessor` mevcut hali channel-key→adapter map'i tutuyor (`:30-31`, `coreInboundAdapters`/`jetStreamInboundAdapters`), subject→model **yok**. Yeni, küçük bir eşleme eklenir:

```java
// NatsChannelDefinitionProcessor içine eklenir (registerJetStreamInbound'un sonuna):
private final Map<String, InboundChannelModel> subjectToModel = new ConcurrentHashMap<>();
// registerJetStreamInbound(...) içinde: subjectToModel.put(model.getSubject(), model);
// unregisterChannelModel(...) içinde: subjectToModel.values().removeIf(m -> m == channelModel);

public Optional<InboundChannelModel> findBySubject(String subject) {
    return Optional.ofNullable(subjectToModel.get(subject));
}
```

`FailureEventBridge` bu metodu doğrudan `NatsChannelDefinitionProcessor` üzerinden çağırır (aynı modül, ek arayüz sınıfı gerekmez — `ChannelModelLookup` ismi bu bölümde kavramsal, gerçek tip `NatsChannelDefinitionProcessor`'dır).

**Bağımlılık:** BR-FLW-003/005, FR-B3/B5, US-B3/B5, ADR-0004.

---

## 3. Boundary-timer (opt-in, model kararı — BR-FLW-004, FR-B4, US-B4)

**Kod YOK.** Bu, bir BPMN **modelleme** kararıdır — süreç geliştirici (P1), gerçek wall-clock SLA'sı olan aktivitelere native BPMN boundary-timer event ekler (Flowable'ın kendi timer-job mekanizması, `ACT_RU_TIMER_JOB`, hiçbir bu-repo kodu gerektirmez). Bu LLD'nin katkısı: (a) dokümantasyon/runbook notu (`99_deployment.md` §3 — hangi modellerde opt-in önerilir), (b) maliyet ölçümü test speki (`TEST_SPECIFICATIONS.md` (c)).

---

## 4. `jobs.*` namespace rezervasyon validasyonu (BAQ-4 · BR-SUB-004 · `VAL_TOPIC_NAMESPACE_COLLISION`)

Mevcut `NatsChannelDefinitionProcessor.validateSubject(String subject, String channelKey)` (`:149-154`, şu an yalnız boş/null kontrolü) **genişletilir**:

```java
private void validateSubject(String subject, String channelKey) {
    if (subject == null || subject.isBlank()) {
        throw new FlowableException("NATS channel '" + channelKey + "': subject is required");
    }
    NamespaceValidator.assertNotReservedForA2(subject, channelKey);   // nats-core, 08_config.md §2 — YENİ satır
}
```

`NamespaceValidator.assertNotReservedForA2(...)` (`nats-core`, `08_config.md` §2) subject `jobs.` ile başlıyorsa `VAL_TOPIC_NAMESPACE_COLLISION` (ERROR) fırlatır — Spring context bootstrap'ı **başarısız olur**, deployment engellenir (BAQ-4 kararı, aktif mekanik kapı).

**Bağımlılık:** BR-SUB-004, FR-C4, US-C4.

---

## 5. Delegate phase-out (Flowable tarafı — US-E1/BR-MIG-001)

`requestreply/NatsRequestReplyDelegate.java:19` (`implements JavaDelegate`, in-tx blocking `:56`-analog) **silinir**; `FlowableNatsAutoConfiguration.natsRequestReply(...)` bean tanımı (`:64-70`) **silinir**; `NatsRequestReplyDelegateTest.java` ve `NatsRequestReplyIntegrationTest.java` (`flowable-nats-channel/src/test/.../requestreply/`) **silinir** (migrasyon yolu: `02_package_structure.md` §4).

**Bağımlılık:** BR-MIG-001, FR-E1, US-E1.

---

## LLD-QUESTIONS (bu dosyaya özgü)

- **`eventReceived` no-match davranışı belirsizliği (§2, HLD §11 kalem 6c):** Flowable Event Registry'nin `eventReceived(...)` çağrısının eşleşmeyen (geç/korelasyon-key-kayıp) bir event'te exception mi fırlattığı yoksa sessizce no-op mu döndüğü resmi dokümanda doğrulanamadı. `FailureEventBridge`'in `catch (NoMatchingSubscriptionException)` dalı **varsayımsaldır** — gerçek davranış Phase 4/5 entegrasyon testiyle (`TEST_SPECIFICATIONS.md` (d)) doğrulanana kadar kodun her iki ihtimali de ele alacak şekilde (sonuç-nesnesi kontrolü + exception yakalama) yazılması **gerekebilir**. Bu, mevcut `[phase3'te doğrulanacak]` kalemin somut kod-etkisidir — Levent onayı gerekmez (zaten devredilmiş test görevi), ama phase5 coder'ın bu belirsizliği görmezden gelmemesi için burada işaretlendi.
