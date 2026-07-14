# 03.1 — `nats-core`: Engine-nötr Ortak Sınıflar

**Modül:** `nats-core` (`com.threeai.nats.core.*`). Bu paket hiçbir engine-özgü modüle bağımlı değildir (§02 §3).
**Kaynak ADR:** 0001 (umbrella-lock), 0002 (leader lease), 0003 (telafi unlock — tüketici tarafı `03_classes/2_camunda_a2.md`'de), 0004 (CB), 0006 (contract-first), 0007 (yerleşim).

---

## 1. Header sabitleri — `headers` paketi

### 1.1 `BpmHeaders` (mevcut sınıf, genişletilir)

Mevcut: `nats-core/src/main/java/com/threeai/nats/core/headers/BpmHeaders.java:12-14` — üç sabit (`TRACE_ID`, `BUSINESS_KEY`, `IDEMPOTENCY_KEY`) + `build(...)` fabrika metodu.

**Yeni sabitler (FR-C7, IR-2 — asyncapi `ReplyHeaders` şemasıyla hizalı, phase5'te eklenir):**

```java
public static final String CORRELATION_ID = "X-Cadenzaflow-Correlation-Id";
public static final String REPLY_SUBJECT  = "X-Cadenzaflow-Reply-Subject";
/** Legacy write-side header name. READ-ONLY fallback (BR-SUB-006) — hiçbir yerde YAZILMAZ. */
public static final String LEGACY_TRACE_ID = "X-Trace-Id";
```

**Trace-header okuma yardımcı metodu (yeni, BR-SUB-006/FR-C7):**

```java
/** Reads TRACE_ID; falls back to LEGACY_TRACE_ID only if TRACE_ID absent. Never writes LEGACY_TRACE_ID. */
public static String extractTraceIdWithFallback(Message msg) {
    String traceId = NatsHeaderUtils.extractHeader(msg, TRACE_ID);
    return traceId != null ? traceId : NatsHeaderUtils.extractHeader(msg, LEGACY_TRACE_ID);
}
```

Bu metod, `04_interfaces/1_contract_fixes.md` Fix#4'ün somut çağrı noktasıdır — üç adapter/subscriber'ın `NatsHeaderUtils.extractHeader(msg, "X-Trace-Id")` satırlarının (flowable `JetStreamInboundEventChannelAdapter.java:119`, camunda `JetStreamMessageCorrelationSubscriber.java:102`, cadenzaflow aynı satır) **tek yerde** yerini alır.

### 1.2 `DlqHeaders` (yeni sınıf)

```java
package com.threeai.nats.core.headers;

public final class DlqHeaders {
    public static final String ORIGINAL_SUBJECT = "X-Cadenzaflow-Dlq-Original-Subject";
    public static final String DELIVERY_COUNT   = "X-Cadenzaflow-Dlq-Delivery-Count";
    public static final String REASON           = "X-Cadenzaflow-Dlq-Reason";
    public static final String TIMESTAMP        = "X-Cadenzaflow-Dlq-Timestamp";
    private DlqHeaders() {}
}
```

**Bağımlılık:** BR-SUB-001, FR-C1, US-C1; DP-6 (`REASON` yalnız hata sınıfı/kodu taşır — payload/PII yazılmaz, bkz. §2.3 aşağıda).

---

## 2. `dlq` paketi — ortak `publishToDlq` (5 kontrat-fix'in çekirdeği)

**Sorumluluk:** ADR-0007 §1'in öngördüğü tek ortak yardımcı. Üç mevcut tekrarı (flowable `JetStreamInboundEventChannelAdapter.publishToDlq` `:210-237`, camunda `JetStreamMessageCorrelationSubscriber.publishToDlq` `:204-231`, cadenzaflow aynı `:202-229`) bu sınıfa **devredilir** — üçü de artık bu sınıfı çağırır, kendi `publishToDlq` private metodlarını **siler**.

### 2.1 `DlqReason` (enum)

```java
package com.threeai.nats.core.dlq;

public enum DlqReason {
    /** Matrix 1.B satır 3 (engine-inbound consumer, deliveryCount>maxDeliver). BR-A2-009/BR-FLW-003. */
    DELIVERY_BUDGET_EXCEEDED("BUS_REPLY_DELIVERY_BUDGET_EXCEEDED"),
    /** BAQ-5 kararı — contract-fix #5. BR-SUB-007. */
    EMPTY_MESSAGE_BODY("VAL_EMPTY_MESSAGE_BODY");

    private final String exceptionCode;
    DlqReason(String exceptionCode) { this.exceptionCode = exceptionCode; }
    /** DP-6: yalnız bu kod string'i header'a yazılır — payload/business-key değeri ASLA. */
    public String headerValue() { return exceptionCode; }
}
```

> **Not (kapsam sınırı):** Worker-tarafı (`jobs.<topic>` job-consumer) delivery-budget-exceeded yönlendirmesi (Matrix 1.A satır 5, `BUS_JOB_DELIVERY_BUDGET_EXCEEDED`) bu reponun DIŞINDadır (worker implementasyonu repo dışı — DECISION_MATRIX.md 1.A başlık notu). `DlqReason` burada yalnız bu reponun **kendi ürettiği** iki DLQ yolunu (engine-inbound consumer + boş-body) kapsar.

### 2.2 `DlqPublishOutcome` (enum — custody-transfer kararını taşır)

```java
package com.threeai.nats.core.dlq;

public enum DlqPublishOutcome {
    /** BR-SUB-002 satır 3: caller ACK'ler. */
    PUBLISHED_JETSTREAM,
    /** BR-SUB-002 satır 5: caller ACK'ler (mevcut kod davranışı korunur — HLD §11 doğrulama #4a:
        core-NATS publish PubAck vermez ama exception fırlatmazsa "başarılı" sayılır, dürüst sınır). */
    PUBLISHED_CORE_FALLBACK,
    /** BR-SUB-002 satır 4 (DÜZELTME): dlqSubject configürasyonu eksik → caller NAK'lar, discard YOK. */
    FAILED_NO_DLQ_SUBJECT,
    /** BR-SUB-002 satır 5 (DÜZELTME): JetStream VE core-NATS ikisi de başarısız → caller nak+alert. */
    FAILED_BOTH_PUBLISH
}
```

### 2.3 `DlqPublisher`

```java
package com.threeai.nats.core.dlq;

public class DlqPublisher {

    public DlqPublisher(JetStream jetStream, Connection coreConnection, NatsChannelMetrics metrics) { ... }

    /**
     * BR-SUB-001 (header preservation) + BR-SUB-003 (dedup id) + BR-SUB-002 (custody-transfer
     * kararı döner, ack/nak YAPMAZ — o kararı caller kendi consumer/subscriber context'inde uygular).
     *
     * @param originalMsg      DLQ'ya yönlenen orijinal mesaj (job/reply/event)
     * @param dlqSubject       hedef DLQ subject'i (null olabilir — config eksikliği, satır 4)
     * @param reason           DLQ nedeni (yalnız sınıf/kod — DP-6)
     * @param subjectTag       metrik tag'i (orijinal subject)
     * @param channelTag       metrik tag'i (channel/messageName)
     */
    public DlqPublishOutcome publish(Message originalMsg, String dlqSubject,
            DlqReason reason, String subjectTag, String channelTag) {

        if (dlqSubject == null) {
            log.warn("DLQ subject not configured — message will be NAKed, not discarded",
                    kv("subject", subjectTag));
            if (metrics != null) metrics.dlqPublishFailureCount(subjectTag, channelTag).increment();
            return DlqPublishOutcome.FAILED_NO_DLQ_SUBJECT;
        }

        Headers dlqHeaders = copyOriginalHeadersVerbatim(originalMsg.getHeaders());   // BR-SUB-001
        appendMetaHeaders(dlqHeaders, originalMsg.getSubject(),
                deliveryCountOf(originalMsg), reason, Instant.now());                 // 4 meta header (§2.4)
        String originalMsgId = extractOriginalMsgId(originalMsg);                     // Nats-Msg-Id veya "unknown-<uuid>" + WARN
        dlqHeaders.put(NATS_MSG_ID_HEADER, List.of(originalMsgId + ".dlq"));          // BR-SUB-003

        NatsMessage dlqMsg = NatsMessage.builder()
                .subject(dlqSubject).data(originalMsg.getData()).headers(dlqHeaders).build();

        try {
            jetStream.publish(dlqMsg);
            if (metrics != null) metrics.dlqCount(subjectTag, channelTag).increment();
            return DlqPublishOutcome.PUBLISHED_JETSTREAM;
        } catch (Exception jsEx) {
            log.warn("JetStream DLQ publish failed, falling back to core NATS",
                    kv("subject", subjectTag), kv("dlq_subject", dlqSubject), jsEx);
            try {
                coreConnection.publish(dlqMsg.getSubject(), dlqMsg.getHeaders(), dlqMsg.getData());
                if (metrics != null) metrics.dlqCount(subjectTag, channelTag).increment();
                return DlqPublishOutcome.PUBLISHED_CORE_FALLBACK;
            } catch (Exception fallbackEx) {
                log.error("DLQ publish failed on both JetStream and core NATS",
                        kv("subject", subjectTag), kv("dlq_subject", dlqSubject), fallbackEx);
                if (metrics != null) metrics.dlqPublishFailureCount(subjectTag, channelTag).increment();
                return DlqPublishOutcome.FAILED_BOTH_PUBLISH;
            }
        }
    }
}
```

### 2.4 Meta header üretimi (BR-SUB-001)

```java
private void appendMetaHeaders(Headers h, String originalSubject, long deliveryCount,
        DlqReason reason, Instant now) {
    h.add(DlqHeaders.ORIGINAL_SUBJECT, originalSubject);
    h.add(DlqHeaders.DELIVERY_COUNT, String.valueOf(deliveryCount));
    h.add(DlqHeaders.REASON, reason.headerValue());              // DP-6: kod, PAYLOAD/PII DEĞİL
    h.add(DlqHeaders.TIMESTAMP, DateTimeFormatter.ISO_INSTANT.format(now));
}
```

### 2.5 Caller entegrasyonu — çağrı sitesi deseni (her üç mevcut sınıfta aynı iskelet)

```java
// (A2CompletionBridge / JetStreamInboundEventChannelAdapter / cadenzaflow ayna — hepsinde birebir)
DlqPublishOutcome outcome = dlqPublisher.publish(msg, dlqSubject, reason, subject, channelKey);
switch (outcome) {
    case PUBLISHED_JETSTREAM, PUBLISHED_CORE_FALLBACK -> msg.ack();     // custody-transfer sağlandı
    case FAILED_NO_DLQ_SUBJECT, FAILED_BOTH_PUBLISH -> {
        if (metrics != null) metrics.nakCount(subject, channelKey).increment();
        msg.nakWithDelay(backoffFor(deliveryCountOf(msg)));             // asla ack-drop (dlq-of-dlq YOK)
    }
}
```

**Boş-body akışı ile ilişki (BR-SUB-007 ↔ BR-SUB-002 sentezi — LLD-düzeyi netleştirme, yeni bir iş kuralı DEĞİL):** BR-SUB-007 metni "boş body → ... + ACK" der; bu, **DLQ publish başarılı olduğu tipik durumu** tarif eder. Custody-transfer ilkesi (BR-SUB-002, NFR-R2) boş-body dalında da geçerlidir — yukarıdaki `switch` bloğu **her iki BR'yi de** aynı mekanizmayla tatmin eder: boş-body → `dlqPublisher.publish(..., DlqReason.EMPTY_MESSAGE_BODY, ...)` → outcome'a göre ack/nak. Bu, iki BR'nin birbirini çelişkili kılmadığını, aynı alt-mekanizmayı paylaştığını gösterir (her iki BR de `06 §7` custody-transfer genel ilkesine dayanır).

**Bağımlılık:** BR-SUB-001/002/003/007, FR-C1/C2/C3, US-C1/C2/C3/C2(B2), ADR-0006/0007.

---

## 3. `jetstream` paketi — Leader Election (ADR-0002) + Telafi-Unlock Yüzeyi

### 3.1 `JetStreamKvManager` (yeni — `JetStreamStreamManager`'ın KV kardeşi)

Mevcut `JetStreamStreamManager.ensureStream(...)` deseni (`nats-core/.../jetstream/JetStreamStreamManager.java:20-47`, `getStreamInfo` → 404 yakala → `addStream`) **KV bucket'ı için aynalanır**:

```java
package com.threeai.nats.core.jetstream;

public class JetStreamKvManager {
    public void ensureBucket(String bucketName, Duration ttl, int replicas, Connection connection) {
        KeyValueManagement kvm = connection.keyValueManagement();
        try {
            kvm.getBucketInfo(bucketName);
        } catch (JetStreamApiException e) {
            if (e.getErrorCode() == 404) {
                KeyValueConfiguration config = KeyValueConfiguration.builder()
                        .name(bucketName).ttl(ttl).replicas(replicas).build();
                kvm.create(config);
            } else {
                throw new IllegalStateException("Failed to check KV bucket '" + bucketName + "'", e);
            }
        }
    }
}
```

**KV bucket:** `a2-sweep-leader` (kebab-case, ADR-0002). Şema: `08_config.md` §2 + `docs/sentinel/phase4/DB_ACCESS_MAP.md` §3.

### 3.2 `SweepLeaderLease`

```java
package com.threeai.nats.core.jetstream;

public class SweepLeaderLease {

    private static final String BUCKET = "a2-sweep-leader";
    private static final String KEY = "leader";

    public SweepLeaderLease(JetStream jetStream, JetStreamKvManager kvManager,
            Connection connection, String nodeId, Duration ttl /* = 2·S, ADR-0002 */) { ... }

    /** İdempotent — leader ise yeniler, değilse devralmayı dener. Her S saniyede bir çağrılır (bkz. A2OrphanSweep §3.2). */
    public boolean tryAcquireOrRenew() {
        KeyValue kv = jetStream.keyValue(BUCKET);
        try {
            long rev = kv.create(KEY, nodeId.getBytes(UTF_8));   // hiç yoksa BEN aldım
            this.heldRevision = rev;
            return true;
        } catch (JetStreamApiException createFailed) {
            // key zaten var — mevcut sahibi kontrol et
            KeyValueEntry entry = kv.get(KEY);
            if (entry != null && nodeId.equals(new String(entry.getValue(), UTF_8))) {
                try {
                    long rev = kv.update(KEY, nodeId.getBytes(UTF_8), entry.getRevision());  // BEN'im, yenile
                    this.heldRevision = rev;
                    return true;
                } catch (JetStreamApiException renewRace) {
                    return false;   // bir başka node arada devraldı — kaybettik
                }
            }
            return false;   // başka node lider
        }
    }

    public boolean isLeader() { return heldRevision != null; }
}
```

**Zamanlama (ADR-0002):** TTL = `2·S` (240s, S=120s default); `SweepLeaderLease.tryAcquireOrRenew()` A2OrphanSweep'in kendi zamanlayıcısından (`@Scheduled(fixedDelayString = "${a2.sweep.period:120}000")` benzeri) her **S**'de bir çağrılır — yenileme periyodu TTL'nin yarısı olduğundan tek bir kaçırılan yenileme küme genelinde sweep'i durdurmaz (bir sonraki S'de tekrar dener); iki ardışık kaçırılan yenileme (2S = TTL) sonrası lease düşer, başka node devralır.

**Bağımlılık:** BR-A2-005, FR-A5, US-A3, ADR-0002. `DB_ACCESS_MAP.md` §3 (KV şeması).

---

## 4. `resilience` paketi — DLQ-bridge Circuit Breaker (ADR-0004)

### 4.1 `DlqBridgeCircuitBreakerFactory`

```java
package com.threeai.nats.core.resilience;

public class DlqBridgeCircuitBreakerFactory {

    /** ADR-0004 eşikleri: 5 ardışık hata→OPEN, 30s→HALF_OPEN, 3 ardışık başarı→CLOSED.
        Resilience4j count-based sliding window (size=5, minimumNumberOfCalls=5,
        failureRateThreshold=100%) ile "5 ardışık hata" yaklaşık/pratik eşdeğeri sağlanır:
        pencerede 5 çağrının TAMAMI başarısız olmalı (tek başarı oranı düşürür → OPEN tetiklenmez). */
    public static CircuitBreaker create(String name, MeterRegistry registry) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(5)
                .minimumNumberOfCalls(5)
                .failureRateThreshold(100.0f)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(3)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
        CircuitBreakerRegistry cbRegistry = CircuitBreakerRegistry.of(config);
        CircuitBreaker cb = cbRegistry.circuitBreaker(name);
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(cbRegistry).bindTo(registry);  // CB metrikleri Resilience4j'den gelir — özel sayaç GEREKMEZ
        cb.getEventPublisher().onStateTransition(event ->
                log.warn("DLQ-bridge circuit breaker state transition",
                        kv("cb_name", name),
                        kv("from", event.getStateTransition().getFromState()),
                        kv("to", event.getStateTransition().getToState())));   // 10_metrics.md alarm kaynağı
        return cb;
    }
}
```

**İzolasyon (ADR-0004 "downstream başına"):** her DLQ-bridge kendi `CircuitBreaker` instance'ını ayrı isimle yaratır — `cb-incident-bridge-camunda`, `cb-incident-bridge-cadenzaflow`, `cb-failure-event-bridge-flowable` (bkz. `03_classes/2_camunda_a2.md` §5, `03_classes/4_flowable.md` §2). Bir idiomun downstream'i (Cockpit DB) kesintiye uğrarsa diğerinin (Event Registry) CB'sini etkilemez.

**Kullanım deseni (DLQ-bridge içinde):**

```java
try {
    circuitBreaker.executeCallable(() -> { doIncidentOrFailureEvent(msg); return null; });
    msg.ack();
} catch (CallNotPermittedException openEx) {
    // CB OPEN — fail-fast, downstream'e hiç gitmedi
    msg.nakWithDelay(backoff);
} catch (Exception processingEx) {
    msg.nakWithDelay(backoff);   // CB'nin kendi sayacı bu başarısızlığı da sayar
}
```

**Bağımlılık:** BR-SUB-008, FR-A10/FR-B3, US-A6/US-B3, ADR-0004.

---

## 5. `exception` paketi — hata hiyerarşisi

Sınıf iskeleti + her 23 kodun bağlanması: `docs/sentinel/phase4/ERROR_REGISTRY.md` §2 (tek doğruluk kaynağı — burada tekrarlanmaz, MASTER_WORKFLOW §0.6). Bu paketin var oluş nedeni: `07_errors.md` bu dosyaya köprü verir.

---

## 6. Config sınıfları

`UmbrellaLockProperties`, `UmbrellaLockCalculator`, `NamespaceValidator` — `08_config.md`'de (bu dosyada TEKRARLANMAZ, tek yerde yaşar).
