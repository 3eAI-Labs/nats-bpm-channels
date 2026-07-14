# 08 — Konfigürasyon Sınıfları

**Kaynak:** ADR-0001 (W/M/S/ε/L), ADR-0002 (leader lease TTL), ADR-0004 (CB eşikleri), BAQ-3/BAQ-4 (validasyon davranışı).

---

## 1. Umbrella-lock parametreleri (ADR-0001 · BR-A2-006/007 · FR-A8/A9 · US-A5)

### 1.1 `UmbrellaLockProperties` (`spring.nats.<engine>.a2.*`)

```java
@ConfigurationProperties(prefix = "spring.nats.camunda.a2")   // cadenzaflow ayna: "spring.nats.cadenzaflow.a2"
public class A2Properties {
    private String sentinelWorkerId = "a2-jetstream-bridge";      // BR-A2-003 — küme-geneli TEK sabit
    private List<String> topics = new ArrayList<>();              // yalnız LİTERAL camunda:topic değerleri (03_classes/2 §1.3)
    private UmbrellaLockDefaults defaults = new UmbrellaLockDefaults();
    private Map<String, TopicLockOverride> topicOverrides = new HashMap<>();
    private boolean allowUnsafeLockDuration = false;               // BAQ-3 escape-flag
}

public class UmbrellaLockDefaults {
    private long ackWaitSeconds = 30;      // W
    private int maxDeliver = 4;            // M — L-türetme formülünün girdisi. Bootstrap wiring, HER topic için
                                            // bu değeri (veya TopicLockOverride.maxDeliver'ı) A2ConsumerConfig.maxDeliver'a
                                            // AYNEN kopyalar (§1.5, `03_classes/2_camunda_a2.md` §4.0) — iki config
                                            // nesnesi aynı M'i farklı tüketim noktaları (L-formülü / JetStream consumer)
                                            // için taşır, ASLA bağımsız olarak override edilmez.
    private long sweepPeriodSeconds = 120;  // S
    private long epsilonSeconds = 60;       // ε
    private Long lockDurationSeconds;       // L — null ise türetilir (§1.2)
}

public class TopicLockOverride {
    private Long ackWaitSeconds;    // topic-başına W override (null = default'ı kullan)
    private Integer maxDeliver;
    private Long lockDurationSeconds;   // manuel L override (null = türetilir)
}
```

### 1.2 `UmbrellaLockCalculator` (stateless, saf fonksiyon)

```java
public final class UmbrellaLockCalculator {

    private static final long MARGIN_SECONDS = 13;   // 320-307=13s marj, ADR-0001 default örneği ile tutarlı

    /** Σbackoff = Σ 2^(n-1), n=1..maxDeliver-1, cap 30s per term (calculateBackoff ile aynı formül). */
    public static long backoffSumSeconds(int maxDeliver) {
        long sum = 0;
        for (int n = 1; n < maxDeliver; n++) {
            sum += Math.min(30, (long) Math.pow(2, n - 1));
        }
        return sum;
    }

    /** Şemsiye alt sınırı: L >= M*W + Σbackoff + S + ε (ADR-0001). */
    public static long floorSeconds(long ackWaitSeconds, int maxDeliver, long sweepPeriodSeconds, long epsilonSeconds) {
        return maxDeliver * ackWaitSeconds + backoffSumSeconds(maxDeliver) + sweepPeriodSeconds + epsilonSeconds;
    }

    /** L verilmemişse türetilir: floor + sabit marj (default parametrelerde 307+13=320 üretir — ADR-0001 ile birebir). */
    public static long deriveDefaultLSeconds(long ackWaitSeconds, int maxDeliver, long sweepPeriodSeconds, long epsilonSeconds) {
        return floorSeconds(ackWaitSeconds, maxDeliver, sweepPeriodSeconds, epsilonSeconds) + MARGIN_SECONDS;
    }
}
```

**Türetme kuralı gerekçesi (LLD-düzeyi karar, ADR-0001'in "L default'u türetilebilir olmalı" ilkesini somutlaştırır):** ADR-0001 yalnız alt-sınır formülünü sabitler, "default L nasıl türetilir" mekanizmasını açıkça vermez (yalnız sayısal örnek: 307+13=320). Bu LLD, türetmeyi **floor + sabit 13s marj** olarak sabitler — bu, ADR'nin verdiği tek sayısal örnekle **birebir tutarlıdır** ve W/M/S/ε herhangi biri değiştiğinde mekanik, öngörülebilir bir L üretir. Bu bir yeni mimari karar DEĞİL, ADR-0001'in eksik bıraktığı (ama tek örnekle ima ettiği) türetme algoritmasının LLD-düzeyi somutlaştırılmasıdır.

**Marj mutlaktır, oransal DEĞİLDİR (review NIT-4 netleştirmesi, 2026-07-15):** `MARGIN_SECONDS = 13` **sabit saniye**dir, `floor`'un bir yüzdesi değildir. Sonuç: büyük `W` override'larında (ör. uzun-işli bir topic için `W=90s`), `floor` orantılı olarak büyür (`M·W` terimi baskın) ama marj **her zaman 13s** kalır — göreli marj (`13/floor`) küçülür. Bu **kabul edilebilir**dir çünkü `UmbrellaLockValidator` (§1.4) her durumda `L >= floor` şartını **mutlak** olarak doğrular — marjın göreli küçülmesi güvenlik garantisini ZAYIFLATMAZ, yalnız "ekstra tampon"un oransal payı azalır. Operatör daha büyük bir marj isterse `TopicLockOverride.lockDurationSeconds`'ı elle `floor`'un üzerinde bir değere ayarlayabilir (validator yine de yalnız `>= floor` şartını kontrol eder, marj büyüklüğüne karışmaz).

### 1.3 `UmbrellaLockResolver` (per-topic çözümleyici — cache'li)

```java
public class UmbrellaLockResolver {
    private final A2Properties properties;
    private final Map<String, Long> resolvedLMillisCache;   // parse-time'da (bootstrap) hesaplanır, immutable

    /** @return topic için L (milisaniye). Override yoksa default'tan türetilir. */
    public long resolveMillis(String topic) { return resolvedLMillisCache.get(topic) * 1000; }
}
```

### 1.4 `UmbrellaLockValidator` (bootstrap-time — `VAL_UMBRELLA_LOCK_TOO_SHORT`, BAQ-3)

```java
public class UmbrellaLockValidator implements InitializingBean {   // NatsSubscriptionRegistrar deseniyle tutarlı (mevcut kod)

    @Override
    public void afterPropertiesSet() {
        for (String topic : properties.getTopics()) {
            TopicLockOverride override = properties.getTopicOverrides().get(topic);
            long w = override != null && override.getAckWaitSeconds() != null
                    ? override.getAckWaitSeconds() : properties.getDefaults().getAckWaitSeconds();
            int m = override != null && override.getMaxDeliver() != null
                    ? override.getMaxDeliver() : properties.getDefaults().getMaxDeliver();
            long floor = UmbrellaLockCalculator.floorSeconds(w, m,
                    properties.getDefaults().getSweepPeriodSeconds(), properties.getDefaults().getEpsilonSeconds());
            long l = override != null && override.getLockDurationSeconds() != null
                    ? override.getLockDurationSeconds()
                    : UmbrellaLockCalculator.deriveDefaultLSeconds(w, m,
                            properties.getDefaults().getSweepPeriodSeconds(), properties.getDefaults().getEpsilonSeconds());

            if (l < floor) {
                if (!properties.isAllowUnsafeLockDuration()) {
                    // VAL_UMBRELLA_LOCK_TOO_SHORT — reject-startup DEFAULT (BAQ-3)
                    throw new UmbrellaLockConfigurationException(topic, l, floor);   // Spring context refresh FAIL
                }
                unsafeTopics.add(topic);   // A2OrphanSweep/A2PostCommitPublisher her döngüde kalıcı WARN loglar (§1.4.1)
            }
            resolvedLMillisCache.put(topic, l * 1000);
        }
    }
}
```

#### 1.4.1 Kalıcı WARN (escape-flag aktifken — "bir kere uyar, unut" YOK)

`A2PostCommitPublisher.publish(...)` ve `A2OrphanSweep.sweepCycle()`, `unsafeTopics.contains(topic)` ise **her çağrıda** `log.warn("Topic '{}' running with unsafe umbrella-lock duration (L < floor) — allow-unsafe-lock-duration=true", topic)` loglar (BAQ-3 kararının "sessiz bir kere uyar YOK" şartı).

### 1.5 Config-default çakışması — **çözüldü (review MINOR-1, 2026-07-15)**

Mevcut `SubscriptionConfig` (`camunda-nats-channel/.../inbound/SubscriptionConfig.java:11`) `maxDeliver` alanı için default **5** taşır — bu, saf message-correlation subject'leri (US-E2'nin koruduğu "gerçek dış event bekleme" yolu) içindir. **A2Properties.UmbrellaLockDefaults.maxDeliver default'u 4'tür** (ADR-0001) — **farklı bir config ağacı, farklı bir varsayılan**. Review bunu bir karışma riski olarak tespit etti (MINOR-1): `A2CompletionBridge`/`A2IncidentBridge` yanlışlıkla `SubscriptionConfig`'i miras alırsa maxDeliver sessizce 5 olurdu (asyncapi `a2JobReply.x-jetstream.maxDeliver: 4` ile **uyumsuz**).

**Çözüm:** `A2CompletionBridge` ve `A2IncidentBridge`, `SubscriptionConfig`'i **hiç kullanmaz** — bunun yerine yeni, A2'ye özgü **`A2ConsumerConfig`** sınıfı (`03_classes/2_camunda_a2.md` §4.0, `maxDeliver` default **4**, asyncapi ile birebir). Bu, config-tipi düzeyinde bir ayrım olduğundan "karıştırmama" artık bir **disiplin uyarısı** değil, **derleme-zamanı yapısal bir garanti**dir (iki sınıf farklı tip, biri diğerinin yerine geçemez).

**Bağımlılık:** BR-A2-006/007, FR-A8/A9, US-A5, ADR-0001, `EXCEPTION_CODES.md` `VAL_UMBRELLA_LOCK_TOO_SHORT`.

---

## 2. `NamespaceValidator` (BAQ-4 · BR-SUB-004 · `VAL_TOPIC_NAMESPACE_COLLISION`)

```java
package com.threeai.nats.core.config;

public final class NamespaceValidator {
    private static final String A2_RESERVED_PREFIX = "jobs.";

    /** Flowable channel kaydı + A2 topic listesi bootstrap'ında çağrılır. */
    public static void assertNotReservedForA2(String subject, String context) {
        if (subject.startsWith(A2_RESERVED_PREFIX)) {
            throw new TopicNamespaceCollisionException(subject, context);   // VAL_TOPIC_NAMESPACE_COLLISION
        }
    }
}
```

**Çağrı siteleri:** `NatsChannelDefinitionProcessor.validateSubject(...)` (Flowable, `03_classes/4_flowable.md` §4) — A2 tarafında **gerekmez** çünkü A2 topic'leri zaten `jobs.<topic>`'i **üretici** olarak kullanır (çakışma riski yalnız Flowable'ın kendi channel subject'ini yanlışlıkla bu önekle tanımlamasından doğar).

---

## 3. KV bucket konfigürasyonu (ADR-0002 + **LLD-Q1** — `a2-sweep-leader`)

| Alan | Değer | Kaynak |
|---|---|---|
| Bucket adı | `a2-sweep-leader` (tek bucket, motor-aileleri arasında **paylaşılır**) | ADR-0002 |
| Replikasyon | 3 (prod) | `stack/NATS_JETSTREAM.md` §3 genel kural — persistent/prod KV için R3 |
| TTL | `2·S` = 240s (S=120s default) | ADR-0002 |
| History | 1 (yalnız güncel lider gerekir) | Lease deseni — `NATS_JETSTREAM.md` §4 |
| Anahtar | **motor-başına ayrı**: `sweep-leader.<engineId>` (ör. `sweep-leader.camunda`, `sweep-leader.cadenzaflow`) | **LLD-Q1 kararı, 2026-07-15** (`03_classes/1_nats_core_common.md` §3.2) |
| Değer | replika kimliği (string, PSEUDONYMOUS — PII yok) | ADR-0002 sonuçlar |

**LLD-Q1 gerekçesi:** Bucket paylaşılır (tek KV altyapısı yeterli) ama anahtar **motor-ailesi başına izoledir** — iki motor ailesi (Camunda/CadenzaFlow) asla aynı anahtar için yarışmaz, her biri kendi replikaları arasında bağımsız lider seçer. Tam KV şeması + oluşturma prosedürü: `docs/sentinel/phase4/DB_ACCESS_MAP.md` §3.

---

## 4. DLQ-bridge Circuit-Breaker eşikleri (ADR-0004 — sabit, konfigüre EDİLMEZ)

ADR-0004'ün eşikleri (5 ardışık hata→OPEN, 30s→HALF_OPEN, 3 izinli deneme→CLOSED/OPEN) **kilitlidir**, operatör tarafından override edilebilir bir config alanı **YOKTUR** (bilinçli — ADR metni bunları sabit değer olarak kabul ediyor, bir override yüzeyi açmak ADR'nin ötesine geçer). `DlqBridgeCircuitBreakerFactory` (`03_classes/1_nats_core_common.md` §4.1) bu sabitleri **kod-içi** taşır.

**HALF_OPEN semantiği (review MAJOR-1b, karar 2026-07-15):** "3 izinli deneme→CLOSED/OPEN" ifadesi bilinçli olarak "3 ardışık başarı→CLOSED"'den farklıdır — Resilience4j'nin gerçek davranışı (erken-CLOSE mümkün) **kütüphaneye uyarlanarak kabul edilmiştir**, özel mantık yazılmamıştır. Ayrıntı: `03_classes/1_nats_core_common.md` §4.3.

**`ignoreExceptions` bir eşik değildir, config alanı da DEĞİLDİR (review MAJOR-1a):** Hangi exception tiplerinin CB muhasebesine girmediği (`NotFoundException` vb.) her çağıranın **kod-içi** kararıdır (`03_classes/1_nats_core_common.md` §4.2, `03_classes/2_camunda_a2.md` §5) — ADR-0004'ün sayısal eşiklerinden **bağımsız** bir düzeltmedir, operatör konfigürasyonu ile ilgisi yoktur.
