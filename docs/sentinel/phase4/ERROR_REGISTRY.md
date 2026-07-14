# Error Registry — Basamak-1 (23 Kod)

**Sentinel fazı:** Phase 4 — Developer (LLD). **Kaynak:** `docs/sentinel/phase2/EXCEPTION_CODES.md` (23 kod, kategori taksonomisi — **DEĞİŞTİRİLMEZ**, yalnız Java sınıfı/log-format/metrik/alarm bağlaması eklenir), `ERROR_HANDLING_GUIDELINE.md` §1/§3/§6.
**Durum:** Onaylı (2026-07-15) — LLD-Q1…3 + review düzeltmeleri işlendi (§4.2 eşik-kalibrasyon notu LLD-Q3; CB `ignoreExceptions` düzeltmesi — MAJOR-1a — LLD modülünde `03_classes/1_nats_core_common.md` §4.2'de, burada yalnız kod-7/kod-11 satırları etkilenir).
**Uyarlama notu:** Bu sistem bir HTTP API değil, asenkron mesajlaşma katmanıdır — `ERROR_HANDLING_GUIDELINE.md` §3.2'nin Java exception hiyerarşisi (HTTP-status odaklı) burada **"Statü" odaklı** (ack/nak/DLQ/incident/failure-event/CRITICAL-page/build-fail) bir hiyerarşiye uyarlanır; kategori semantiği (`VAL_`/`BUS_`/`RES_`/`SYS_`/`EXT_`) korunur — bu, `EXCEPTION_CODES.md`'nin zaten yaptığı uyarlamanın LLD-düzeyi (Java sınıf) somutlaştırmasıdır.

---

## 1. Hata sınıfı hiyerarşisi (`com.threeai.nats.core.exception`)

Yalnız **kontrol akışını kesen** (bootstrap fail, invariant-ihlali) kodlar **gerçek Java exception sınıfı** olarak modellenir; geri kalanı (çoğunluk) mevcut motor exception'larının (`NotFoundException`, `BadUserRequestException`) **yakalanıp** yapılandırılmış log+metrik'e **bağlanmasıdır** (yeni bir exception türü icat edilmez — mevcut motor sözleşmesi kullanılır, `03_classes/2_camunda_a2.md` §4).

```java
package com.threeai.nats.core.exception;

public enum ErrorCategory { VAL, BUS, RES, SYS, EXT }
public enum LogSeverity { DEBUG, INFO, WARN, ERROR, CRITICAL }   // CRITICAL = ERROR + sayfalama (guideline §6 kapsamı DIŞINDA, invariant)

/** Bootstrap-time fail-fast — Spring context refresh'i durdurur. */
public class UmbrellaLockConfigurationException extends RuntimeException {
    public UmbrellaLockConfigurationException(String topic, long configuredL, long floor) { ... }
    // code = "VAL_UMBRELLA_LOCK_TOO_SHORT"
}
public class TopicNamespaceCollisionException extends org.flowable.common.engine.api.FlowableException {
    public TopicNamespaceCollisionException(String subject, String channelKey) { ... }
    // code = "VAL_TOPIC_NAMESPACE_COLLISION"
}
```

Diğer 21 kod için **exception sınıfı YOK** — her biri ilgili LLD sınıfının `catch`/`if` dalında **yapılandırılmış log satırı + metrik artışı + (gerekiyorsa) alarm tetikleyici** olarak somutlaşır (aşağıdaki §2 tablosu, "Fırlatan/Loglayan sınıf" kolonu).

---

## 2. MDC alan seti (kanonik — her log satırında ilgili olanlar bulunur)

| MDC alanı | Ne zaman set edilir | Kaynak |
|---|---|---|
| `trace_id` | Her mesaj işlemede | `BpmHeaders.extractTraceIdWithFallback(msg)` |
| `external_task_id` | A2 kod yollarında | `Nats-Msg-Id` (A2 dedup anahtarı) |
| `correlation_key` | Event Registry kod yollarında | Event correlation header'ı |
| `topic` / `channel` | Her zaman | Subject'ten türetilir (düşük kardinalite) |
| `subject` | Her zaman | `msg.getSubject()` |
| `delivery_count` | Redelivery/DLQ yollarında | `msg.metaData().deliveredCount()` |
| `sentinel_worker_id` | A2 completion/incident yollarında (audit) | config sabiti (asla PII) |

**DP-1/DP-2 (NFR-S1) uyumu:** payload/business-key **değeri** hiçbir MDC alanına veya log mesajına yazılmaz — yukarıdaki liste bununla tutarlıdır (yalnız kimlik/meta alanları).

---

## 3. Kod → LLD bağlaması (23/23)

| # | Kod | Kategori | Statü | Fırlatan/Loglayan sınıf | Log seviyesi | MDC | Metrik |
|---|---|---|---|---|---|---|---|
| 1 | `BUS_WORKER_BUSINESS_ERROR` | BUS | error-reply→ack | (Worker, repo dışı — bu repo yalnız `handleBpmnError` çağrısını yapar, `A2CompletionBridge`) | WARN | trace_id, external_task_id | — (native lifecycle) |
| 2 | `SYS_WORKER_TRANSIENT_FAILURE` | SYS | nak-backoff | (Worker, repo dışı) | WARN→ERROR (M'e yaklaşırken) | trace_id, delivery_count | worker-taraflı, bu repo gözlemlemez |
| 3 | `BUS_JOB_DELIVERY_BUDGET_EXCEEDED` | BUS | DLQ-route | (Worker'ın kendi job-consumer'ı, repo dışı) | WARN | trace_id, delivery_count | worker-taraflı |
| 4 | `VAL_EMPTY_MESSAGE_BODY` | VAL | WARN+DLQ-route | `A2CompletionBridge` / `JetStreamInboundEventChannelAdapter` (`DlqPublisher.publish(..., EMPTY_MESSAGE_BODY)`) | WARN | trace_id, subject, channel | `nats.jetstream.inbound.dlq` (dlqCount) |
| 5 | `BUS_REPLY_DELIVERY_BUDGET_EXCEEDED` | BUS | DLQ-route | `A2CompletionBridge` / `JetStreamInboundEventChannelAdapter` (`DlqPublisher.publish(..., DELIVERY_BUDGET_EXCEEDED)`) | WARN | trace_id, external_task_id/correlation_key, delivery_count | `nats.jetstream.inbound.dlq` |
| 6 | `SYS_DLQ_PUBLISH_FAILED` | SYS | nak+alert | `DlqPublisher` (`FAILED_NO_DLQ_SUBJECT`/`FAILED_BOTH_PUBLISH`) | ERROR | subject, channel | `nats.jetstream.dlq.publish.failures` (**yeni**, `10_metrics.md` §1) |
| 7 | `RES_EXTERNAL_TASK_NOT_FOUND` | RES | ack (idempotent yut) | `A2CompletionBridge` (`catch NotFoundException`) | WARN | trace_id, external_task_id | `nats.inbound.consumed` (ack) |
| 8 | `SYS_SENTINEL_WORKER_CONFLICT` | SYS | **CRITICAL+page, ACK YOK** | `A2CompletionBridge` (`catch BadUserRequestException`) | **CRITICAL** | trace_id, external_task_id, sentinel_worker_id | `nats.a2.sentinel_worker_conflict` (**yeni**) — bkz. §4.1 |
| 9 | `BUS_EVENT_CORRELATION_NOT_FOUND` | BUS | ack+log+metric (drop) | `JetStreamInboundEventChannelAdapter` (geç-sonuç, interrupting escalation sonrası) | WARN | trace_id, correlation_key, channel | `nats.inbound.errors` veya özel sayaç (Phase 5 kararı) |
| 10 | `RES_FAILURE_EVENT_CORRELATION_MISS` | RES | WARN+metrik+eşik-alarmı | `FailureEventBridge` (`catch NoMatchingSubscriptionException` / no-match dalı) | WARN | trace_id, correlation_key, channel | `nats.flowable.failure_event.correlation_miss` — bkz. §4.2 |
| 11 | `SYS_DLQ_BRIDGE_PROCESSING_FAILED` | SYS | nak-backoff+CB | `A2IncidentBridge` / `FailureEventBridge` (`catch Exception downstreamFailure`) | ERROR (+ALERT CB OPEN'da) | subject, channel | Resilience4j binder (`10_metrics.md` §2) |
| 12 | `BUS_INCIDENT_ALREADY_CREATED` | BUS | ack (no-op) | `A2IncidentBridge` (doğal idempotency, `ExternalTaskEntity.java:443-448`) | DEBUG/INFO | external_task_id | — |
| 13 | `SYS_SWEEP_QUERY_FAILED` | SYS | log-only, döngü atlanır | `A2OrphanSweep.sweepCycle()` (`catch Exception` — fetchable-parite SELECT) | ERROR | topic | — (döngü sayacı, opsiyonel) |
| 14 | `SYS_SWEEP_RELOCK_FAILED` | SYS | log-only, satır atlanır | `A2OrphanSweep.relockThenPublish()` (`catch Exception relockEx`) | ERROR | external_task_id, topic | — |
| 15 | `SYS_SWEEP_REPUBLISH_FAILED` | SYS | kabul edilen nadir risk | `A2OrphanSweep.relockThenPublish()` (`catch Exception publishEx` — telafi başarılı/başarısız iki alt-dal) | ERROR | external_task_id, topic | `nats.a2.sweep.oldest_orphan_age_seconds` (gauge, §4.3) |
| 16 | `BUS_TASK_RETRIES_EXHAUSTED` | BUS | atla (no-op) | `A2OrphanSweep` (fetchable-parite SELECT zaten filtreler — `RETRIES_>0`) | DEBUG (rutin) | — | — |
| 17 | `RES_TASK_SUSPENDED` | RES | atla (no-op) | `A2OrphanSweep` (aynı SELECT filtresi) | DEBUG (rutin) | — | — |
| 18 | `SYS_BENCH_ENVIRONMENT_UNAVAILABLE` | SYS | build-warn-only | `nats-bpm-bench` (`ContainerLaunchException` → `Assumptions.abort(...)`) | WARN | — | — (test-zamanı) |
| 19 | `BUS_BENCH_METRIC_REGRESSION` | BUS | **build-fail (sert kapı)** | `nats-bpm-bench` (`DbRoundTripReport.passesHardGate()==false` → `assertThat(...).isTrue()` fail) | ERROR (bench raporunda) | mode, task counts | bench raporu (CI artifact) |
| 20 | `SYS_BENCH_SLI_DRIFT` | SYS | build-warn-only | `nats-bpm-bench` (`SupportingSliReport.withinSoftTargets()==false` → rapora yazılır, assert YOK) | WARN (bench raporunda) | — | bench raporu |
| 21 | `VAL_UMBRELLA_LOCK_TOO_SHORT` | VAL | **reject-startup DEFAULT** | `UmbrellaLockValidator` (`UmbrellaLockConfigurationException`, bootstrap `InitializingBean`) | ERROR (reject) / kalıcı WARN (escape-flag) | topic | — (bootstrap-zamanı) |
| 22 | `VAL_TOPIC_NAMESPACE_COLLISION` | VAL | **aktif validasyon (engeller)** | `NamespaceValidator.assertNotReservedForA2(...)` (`TopicNamespaceCollisionException`) | ERROR (engeller) | subject, channel | — (bootstrap-zamanı) |
| 23 | `EXT_JETSTREAM_PUBLISH_UNAVAILABLE` | EXT | log-only (tolere edilir) | `A2PostCommitPublisher.publish()` (`catch Exception e`) | WARN | external_task_id, topic | `nats.jetstream.outbound.errors` (`jsPublishErrorCount`, mevcut) |

**Sayım doğrulaması:** 23/23 — `EXCEPTION_CODES.md` §8 kategori özetiyle birebir (VAL=3, BUS=7, RES=3, SYS=9, EXT=1).

---

## 4. Alarm tanımları (somutlaştırma — HLD §8'in "eşik" notlarının Phase 4 karşılığı)

### 4.1 `SYS_SENTINEL_WORKER_CONFLICT` — CRITICAL + page (BAQ-7)

- **Metrik (yeni):** `NatsChannelMetrics.sentinelWorkerConflictCount(String topic)` — `Counter("nats.a2.sentinel_worker_conflict").tag("topic", topic)` (`10_metrics.md` §1'e eklenir).
- **Log kanalı:** `log.error(...)` + yapılandırılmış marker (ör. Logback `MarkerFactory.getMarker("PAGE")`) — log pipeline'ı (Loki/Alertmanager) bu marker'ı **anında** eşleştirip PagerDuty/on-call'a yönlendirir.
- **Prometheus alert (öneri, DevOps Phase 5'te uygular):**
  ```yaml
  - alert: SentinelWorkerConflict
    expr: increase(nats_a2_sentinel_worker_conflict_total[1m]) > 0
    for: 0m
    labels: { severity: critical }
    annotations: { summary: "SENTINEL workerId invariant violated — config drift or forged reply" }
  ```
  **Gecikme sıfır** (`for: 0m`) — tek bir olay bile anında sayfalar (invariant ihlali, BAQ-7).

### 4.2 `RES_FAILURE_EVENT_CORRELATION_MISS` — eşik-alarmı (BAQ-8 · **LLD-Q3 kararı, Levent onayı 2026-07-15**)

BAQ-8 kararı yalnız "tek olay WARN+metrik, süreklilik ayrı alarm" der; **eşik sayısı BR-FLW-003'te açıkça Phase 3/4'e bırakılmıştı**. **Karar (LLD-Q3, 2026-07-15):** Bu LLD'nin önerdiği somut değerler **konfigürable başlangıç default'u** olarak kabul edildi:

```yaml
- alert: FailureEventCorrelationMissSustained
  expr: increase(nats_flowable_failure_event_correlation_miss_total[10m]) >= 3
  labels: { severity: warning }
  annotations: { summary: "Repeated failure-event correlation-miss — possible correlation-key loss (NFR-R6 risk)" }
- alert: FailureEventCorrelationMissCritical
  expr: increase(nats_flowable_failure_event_correlation_miss_total[60m]) >= 10
  labels: { severity: critical }
  annotations: { summary: "Sustained correlation-miss over 1h — likely systemic token-leak, page on-call" }
```

**Gerekçe:** tek olay benign yarış kabul edilir (BAQ-8); 10 dakikada ≥3 tekrar "benign yarış" ihtimalinin ötesine geçen bir **desen** işaret eder (WARN); 60 dakikada ≥10 ise NFR-R6 (token-leak yasağı) riskinin gerçekleştiğine dair güçlü sinyal (CRITICAL).

**Kalibrasyon notu (LLD-Q3 kabul şartı):** bu sayılar (3/10dk, 10/60dk) **keyfi bir başlangıç noktasıdır** — formülden türetilmedi (L'nin `UmbrellaLockCalculator`'dan türetilmesi gibi değil), gerçek trafik deseniyle (pilot deployment / bench telemetrisi) **kalibre edilmesi gerekir**. Prometheus alert kuralı olduğundan zaten operasyonel olarak ayarlanabilir (config değişikliği, kod değişikliği DEĞİL) — bu not, "kesin doğru sayı" olarak yanlış yorumlanmaması için eklenmiştir.

### 4.3 En-yaşlı-orphan yaşı (ADR-0003 telafi-başarısızlığı sinyali)

```yaml
- alert: OldestOrphanAgeWarning
  expr: nats_a2_sweep_oldest_orphan_age_seconds > 440   # L+S = 320+120
  labels: { severity: warning }
  annotations: { summary: "Orphan older than L+S tolerance (NFR-R3) — sweep or telafi-unlock may be failing" }
- alert: OldestOrphanAgeCritical
  expr: nats_a2_sweep_oldest_orphan_age_seconds > 560   # L+2S = 320+240
  labels: { severity: critical }
```

**Gerekçe:** NFR-R3'ün belgelediği tolerans ≤L+S (~440s); bu sınırın aşılması ADR-0003'ün telafi mekanizmasının **beklenenden sık** başarısız olduğuna işaret eder (DB+broker eşzamanlı kesinti deseni).

### 4.4 DLQ-bridge Circuit-Breaker OPEN geçişi (ADR-0004)

Resilience4j'nin kendi event publisher'ı (`03_classes/1_nats_core_common.md` §4) her state-transition'da `log.warn(...)` üretir; bu log satırı ops dashboard'unda (Loki) izlenir, ayrıca `resilience4j_circuitbreaker_state{state="open"}` gauge'i üzerinden Prometheus alarm:

```yaml
- alert: DlqBridgeCircuitBreakerOpen
  expr: resilience4j_circuitbreaker_state{state="open"} == 1
  for: 1m
  labels: { severity: warning }
```

---

## 5. `BUS_BENCH_METRIC_REGRESSION` — CI build-fail entegrasyonu

Bu kod, HTTP-analog "statü" kavramının dışına çıkan **tek** kod türüdür: bir test assertion'ının (JUnit) CI pipeline'ında **build'i kırmasıdır**, çalışma-zamanı bir olay değil. `nats-bpm-bench` (`03_classes/5_bench.md` §2) bu kodu doğrudan bir `AssertionError` olarak üretir — ayrı bir exception sınıfı **gerekmez** (JUnit'in kendi assertion mekanizması yeterli).

---

## 6. İzlenebilirlik

Bu kayıt, `EXCEPTION_CODES.md` §9'un (23 satır) **birebir üstüne kuruludur** — kod adları, statüleri, BR/FR/US bağlantıları oradan DEĞİŞTİRİLMEDEN alınmıştır; bu belge yalnız **Java sınıfı + log/MDC/metrik/alarm** katmanını ekler.
