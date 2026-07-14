# ADR-0007 — A2 bileşenlerinin modül/paket yerleşimi

- **Durum:** Önerildi — yerleşim kabul, paylaşım-stratejisi ARCH-Q4 (2026-07-14, Phase 3)
- **İzlenebilirlik:** tüm EPIC-A…E bileşenleri → NFR-M1/M2/M3 → mevcut repo yapısı (`nats-core` + üç channel modülü + `pom.xml`)

## Bağlam

Mevcut repo (doğrulandı): `pom.xml` modülleri = `nats-core`, `flowable-nats-channel`, `camunda-nats-channel`, `cadenzaflow-nats-channel`. Paket kökleri: `com.threeai.nats.core`, `com.threeai.nats.camunda`, `com.threeai.nats.cadenzaflow`, `org.flowable.eventregistry.spring.nats`. Mevcut ayna kanıtı: `JetStreamMessageCorrelationSubscriber` **hem** camunda **hem** cadenzaflow modülünde neredeyse birebir aynı (yalnız engine import paketi farklı: `org.camunda.*` vs `org.cadenzaflow.*`).

Yeni basamak-1 kod kapsamı yerleştirilmeli: custom activity behavior + post-commit publisher (A2), sweep+leader, inbound completion-bridge, incident-bridge, failure-event-bridge, 5 kontrat-fix, Testcontainers bench.

## Karar

### 1. Engine-nötr parçalar → `nats-core`
- `BpmHeaders` (mevcut) + DLQ meta header sabitleri (`X-Cadenzaflow-Dlq-*`).
- `publishToDlq` ortak yardımcı (header-preserving + `Nats-Msg-Id=<id>.dlq` + custody-transfer nak) — **5 kontrat-fix'in ortak çekirdeği** (FR-C1/C2/C3/C7 + boş-body). İki adapter (flowable/cadenzaflow) bu ortak yardımcıyı çağırır → fix tek yerde.
- `SweepLeaderLease` (ADR-0002, JetStream KV), `NatsChannelMetrics` genişletmeleri (sweep-republish sayacı, en-yaşlı-orphan yaşı, `failure_event_correlation_miss`), umbrella-lock parametre modeli + config validasyon (`VAL_UMBRELLA_LOCK_TOO_SHORT`, `VAL_TOPIC_NAMESPACE_COLLISION`).

### 2. A2 (Camunda/CadenzaFlow) → **ilgili channel modülüne, ayna olarak**
- `camunda-nats-channel`: `A2ExternalTaskBehavior`, `A2BpmnParseListener`, `A2PostCommitPublisher`, `A2OrphanSweep`, `A2CompletionBridge`, `A2IncidentBridge` — paket `com.threeai.nats.camunda.a2` (`org.camunda.bpm.*` importları).
- `cadenzaflow-nats-channel`: birebir ayna, paket `com.threeai.nats.cadenzaflow.a2` (`org.cadenzaflow.*` importları).
- Gerekçe: impl-sınıf bağımlılığı (ADR-0005) engine-özgü paket ister; mevcut repo deseni zaten ayna. NFR-M2 (birebir taşınabilirlik) böyle korunur.

### 3. Flowable (Event Registry) → `flowable-nats-channel`
- `A2FailureEventBridge` (DLQ→failure-event), boş-body fix, JetStream inbound sağlamlığı → mevcut `jetstream/` paketi. Boundary-timer opt-in **model kararı** (kod değil).

### 4. Bench → **yeni modül `nats-bpm-bench`** (`@Tag("bench")`)
- Testcontainers PG+engine+NATS+N worker; native-poll ↔ A2-push iki mod; nightly/manuel. Ana CI'yı bloklamaz (`SYS_BENCH_ENVIRONMENT_UNAVAILABLE` → warn-only). `pom.xml`'e beşinci modül olarak eklenir.
- Gerekçe: bench çok-motor senaryosu koşar (camunda + cadenzaflow) → tek channel modülüne sığmaz; ayrı modül test-scope bağımlılık karmaşasını izole eder.

### 5. Delegate phase-out (US-E1) → ilgili modüllerde silme
`camunda-nats-channel/.../outbound/{NatsPublish,JetStreamPublish,NatsRequestReply}Delegate`, cadenzaflow karşılıkları, `flowable-.../requestreply/NatsRequestReplyDelegate` kaldırılır.

## Sonuçlar

**Olumlu:** Mevcut repo yapısına ve mevcut ayna-desenine uyar; impl-bağımlılık engine-modülünde izole; ortak kontrat-fix tek yerde (`nats-core`) → 5 fix çiftlenmez; bench izole.

**Olumsuz / kabul edilen:** A2 mantığı iki modülde **aynalanır** (camunda + cadenzaflow) → kod tekrarı. Mevcut repo bunu zaten yapıyor (`JetStreamMessageCorrelationSubscriber`). Alternatif — ortak `a2-core` soyutlama modülü + engine SPI — impl-sınıf paket farkını reflection/SPI ile köprülemeyi gerektirir (daha kırılgan, ADR-0005 ile gerilim).

**ARCH-Q4 (Levent onayı):** Ayna-tekrar (mevcut desenle tutarlı, önerilen) mi kabul edilsin, yoksa ortak `a2-core` soyutlaması mı hedeflensin? Öneri: ayna-tekrar (basamak-1'de; soyutlama basamak-6 native-core'a doğru ayrı bir refactor işi).
