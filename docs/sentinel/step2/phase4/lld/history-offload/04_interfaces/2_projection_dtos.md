# 04.2 — DTO'lar (wire-şema → JDBC satırı köprüsü)

Bu dosya `api/asyncapi.yaml`'ın `HistoryEventPayload`/`HistoryEventEnvelope` şemalarının (tam alan listesi orada — burada tekrarlanmaz) Java tarafı taşıyıcılarını + `ProjectionStore` (§`03_classes/2_relay_projection.md` §3) girdi tiplerini tanımlar.

---

## 1. Wire-contract taşıyıcıları (`nats-core`, `com.threeai.nats.core.history`)

```java
package com.threeai.nats.core.history;

/** Java-tarafı karşılığı: api/asyncapi.yaml components/schemas/HistoryEventPayload.
 *  payload alanı OPAK string (ARCH-Q1) -- relay/consumer onu deserialize ETMEDEN taşır;
 *  yalnız ProjectionStore çağrısı sırasında historyClass'a göre spesifik alan çıkarımı yapılır. */
public record HistoryEventEnvelope(
        String engineId, String historyClass, String eventType, String historyEventId,
        String processInstanceId, String businessKey /* nullable, subject'e GÖMÜLMEZ */,
        long streamSequence /* consumer tarafından JetStream metadata'sından doldurulur, ADR-0012 */,
        Instant eventTime, String payload /* opak — ARCH-Q1 referans veya inline scalar JSON */) {}
```

---

## 2. `ProjectionStore` girdi tipleri (`nats-history-projection`, `com.threeai.nats.history.projection`)

```java
package com.threeai.nats.history.projection;

/** Entity-lifecycle tabloları (DB_SCHEMA.md §2.2) için ortak zarf -- alan haritalama historyClass'a
 *  göre HistoryProjectionConsumer'da yapılır (bu record yalnız TAŞIYICI, iş kuralı taşımaz). */
public record EntityHistoryRecord(
        String engineId, String entityId /* processInstanceId|activityInstanceId|... */,
        String processInstanceId, long streamSequence, Instant eventTime,
        Map<String, Object> fields /* class-specific columns, DB_SCHEMA.md §2.2 sütun listesi */) {}

/** Append-only log tabloları (DB_SCHEMA.md §2.4) için ortak zarf. */
public record LogHistoryRecord(
        String engineId, String processInstanceId, String historyEventId, String eventType,
        long streamSequence, Instant eventTime,
        Map<String, Object> fields) {}

public enum UpsertOutcome { APPLIED, STALE_DISCARDED, DEDUP_SKIPPED }
```

**Neden `Map<String,Object> fields` (jenerik) ve 15 ayrı record DEĞİL:** `ProjectionStore.upsertEntity`/`insertLogEvent` tek bir JDBC şablonunu (protokol, `DB_SCHEMA.md §2.3`) 15 tabloya uygular — sınıf-özgü alan adları `HistoryClassColumnMapping` (config-benzeri sabit harita, Phase 5 detayı) üzerinden çözülür. Bu, 15 ayrı record + 15 ayrı upsert-metodu tekrarını önler; LLD düzeyinde kontrat (girdi/çıktı tipleri + davranış) sabit kalır, Phase 5 iç uygulaması bu şablonu doldurur.

**Bağımlılık:** BR-REL-002/003, ADR-0011/0012.
