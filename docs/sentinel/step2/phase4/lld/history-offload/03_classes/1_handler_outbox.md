# 03.1 — EPIC-A: Handler + Hibrit Yol

**Modül:** `camunda-nats-channel` (`com.threeai.nats.camunda.history.*`), ayna `cadenzaflow-nats-channel`.
**Kaynak ADR:** 0009 (composite handler plug-in), 0010 (hibrit yayın topolojisi + ARCH-Q1).
**HLD:** §3.1.1…§3.1.4.

---

## 1. `NatsHistoryEventHandler` — composite, sınıf-bazlı router (BR-HDL-001/002/005/007, ADR-0009)

```java
package com.threeai.nats.camunda.history;

/** Registered into ProcessEngineConfigurationImpl.customHistoryEventHandlers.
 *  enableDefaultDbHistoryEventHandler is ALWAYS set to false at bootstrap (this class owns its own
 *  internal DbHistoryEventHandler delegate -- see §1.4 fork-evidence rationale, 01_overview.md
 *  "Hot-reconfigure" closure). */
public class NatsHistoryEventHandler implements org.camunda.bpm.engine.impl.history.handler.HistoryEventHandler {
// (camunda-nats-channel modülü org.camunda.* hedefler — mevcut modül desenine uygun [phase4-review F-001];
//  cadenzaflow-nats-channel byte-aynası org.cadenzaflow.* FQN'ini kullanır, ADR-0007)

    public NatsHistoryEventHandler(
            ClassCutoverStateRegistry cutoverRegistry,          // §1.4 -- per-class DB-write routing
            HistoryClassificationProperties classification,     // §08_config.md §1 -- audit-critical/bulk map
            CompactHistoryOutboxWriter outboxWriter,             // §2
            HistoryPostCommitPublisher postCommitPublisher,      // §3
            DbHistoryEventHandler internalDbDelegate) { ... }    // fork class, instantiated by US (not by CompositeDbHistoryEventHandler)

    /** Fork SPI entry point. Classifies the event's ACT_HI class, checks HistoryLevel-produced
     *  precondition (BA-Q4), routes to outbox (audit-critical) or post-commit (bulk), and --
     *  IF the class has not yet been cut over (ClassCutoverStateRegistry) -- ALSO forwards to the
     *  internally-held DbHistoryEventHandler (dual-run ACT_HI write). */
    @Override
    public void handleEvent(HistoryEvent historyEvent);

    /** Fork's own CompositeHistoryEventHandler.handleEvents(...) degrades to a for-loop over
     *  handleEvent(...) (fork-verified, CompositeHistoryEventHandler.java:100-105, HLD §11 kalem 2)
     *  -- this override exists only to preserve the SPI contract; no batch-specific optimization. */
    @Override
    public void handleEvents(List<HistoryEvent> historyEvents);
}
```

**Sorumluluk akışı (BUSINESS_LOGIC.md §1.1 flow 1.1 ile birebir):**
1. `HistoryLevel` önkoşulu zaten motor tarafından uygulanmış (yalnız üretilen event'ler bu metoda ulaşır) — bootstrap-time `VAL_HISTORY_LEVEL_AUDIT_CRITICAL_MISMATCH` guard'ı §08_config §1'de.
2. `historyEvent`'in ACT_HI sınıfını `HistoryClassificationProperties`'ten çöz; haritada YOKSA → `VAL_HISTORY_CLASS_UNCLASSIFIED` (fail-safe bulk + WARN).
3. Sınıf audit-kritik mi bulk mu? → ilgili yayın yoluna delege (§2 / §3).
4. `ClassCutoverStateRegistry.isCutOver(engineId, class)` **false** ise → `internalDbDelegate.handleEvent(historyEvent)` DE çağrılır (dual-run, ACT_HI satırı yazılır); **true** ise ATLANIR (ACT_HI yazım bileşeni = 0, NFR-P1).

### 1.4 Neden `internalDbDelegate` bizim tarafımızdan tutuluyor (fork'un `CompositeDbHistoryEventHandler`'ı DEĞİL) — fork-kanıtlı gerekçe

`01_overview.md` "Phase3'ün devrettiği doğrulamalar #1" bu kararın TAM kanıt zincirini taşır (`ProcessEngineConfigurationImpl.java:1134-1141,2788-2796`, `CompositeDbHistoryEventHandler.java:70-72`, `DbHistoryEventHandler.java:40`, `HistoryEventProcessor.java:74-75`). Özet: fork'un `enableDefaultDbHistoryEventHandler` bayrağı motor-genel + tek-seferlik-boot'tur, sınıf-başına DEĞİLDİR — bu yüzden per-class cutover (BR-CUT-002) yalnız BİZİM composite'imizin İÇİNDE gerçekleşebilir. `DbHistoryEventHandler`'ın implicit public no-arg constructor'ı (sınıf bildirimi `DbHistoryEventHandler.java:40`; açık ctor yok) bunu mümkün kılar.

**Bağımlılık:** BR-HDL-001/002/005/007, FR-A1/A2/A3/A6, US-A1/A2/A5, ADR-0009. `ClassCutoverStateRegistry`: `03_classes/4_cutover_reconciliation.md` §2.2, `08_config.md` §2.

---

## 2. `CompactHistoryOutboxWriter` — audit-kritik tx-içi yazıcı (BR-HDL-003, ADR-0010 + ARCH-Q1)

```java
package com.threeai.nats.camunda.history;

public class CompactHistoryOutboxWriter {

    public CompactHistoryOutboxWriter(DataSource engineDataSource, PseudonymTokenGenerator pseudonymGenerator) { ... }

    /** Called from NatsHistoryEventHandler.handleEvent(...) -- SAME transaction as the runtime write
     *  (BR-HDL-003, NFR-P2 <=1 row). Extracts scalar audit-critical fields into payload_scalar JSONB
     *  (LLD-Q1 default); if a byte-array payload is present on the source event (e.g.
     *  EXT_TASK_LOG.errorDetails), ALSO inserts compact_history_outbox_payload in the same tx and
     *  sets payload_large_ref. If the class has opt-in pseudonymization (userId field) and the
     *  tenant enabled it, computes pseudonym_token via PseudonymTokenGenerator.generate(...) (pure,
     *  no I/O, BA-Q5) BEFORE building payload_scalar -- the raw value is never persisted alongside
     *  the token in the same row. */
    public void write(HistoryEvent historyEvent, String historyClass, Connection engineTxConnection);
}
```

**DB:** `compact_history_outbox` / `compact_history_outbox_payload` (engine DB eklentisi — `DB_SCHEMA.md §1`). **Bağımlılık:** BR-HDL-003, FR-A4, US-A3, ADR-0010.

---

## 3. `HistoryPostCommitPublisher` — bulk yol, sıfır DB yazımı (BR-HDL-004, ADR-0010)

```java
package com.threeai.nats.camunda.history;

public class HistoryPostCommitPublisher {

    public HistoryPostCommitPublisher(JetStream jetStream, NatsChannelMetrics metrics) { ... }

    /** Registered via TransactionContext.addTransactionListener(TransactionState.COMMITTED, ...)
     *  from NatsHistoryEventHandler.handleEvent(...) -- basamak-1 post-commit TransactionListener
     *  deseni [07§4] birebir yeniden kullanım. Zero DB reads/writes. Publish exception is caught and
     *  logged (EXT_JETSTREAM_PUBLISH_UNAVAILABLE-equivalent, basamak-1 pattern) -- CANNOT roll back
     *  the already-committed runtime transaction (D-A guarantee). */
    public void publish(HistoryEvent historyEvent, String historyClass, String engineId);
}
```

**Bağımlılık:** BR-HDL-004/005, FR-A5/A6, US-A4/A5, ADR-0010.

---

## 4. `HistoryClassificationProperties` — pointer

Config sınıfı (audit-kritik/bulk haritası, PO-Q5 default + kiracı override) `08_config.md` §1'de tanımlanır (bu dosyada TEKRARLANMAZ — basamak-1 dersinin (`A2ConsumerConfig` config sınıf-adlandırma çakışması) burada da uygulanır: `HistoryClassificationProperties` mevcut `A2Properties`/`SubscriptionConfig` ile İSİM ÇAKIŞMAZ, farklı bir config-ağacıdır, `spring.nats.<engine>.history.*` prefix'i taşır).

**Bağımlılık:** BR-HDL-002/007, FR-A3, US-A2, PO-Q5.
