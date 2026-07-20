# 03.2 — EPIC-B: Relay + Projeksiyon

**Modül:** `HistoryOutboxRelay` → `camunda-nats-channel` (+ ayna); `HistoryProjectionConsumer`/`ProjectionStore`/`HistoryDlqConsumer` → **`nats-history-projection`** (motor-dışı).
**Kaynak ADR:** 0010 (relay), 0011 (projeksiyon store + ARCH-Q3), 0012 (merge-upsert tie-break), 0013 (wire-contract), 0019 (DLQ + authz).
**HLD:** §3.2.1…§3.2.5.

---

## 1. `HistoryOutboxRelay` — leader-elected, custody-transfer (BR-REL-001, ADR-0010+0002)

```java
package com.threeai.nats.camunda.history;

public class HistoryOutboxRelay {

    public HistoryOutboxRelay(
            DataSource engineDataSource,
            JetStream jetStream,
            SweepLeaderLease leaderLease,          // basamak-1 sınıf YENİDEN KULLANILIR, farklı bucket/key (§08_config §3)
            HistoryOutboxProperties properties,     // relayCyclePeriod, stuckThresholdMultiplier (LLD-Q5)
            NatsChannelMetrics metrics) { ... }

    /** Scheduled at relayCyclePeriod (default 30s, LLD "Phase3'ün devrettiği doğrulamalar #3").
     *  No-op if !leaderLease.isLeader() (zero DB reads on non-leader nodes, basamak-1 parity). */
    @Scheduled(fixedDelayString = "${history.outbox.relay-cycle-period-seconds:30}000")
    public void relayCycle();

    /** Reads compact_history_outbox oldest-first (idx_compact_history_outbox_created_at), dereferences
     *  compact_history_outbox_payload when payload_large_ref is set, builds the HistoryEventEnvelope
     *  wire-contract message (Nats-Msg-Id=<historyEventId>:<eventType>), publishes to
     *  history.<engineId>.<class>.<processInstanceId>. On PubAck -> DELETE the outbox row (+ CASCADE
     *  companion). On publish failure -> retry/backoff, row NOT deleted (SYS_OUTBOX_RELAY_PUBLISH_FAILED). */
    protected void relayRow(OutboxRow row);

    /** Age check against stuckThresholdMultiplier * relayCyclePeriod (default 5x30s=150s, LLD-Q5)
     *  -> SYS_OUTBOX_ROW_STUCK ops-alert (row NOT lost, exposure window signal, DP-12). */
    protected void checkStuckRows();
}
```

**Lease:** `history-relay-leader` KV bucket, anahtar `relay-leader.<engineId>` (basamak-1 LLD-Q1 dersinin ÖNCEDEN uygulanması — motor-başına izole anahtar, çapraz-motor yarış riski baştan yok). Detay: `08_config.md` §3, `DB_ACCESS_MAP.md` §4.

**Bağımlılık:** BR-REL-001, FR-B1, US-B1, ADR-0010/0002.

---

## 2. `HistoryProjectionConsumer` — instance-partition + merge-upsert (BR-REL-002/006, ADR-0011/0012)

```java
package com.threeai.nats.history.projection;

public class HistoryProjectionConsumer {

    /** One instance per partition (ARCH-Q3, N=8 default, LLD-Q2). partitionIndex identifies which
     *  filter_subject (history.>.part.<partitionIndex>) this instance is durably bound to. */
    public HistoryProjectionConsumer(
            int partitionIndex,
            JetStream jetStream,
            ProjectionStore projectionStore,       // §3
            HistoryDlqConsumer dlqConsumer,         // §4 escalation target
            PseudonymizationVaultClient vaultClient, // §03_classes/6_governance.md §3
            NatsChannelMetrics metrics) { ... }

    /** JetStream push subscription callback. Deserializes HistoryEventEnvelope (asyncapi contract),
     *  routes to ProjectionStore.upsert(...) (entity-lifecycle merge-upsert, DB_SCHEMA.md §2.3) or
     *  ProjectionStore.insertLogEvent(...) (append-only dedup insert, DB_SCHEMA.md §2.4) depending on
     *  historyClass. On success -> ack. On BUS_PROJECTION_STALE_EVENT_DISCARDED (no-op) -> ack
     *  (custody transferred, no error). On SYS_PROJECTION_WRITE_FAILED (transient DB error) ->
     *  nakWithDelay. On SYS_PROJECTION_SCHEMA_DRIFT (envelope doesn't match contract) -> route to DLQ
     *  via dlqConsumer, then ack (custody-transfer, IR-4). If class is opt-in-pseudonymized and
     *  message carries a pseudonym_token -> async-enqueues a vault write (BA-Q5, does not block ack). */
    public void onMessage(Message msg);
}
```

**Partitioning (ARCH-Q3 + LLD-Q2):** stream `HISTORY`'nin `SubjectTransform`'u `processInstanceId` token'ından `Partition(8, 3)` hesaplar (bkz. `01_overview.md` "#2"); `HistoryProjectionConsumerBootstrap` (Spring `@Configuration`) her partition için bir `HistoryProjectionConsumer` bean'i kaydeder (`partitionAssignment` config listesi veya `replicaOrdinal % N`, `08_config.md` §5).

**Bağımlılık:** BR-REL-002/006, FR-B2, US-B2, ADR-0011/0012.

---

## 3. `ProjectionStore` — JDBC erişim katmanı (BR-REL-003, ADR-0011)

```java
package com.threeai.nats.history.projection;

public class ProjectionStore {

    public ProjectionStore(DataSource projectionDataSource) { ... }

    /** Entity-lifecycle merge-upsert (process_instance_history, activity_instance_history,
     *  variable_instance_history, task_instance_history, incident_history, case_instance_history) --
     *  implements the 3-step protocol from DB_SCHEMA.md §2.3 (SELECT partition_anchor_at -> UPDATE in
     *  found partition OR INSERT). Returns UpsertOutcome (APPLIED | STALE_DISCARDED). */
    public UpsertOutcome upsertEntity(String historyClass, EntityHistoryRecord record);

    /** Append-only dedup insert (variable_detail_history, identity_link_history,
     *  operation_log_history, ext_task_log_history, job_log_history, comment_history,
     *  attachment_history, decision_evaluation_history, batch_history) --
     *  INSERT ... ON CONFLICT (engine_id, history_event_id, event_type, event_time) DO NOTHING. */
    public UpsertOutcome insertLogEvent(String historyClass, LogHistoryRecord record);

    /** Writes a large byte-array payload into projection_large_payload and returns its id, for
     *  callers to populate variable_value_ref / error_details_ref / content_ref (ARCH-Q1 referans
     *  pattern reused inside the projection store). */
    public UUID storeLargePayload(byte[] payload, String sourceTable);
}
```

**Şema:** `DB_SCHEMA.md §2`, migration'lar `db/migrations/projection/001…003_*.sql`. **Bağımlılık:** BR-REL-002/003, FR-B2/B3, US-B2/B3, ADR-0011.

---

## 4. `HistoryDlqConsumer` — `dlq.history.>` (BR-REL-005, ADR-0013/0019+0004)

```java
package com.threeai.nats.history.projection;

public class HistoryDlqConsumer {

    /** deliveryCount>maxDeliver escalation target from HistoryProjectionConsumer AND schema-drift
     *  escalation. Reuses nats-core DlqPublisher.publish(...) (basamak-1 [07§4] asset, byte-mirror +
     *  Nats-Msg-Id=<original>.dlq + custody-transfer). */
    public DlqPublishOutcome routeToDlq(Message originalMsg, DlqReason reason);
}

/** Ops-only inspection consumer (RES_HISTORY_DLQ_ACCESS_DENIED enforced at subject-ACL layer,
 *  09_security/1_transport_authz.md §2 -- this class does not implement authz itself). */
package com.threeai.nats.history.projection;
public class HistoryDlqInspectionConsumer {
    /** CB-protected (Resilience4j, basamak-1 DlqBridgeCircuitBreakerFactory reused,
     *  cb-history-dlq-inspection). */
    public void onMessage(Message dlqMsg);
}
```

**Stream:** `DLQ_HISTORY` (Limits, ayrı-stream CQ-6, default 14g — `API_CONTRACTS.md §2.2`). **Bağımlılık:** BR-REL-005, FR-B5, US-B5, ADR-0013/0019/0004.
