# 03.4 — EPIC-D: Reconciliation + Cutover

**Modül:** `nats-history-projection` (`com.threeai.nats.history.cutover.*`) — DB tarafı; `ClassCutoverStateRegistry` (engine-side READER) → `camunda-nats-channel`/`cadenzaflow-nats-channel`.
**Kaynak ADR:** 0015 (kademeli cutover + reconciliation, ARCH-Q5), 0009 (handler senaryosu).
**HLD:** §3.4.1…§3.4.3. **State machine kaynağı:** `BUSINESS_LOGIC.md §2.1` (bu dosya TEKRARLAMAZ, `06_state_machines.md`'ye köprü verir).

---

## 1. `ReconciliationJob` (BR-CUT-001/004, ADR-0015)

```java
package com.threeai.nats.history.cutover;

public class ReconciliationJob {

    public ReconciliationJob(DataSource projectionDataSource, DataSource engineDataSourceReadOnly,
            ClassCutoverStateStore stateStore, NatsChannelMetrics metrics) { ... }

    /** Scheduled (default daily). Per class: counts projection rows vs ACT_HI rows (read-only JDBC
     *  against engine DB, DP-14 -- counters/ids only, never PII values). "Clean" definition (BA-Q2):
     *  audit-critical -> diff==0; bulk -> diff<=epsilon (default 0, per-class override) AND no
     *  increasing trend (compares against last_diff_count). On clean -> clean_streak_days++; else ->
     *  streak reset to 0 (BUS_RECONCILIATION_DIFF_DETECTED). On DB read failure for either side ->
     *  SYS_RECONCILIATION_JOB_FAILED (log-only, streak untouched, cycle skipped). Sustained/threshold
     *  diff -> RES_RECONCILIATION_DIFF_THRESHOLD_EXCEEDED (ops alert). */
    @Scheduled(cron = "${history.reconciliation.cron:0 0 3 * * *}")
    public void reconcileAllClasses();

    /** clean_streak_days >= clean_streak_target (default 7, PO-Q4) -> transitions
     *  class_cutover_state.state DUAL_RUN/RECONCILING -> N_GUN_TEMIZ. */
    protected void evaluateGate(ClassCutoverState state, long diffCount);
}
```

**Bağımlılık:** BR-CUT-001/004, FR-D1, US-D1, ADR-0015. `class_cutover_state` şeması: `DB_SCHEMA.md §2.7`.

---

## 2. `CutoverControlPlane` (BR-CUT-002 + BR-HDL-005, ADR-0015+0009, ARCH-Q5)

```java
package com.threeai.nats.history.cutover;

public class CutoverControlPlane {

    public CutoverControlPlane(ClassCutoverStateStore stateStore, JetStreamKvManager kvManager,
            HistoryCutoverProperties properties /* volume-priority queue order */) { ... }

    /** Operator/automation-triggered. Rejects if gate not open (state != N_GUN_TEMIZ) ->
     *  BUS_CUTOVER_GATE_NOT_MET. On success: (1) writes KV key
     *  history-cutover-state/cutover.<engineId>.<class>=true (LLD-Q3 mechanism, §2.2 below),
     *  (2) sets class_cutover_state.state=CUTOVER_TALEP, (3) triggers/signals a rolling-restart of
     *  engine node replicas (ARCH-Q5 -- deployment-specific trigger, 99_deployment.md §3). On KV
     *  write or state-transition failure -> SYS_CUTOVER_CONFIG_APPLY_FAILED, dual-run continues
     *  (fail-safe, state stays N_GUN_TEMIZ). On confirmed rolling-restart completion (health-check
     *  signal, deployment-specific) -> state=CUTOVERLANMIS, cutover_applied_at=now(). */
    public CutoverOutcome requestCutover(String engineId, String historyClass);
}
```

### 2.2 KV yansıması (LLD-Q3) — `ClassCutoverStateRegistry` (engine-side READER)

```java
package com.threeai.nats.camunda.history;

/** Read ONCE at engine bootstrap (ProcessEngineConfigurationImpl.init() happens exactly once,
 *  01_overview.md fork-evidence) via JetStreamKvManager.ensureBucket("history-cutover-state", ...)
 *  + kv.get(...) for every configured class. NOT watched live in v1 (ARCH-Q5 rolling-restart is the
 *  locked apply mechanism) -- forward-compatible with a future kv.watch(...) upgrade (no fork change
 *  required, see 01_overview.md "Hot-reconfigure" closure). */
public class ClassCutoverStateRegistry {

    public ClassCutoverStateRegistry(JetStreamKvManager kvManager, Connection connection, String engineId) { ... }

    /** Called once at bootstrap; builds an immutable in-memory Map<String,Boolean>. */
    public void loadAtBootstrap();

    /** Consulted by NatsHistoryEventHandler.handleEvent(...) on every event -- O(1) map lookup, no I/O. */
    public boolean isCutOver(String historyClass);
}
```

**Bağımlılık:** BR-CUT-002, BR-HDL-005, FR-D2/FR-A6, US-D2/US-A5, ADR-0015/0009. KV bucket şeması: `08_config.md §3`.

---

## 3. `CutoverRollback` (BR-CUT-003, ADR-0015)

```java
package com.threeai.nats.history.cutover;

public class CutoverRollback {

    public CutoverRollback(ClassCutoverStateStore stateStore, JetStreamKvManager kvManager) { ... }

    /** Operator-triggered. Writes KV cutover.<engineId>.<class>=false, sets
     *  class_cutover_state.state=DUAL_RUN, clean_streak_days=0, rollback_count++,
     *  last_rollback_at=now() -- audit-logged (BUS_CUTOVER_ROLLBACK_TRIGGERED, INFO, operator-planned).
     *  Requires the SAME rolling-restart trigger as forward cutover (ARCH-Q5 -- symmetric mechanism). */
    public void rollback(String engineId, String historyClass, String operatorId, String reason);
}
```

**Kalıcı dual-run reddi (NFR-R5):** `CutoverRollback` sınıf durumunu yalnız `DUAL_RUN`'a döndürür — sınıfı "cutover kuyruğundan sil" opsiyonu YOKTUR (BR-HDL-005: sınıf yeniden `RECONCILING`'e girer, nihayetinde tekrar cutover'lanır).

**Bağımlılık:** BR-CUT-003, FR-D3, US-D3, ADR-0015.
