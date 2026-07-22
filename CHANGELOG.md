# Changelog

All notable changes to `nats-bpm-channels` are documented in this file.
Format loosely follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [SemVer](https://semver.org/) (pre-1.0: any 0.x change may be breaking).

## [0.4.0] — 2026-07-22 — Basamak-3: Large Variable Externalization

Sentinel-governed lean track (evidence-first design doc `docs/08-large-variable-externalization.md`
with 7 locked decisions D-A'..D-G'; direct implementation + one consolidated fresh-context review
`docs/sentinel/step3/PHASE_REVIEW.md`). Additive, pure-SPI, no fork change; `flowable-nats-channel`
untouched (D-G).

### Added

- **Large-variable externalization (BYTES/OBJECT/FILE)** — a custom `TypedValueSerializer`
  (`LargeVariableSerializer`, registered via `customPreVariableSerializers`, zero fork change)
  moves above-threshold variable payloads out of the engine DB's `ACT_GE_BYTEARRAY` to
  basamak-2's Postgres projection store, leaving a small marker in an existing `ValueFields`
  column (zero schema change to `ACT_RU_VARIABLE`). Size-thresholded (configurable, default
  ~4-8KB); below-threshold values keep their built-in behaviour (D-A'/D-C'/D-E').
- **Deferred/post-commit externalization** — `LargeVariablePostCommitExternalizer` (background,
  never blocks the engine command critical path) + `LargeVariableExternalizationSweep`
  (leader-elected catch-all + RUNTIME reference reconciliation), reusing basamak-2's
  post-commit/relay + leader-lease patterns (D-A').
- **Content-addressed unified store** (`nats-core` `ContentAddressedLargePayloadStore`,
  SHA-256, atomic `INSERT ... ON CONFLICT ... RETURNING` dedup + refcount) — shared by the
  HISTORY side (basamak-2 `projection_large_payload`, migrated to content-addressing in `V4`)
  and the RUNTIME side, with a `runtime_large_variable_ref` ledger (`V5`) (D-B'/D-D'/D-F').
- **Refcount-based GC integrated into basamak-2 retention/erasure** — a content object is
  deleted only when its last reference is released; RUNTIME references are released on
  overwrite (eager) and on hard variable/process deletion (reconciliation sweep). KVKK erasure
  now genuinely removes externalized PII (D-F').
- **`nats-bpm-bench`** large-variable threshold-calibration scenario.

### Changed

- **No breaking change to basamak-1/2 public API** (SemVer minor; additive). `nats-core` gains a
  `largepayload` package; basamak-2's `ProjectionStore.storeLargePayload` evolved to
  content-addressed + refcount (backward-compatible; `V4` migration merges pre-existing
  duplicate-content rows and backfills refcounts).

### Known limitations

- **Dedup benefit (3-copy → 1 object) not yet live** (review FINDING-002, written-acknowledged):
  the dedup infrastructure is correct and atomic, but the RUNTIME↔HISTORY unification benefit
  activates only once basamak-2's variable-value HISTORY emission gap is closed (a future
  increment). Tracked debt; not a correctness issue.
- **Externalized-variable read** requires the projection DB to be reachable (FINDING-005);
  below-threshold variables are unaffected.

## [0.3.0] — 2026-07-21 — Basamak-2: History Offload

Sentinel-governed feature branch `feature/step2-history-offload` (30 commits: implementation +
QA fix-packages + phase 1–5.5 review closure records). Full design trail:
`docs/sentinel/step2/phase3/ADR/0009…0019`,
`docs/sentinel/step2/phase3/api/{asyncapi,openapi}.yaml`, `docs/sentinel/step2/phase4/lld/`,
`docs/sentinel/step2/phase6/{PHASE6_REVIEW,RELEASE_NOTES}.md`. No breaking changes — this release
is entirely additive (new module + new packages in existing engine adapters); `flowable-nats-channel`
is untouched (D-G — Flowable deferred to basamak-2b).

### Added

- **ACT_HI history offload (EPIC-A/B)**, mirrored byte-for-byte across two engine idioms
  (`camunda-nats-channel` / `cadenzaflow-nats-channel`, package `*.history`, ADR-0009/0010):
  - `NatsHistoryEventHandler` — a `CompositeHistoryEventHandler` plug-in (fork engine unmodified,
    ADR-0009) that intercepts every `ACT_HI_*` history event alongside the engine's own default
    handler (dual-run capable).
  - `CompactHistoryOutboxWriter` + `HistoryOutboxRelay` + `HistoryOutboxRelayScheduler` — hybrid
    publish topology (ADR-0010) for audit-critical classes (OP_LOG/INCIDENT/EXT_TASK_LOG): tx-local
    compact outbox write (≤1 row) plus a leader-elected (`SweepLeaderLease`, reused from
    basamak-1), TTL-lease relay with custody-transfer semantics (RPO=0, at-least-once +
    idempotent merge-upsert on the projection side).
  - `HistoryPostCommitPublisher` — zero-DB, at-most-once post-commit publish for bulk classes
    (D-A accepted-loss trade-off, same pattern as basamak-1's A2 post-commit publisher).
  - `HistoryEventFieldExtractor` / `HistoryWireMessageFactory` / `HistoryEventClassResolver` /
    `HistoryClassificationProperties` / `HistoryOutboxProperties` / `HistoryBootstrapValidator` /
    `ClassCutoverStateRegistry` — supporting classes for field extraction, wire-message assembly,
    class-based audit-critical/bulk routing, and bootstrap-time cutover-state loading.
- **`nats-history-projection`** — new Maven module (EPIC-B/C/D/G):
  - `ProjectionStore` + `HistoryProjectionConsumer` — separate-Postgres, denormalized query-store
    (ADR-0011); merge-upsert conflict resolution via NATS JetStream `stream_sequence` monotonic
    versioning, 3-step protocol: INSERT → conditional UPDATE → stale-guard (ADR-0012).
  - `HistoryQueryApi` + `HistoryQueryController` + `HistoryQueryAuthzSpi` — read-only REST/JSON
    query API, core-4 (process-instance/activity/task/variable history), pluggable authz
    (ADR-0014); `PiiMaskingService` for response-level PII masking.
  - `CutoverControlPlane` + `ClassCutoverStateStore` + `CutoverRollback` + `ReconciliationJob` —
    gradual class-based cutover control plane with a two-gate design and reconciliation
    (ADR-0015); NATS KV bootstrap-read (rolling-restart safe); rollback only returns to DUAL_RUN
    (no permanent-delete API, NFR-R5).
  - `RetentionEnforcementJob` + `RetentionAuditLogger` — class-based retention enforcement
    (bulk default 90d / audit-critical legal-hold, tenant override) with a compensating-rollback
    atomicity guarantee between partition DROP and the audit-log write (ADR-0018,
    fault-injection tested, see Fixed).
  - `ErasurePipeline` + `ErasureScopeResolver` + `ErasureAuditLogger` — bulk PII erasure pipeline
    (right-to-erasure, allowlist+regex-revalidated direct-SQL anonymization; ADR-0017).
  - `HistoryDlqConsumer` + `HistoryDlqInspectionConsumer` — DLQ routing and inspection for the
    projection consumer, circuit-breaker protected.
- **`nats-core` shared history/vault substrate**: `HistoryHeaders`/`HistoryEventEnvelope`/
  `HistoryClassNames` (wire contract + shared class-name constants), `SqlMigrationRunner`
  (classpath-based idempotent SQL migration runner, shared by all three new migration sets),
  `PseudonymizationVaultClient` + `VaultAccessAuditor` + `PseudonymTokenGenerator` +
  `PseudonymVaultDataSourceProperties` — pseudonymization vault client (ADR-0016): identity↔alias
  map in a physically isolated, separate-Postgres store (L4-adjacent, `history.vault.datasource.*`);
  keyed-hash pseudonym value computed synchronously in-tx (no I/O), vault persist happens
  downstream/async (BA-Q5 extension of D-A); deletion of the map row makes the alias
  irreversible (erasure semantics without destroying the audit trail's structure).
- **Wire contract (`docs/sentinel/step2/phase3/api/asyncapi.yaml`)**: history channel contract
  (ADR-0013, basamak-1 ADR-0006 pattern's history projection) with a mandatory
  `X-Cadenzaflow-History-Event-Time` header (engine event-time carried on the wire — dedup key and
  date-partition anchor for audit-critical append-log classes).
- **`nats-bpm-bench` history mode** — `RelayFailoverBenchScenario` real multi-replica KV-lease
  failover measurement (`@Tag("bench")`, nightly/manual): proves RPO=0 (zero audit-critical row
  loss across a real 3-replica JetStream KV failover) and documents the RTO≤60s structural lower
  bound (TTL-expiry-driven handover).
- **11 new ADRs** (`docs/sentinel/step2/phase3/ADR/0009…0019`): composite history-event-handler
  plug-in strategy, hybrid publish topology, separate-Postgres projection store, merge-upsert
  stream-sequence tie-break, history wire-contract, history query API (core-4, read-only),
  gradual cutover control plane + reconciliation, pseudonymization vault, erasure pipeline +
  scope-approval flow, class-based retention enforcement, history stream retention + subject
  authz.
- JaCoCo coverage: `nats-history-projection` reaches 83.2% line coverage (new module, above the
  80% threshold); weighted reactor average 78.9% (up from basamak-1's ~74.0%).

### Changed

- **No breaking change to basamak-1's public API** — no public class/method/signature was removed
  or changed incompatibly (SemVer minor holds; verified at signature level, `PHASE6.5_REVIEW.md`).
- Several basamak-1 `nats-core` classes were **extended additively** to serve the history/vault
  substrate: `DlqReason` (new history/vault reason values), `JetStreamStreamManager` (HISTORY /
  DLQ_HISTORY stream provisioning), `NatsChannelMetrics` (history SLIs), and `SweepLeaderLease`
  (the `heldRevision`-reset correctness fix — see Fixed). Both engine auto-configurations
  (`CamundaNatsAutoConfiguration`, `CadenzaFlowNatsAutoConfiguration`) gained opt-in wiring for the
  engine-side compact-outbox/relay + vault DataSource (inactive unless the tenant supplies the
  history beans). Existing behaviour on the basamak-1 A2 path is unchanged (regression-verified,
  full reactor green).

### Fixed

- **SQL injection via unvalidated column names (BLOCKING, fixed during Phase 5 — `17099d4`):**
  `ProjectionStore`'s dynamic column-list construction (from wire-message field keys) is
  allowlist + `SAFE_IDENTIFIER` regex validated before reaching SQL string construction; any
  non-matching field is silently dropped rather than reaching the query. Verified with a
  reactor-wide independent re-scan during Phase 5.5/6 review — 11 production dynamic-SQL sites
  total, all either allowlist-protected (attacker-influenceable field names) or sourced from
  compile-time-constant table/column maps or the Postgres system catalog (no external input in
  the chain); zero remaining injection vectors (`docs/sentinel/step2/phase55/SECURITY_SCAN.md §2`).
- **Retention deletion / audit-log write atomicity, `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED`
  (Phase 5.5 review FINDING-001):** this CRITICAL (on-call-page) code path had zero test coverage;
  the DROP-partition/audit-log-write invariant (`DATA_GOVERNANCE.md §4.4`) was only assumed via a
  design-argument code comment, not proven. A real-Postgres fault-injection test now proves: (a)
  `RetentionAuditLogWriteFailedException` propagates uncaught when the audit-log write fails, (b)
  the partition DROP is rolled back via a compensating `connection.rollback()` on the same DDL
  connection, so no orphan deletion (deletion without an audit trail) can occur. No production bug
  found — the atomicity invariant holds; only the design-comment's mechanism description was
  corrected (compensating-rollback across two connections, not a single shared transaction).
- **Motor event_time now carried on the wire (Phase 5 review FINDING-001, MAJOR):** the projection
  consumer previously derived `event_time` from `Instant.now()` at consume time, which broke
  redelivery idempotency (dedup key / partition anchor would shift on redelivery). The engine's
  real event_time is now carried via the mandatory `X-Cadenzaflow-History-Event-Time` wire header
  (both the audit-critical relay path and the bulk post-commit path set it); the consumer reads
  it from the header, routing missing/malformed values to DLQ as a wire-contract violation instead
  of silently falling back to `now()`.
- **Erasure verification scope widened beyond ACTINST assignee (Phase 5 review FINDING-002):**
  `ErasurePipeline.verifyErasure` now also directly re-checks every allowlisted anonymization
  column (VARINST value, DETAIL, TASKINST name/description, COMMENT) via an allowlist-revalidated
  direct-SQL probe, not just the pre-existing HistoryQueryApi-surface ACTINST check.
- **`SweepLeaderLease.heldRevision` staleness (Phase 5 review NEW-001):** the relay's
  `SYS_OUTBOX_RELAY_LEADER_LOST` transition warning could re-fire on every non-leader cycle after
  a genuine leadership loss (instead of once, at the actual transition) because `heldRevision` was
  never nulled on a renew-failure path. Fixed by resetting `heldRevision = null` on every
  acquire/renew failure branch — `isLeader()` now reflects the true current state. No
  data/audit-integrity impact either before or after (the relay's actual leader gate is
  `tryAcquireOrRenew()`'s return value, not `isLeader()`); this only affects observability signal
  fidelity (alarm-fatigue prevention).

### Security

- **Pseudonymization vault L4-adjacent isolation (ADR-0016):** identity↔alias map stored in a
  physically separate Postgres instance/schema (`history.vault.datasource.*`), independent
  `HikariDataSource` bean, zero shared connection pool with the projection store;
  `nats-history-projection` contains zero references to the `pseudonym_map` table (vault-unaware
  by design post-CQ-1). Unauthorized access attempts raise
  `AUTH_PSEUDONYM_VAULT_ACCESS_DENIED` — a CRITICAL, security-page-worthy invariant violation
  (same severity class as basamak-1's `SYS_SENTINEL_WORKER_CONFLICT`).
- **DP-1 (PII in logs) — verified clean for all new code:** grep sweep across
  `nats-history-projection` + both engines' `history` packages + `nats-core` `vault`/`history`
  packages for raw-value log patterns found zero matches; only class/subject/outcome metadata is
  logged, never raw userId/businessKey/variable values.
- **SAST (SpotBugs 4.9.8.2, effort=Max):** 184 findings across all six modules, zero CRITICAL/HIGH
  — the overwhelming majority (~88%) are `EI_EXPOSE_REP`/`EI_EXPOSE_REP2` (constructors not
  defensively copying shared infra objects like `DataSource`/`JetStream`, a non-issue for
  DI-managed singletons), the remainder are LOW-severity code-style findings.
- **Retention/erasure/pseudonymization mechanism ↔ KVKK/GDPR traceability:** every EPIC-G
  mechanism is traced to a User Story (US-G1/G2/G3), an ADR (0016/0017/0018), and a
  `DATA_CLASSIFICATION.md` DP-item (DP-9/10/16); the legal basis for the audit-critical
  legal-hold exception is explicitly flagged as pending DPO (Data Protection Officer)
  verification — this repository provides the mechanism, not a legal opinion.
- **Residual dependency-CVE risk accepted (PO, 2026-07-21):** OWASP dependency-check (SCA) could
  not complete in this environment (no NVD API key, rate-limited sync); SAST is clean and a
  manual cross-check of critical libraries (postgresql 42.7.4, logback 1.5.12, snakeyaml 2.2,
  jackson 2.17.3, spring 6.1.15/Boot 3.3.6, jnats 2.20.5) shows no known open CVEs. Product Owner
  accepted this residual risk in writing; adding an `NVD_API_KEY` CI secret is tracked as a
  DevOps backlog item.

---

## [0.2.0] — 2026-07-15 — Basamak-1: External Task / Event-Driven Work Offload over JetStream

Sentinel-governed feature branch `feature/step1-a2-implementation` (9 Phase 5 implementation +
5 Phase 5.5 QA test/characterization + 7 QA fix-package + 1 doc/registry correction + Phase 6
conditional-approval follow-up: F-1/F-2 fixes + this release finalization, see Fixed). Full
design trail: `docs/sentinel/phase3/ADR/0001…0008`,
`docs/sentinel/phase4/lld/external-task-jetstream/`, `docs/sentinel/phase3/api/asyncapi.yaml`,
`docs/sentinel/phase6/PHASE6_REVIEW.md`, `docs/sentinel/phase6/RELEASE_NOTES.md`.

### Added

- **A2 external-task-over-JetStream pipeline**, mirrored byte-for-byte across two engine idioms
  (`camunda-nats-channel` / `cadenzaflow-nats-channel`, package `*.a2`, ADR-0005/0007):
  - `A2ExternalTaskBehavior` + `A2BpmnParseListener` — parse-time swap of literal
    `camunda:topic="external"` service tasks into a SENTINEL-pre-locked external task, born
    already locked in the same DB transaction (zero extra DB round-trip, guard-tested).
  - `A2PostCommitPublisher` — post-commit (COMMITTED transaction-listener), DB-query-free
    dispatch to `jobs.<topic>`.
  - `A2OrphanSweep` + `SweepLeaderLease` (nats-core) — JetStream-KV-lease-elected, leader-only,
    read-only cold sweep recovering crash-orphaned tasks; re-lock-then-publish with ADR-0003
    compensating `unlock()` on publish failure (narrows the invisible-orphan window from ≤L to ≤S).
  - `A2CompletionBridge` — consumes `jobs.<topic>.reply`, dispatches to
    `complete`/`handleBpmnError`/`handleFailure` by wire `type` discriminator; custody-transfer
    ack (ack only after the engine call succeeds); `SYS_SENTINEL_WORKER_CONFLICT` is a
    CRITICAL, no-ack, page-on-call invariant violation (never expected in normal operation).
  - `A2IncidentBridge` — consumes `dlq.jobs.<topic>`, converts delivery-budget-exceeded replies
    into a Cockpit incident (`handleFailure(retries=0, retryDuration=0)`), circuit-breaker
    protected (ADR-0004).
  - `A2SubscriptionRegistrar` — bootstrap wiring (one completion-bridge per topic, one wildcard
    incident-bridge, sweep scheduler, KV bucket provisioning).
- **`FailureEventBridge` + `FailureEventCorrelationMissConsumer`** (`flowable-nats-channel`) —
  routes the shared DLQ (excluding the `dlq.jobs.>` slice A2 owns) back into Flowable as a
  failure-event; registers as the engine-wide `EventRegistryNonMatchingEventConsumer` SPI, the
  empirically-verified (bytecode-read) real trigger point for `RES_FAILURE_EVENT_CORRELATION_MISS`
  (`EventRegistry.eventReceived(...)` does **not** throw on no-match — see Fixed).
- **`nats-bpm-bench`** — new Maven module (Testcontainers: PostgreSQL + embedded engine + NATS +
  simulated workers), two-mode (`NATIVE_POLL_BASELINE` / `A2_PUSH`) DB-round-trip benchmark using
  `pg_stat_statements` query-fingerprint counting. `@Tag("bench")`, nightly/manual only — does not
  gate the main CI pipeline. Sole hard gate: `A2_PUSH` produces zero poll queries and zero
  `fetchAndLock` UPDATEs (`BUS_BENCH_METRIC_REGRESSION`).
- **`nats-core` common substrate**: `DlqPublisher`/`DlqReason`/`DlqPublishOutcome`/`DlqHeaders`
  (single shared `publishToDlq`, replacing three near-duplicated private methods), `SweepLeaderLease`
  + `JetStreamKvManager` (per-engine-family lease key on one shared `a2-sweep-leader` KV bucket —
  Camunda and CadenzaFlow never contend for the same key), `DlqBridgeCircuitBreakerFactory`
  (Resilience4j, one isolated breaker per downstream, with a `benignExceptions`/`ignoreExceptions`
  parameter so idempotent "already resolved" exceptions never count as a CB failure),
  `UmbrellaLockCalculator`/`NamespaceValidator`, `NatsTransportSecurityGuard`.
- **Wire contract (`docs/sentinel/phase3/api/asyncapi.yaml`)**: mandatory `type:
  SUCCESS|BPMN_ERROR|TRANSIENT` discriminator on every `jobs.<topic>.reply` payload (replacing an
  implicit Content-Type/errorCode-presence heuristic); optional `variables` object on
  `A2JobRequestPayload` (topic-scoped `variableAllowlist`, opt-in, default empty — PII
  minimization by default); `VAL_INVALID_REPLY_TYPE` (error code 24) for a missing/unrecognized
  `type` value, routed to DLQ instead of guessed.
- **In-tx variable capture** — `A2ExternalTaskBehavior` captures a topic-configured
  `variableAllowlist` from the process-variable scope during `execute()` (the only point a DB
  read is still legal) and hands an already-resolved `Map` to the post-commit publisher, so the
  publish path itself stays DB-query-free (BR-A2-004 unaffected). Sweep re-publish does **not**
  carry captured variables — see Known Limitations.
- Five contract-fixes, applied identically across the flowable/camunda/cadenzaflow inbound
  adapters via the new `nats-core` `DlqPublisher`/`BpmHeaders`: (1) DLQ header preservation
  (verbatim copy + 4 meta headers), (2) custody-transfer ack (ack only on successful DLQ publish;
  nak — never a silent ack-drop — on missing DLQ subject or double publish failure), (3) DLQ
  dedup id (`Nats-Msg-Id = <original>.dlq`), (4) trace-header read-fallback
  (`X-Cadenzaflow-Trace-Id`, falling back to legacy `X-Trace-Id`; write side unchanged), (5)
  empty message body now routes to DLQ with a WARN instead of a silent debug-log ack.
- `JetStreamStreamManager.ensureStream(...)` optional 4-argument `maxAge` overload; `dlq.`-prefixed
  subjects now default to 14-day retention on stream creation (DATA_CLASSIFICATION.md §5 Q3).
- JaCoCo line/branch coverage reporting wired into the reactor build (`mvn test` now also produces
  `*/target/site/jacoco/`), reporting-only, no behavior impact.

### Changed

- `JetStreamMessageCorrelationSubscriber` (Camunda/CadenzaFlow) and
  `JetStreamInboundEventChannelAdapter` (Flowable): `publishToDlq` private methods removed in
  favor of the shared `nats-core` `DlqPublisher`; ack is now conditional on
  `DlqPublishOutcome` instead of unconditional.
- `JobSuccessReply` payload `contentType` changed from `application/octet-stream` to
  `application/json` (required to carry the new mandatory `type` discriminator field).
- `NatsChannelDefinitionProcessor` (Flowable) gained a `subject -> InboundChannelModel` lookup
  (`findBySubject`) for `FailureEventBridge`, and `validateSubject(...)` now also rejects any
  Flowable channel subject starting with the A2-reserved `jobs.` prefix
  (`VAL_TOPIC_NAMESPACE_COLLISION`, BAQ-4).

### Removed — **BREAKING**

- **`JavaDelegate`-based outbound classes phased out** (US-E1/BR-MIG-001) — all seven classes and
  their test suites deleted:
  - `camunda-nats-channel` / `cadenzaflow-nats-channel` (each): `NatsPublishDelegate`,
    `JetStreamPublishDelegate`, `NatsRequestReplyDelegate` (in-tx blocking `connection.request(...)`,
    up to 30s inside the engine DB transaction).
  - `flowable-nats-channel`: `NatsRequestReplyDelegate`.
  - Corresponding Spring `@Bean` definitions removed from `CamundaNatsAutoConfiguration` /
    `CadenzaFlowNatsAutoConfiguration` / `FlowableNatsAutoConfiguration`.
  - **Migration:** any BPMN model still referencing these delegate classes
    (`camunda:class="...NatsPublishDelegate"` etc.) will fail to deploy/execute after upgrading.
    Model authors must migrate the corresponding service tasks to the A2 external-task pattern
    (`camunda:type="external" camunda:topic="..."`, topic registered in
    `spring.nats.{camunda,cadenzaflow}.a2.topics[]`) or to Flowable's native `sendEvent`/Event
    Registry idiom, before adopting this release. There is no compatibility shim.

### Fixed

- **DP-1 (PII in logs, HIGH):** `NatsMessageCorrelationSubscriber` (all three engine modules)
  logged the raw `businessKey` value at DEBUG — in telco deployments this can be an MSISDN/
  subscriber id. Replaced with a `has_business_key` boolean flag; regression-tested with a real
  Logback `ListAppender` capture.
- **JPMS reflection failure (BLOCKING — silently disabled the entire orphan-sweep safety net):**
  `A2OrphanSweep.fetchFetchableParity()` passed a live `Map.values()` view
  (`java.util.HashMap$Values`) into MyBatis' OGNL evaluator, which JDK16+/21 module boundaries
  deny reflective access to (no `--add-opens` configured anywhere in this repo) — every
  `sweepCycle()` silently threw `InaccessibleObjectException`. Fixed by materializing a plain
  `ArrayList` before crossing into MyBatis/OGNL-reflected code. Regression-guarded by
  `A2OrphanSweepFetchableParityIntegrationTest` (real embedded engine, no mocks) in both engine
  modules.
- **`RES_FAILURE_EVENT_CORRELATION_MISS` was never actually triggered (HIGH):**
  `EventRegistry.eventReceived(...)` does not throw on "no waiting subscription" — it returns
  silently (proven via a real embedded-engine characterization test plus bytecode inspection of
  the compiled Flowable 7.1.0 engine). `FailureEventBridge`'s `catch (FlowableException)` branch
  was dead code. The real trigger is now `FailureEventCorrelationMissConsumer`, registered as the
  engine's `EventRegistryNonMatchingEventConsumer` SPI.
- **Reply-classification heuristic replaced (correctness):** the previous Content-Type +
  errorCode-presence heuristic could silently misclassify a `TRANSIENT` reply that happened to
  include an errorCode-shaped field. Replaced with a mandatory wire-level `type` discriminator;
  missing/unrecognized values now route to DLQ (`VAL_INVALID_REPLY_TYPE`) instead of being guessed.
- **Job/reply same-stream dedup hazard (documented + regression-tested):** `jobs.<topic>` and
  `jobs.<topic>.reply` intentionally share the same `Nats-Msg-Id` (`= externalTaskId`); JetStream
  `duplicate_window` dedup is stream-scoped, not subject-scoped. Provisioning both subjects on one
  combined stream (a plausible simplification since `jobs.*` is a single reserved namespace)
  causes the worker's reply to be silently dropped as a duplicate of its own job — masking the
  defect as a "slow worker". Now documented as a `[ZORUNLU]` deployment requirement
  (`99_deployment.md` §2.1) and covered by `JobReplySameStreamDedupRegressionTest`.
- **F-1 (Phase 6 review, MAJOR) — depth-unaware JSON field extraction for the wire-critical
  `type` discriminator:** `A2ReplyPayloadDecoder.extractJsonField` was a string search
  (`json.indexOf("\"type\"")`) that could match a same-named key nested inside an object-valued
  field (asyncapi permits nested objects via `additionalProperties: true`), letting a payload
  like `{"data":{"type":"BPMN_ERROR"},"type":"SUCCESS"}` misclassify the reply. Field extraction
  now parses the body with Jackson and reads only direct children of the root object, so a
  nested same-named key can never shadow a top-level field. `jackson-databind` — already
  transitively present via `nats-core -> logstash-logback-encoder` and version-pinned by the
  root `spring-boot-dependencies` BOM import — is now declared directly in both engine poms.
  Mirrored byte-for-byte in `camunda-nats-channel` / `cadenzaflow-nats-channel`. Closed 2026-07-15
  (Phase 6 conditional-approval condition #1).
- **F-2 (Phase 6 review, MAJOR, pre-existing) — `WorkQueue`/`Limits` retention drift:**
  `asyncapi.yaml` declares `streamRetention: WorkQueue` for `a2JobDispatch`/`a2JobReply`, but
  `JetStreamStreamManager.ensureStream`'s dev/test/preflight auto-create path always used
  `RetentionPolicy.Limits` regardless of subject. `ensureStream` gains a `retentionPolicy`
  parameter (symmetric to the existing `maxAge` parameter): `jobs.`-prefixed subjects now
  default to `WorkQueue`, `dlq.`-prefixed subjects keep the existing `Limits`+14-day default, all
  other subjects keep `Limits`. Production stream provisioning remains a separate ops/PR'lı-YAML
  concern (`99_deployment.md` §5); this only aligns the repo's own auto-create default with the
  declared contract. Closed 2026-07-15 (Phase 6 conditional-approval condition #2).

### Security

- **ADR-0008 transport guard:** new `NatsTransportSecurityGuard` (bootstrap `InitializingBean`,
  registered in all three engine auto-configurations) rejects startup in the `production` Spring
  profile unless `spring.nats.tls.enabled=true` and either an NKey or credentials-file identity is
  configured — closes the "unauthenticated client can publish a forged reply" attack surface
  (NFR-S3/S4, DP-4/DP-5) at the transport layer; subject-level ACL (per ADR-0008 §2) is the
  complementary broker-side control (deployment-time, not code).
- **`jobs.*` namespace reservation:** `NamespaceValidator.assertNotReservedForA2(...)` rejects any
  Flowable Event Registry channel subject starting with `jobs.` at bootstrap
  (`VAL_TOPIC_NAMESPACE_COLLISION`) — prevents an accidental Flowable channel from colliding with
  the A2 job-dispatch namespace.
- **Circuit-breaker benign-exception isolation:** `DlqBridgeCircuitBreakerFactory` now accepts an
  `ignoreExceptions` list per caller so idempotent "already resolved via another path" exceptions
  (e.g. `NotFoundException` on a redelivered, already-completed DLQ message) never count toward a
  circuit breaker's failure accounting — prevents a benign redelivery storm from producing a false
  CB-OPEN against a healthy downstream.

---

*Older entries predate this file's introduction; see `git log` for the full project history
before basamak-1.*
