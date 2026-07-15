# Changelog

All notable changes to `nats-bpm-channels` are documented in this file.
Format loosely follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [SemVer](https://semver.org/) (pre-1.0: any 0.x change may be breaking).

## [Unreleased] — Basamak-1: External Task / Event-Driven Work Offload over JetStream

Sentinel-governed feature branch `feature/step1-a2-implementation` (22 commits: 9 Phase 5
implementation + 5 Phase 5.5 QA test/characterization + 7 QA fix-package + 1 doc/registry
correction). Full design trail: `docs/sentinel/phase3/ADR/0001…0008`,
`docs/sentinel/phase4/lld/external-task-jetstream/`, `docs/sentinel/phase3/api/asyncapi.yaml`.

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
