# Changelog

All notable changes to `nats-bpm-channels` are documented in this file.
Format loosely follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [SemVer](https://semver.org/) (pre-1.0: any 0.x change may be breaking).

## [0.8.0] — 2026-08-04 — Buildable from a clean clone; CadenzaFlow on Central

No functional change to any adapter. This release fixes the fact that nobody outside the project
could build it, and publishes the fourth engine adapter.

### Fixed

- **A clean clone could not be built.** The `cadenzaflow-mirror` profile activated on
  `!skipCadenzaflow` — that is, *by default* — and pulled `cadenzaflow-nats-channel` into the
  reactor, which pinned `cadenzaflow-engine` 1.2.0. That version was never published to Maven
  Central, and a `provided` scope still has to resolve, so `git clone && mvn install` failed on
  dependency resolution for everyone without a private repository. The README documented no build
  command at all, and the only mention of `-DskipCadenzaflow` sat in a blockquote 290 lines down.

- **Documentation overstated Flowable's coverage.** The README promised "full Flowable
  history/offload parity is on the roadmap" while the roadmap table carried no such row, and the
  datasheet listed the four offload paths under a heading with no engine scope at all — a reader
  could reasonably conclude Flowable had them. Both now state plainly that Flowable ships the
  messaging foundation and none of the four offload paths, that its outbound publishing is
  DLQ-on-failure rather than outbox-backed, and the roadmap has a tracked
  **Flowable database-offload parity** row.

### Added

- **`cadenzaflow-nats-channel` is published to Maven Central**, taking the published artifact count
  from 6 to 7. It carries the same increment 1–4 feature set as the other Camunda-lineage adapters.
- README **Building from source** section: Java 21, a Docker daemon for the Testcontainers
  integration tests, and the command itself.
- `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, issue forms and a pull request template.
  `CONTRIBUTING.md` records the constraint that cannot be inferred from the code: the three
  Camunda-lineage adapters are byte-mirrors of one another, so a change to one belongs in all three.

### Changed

- **BREAKING — three `CutoverState` values are renamed.** `ClassCutoverState.CutoverState` is public
  API of the published `nats-history-projection` artifact, and three of its values were
  Turkish-derived: `N_GUN_TEMIZ` → **`CLEAN_STREAK`**, `CUTOVER_TALEP` → **`CUTOVER_REQUESTED`**,
  `CUTOVERLANMIS` → **`CUTOVER_APPLIED`**. `CLEAN_STREAK` matches the vocabulary the table already
  used in `clean_streak_days` / `clean_streak_target`. The values are also persisted, so migration
  `V6__cutover_state_english_names.sql` swaps the `chk_class_cutover_state_state` constraint and
  rewrites existing rows. `V3` is deliberately left untouched — it is published DDL a tenant may
  have adopted into their own Flyway/Liquibase chain, where editing it in place would break the
  checksum; a fresh install runs V3 then V6, an existing install runs only V6. Apply V6 with the
  rest of your projection-schema chain. Only code that references these enum values or matches on
  the stored string is affected; the state machine itself is unchanged.
- **`cadenzaflow-engine` 1.2.0 → 1.2.1**, which *is* on Central. The `cadenzaflow-mirror` profile
  and the `-DskipCadenzaflow` flag are removed from the parent pom, both workflows, `RELEASING.md`
  and the user documentation. The full reactor now builds with no profile flags. Verified against a
  pristine local Maven repository, and 293 module tests pass against 1.2.1 with no source change.
- **Source comments are now English throughout.** Roughly 340 files had Turkish comments and
  references to internal design documents; both are gone. Decision identifiers (`D-A'..D-G'`,
  `ADR-00NN`, `DP-N`, `FINDING-00N`) are unchanged — they remain the traceability anchors. Five
  user-visible strings were affected: the history query API returned Turkish error messages, and two
  exception messages plus a log message carried an internal increment label.
- Development moved to a private repository; this one is published as a full-tree snapshot per
  release. Issues and pull requests are still handled here.

## [0.7.0] — 2026-07-25 — Maven Central publishing (`com.3eai-labs`)

First release actually published to Maven Central. No functional change to the adapters; the
release machinery was pointing at a service that no longer exists.

### Changed

- **BREAKING — groupId is now `com.3eai-labs` (was `com.3eai`).** A Central namespace can only be
  verified by proving control of the matching domain, and `3eai.com` has belonged to a third party
  since August 2022 — so `com.3eai` was never obtainable. `com.3eai-labs` maps to `3eai-labs.com`,
  verified by DNS TXT. Java packages are unaffected and remain `com.threeai.*`.
- **Publishing moved to the Central Portal.** `nexus-staging-maven-plugin` and the
  `s01.oss.sonatype.org` `distributionManagement` entries were replaced by
  `central-publishing-maven-plugin` 0.11.0. The OSSRH endpoints they targeted were shut down on
  30 June 2025 and are unreachable from a Central Portal account — every previous release attempt
  would have failed regardless of credentials.
- `maven-gpg-plugin` now passes `--pinentry-mode loopback`, without which signing dies on a
  TTY-less CI runner.
- Release workflow now uses `server-id: central` with Central Portal User Token secrets
  (`CENTRAL_USERNAME` / `CENTRAL_PASSWORD`, replacing `OSSRH_USERNAME` / `OSSRH_TOKEN`).
- `autoPublish` is `false`: a successful workflow leaves the deployment validated but unpublished,
  requiring a manual **Publish** in the portal. Publication to Central is irreversible.
- Each module now declares its own `<url>` instead of inheriting one; inherited URLs get the module
  name appended and resolve to 404.
- `<scm><developerConnection>` corrected from `ssh://github.com:3eAI-Labs/…` to
  `ssh://git@github.com/3eAI-Labs/…`; `<organizationUrl>` corrected to `https://3eai-labs.com`.

### Fixed

- **11 javadoc doclint errors that broke `-P release` in every module.** Found by a local dry run;
  they would have failed the release workflow after the tag was pushed. Causes: subject templates
  written in backticks so bare `<engineId>` parsed as an HTML tag; `{@link}` targets in test
  sources or in downstream modules that the linking module cannot see; and `ErasurePipeline`
  referencing `#anonymizeVarinstLargePayloads`, a method that does not exist (the behaviour lives
  in `findVarinstLargePayloadIds` → `deleteLargePayloads`).

### Not published

- `nats-bpm-bench` — benchmark harness, leaf module. Excluded via `<excludeArtifacts>`;
  `maven.deploy.skip` alone does not work, as `central-publishing-maven-plugin` ignores it.
- `cadenzaflow-nats-channel` — the build pins `cadenzaflow-engine` **1.2.0**, which is not on
  Central (1.1.0 is the latest published there) and resolves from a local/private repository, so
  CI cannot build the module. The engine itself *is* on Central; the blocker is a version pin, not
  an availability problem, and the module could be published once 1.2.0 lands there. The
  dependency is `provided` scope and therefore not transitive — consumers supply the engine
  themselves.

### Added

- **`META-INF/LICENSE` and `META-INF/NOTICE` in every published jar.** The POM's `<licenses>`
  declaration covers the machine-readable side — it is what Central validates and what licence
  scanners and SBOM tools read — but a jar taken on its own carried no licence text. Sources and
  javadoc jars deliberately do not carry them: adding those would require declaring `<resources>`
  in the parent POM, which overrides the default `src/main/resources` that all seven modules
  depend on. Not worth the risk for a supplementary artifact that ships alongside the main jar
  under the same coordinates and POM.
- `NOTICE` at the repository root, recording copyright and stating explicitly that no third-party
  code is bundled — the engines are `provided`-scope dependencies supplied by the consumer.
- `scripts/gpg-setup.sh` — generates the signing key, publishes the public half to a keyserver and
  exports the private half for the GitHub secret, without ever exposing the passphrase.
- `RELEASING.md` rewritten for the Central Portal; the old version instructed opening a JIRA ticket
  at `issues.sonatype.org`, a process that no longer exists.

## [0.6.0] — 2026-07-24 — CIBSeven engine adapter

### Added

- **CIBSeven engine adapter (`cibseven-nats-channel`)** — a fourth engine binding targeting
  [CIBSeven](https://cibseven.org/) 2.x (`org.cibseven.bpm:cibseven-engine`), the Apache-2.0
  Camunda 7 community fork by CIB software GmbH. Byte-for-byte a rename-mirror of
  `camunda-nats-channel` (`org.camunda.* → org.cibseven.*`, `com.threeai.nats.camunda → …cibseven`),
  so it carries the full increment 1–4 feature set (dispatch, history-offload, large-variable
  externalization, outbound-handoff) with zero behavioral divergence. The BPMN extension namespace
  is unchanged (CIBSeven retains `http://camunda.org/schema/1.0/bpmn` and the `camunda:` prefix —
  verified: `BpmnParse.CAMUNDA_BPMN_EXTENSIONS_NS` and `BpmnModelConstants.CAMUNDA_NS` are kept),
  so Camunda 7 BPMN files run unmodified. 285 tests pass against the real CIBSeven 2.2.0 engine
  (unit + Testcontainers integration on Postgres + NATS JetStream).
- Unlike the pinned `cadenzaflow-engine` 1.2.0 (resolved from a local/private repository; only
  1.0.0–1.1.0 are on Central), `cibseven-engine` 2.2.0 is on Central, so
  `cibseven-nats-channel` is a first-class default reactor module — CI builds it under
  `-DskipCadenzaflow`, and it is Maven-Central-publishable alongside the Flowable and Camunda 7
  adapters. The `-DskipCadenzaflow` boundary now cleanly separates the three public engine adapters
  from the single private/commercial CadenzaFlow one.

## [0.5.1] — 2026-07-24 — Test hardening & concurrency fixes

Patch release: coverage raised to ≥90% line on all production modules (weighted 93.0%,
up from ~76%) with meaningful real-dependency tests, plus a `@Tag("reliability")` load/stress
suite (excluded from the default build). This effort surfaced and fixed **four real production
bugs present in 0.3.0–0.5.0** — anyone on those versions should upgrade.

### Fixed

- **[HIGH] `NoClassDefFoundError` at auto-configuration when a `MeterRegistry` is present** —
  `DlqBridgeCircuitBreakerFactory` referenced `TaggedCircuitBreakerMetrics` (from the *optional*
  `resilience4j-micrometer`) unconditionally, so any deployment with Spring Boot Actuator +
  Micrometer/Prometheus (but without that optional jar transitively present) crashed at startup
  via `FailureEventBridge` / `A2SubscriptionRegistrar` / `HistoryDlqInspectionConsumer`. The
  metrics binding is now behind a `ClassUtils.isPresent(...)` classpath guard (isolated in
  `Resilience4jMicrometerMetricsBinder`); the circuit breaker works with or without the jar,
  metrics are registered only when it is present.
- **[HIGH] `ProjectionStore.upsertEntity` could split one entity across multiple rows under
  concurrent first-writes** — the `selectExisting()`→`insertNew()` check-then-act was not atomic
  and the per-writer `partition_anchor_at` differed, so the unique index never collided
  (10 concurrent first-events produced up to 7 rows). Now serialized per entity with a
  `pg_advisory_xact_lock` (zero schema change; the 3-step protocol and range-partitioning are
  unchanged).
- **[HIGH] `ProjectionStore.upsertEntity` could lose a newer state under concurrent updates** —
  `updateExisting()` had no `stream_sequence` guard, so the last physical writer won rather than
  the highest-sequence writer (violating the ADR-0012 stream-sequence tie-break / NFR-R4/R6
  idempotency under concurrency). Fixed with the same advisory lock plus an `AND stream_sequence < ?`
  CAS guard.
- **[MEDIUM] `nats-history-projection` failed to start standalone** —
  `NatsHistoryProjectionAutoConfiguration#natsConnection` lacked
  `@EnableConfigurationProperties(NatsProperties.class)` (unlike `FlowableNatsAutoConfiguration`),
  so a standalone deployment with no tenant `Connection` bean hit `NoSuchBeanDefinitionException`.

### Tests / internal

- Line coverage ≥90% on every production module (nats-core 98.9%, flowable 96.5%,
  nats-history-projection 92.9%, cadenzaflow 90.8%, camunda 90.7%); reactor branch coverage
  66.5% → 80.4%. ~351 new tests, real Testcontainers (Postgres/NATS) + fault-injection, no
  coverage-padding; unreachable defensive paths left documented rather than forced.
- New `@Tag("reliability")` load/stress suite (excluded from the default `mvn test`; run with
  `-Dgroups=reliability`): leader-lease split-brain (real KV CAS, N-candidate race → single
  winner), outbound-relay failover (RPO=0, RTO ≈ lease-TTL), custody-transfer under real broker
  outage (Docker pause/unpause), content-addressed refcount under concurrency, DLQ under
  continuous failure, and throughput/backpressure baselines.

## [0.5.0] — 2026-07-22 — Increment 4: Outbound Handoff

Lean track (an evidence-first design record with 6 locked decisions
D-A'..D-G'; direct implementation + one consolidated fresh-context review). Additive,
pure-SPI, no fork change.

### Added

- **Outbound message handoff (BPMN message-throw + send-task)** — `NatsOutboundPublisher`, a shared
  `ExecutionListener` (D-A', the transactionally-safe successor to the deleted outbound
  JavaDelegates) that a tenant wires via `camunda:executionListener event="end"
  delegateExpression="${natsOutboundPublisher}"`. It classifies the message and either writes a
  tx-local outbox row (critical) or registers a post-commit publish (best-effort) — it never
  publishes inline, so the engine command critical path is never blocked. The `ExecutionListener ->
  Context.getCommandContext()` seam was fork-source + end-to-end verified (D-A' foundation).
- **Critical outbound = at-least-once** — `outbound_message_outbox` (tx-local) + `OutboundMessageRelay`
  (leader-elected via increment 1 `SweepLeaderLease`, PubAck-after-delete custody-transfer),
  transplanting increment 2's compact-outbox+relay pattern (D-C'/D-F'). Best-effort = post-commit
  at-most-once (3rd reuse of the post-commit `TransactionListener` pattern).
- **Outbound wire contract** — subject `events.<engineId>.<type>.<processInstanceId>` (instance-keyed,
  ordered; D-E'), `events.*`/`dlq.events.*` reserved in the namespace guard (`jobs.*` stays A2's).
  `messageType` is validated against `^[A-Za-z0-9_-]+$` so a tenant type can never build a
  malformed/wildcard subject (review FINDING-003).
- **Config-split criticality** — `OutboundClassificationProperties` (tenant marks message types
  critical vs best-effort; default empty = best-effort, since outbound types are fully
  tenant-defined) (D-C').
- **Flowable outbound hardening (D-G')** — `NatsOutboundEventChannelAdapter` /
  `JetStreamOutboundEventChannelAdapter` gain DLQ custody on publish failure + outbound metrics
  wiring + `events.*` namespace-guard integration.
- **`nats-bpm-bench`** outbound DB-write-op scenario (best-effort = 0 extra writes, critical = ≤1
  row/tx).
- Scope note: Signal/Escalation throws are **out of scope** (engine-internal, DB-local); their
  cross-shard propagation belongs to increment 5. `nats-core` gains an engine-neutral `outbound`
  package (`OutboundMessageOutboxWriter`/`OutboundMessageRelay`/`OutboundPostCommitPublisher` live
  once, only `NatsOutboundPublisher`/`OutboundMessageTypeResolver` are engine-mirrored).

### Known limitations

- **Flowable critical outbound is DLQ-on-failure only, not outbox-backed at-least-once**
  (review FINDING-001, written-acknowledged): within the locked D-G' scope ("harden" = DLQ);
  full Flowable at-least-once outbox is tracked debt, pending verification of whether Flowable's
  `sendEvent` runs inside the engine command transaction (Flowable engine source not available in
  this workspace). Camunda/CadenzaFlow critical outbound **does** retain at-least-once (outbox+relay).

## [0.4.0] — 2026-07-22 — Increment 3: Large Variable Externalization

Lean track (an evidence-first design record with 7 locked decisions
D-A'..D-G'; direct implementation + one consolidated fresh-context review). Additive,
pure-SPI, no fork change; `flowable-nats-channel` untouched (D-G).

### Added

- **Large-variable externalization (BYTES/OBJECT/FILE)** — a custom `TypedValueSerializer`
  (`LargeVariableSerializer`, registered via `customPreVariableSerializers`, zero fork change)
  moves above-threshold variable payloads out of the engine DB's `ACT_GE_BYTEARRAY` to
  increment 2's Postgres projection store, leaving a small marker in an existing `ValueFields`
  column (zero schema change to `ACT_RU_VARIABLE`). Size-thresholded (configurable, default
  ~4-8KB); below-threshold values keep their built-in behaviour (D-A'/D-C'/D-E').
- **Deferred/post-commit externalization** — `LargeVariablePostCommitExternalizer` (background,
  never blocks the engine command critical path) + `LargeVariableExternalizationSweep`
  (leader-elected catch-all + RUNTIME reference reconciliation), reusing increment 2's
  post-commit/relay + leader-lease patterns (D-A').
- **Content-addressed unified store** (`nats-core` `ContentAddressedLargePayloadStore`,
  SHA-256, atomic `INSERT ... ON CONFLICT ... RETURNING` dedup + refcount) — shared by the
  HISTORY side (increment 2 `projection_large_payload`, migrated to content-addressing in `V4`)
  and the RUNTIME side, with a `runtime_large_variable_ref` ledger (`V5`) (D-B'/D-D'/D-F').
- **Refcount-based GC integrated into increment 2 retention/erasure** — a content object is
  deleted only when its last reference is released; RUNTIME references are released on
  overwrite (eager) and on hard variable/process deletion (reconciliation sweep). KVKK erasure
  now genuinely removes externalized PII (D-F').
- **`nats-bpm-bench`** large-variable threshold-calibration scenario.

### Changed

- **No breaking change to increment 1/2 public API** (SemVer minor; additive). `nats-core` gains a
  `largepayload` package; increment 2's `ProjectionStore.storeLargePayload` evolved to
  content-addressed + refcount (backward-compatible; `V4` migration merges pre-existing
  duplicate-content rows and backfills refcounts).

### Known limitations

- **Dedup benefit (3-copy → 1 object) not yet live** (review FINDING-002, written-acknowledged):
  the dedup infrastructure is correct and atomic, but the RUNTIME↔HISTORY unification benefit
  activates only once increment 2's variable-value HISTORY emission gap is closed (a future
  increment). Tracked debt; not a correctness issue.
- **Externalized-variable read** requires the projection DB to be reachable (FINDING-005);
  below-threshold variables are unaffected.

## [0.3.0] — 2026-07-21 — Increment 2: History Offload

Feature branch `feature/step2-history-offload` (30 commits: implementation +
QA fix-packages + design and QA review closure records). Full design trail: ADR-0009 through
ADR-0019, the AsyncAPI and OpenAPI wire specifications, the low-level design set, and the
code review and release-notes records. No breaking changes — this release
is entirely additive (new module + new packages in existing engine adapters); `flowable-nats-channel`
is untouched (D-G — Flowable deferred to a later Flowable-specific increment).

### Added

- **ACT_HI history offload (EPIC-A/B)**, mirrored byte-for-byte across two engine idioms
  (`camunda-nats-channel` / `cadenzaflow-nats-channel`, package `*.history`, ADR-0009/0010):
  - `NatsHistoryEventHandler` — a `CompositeHistoryEventHandler` plug-in (fork engine unmodified,
    ADR-0009) that intercepts every `ACT_HI_*` history event alongside the engine's own default
    handler (dual-run capable).
  - `CompactHistoryOutboxWriter` + `HistoryOutboxRelay` + `HistoryOutboxRelayScheduler` — hybrid
    publish topology (ADR-0010) for audit-critical classes (OP_LOG/INCIDENT/EXT_TASK_LOG): tx-local
    compact outbox write (≤1 row) plus a leader-elected (`SweepLeaderLease`, reused from
    increment 1), TTL-lease relay with custody-transfer semantics (RPO=0, at-least-once +
    idempotent merge-upsert on the projection side).
  - `HistoryPostCommitPublisher` — zero-DB, at-most-once post-commit publish for bulk classes
    (D-A accepted-loss trade-off, same pattern as increment 1's A2 post-commit publisher).
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
- **Wire contract (AsyncAPI specification)**: history channel contract
  (ADR-0013, increment 1 ADR-0006 pattern's history projection) with a mandatory
  `X-Cadenzaflow-History-Event-Time` header (engine event-time carried on the wire — dedup key and
  date-partition anchor for audit-critical append-log classes).
- **`nats-bpm-bench` history mode** — `RelayFailoverBenchScenario` real multi-replica KV-lease
  failover measurement (`@Tag("bench")`, nightly/manual): proves RPO=0 (zero audit-critical row
  loss across a real 3-replica JetStream KV failover) and documents the RTO≤60s structural lower
  bound (TTL-expiry-driven handover).
- **11 new ADRs** (ADR-0009 through ADR-0019): composite history-event-handler
  plug-in strategy, hybrid publish topology, separate-Postgres projection store, merge-upsert
  stream-sequence tie-break, history wire-contract, history query API (core-4, read-only),
  gradual cutover control plane + reconciliation, pseudonymization vault, erasure pipeline +
  scope-approval flow, class-based retention enforcement, history stream retention + subject
  authz.
- JaCoCo coverage: `nats-history-projection` reaches 83.2% line coverage (new module, above the
  80% threshold); weighted reactor average 78.9% (up from increment 1's ~74.0%).

### Changed

- **No breaking change to increment 1's public API** — no public class/method/signature was removed
  or changed incompatibly (SemVer minor holds; verified at signature level during the pre-release
  review).
- Several increment 1 `nats-core` classes were **extended additively** to serve the history/vault
  substrate: `DlqReason` (new history/vault reason values), `JetStreamStreamManager` (HISTORY /
  DLQ_HISTORY stream provisioning), `NatsChannelMetrics` (history SLIs), and `SweepLeaderLease`
  (the `heldRevision`-reset correctness fix — see Fixed). Both engine auto-configurations
  (`CamundaNatsAutoConfiguration`, `CadenzaFlowNatsAutoConfiguration`) gained opt-in wiring for the
  engine-side compact-outbox/relay + vault DataSource (inactive unless the tenant supplies the
  history beans). Existing behaviour on the increment 1 A2 path is unchanged (regression-verified,
  full reactor green).

### Fixed

- **SQL injection via unvalidated column names (BLOCKING, fixed during implementation — `17099d4`):**
  `ProjectionStore`'s dynamic column-list construction (from wire-message field keys) is
  allowlist + `SAFE_IDENTIFIER` regex validated before reaching SQL string construction; any
  non-matching field is silently dropped rather than reaching the query. Verified with a
  reactor-wide independent re-scan during QA and code review — 11 production dynamic-SQL sites
  total, all either allowlist-protected (attacker-influenceable field names) or sourced from
  compile-time-constant table/column maps or the Postgres system catalog (no external input in
  the chain); zero remaining injection vectors (verified by security scan).
- **Retention deletion / audit-log write atomicity, `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED`
  (QA review FINDING-001):** this CRITICAL (on-call-page) code path had zero test coverage;
  the DROP-partition/audit-log-write invariant was only assumed via a design-argument code
  comment, not proven. A real-Postgres fault-injection test now proves: (a)
  `RetentionAuditLogWriteFailedException` propagates uncaught when the audit-log write fails, (b)
  the partition DROP is rolled back via a compensating `connection.rollback()` on the same DDL
  connection, so no orphan deletion (deletion without an audit trail) can occur. No production bug
  found — the atomicity invariant holds; only the design-comment's mechanism description was
  corrected (compensating-rollback across two connections, not a single shared transaction).
- **Engine event_time now carried on the wire (code review FINDING-001, MAJOR):** the projection
  consumer previously derived `event_time` from `Instant.now()` at consume time, which broke
  redelivery idempotency (dedup key / partition anchor would shift on redelivery). The engine's
  real event_time is now carried via the mandatory `X-Cadenzaflow-History-Event-Time` wire header
  (both the audit-critical relay path and the bulk post-commit path set it); the consumer reads
  it from the header, routing missing/malformed values to DLQ as a wire-contract violation instead
  of silently falling back to `now()`.
- **Erasure verification scope widened beyond ACTINST assignee (code review FINDING-002):**
  `ErasurePipeline.verifyErasure` now also directly re-checks every allowlisted anonymization
  column (VARINST value, DETAIL, TASKINST name/description, COMMENT) via an allowlist-revalidated
  direct-SQL probe, not just the pre-existing HistoryQueryApi-surface ACTINST check.
- **`SweepLeaderLease.heldRevision` staleness (code review NEW-001):** the relay's
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
  (same severity class as increment 1's `SYS_SENTINEL_WORKER_CONFLICT`).
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
  data-classification DP-item (DP-9/10/16); the legal basis for the audit-critical
  legal-hold exception is explicitly flagged as pending DPO (Data Protection Officer)
  verification — this repository provides the mechanism, not a legal opinion.
- **Residual dependency-CVE risk accepted (PO, 2026-07-21):** OWASP dependency-check (SCA) could
  not complete in this environment (no NVD API key, rate-limited sync); SAST is clean and a
  manual cross-check of critical libraries (postgresql 42.7.4, logback 1.5.12, snakeyaml 2.2,
  jackson 2.17.3, spring 6.1.15/Boot 3.3.6, jnats 2.20.5) shows no known open CVEs. Product Owner
  accepted this residual risk in writing; adding an `NVD_API_KEY` CI secret is tracked as a
  DevOps backlog item.

---

## [0.2.0] — 2026-07-15 — Increment 1: External Task / Event-Driven Work Offload over JetStream

Feature branch `feature/step1-a2-implementation` (9 implementation +
5 QA test/characterization + 7 QA fix-package + 1 doc/registry correction + post-review
conditional-approval follow-up: F-1/F-2 fixes + this release finalization, see Fixed). Full
design trail: ADR-0001 through ADR-0008, the external-task-over-JetStream low-level design set,
the AsyncAPI wire specification, and the code review and release-notes records.

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
- **Wire contract (AsyncAPI specification)**: mandatory `type:
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
  subjects now default to 14-day retention on stream creation (data-classification requirement).
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
  defect as a "slow worker". Now documented as a `[MANDATORY]` deployment requirement and
  covered by `JobReplySameStreamDedupRegressionTest`.
- **F-1 (code review, MAJOR) — depth-unaware JSON field extraction for the wire-critical
  `type` discriminator:** `A2ReplyPayloadDecoder.extractJsonField` was a string search
  (`json.indexOf("\"type\"")`) that could match a same-named key nested inside an object-valued
  field (the AsyncAPI contract permits nested objects via `additionalProperties: true`), letting a
  payload like `{"data":{"type":"BPMN_ERROR"},"type":"SUCCESS"}` misclassify the reply. Field extraction
  now parses the body with Jackson and reads only direct children of the root object, so a
  nested same-named key can never shadow a top-level field. `jackson-databind` — already
  transitively present via `nats-core -> logstash-logback-encoder` and version-pinned by the
  root `spring-boot-dependencies` BOM import — is now declared directly in both engine poms.
  Mirrored byte-for-byte in `camunda-nats-channel` / `cadenzaflow-nats-channel`. Closed 2026-07-15
  (review conditional-approval condition #1).
- **F-2 (code review, MAJOR, pre-existing) — `WorkQueue`/`Limits` retention drift:**
  the AsyncAPI specification declares `streamRetention: WorkQueue` for `a2JobDispatch`/`a2JobReply`,
  but `JetStreamStreamManager.ensureStream`'s dev/test/preflight auto-create path always used
  `RetentionPolicy.Limits` regardless of subject. `ensureStream` gains a `retentionPolicy`
  parameter (symmetric to the existing `maxAge` parameter): `jobs.`-prefixed subjects now
  default to `WorkQueue`, `dlq.`-prefixed subjects keep the existing `Limits`+14-day default, all
  other subjects keep `Limits`. Production stream provisioning remains a separate ops/PR-reviewed
  YAML concern; this only aligns the repo's own auto-create default with the
  declared contract. Closed 2026-07-15 (review conditional-approval condition #2).

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
before increment 1.*
