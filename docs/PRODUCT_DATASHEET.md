# Product Datasheet

**nats-bpm-channels 0.8.0** · NATS.io messaging and database offload for open-source BPM engines
Apache License 2.0 · 3eAI Labs Ltd · Maven Central namespace `com.3eai-labs`

---

## Overview

A Spring Boot library that connects a BPM engine to NATS.io and moves high-volume engine work off
the relational database transaction. Four engines are supported through their public extension
points; no engine is forked or patched. Every capability is independent and opt-in.

---

## Artifacts

| Artifact | Purpose | Maven Central |
|---|---|---|
| `com.3eai-labs:nats-channel-parent` | Dependency and plugin management (pom) | Yes |
| `com.3eai-labs:nats-core` | Connection, auth, TLS, JetStream management, metrics, headers | Yes |
| `com.3eai-labs:flowable-nats-channel` | Flowable 7.x Event Registry adapter | Yes |
| `com.3eai-labs:camunda-nats-channel` | Camunda 7.x adapter | Yes |
| `com.3eai-labs:cibseven-nats-channel` | CIBSeven 2.x adapter | Yes |
| `com.3eai-labs:nats-history-projection` | History projection, retention, erasure, reconciliation | Yes |
| `com.3eai-labs:cadenzaflow-nats-channel` | CadenzaFlow 1.x adapter | No — build from source |

All published artifacts ship `.jar`, `-sources.jar`, `-javadoc.jar` and `.pom`, each GPG-signed.
Signing key `E610505884534DB9` (`oss@3eai-labs.com`) is on `keyserver.ubuntu.com`.

---

## Capability matrix

### Messaging — all engines

| Feature | Detail |
|---|---|
| Core NATS pub/sub | Inbound and outbound channels; queue groups |
| JetStream durable delivery | Ack/nak, exponential backoff (`nakWithDelay`), configurable `ackWait`, `maxDeliver`, `deliverPolicy` |
| Dead-letter queue | JetStream primary with core-NATS fallback; in-band detection at `maxDeliver + 1`; no dlq-of-dlq, never ack-drop |
| Request-reply | BPMN service task to external worker, any language |
| Message correlation | `RuntimeService.createMessageCorrelation()`; sets `natsPayload` and `natsSubject` |
| Auth | Username/password, token, credentials file, NKey |
| Transport security | TLS with client certificate, key and CA |
| Concurrency | Java 21 virtual threads for non-blocking I/O |

### Database offload — Camunda 7, CIBSeven and CadenzaFlow

Flowable ships the messaging set above but none of these four paths; parity is on the roadmap.

| Capability | Version | Prefix | Effect |
|---|---|---|---|
| External task dispatch over JetStream | v0.2.0 | `spring.nats.<engine>.a2` | Workers stop polling the engine database; `fetchAndLock` = 0 on the happy path |
| History offload | v0.3.0 | `spring.nats.<engine>.history` | `ACT_HI_*` traffic leaves the engine database for a separate PostgreSQL projection |
| Large variable externalisation | v0.4.0 | `history.large-variable` | Variables above threshold move to a content-addressed store with SHA-256 dedup and reference counting |
| Outbound handoff | v0.5.0 | `spring.nats.outbound` | Dual-write-safe delivery of message-throw and send-task |

### Data governance

| Feature | Detail |
|---|---|
| Retention | Per class; audit-critical default `P7Y`, bulk default 90 days |
| Erasure | Reference-counted deletion — releasing the last reference deletes the payload itself |
| Pseudonymisation | Opt-in, tenant-keyed with key versioning; separate vault datasource with column encryption key reference |
| Reconciliation | Scheduled (default `0 0 3 * * *`) with clean-streak targets before cutover |
| Cutover | Gradual, dual-store with reconciliation gates and a query API that hides which store answered |

---

## Delivery guarantees

| Path | Guarantee | Cost |
|---|---|---|
| Transactional outbox + leader relay | At-least-once, no loss | One row insert inside the transaction |
| Post-commit publish | At-most-once | No additional database write |
| JetStream consume | At-least-once | Consumers must be idempotent |
| External task net path | At-least-once | post-commit at-most-once + orphan sweep at-least-once |

Selection is per history class or per message type, by configuration. There is no exactly-once mode;
duplicates are suppressed with `Nats-Msg-Id` and idempotent complete/correlate.

---

## Reliability evidence

| Property | Result | Method |
|---|---|---|
| Audit-critical loss on relay failover | RPO = 0 | Real 3-replica JetStream KV failover |
| Recovery time | RTO ≤ 60 s | Structural bound from lease TTL |
| Leader-lease split-brain | 0 | N-candidate race against real KV compare-and-swap |
| Test suite | 1,416 tests passing | Testcontainers (PostgreSQL, NATS) + fault injection |
| Line coverage | ≥ 90% per production module, 93.0% weighted | JaCoCo; branch 80.4% |

**Not measured.** Throughput and latency improvement is unpublished. Target SLIs are dispatch
p95 ≤ 200 ms, reduced connection-pool pressure and lock-wait ≈ 0. A two-mode benchmark comparing
native polling against JetStream push is included in the repository for evaluation in your own
environment.

---

## Technology stack

| Component | Technology | Version | Licence |
|---|---|---|---|
| Runtime | Java | 21+ | — |
| Framework | Spring Boot | 3.3+ | Apache 2.0 |
| Messaging | NATS.io + JetStream | 2.10+ | Apache 2.0 (CNCF incubating) |
| NATS client | `io.nats:jnats` | 2.20+ | Apache 2.0 |
| Engine | Flowable | 7.1+ | Apache 2.0 |
| Engine | Camunda 7 | 7.24+ | Apache 2.0 |
| Engine | CIBSeven | 2.2+ | Apache 2.0 |
| Engine | CadenzaFlow | 1.2+ | Apache 2.0 |
| Projection store | PostgreSQL | 12+ | PostgreSQL Licence |
| Metrics | Micrometer | via Spring Boot | Apache 2.0 |
| Test | JUnit 5, Testcontainers, Mockito, AssertJ | — | — |

Total licence cost for the stack: none.

---

## Prerequisites

- Java 21 or later
- Spring Boot 3.x application
- NATS server 2.10+ (2.10 is required for `nakWithDelay`)
- One supported BPM engine
- PostgreSQL 12+ — only if history offload or large variable externalisation is enabled
- `spring.threads.virtual.enabled: true` recommended
- Actuator and a Micrometer registry in the host application, if metrics are to be exported

---

## Deployment

| Option | Notes |
|---|---|
| Embedded library | The adapter runs inside the application hosting the engine; it is not a separate service |
| Additional infrastructure | NATS cluster and, for offload, a projection PostgreSQL instance |
| NATS replication | Factor 3 — this is what the reliability evidence was produced against; the leader-election KV lease depends on it |
| Workers | Independent processes in any language; require only a NATS client |
| Projection scaling | By partition, default 8 |
| Container / Kubernetes | No special requirement — a standard Spring Boot deployment |

---

## Observability

41 Micrometer metrics. Recommended alerts:

| Metric | Signals |
|---|---|
| `nats.history.outbox.oldest_row_age_seconds` | History relay stalled — data at risk of falling behind |
| `nats.outbound.outbox.oldest_row_age_seconds` | Outbound relay stalled |
| `nats.history.projection.lag_seconds` | Projection falling behind the stream |
| `nats.a2.sweep.oldest_orphan_age_seconds` | External tasks dispatched but not completed |
| `nats.jetstream.dlq.publish.failures` | DLQ route itself failing — the last line before loss |
| `nats.connection.slow.consumers` | Consumer cannot keep up with the subject |

Structured logging with SLF4J `kv()` and `trace_id` in MDC.

---

## Configuration surface

59 documented properties across these prefixes:

`spring.nats` · `spring.nats.<engine>.subscriptions` · `spring.nats.<engine>.a2` ·
`spring.nats.<engine>.history` · `spring.nats.outbound` · `history.projection` ·
`history.large-variable` · `history.retention` · `history.reconciliation` · `history.cutover` ·
`history.vault.datasource`

Full reference with types and defaults: [User Guide](user/USER_GUIDE.md).

---

## Licensing and support

Apache License 2.0. No licence fee, no per-instance cost, no feature gating.

Commercial services from 3eAI Labs: migration consulting from Camunda 8, NATS deployment and
tuning, and support agreements.

Repository <https://github.com/3eAI-Labs/nats-bpm-channels> · Contact `oss@3eai-labs.com`

---

*Copyright 2026 3eAI Labs Ltd. Camunda is a trademark of Camunda Services GmbH; Flowable of
Flowable AG; CIBSeven is a project of CIB software GmbH.*
