# Solution Brief — Financial Services

**nats-bpm-channels 0.7.0** · Apache License 2.0 · 3eAI Labs Ltd

---

> **Scope note, stated up front.** This project does not process cardholder data. PCI-DSS is
> explicitly marked out of scope in its own compliance manifest, and that exclusion has been
> verified by review. Nothing in this brief should be read as a PCI-DSS control, a scope-reduction
> claim, or a tokenisation capability. What follows concerns process orchestration, delivery
> integrity and audit — which is where the engine actually sits.

---

## The challenge

Financial back-office processes are orchestration problems: loan origination, onboarding and KYC,
dispute and chargeback handling, payment instruction workflows, limit and collateral review,
regulatory reporting. They involve human decisions, timers, escalations and integrations with
core banking, ledgers and messaging networks. That is exactly what a BPM engine is for.

Three properties of financial workloads collide with how these engines are built.

**Every integration is a dual-write.** A process approves a payment instruction, commits, and must
tell the payment system. If the transaction commits and the message is never sent, the ledger and
the process disagree — and nobody finds out until reconciliation, or a customer complaint. If the
message is sent and the transaction rolls back, you have instructed something that was never
approved. There is no atomic commit across two systems, so the question is only whether the
failure mode is chosen or discovered.

**The audit trail is the product.** In a regulated institution, "who approved this, when, on what
basis" is not diagnostics — it is the record you are obliged to produce. It must be complete and
tamper-evident, and it must survive the retention period.

**Volume arrives in bursts.** End-of-day batch, settlement windows, campaign-driven onboarding.
BPM engines persist through a relational database — every token move a write, every wait state a
row, every command its own JDBC transaction. Under burst load the ceiling is the single database
every engine node shares, not the engine.

---

## What this changes

### Delivery integrity: choose the failure mode

Outbound handoff makes the dual-write trade explicit and configurable per message type, instead of
leaving it to whatever the default happened to be.

**Critical types — transactional outbox with leader relay.** The message is written to an outbox
table in the same transaction as the state change, so it commits or rolls back with it. A
leader-elected relay publishes it and marks the row done. Delivery is at-least-once; nothing is
lost. Cost: one row insert inside the transaction.

**Everything else — post-commit publish.** No additional database write, at-most-once, loss window
accepted deliberately.

```yaml
spring:
  nats:
    outbound:
      critical-types:
        - PaymentInstruction
        - LimitBreachNotification
      outbox:
        relay-cycle-period-seconds: 30
        stuck-threshold-multiplier: 5
```

Leader election runs on a NATS KV lease with compare-and-swap, so exactly one relay publishes and
the lease expires if the holder dies. This is measured, not asserted: **RPO = 0** across a real
3-replica JetStream KV failover, **zero split-brain** under an N-candidate leader race, and
**RTO ≤ 60 s** bounded by the lease TTL.

Consumers must be idempotent — delivery is at-least-once by design, and duplicate suppression uses
`Nats-Msg-Id` plus idempotent completion. Exactly-once is not offered, because it is not available.

The operational signal that matters is `nats.outbound.outbox.oldest_row_age_seconds`. Alert on it:
a growing oldest row means the relay has stalled, and it tells you before divergence becomes a
reconciliation problem.

### Audit: complete, and separable from personal data

History offload moves `ACT_HI_*` traffic to a separate PostgreSQL projection. Classification decides
the delivery guarantee per class, so the classes that constitute the audit trail get the
at-least-once path while bulk reporting data takes the cheap one.

```yaml
        history:
          audit-critical-classes: [ OP_LOG, INCIDENT, EXT_TASK_LOG ]
```

`OP_LOG` is the who-did-what record: operator id, operation type, entity, property, old value, new
value. It is the audit trail, and it contains personal data — of your employees.

That produces the tension every regulated institution meets eventually: a data subject exercises the
right to erasure, and the approval action sits in a record you are obliged to retain. Both
obligations are real and they contradict.

The mechanism offered here preserves the audit trail's *structure* while making the personal field
irreversible. The operator identifier is replaced by a pseudonym; the identity-to-pseudonym map
lives in a separate vault with its own datasource and column encryption key reference. Erasure means
deleting the map entry — after which the pseudonym cannot be resolved by anyone, including you.
The audit record stays complete and internally consistent; the person is no longer identifiable.

Retention is configured per class, with a statutory-retention window for audit-critical classes:

```yaml
history:
  retention:
    audit-critical-default-window: P7Y
    bulk-default-days: 90
```

> The legal basis for any statutory-retention exception remains subject to verification by your Data
> Protection Officer. This project supplies the mechanism; the retention and erasure posture is the
> controller's decision.

### Volume: four workloads off the transaction

| Capability | Effect |
|---|---|
| External task dispatch over JetStream | Integration workers stop polling the engine database; `fetchAndLock` is 0 on the happy path, so worker scale-out no longer scales database load |
| History offload | Audit and reporting writes leave the engine database — typically the largest share of write volume |
| Large variable externalisation | Documents, statements and evidence bundles above 4 KB move to a content-addressed store, SHA-256 deduplicated and reference-counted |
| Outbound handoff | Delivery integrity as described above |

Each is independent and opt-in. Orchestration state remains in the engine database, transactional
and authoritative.

### Data minimisation on the wire

Process variables cross a trust boundary when work leaves the engine for an external worker. The
per-topic variable allowlist controls exactly which variables are placed on a NATS subject —
default-deny rather than publish-everything.

Transport security is not optional in production: TLS with NKey or JWT identity, enforced at
bootstrap; an unauthenticated or plain connection is refused. Subject-level authorisation restricts
each worker account to its own topic's subjects, so a worker cannot write a reply for a topic it
does not own. Behind that sits a second layer: a completion is only accepted for an existing,
correctly-locked task, and a mismatch raises a critical alert rather than silently advancing a
token.

---

## Architecture fit

Financial institutions run on-premise, segregate networks, and change infrastructure slowly.

- **NATS is a single binary** — no external coordinator, no cloud service dependency
- **The adapter is a library**, running inside the application that already hosts the engine; the
  only new infrastructure is NATS and the projection database
- **The engine is not forked** — every capability is built on public extension points, so engine
  security updates apply normally
- **Workers are language-agnostic** — a Java integration adapter, a Go reconciliation worker, a
  Python reporting job, all speaking plain NATS with no engine SDK
- **Apache-2.0 throughout** — engine, broker, client and this library; no licence renegotiation, no
  per-instance metering, no feature gating

Reliability evidence: 1,416 tests against real PostgreSQL and NATS with fault injection, ≥ 90% line
coverage on every production module, and the failover results quoted above measured against real
infrastructure rather than mocks.

---

## What to measure in an evaluation

We do not publish throughput figures and will not estimate a return on a system we have not seen.
What is worth measuring, before and after, in your environment:

| Metric | Why |
|---|---|
| Connection pool active connections during settlement or batch windows | The clearest signal of transaction pressure |
| Database lock wait time at peak | Where burst contention actually appears |
| History write volume as a share of total engine writes | Usually the largest offload opportunity |
| Reconciliation breaks attributable to missed outbound messages | The dual-write failure mode, quantified |
| `nats.outbound.outbox.oldest_row_age_seconds` at peak | Whether relay throughput keeps up with commit rate |

A two-mode benchmark comparing native polling against JetStream push ships in the repository so this
can be measured rather than argued.

---

## Getting started

```xml
<dependency>
    <groupId>com.3eai-labs</groupId>
    <artifactId>camunda-nats-channel</artifactId>
    <version>0.7.0</version>
</dependency>
```

On Maven Central under `com.3eai-labs`, GPG-signed, Apache-2.0. Supported engines: Flowable 7.x,
Camunda 7.x, CIBSeven 2.x, CadenzaFlow 1.x.

For most institutions the first capability worth adopting is outbound handoff — it addresses a
correctness problem rather than a performance one, and correctness problems are the ones that
surface in audit findings.

📖 [White paper](../WHITE_PAPER.md) · [Architecture overview](../ARCHITECTURE_OVERVIEW.md) ·
[User guide](../user/USER_GUIDE.md) · <https://github.com/3eAI-Labs/nats-bpm-channels> ·
`oss@3eai-labs.com`

---

*Copyright 2026 3eAI Labs Ltd. This brief describes software mechanisms and is not legal,
regulatory or compliance advice.*
