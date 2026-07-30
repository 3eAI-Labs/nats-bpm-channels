# Solution Brief — Telecommunications

**nats-bpm-channels 0.7.0** · Apache License 2.0 · 3eAI Labs Ltd

---

## The challenge

Telecom process automation is high-volume by nature. Order management, provisioning, OTA campaigns,
number portability, device lifecycle — each runs as a business process, and each runs at subscriber
scale rather than employee scale. A campaign touching two million subscribers is two million process
instances.

Two things follow from that, and they pull in opposite directions.

**Volume.** BPM engines built on Camunda 7 lineage or Flowable persist through a relational
database. Every token move is a write, every wait state is a row, every variable is a row, every
command runs in its own JDBC transaction. At subscriber scale, the ceiling is not the engine — it is
the single database every engine node shares. Adding nodes adds pressure to the same place.

**Subscriber identifiers are personal data, and they are everywhere.** In telecom processes the
business key is rarely an abstract order id. It is an MSISDN. Process variables carry IMSI, IMEI,
ICCID, subscriber id. This is not hypothetical for us: it is documented in this project's own data
classification, which defaults the business key to CONFIDENTIAL (conditional PII) and process
variable payloads to RESTRICTED/PII unless proven otherwise.

So the process engine's database quietly becomes a subscriber-PII store — and history makes it
worse. History is not a side effect of the PII surface; it *is* the PII surface: long retention,
random access, and a second copy of everything. It also persists **operator identities** —
`OP_LOG.userId`, `TASKINST.assignee`/`owner`, `PROCINST.startUserId`, `IDENTITYLINK.userId`. Those
are employees' personal data, retained for audit reasons and squarely within KVKK scope.

Which produces the conflict every telecom compliance team eventually meets:

> A subscriber exercises the right to erasure. The operator action that provisioned their line is in
> the audit log, and the audit log must be retained. Both obligations are real. They contradict.

In a monolithic `ACT_HI` schema this contradiction is implicit and therefore unmanaged. Moving
history to a queryable projection makes it explicit — and enforceable in SQL.

---

## What this changes

### Volume: four workloads off the database transaction

| Capability | Effect in a telecom workload |
|---|---|
| **External task dispatch over JetStream** | Provisioning and OTA workers stop polling the engine database for jobs. Scaling workers no longer scales database load; `fetchAndLock` is 0 on the happy path |
| **History offload** | `ACT_HI_*` traffic — the dominant write volume in campaign processing — leaves the engine database for a separate PostgreSQL projection |
| **Large variable externalisation** | Device profiles, provisioning payloads and attachments above 4 KB move to a content-addressed store, SHA-256 deduplicated and reference-counted. The same campaign template across two million instances is one row |
| **Outbound handoff** | Events to BSS, OSS, charging and notification systems are delivered dual-write-safe: a committed process cannot silently fail to notify downstream |

Orchestration state stays in the engine database. Everything around it moves.

### Data protection: mechanism, per class, enforceable

The layered policy this project implements addresses the erasure/audit conflict directly rather
than deferring it.

**1 — Legal-retention exception for audit-critical classes.** `OP_LOG`, `INCIDENT` and
`EXT_TASK_LOG` are exempt from erasure on a statutory-retention basis, bounded by the audit
retention period (default `P7Y`).

**2 — Bulk PII erasure pipeline.** PII in bulk classes — variable values, task name and description,
free text — runs through soft-delete → anonymisation → hard-delete on the projection database.
Because large payloads are reference-counted, releasing the last reference deletes the payload
itself. Erasure is actual deletion, not a flag.

**3 — Pseudonymisation for audit-critical classes (opt-in).** This is the mechanism that resolves
the conflict. The audit trail's *structure* is preserved while the PII field — the operator
`userId` — is replaced by an irreversible pseudonym. The identity-to-pseudonym map lives in a
**separate vault** with its own datasource and column encryption key reference.

Erasure then means deleting the map entry. The pseudonym becomes permanently unresolvable:
re-identification is impossible, and the audit trail remains internally consistent and complete.
Nothing is deleted from the audit log, and nothing in it can be traced back to a person.

**4 — Per-tenant policy.** Retention and erasure posture is the tenant's data-controller
obligation. This project supplies the mechanism; the tenant supplies the policy through a PII field
checklist — field by field, with classification, retention window, and masking or anonymisation
action.

```yaml
spring:
  nats:
    camunda:
      history:
        audit-critical-classes: [ OP_LOG, INCIDENT, EXT_TASK_LOG ]
        pseudonymization-opt-in: true
        tenant-key-id: operator-a
        tenant-key-version: 1
history:
  retention:
    audit-critical-default-window: P7Y
    bulk-default-days: 90
  vault:
    datasource:
      jdbc-url: jdbc:postgresql://vault-host:5432/pii_vault
      vault-column-encryption-key-ref: ${VAULT_KEY_REF}
```

---

## Compliance coverage

| Obligation | Mechanism |
|---|---|
| Right to erasure (KVKK, GDPR) | Bulk PII erasure pipeline with reference-counted deletion; pseudonym-vault map deletion for audit-critical classes |
| Audit log retention | Statutory-retention exception for `OP_LOG` / `INCIDENT` / `EXT_TASK_LOG`, default `P7Y` window |
| Data minimisation on the wire | Per-topic variable allowlist — only permitted variables are placed on NATS subjects |
| Classification of subscriber identifiers | MSISDN / IMSI / IMEI / subscriber id classified as RESTRICTED/PII by default; business key CONFIDENTIAL (conditional PII) |
| Operator (employee) personal data | Identified explicitly across `OP_LOG`, `TASKINST`, `PROCINST`, `IDENTITYLINK`; covered by the pseudonymisation option |
| Auditability of the erasure itself | Erasure operations are metered (`nats.history.erasure.processed`) and vault access is metered (`nats.history.vault.access`) |
| Reconciliation before cutover | Scheduled comparison between engine history and projection, with clean-streak targets gating the switch |

> **Scope statement.** This project supplies mechanisms and defaults. It does not constitute legal
> advice, and the legal basis for any statutory-retention exception remains subject to verification
> by the tenant's Data Protection Officer. That caveat is recorded in the project's own data
> classification, not added here for form.

---

## Architecture fit for telecom operations

Telecom platforms are predominantly on-premise, frequently network-segregated, and operationally
conservative. The design suits that.

- **NATS is a single binary** with no ZooKeeper, no external coordinator and no cloud dependency.
  Compare the operational footprint with a Kafka cluster before assuming they are equivalent.
- **No cloud services are required.** Everything runs in your data centre; the stack is
  air-gappable.
- **The adapter is a library, not a service.** It runs inside the application that already hosts the
  engine. The only new infrastructure is NATS and the projection database.
- **Workers are language-agnostic.** Provisioning logic in Go, an SMPP bridge in Java, analytics in
  Python — all speak plain NATS, with no engine SDK.
- **The engine is not forked.** Every capability is built on public extension points, so engine
  security updates apply normally. For an engine lineage with a maintenance history worth watching,
  that matters.

Reliability, measured against real infrastructure rather than asserted: **RPO = 0** for
audit-critical data across a real 3-replica JetStream KV failover, **RTO ≤ 60 s** bounded by lease
TTL, and **zero split-brain** under an N-candidate leader race. The full suite is 1,416 tests
against real PostgreSQL and NATS with fault injection.

---

## What to measure in an evaluation

We do not publish a throughput figure, and we will not estimate a return on a system we have not
seen. Gains depend on your database, your load shape and which capabilities you enable — quoting a
number here would be guesswork wearing a suit.

What is worth measuring in your own environment, before and after:

| Metric | Why it matters |
|---|---|
| Connection pool active connections at peak | The clearest signal of database transaction pressure |
| Database lock wait time | Where contention actually shows up |
| `ACT_HI_*` write volume as a share of total | Usually the largest single offload opportunity |
| Worker `fetchAndLock` query rate | Goes to zero on the happy path with JetStream dispatch |
| Campaign wall-clock time at subscriber scale | The number the business cares about |

A two-mode benchmark comparing native polling against JetStream push ships in the repository
(`nats-bpm-bench`) precisely so this can be measured rather than argued.

---

## Getting started

```xml
<dependency>
    <groupId>com.3eai-labs</groupId>
    <artifactId>camunda-nats-channel</artifactId>
    <version>0.7.0</version>
</dependency>
```

On Maven Central under `com.3eai-labs`, GPG-signed, Apache-2.0.

A sensible sequence: start with messaging, add history offload — usually the largest write-volume
share and the least transactionally interesting — then evaluate the remaining capabilities against
what your measurements show. Each is independent and opt-in.

3eAI Labs builds telecom systems as its primary business, including OTA, messaging, subscriber
registry and signalling gateways. This project came out of that work and is used in it.

📖 [White paper](../WHITE_PAPER.md) · [Architecture overview](../ARCHITECTURE_OVERVIEW.md) ·
[User guide](../user/USER_GUIDE.md) · <https://github.com/3eAI-Labs/nats-bpm-channels> ·
`oss@3eai-labs.com`

---

*Copyright 2026 3eAI Labs Ltd.*
