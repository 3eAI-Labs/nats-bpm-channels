# Solution Brief — Public Sector

**nats-bpm-channels 0.7.0** · Apache License 2.0 · 3eAI Labs Ltd

---

> **Scope note.** This brief describes software properties that can be verified in the source and in
> the deployment topology. It does not claim conformance to any national data-protection statute,
> certification scheme or government assurance programme. Where a regulation is named, it is one the
> project's own compliance manifest lists as in scope.

---

## The challenge

Public-sector process automation runs at population scale and under obligations that private-sector
systems rarely carry simultaneously: permit and licence issuance, benefit and entitlement
determination, registry updates, inter-agency case referral, inspection workflows. Each is a
business process with human decisions, statutory deadlines and appeal paths — the natural shape of
a BPM engine.

Four constraints make the standard architecture uncomfortable.

**Where the data lives is not negotiable.** Citizen data frequently cannot leave national
infrastructure, and often cannot leave a specific agency's data centre. Any component with a runtime
dependency on an external service is a procurement problem before it is a technical one.

**Accountability is the deliverable.** "Which official decided this, when, and on what basis" is
what an ombudsman, an auditor or a court will ask for. The record must be complete, must resist
alteration, and must survive a long retention period.

**The personal data includes your own staff.** The audit trail identifies officials — that is its
purpose. Those officials are data subjects too, with their own rights, and their identifiers sit
inside the record you are obliged to retain.

**Scale is citizen-scale, and lifetimes are long.** These systems are procured for a decade or
more. BPM engines persist through a relational database — every token move a write, every wait state
a row, every command its own transaction — so under population-scale load the ceiling is the single
shared database, and licensing decisions made today are inherited by the next decade's budget.

---

## What this changes

### Sovereignty by architecture, not by assurance

This is verifiable rather than promised. In the published production code there is **no HTTP client
of any kind** — no `HttpClient`, `RestTemplate`, `WebClient`, `URLConnection` or third-party HTTP
library. The only `http://` strings in the source are XML namespace identifiers, which are labels
and are never resolved. The runtime dependency set is the NATS Java client, Spring Boot
auto-configuration, SLF4J, Micrometer, Logback, Resilience4j and the PostgreSQL driver.

Nothing calls out. Nothing phones home. There is no telemetry endpoint, no licence server, no
activation check. That is a property you can confirm by grep before you procure, and confirm again
after any upgrade.

Operationally:

- **NATS is a single binary** — no external coordinator, no managed service, no cloud control plane
- **The adapter is a library**, running inside the application that already hosts the engine
- **The only new infrastructure** is a NATS cluster and a PostgreSQL instance, both self-hosted
- **The deployment is therefore isolatable** — the runtime has no dependency on anything outside your
  network

### Accountability that survives erasure requests

History offload moves `ACT_HI_*` to a separate PostgreSQL projection and classifies each history
class by delivery guarantee. The classes that constitute the accountability record take the
at-least-once transactional-outbox path; bulk reporting data takes the cheaper post-commit path.

```yaml
spring:
  nats:
    camunda:
      history:
        audit-critical-classes: [ OP_LOG, INCIDENT, EXT_TASK_LOG ]
```

`OP_LOG` is the who-did-what record — official identifier, operation type, entity, property, previous
value, new value. It is exactly the artefact an audit or an appeal will request.

Which creates the conflict this sector meets more often than most:

> An official exercises the right to erasure. Their identifier appears in decision records the
> institution is legally obliged to retain, possibly for decades. Both obligations are binding.

The mechanism offered here preserves the record's *structure* and completeness while making the
personal field irreversible. The official's identifier is replaced by a pseudonym; the
identity-to-pseudonym map lives in a separate vault with its own datasource and column encryption
key reference. Erasure means deleting the map entry — after which nobody, including the operator of
the system, can resolve the pseudonym.

The decision record remains complete, internally consistent and auditable. The individual is no
longer identifiable from it. Neither obligation is traded away for the other.

```yaml
        pseudonymization-opt-in: true
        tenant-key-id: agency-a
        tenant-key-version: 1
history:
  retention:
    audit-critical-default-window: P7Y     # set to your statutory period
    bulk-default-days: 90
  vault:
    datasource:
      jdbc-url: jdbc:postgresql://vault-host:5432/pii_vault
      vault-column-encryption-key-ref: ${VAULT_KEY_REF}
```

> The legal basis for any statutory-retention exception is subject to verification by your data
> protection authority or officer. This project provides the mechanism; the retention and erasure
> policy is the controller's decision, recorded field by field in a per-tenant PII checklist.

### Access control between agencies

Transport security is mandatory in production, enforced at bootstrap: TLS with NKey or JWT identity;
an unauthenticated or plain connection is refused rather than warned about.

Authorisation is subject-scoped. Each worker account may publish and subscribe only on its own
topic's subjects, so a worker cannot inject a result for a topic it does not own. Behind that sits a
second layer: a completion is accepted only for an existing, correctly-locked task, and a mismatch
raises a critical alert instead of silently advancing a case.

Per-tenant subject isolation — a separate account hierarchy per agency — is a deployment decision
rather than a built-in policy. The mechanism (subject-level access control) is provided; the account
scheme is applied at deploy time to match your organisational boundaries. That distinction is
deliberate and is recorded in the project's architecture decisions.

### Scale, without a licence renegotiation

| Capability | Effect |
|---|---|
| External task dispatch over JetStream | Integration workers stop polling the engine database; `fetchAndLock` is 0 on the happy path |
| History offload | Audit and reporting writes leave the engine database — usually the largest share of write volume |
| Large variable externalisation | Scanned documents, evidence bundles and attachments above 4 KB move to a content-addressed store, deduplicated by SHA-256 and reference-counted |
| Outbound handoff | Inter-agency notifications delivered dual-write-safe: a committed decision cannot silently fail to notify |

Each is independent and opt-in. Orchestration state stays in the engine database, transactional and
authoritative.

The whole stack is Apache-2.0 — engine, broker, client and this library. There is no per-instance
metering, no feature gating and no renewal negotiation, which matters for systems whose budget
horizon is measured in decades. The engine is not forked, so upstream security updates apply
normally.

---

## Reliability

Measured against real infrastructure rather than asserted:

| Property | Result |
|---|---|
| Audit-critical data loss on relay failover | RPO = 0, across a real 3-replica JetStream KV failover |
| Recovery time | RTO ≤ 60 s, bounded by lease TTL |
| Leader-lease split-brain | 0, under an N-candidate race against real compare-and-swap |
| Test suite | 1,416 tests, real PostgreSQL and NATS, fault injection |
| Line coverage | ≥ 90% per production module, 93.0% weighted |

Delivery is at-least-once by design. Exactly-once is not offered, because it is not achievable
across two systems; duplicates are suppressed by message id and idempotent completion.

---

## What to measure in an evaluation

We publish no throughput figure and will not estimate a return on a system we have not seen. What is
worth measuring in your environment, before and after:

| Metric | Why |
|---|---|
| Connection pool active connections at peak | The clearest signal of database transaction pressure |
| Database lock wait time | Where contention appears under population-scale load |
| History write volume as a share of total | Usually the single largest offload opportunity |
| Case throughput during statutory deadline periods | The number that determines service levels |
| `nats.history.outbox.oldest_row_age_seconds` | Whether the audit path keeps pace with commit rate |

A two-mode benchmark comparing native polling against JetStream push ships in the repository so this
can be measured in your own environment rather than argued from a slide.

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
Camunda 7.x, CIBSeven 2.x, CadenzaFlow 1.x. Artifacts can be mirrored into an internal repository
for isolated environments.

A sensible sequence for public-sector deployments: start with the audit and accountability path —
history offload with audit-critical classification — because it addresses an obligation rather than
a performance target, and obligations are what appear in findings.

📖 [White paper](../WHITE_PAPER.md) · [Architecture overview](../ARCHITECTURE_OVERVIEW.md) ·
[User guide](../user/USER_GUIDE.md) · <https://github.com/3eAI-Labs/nats-bpm-channels> ·
`oss@3eai-labs.com`

---

*Copyright 2026 3eAI Labs Ltd. This brief describes software mechanisms and is not legal,
regulatory or compliance advice.*
