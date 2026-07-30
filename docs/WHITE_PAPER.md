# Moving BPM Work Off the Database Transaction

**A NATS.io messaging and offload layer for open-source BPM engines**

3eAI Labs Ltd · Version 0.7.0 · July 2026 · Apache License 2.0

---

## Executive summary

Business process engines built on the Camunda 7 lineage — Camunda 7 itself, CIBSeven, CadenzaFlow —
and Flowable share an architectural property: they persist through a relational database, and every
token move, wait state and variable write happens inside a JDBC transaction. That design is what
makes them reliable. It is also what limits them. Past a certain load, the ceiling is not the
engine; it is the single database behind it.

Two things happened in 2024 and 2025 that made this matter more. Camunda 8 moved every component,
including the Zeebe engine, behind a paid enterprise licence at a published list price above
$50,000 per year. Camunda 7 reached end of life in October 2025 and stopped receiving security
patches. A large installed base was left choosing between a licence it had not budgeted for and an
engine that no longer receives fixes.

`nats-bpm-channels` addresses both. It gives four open-source engines a shared NATS.io messaging
layer at zero licence cost — and, more consequentially, it moves four categories of high-volume
work off the engine's database transaction. Four increments have shipped: external task dispatch,
history events, large variable payloads and outbound message delivery. All of it is built on the
engines' public extension points; no engine is forked or patched.

The offload work is not only an optimisation. Each increment moves a workload from *relational
storage with per-command ACID transactions* to *a partitioned, replayable log*. That is the same
direction of travel a log-structured execution core requires, and it is deliberate preparation for
one.

---

## 1. The problem landscape

### 1.1 The licence gap

Camunda 8 version 8.6, released in October 2024, moved all self-managed components — Zeebe included
— to a paid enterprise licence. Camunda 7 reached end of life in October 2025; no further security
patches are issued. Organisations running Camunda 7 in production therefore face a choice between
an enterprise licence and an unmaintained engine.

Community forks answered the maintenance half of the problem. CIBSeven, an Apache-2.0 fork by CIB
software GmbH, is published on Maven Central and actively maintained. CadenzaFlow continues the same
lineage. Flowable, unrelated to Camunda, is Apache-2.0 and supports BPMN, CMMN and DMN in full.

What none of them answered is messaging. Flowable's Event Registry ships adapters for Kafka,
RabbitMQ and JMS — not NATS. The Camunda 7 forks inherited a messaging story designed before
modern event infrastructure was common. Teams that wanted a lightweight, push-based broker had to
write and maintain the integration themselves.

### 1.2 The performance ceiling

The deeper problem is structural rather than commercial.

These engines are database-bound by design. Every token movement is a database write. Every wait
state is a row. Every process variable is a row. Every engine command runs inside its own JDBC
transaction. Under high load the bottleneck is not CPU on the engine node; it is the throughput of
one relational database that every engine instance shares.

Common mitigations do not remove the constraint. Adding engine nodes increases pressure on the same
database. Asynchronous continuations move work between transactions but do not reduce the number of
transactions. Even the external task pattern — often described as the way to decouple work from the
engine — has workers polling the engine database for tasks, which converts worker scale-out into
database load.

One case deserves particular attention because it is easy to adopt without noticing the cost.
Synchronous request-reply from a service task holds the engine's database transaction open for the
entire duration of the external call. A worker that takes two seconds holds a database transaction
for two seconds. At a hundred concurrent instances, that is a hundred open transactions doing
nothing but waiting. The pattern is convenient and, at volume, directly opposed to the goal.

### 1.3 What the database is actually being asked to do

Not all of that load is equally necessary.

Orchestration state — where each token sits, which instance is waiting on what — must be durable and
must be transactionally consistent. That is the engine's core responsibility and it belongs in a
store that can guarantee it.

But a large share of database traffic is not orchestration state at all:

- **History rows.** `ACT_HI_*` writes are audit and reporting data. They are written inside the same
  transactions as state changes, and in many deployments they exceed runtime rows in volume.
- **Large variable payloads.** A document, an image, a serialised object placed in a process
  variable goes into the variable table and is copied into history.
- **Work dispatch.** Telling a worker there is a job to do is a messaging problem being solved with
  a polling loop against a relational table.
- **Outbound messages.** Sending an event to another system is an integration concern, but the
  reliability requirement — do not commit the transaction and lose the message — pulls it into the
  database.

Each of these is high-volume, and none of them is orchestration state.

---

## 2. Where to intervene

There are three plausible places to solve this, and they are not equally good.

**Fork the engine.** Modify the persistence layer directly. This gives complete freedom and creates
a permanent maintenance obligation: every upstream release must be merged, and every security patch
becomes your problem. For an engine lineage that already has a maintenance gap, adding another fork
is the wrong direction.

**Sit outside the engine.** Build a sidecar that observes the database or wraps the API. This avoids
touching the engine but cannot influence what the engine writes, which is the entire point.

**Use the engine's own extension points.** Every one of these engines exposes documented service
provider interfaces — history event handlers, execution listeners, variable serializers, external
task APIs. These are public, versioned contracts that upstream maintains. Work placed behind them
survives engine upgrades.

`nats-bpm-channels` takes the third path exclusively. Every offload capability is implemented
against public SPIs. No engine source is modified, no fork is maintained, and an engine upgrade does
not invalidate the integration.

This constraint has a cost, and it is worth stating plainly: it bounds how far the work can go.
The engine still owns its state machine and still writes state transitions to its database. What
can be moved is everything around that. Removing the last constraint requires a different execution
core — which is the subject of section 5.

---

## 3. Architecture

### 3.1 Two layers

```
        ┌──────────────────────────────────────────────┐
        │  BPM engine                                   │
        │  Flowable · Camunda 7 · CIBSeven · CadenzaFlow│
        │                                               │
        │  Orchestration state, BPMN semantics,         │
        │  DMN, human tasks         → engine database   │
        └──────┬───────────────────────────────┬────────┘
               │ public SPI seams              │
    ┌──────────▼───────────┐       ┌───────────▼──────────┐
    │  history events       │       │  outbound messages    │
    │  large variables      │       │  external task jobs   │
    └──────────┬───────────┘       └───────────┬──────────┘
               │                                │
        ┌──────▼────────────────────────────────▼────────┐
        │  NATS.io + JetStream                            │
        │  partitioned, replayable, at-least-once         │
        └──────┬───────────────────────────┬─────────────┘
               │                            │
    ┌──────────▼─────────┐        ┌─────────▼──────────┐
    │  history projection │        │  workers (any      │
    │  (separate Postgres)│        │  language)         │
    └────────────────────┘        └────────────────────┘
```

The engine database keeps orchestration state. Everything with volume moves through NATS.

### 3.2 Two delivery guarantees, chosen explicitly

Moving work out of a transaction creates the dual-write problem: the transaction commits but the
message is never sent, or the message is sent and the transaction rolls back. There is no way to
make two systems commit atomically without cost, so the design does not pretend otherwise. It offers
both options and makes the choice explicit and per-class.

**Critical path — transactional outbox with leader relay.** The message is written to an outbox
table in the same transaction as the state change, so it commits or rolls back with it. A
leader-elected relay then publishes to NATS and marks the row done. Delivery is at-least-once and
nothing is lost. The cost is one row insert per message inside the transaction.

**Best-effort path — post-commit publish.** The message is published after the transaction commits.
No additional database write occurs. If the process dies between commit and publish, the message is
lost. Delivery is at-most-once.

Which classes take which path is configuration, not a hardcoded assumption:

```yaml
spring:
  nats:
    camunda:
      history:
        audit-critical-classes: [ PROCESS_INSTANCE, VARIABLE ]   # outbox
```

Anything not listed takes the cheap path. A team that needs everything durable can say so and pay
for it; a team that only needs audit-critical classes durable pays only for those.

Leader election uses a NATS KV lease with compare-and-swap. Only one relay publishes at a time,
and the lease expires if the holder dies.

---

## 4. What has shipped

Four increments, each independently deployable and independently opt-in.

### 4.1 External task dispatch over JetStream — v0.2.0

Workers no longer poll the engine database for external tasks. Tasks are dispatched over JetStream
and pushed to workers.

Structurally this converts a repeated read against a relational table — performed by every worker,
continuously, whether or not there is work — into a push over a message broker. Worker scale-out no
longer translates into database load.

Lock durations are validated against acknowledgement windows at startup. A lock shorter than the
acknowledgement window means the engine can reclaim a task while the worker is still processing it;
the adapter refuses that configuration rather than letting it produce duplicate work in production.

### 4.2 History offload — v0.3.0

History events are routed to NATS and projected into a separate PostgreSQL database. `ACT_HI_*`
traffic leaves the engine database.

Classification decides the delivery path per history class, as described in section 3.2. The
projection is partitioned, and ordering within a process instance is preserved by keying on the
instance. Cutover is gradual: both stores can run in parallel with reconciliation comparing them,
and a query API reads from the projection so consumers do not need to know which store answered.

Retention, erasure and pseudonymisation are part of this capability rather than an afterthought,
because moving personal data to a second store creates a second place it must be deleted from.

### 4.3 Large variable externalisation — v0.4.0

Variables above a configurable threshold — 4 KB by default — are stored in a content-addressed store
keyed by SHA-256 rather than in the engine's variable table.

Identical content is stored once and reference-counted. A document attached to ten thousand process
instances occupies one row, not ten thousand. Externalisation happens after commit, so it does not
extend the engine transaction. Reference counting is integrated with retention: when the last
reference is released, the payload is deleted — which is what makes erasure of personal data
actually complete rather than merely marked.

### 4.4 Outbound handoff — v0.5.0

Outbound BPMN messages and events — message-throw, send-task — are delivered to NATS with the same
explicit choice between transactional outbox and post-commit publish, per message type.

This closes the dual-write problem at the outbound edge, which is where it most often bites:
a process commits, an integration message is lost, and the two systems silently diverge.

---

## 5. Where this is going: the log-structured direction

The four increments above share a property that is easy to miss when they are described
individually.

Each one takes a workload that lived in *relational storage with per-command ACID transactions* and
moves it to *a partitioned, replayable log*. History events become a stream. Task dispatch becomes a
stream. Outbound messages become a stream. Large payloads become content-addressed objects
referenced from that stream.

This is the same substrate a log-structured execution engine is built on. Zeebe made that choice for
Camunda 8: the log is the source of truth, state is a materialised projection of the log, and
partitioning is the scaling axis. The trade is well understood — you gain throughput and horizontal
scale, and you give up the ability to ask arbitrary relational questions of live state.

`nats-bpm-channels` is deliberately not that engine. What the offload work produces is design
knowledge rather than a migration path: every workload already moved onto the log is a problem
solved once under real load, and every offload seam proven in production is a seam whose semantics
are no longer guesswork.

That is worth stating precisely, because the adjacent claim would be easy to make and would not be
serviceable. A NATS-native core would be an independent project with its own wire contract. There
is no inter-project compatibility commitment here — not for workers, not for stream formats — and
neither side could honour one without freezing the other's roadmap.

The remaining constraint is the state machine itself. As long as the engine owns token movement and
writes it relationally, the floor is set by that. Removing it means a NATS-native execution core
where the log is authoritative — work that is under way separately, informed by what these four
increments proved.

The honest framing: this project makes an existing engine go considerably further than it otherwise
would, and it settles — in production, against real failure — the questions any log-structured
successor would otherwise have to answer from scratch. That is a claim about knowledge, not about
interfaces.

---

## 6. Evidence

We distinguish between what has been measured and what has not.

### Measured

| Property | Result | How |
|---|---|---|
| Audit-critical data loss on relay failover | **RPO = 0** | Real 3-replica JetStream KV failover, zero rows lost |
| Recovery time on relay failover | **RTO ≤ 60s** | Structural bound set by lease TTL |
| Leader-lease split-brain | **0** | N-candidate race against real KV compare-and-swap; single winner |
| Test suite | **1,416 tests**, all passing | Real Testcontainers (PostgreSQL, NATS), fault injection |
| Line coverage, production modules | **≥ 90% each; 93.0% weighted** | JaCoCo; branch coverage 80.4% |
| Custody transfer under broker failure | Verified | Reliability suite under real broker restart |

The reliability suite is not a smoke test. It runs split-brain races, broker restarts, relay
failover and dead-letter routing against real infrastructure. That work found and fixed four
genuine defects that existed in shipped versions, including a concurrency bug in the projection
store where concurrent writers could split an entity and lose updates.

### Not yet measured

**Throughput and latency improvement is not published.** A two-mode benchmark comparing native
polling against JetStream push exists in the repository and runs on demand, but its results have not
been published, and this document will not quote a figure that has not been measured under stated
conditions.

What can be claimed without a benchmark is structural: work that previously required a database
round trip no longer does. How much that is worth depends on the deployment, the database, and the
shape of the load. Anyone evaluating this should run the benchmark against their own environment;
that is what it is there for.

---

## 7. Getting started

All artifacts are published to Maven Central under `com.3eai-labs` and are GPG-signed.

```xml
<dependency>
    <groupId>com.3eai-labs</groupId>
    <artifactId>camunda-nats-channel</artifactId>
    <version>0.7.0</version>
</dependency>
```

| Engine | Artifact |
|---|---|
| Flowable 7.x | `flowable-nats-channel` |
| Camunda 7.x | `camunda-nats-channel` |
| CIBSeven 2.x | `cibseven-nats-channel` |
| CadenzaFlow 1.x | `cadenzaflow-nats-channel` (build from source) |

Messaging works with connection configuration alone. Each offload capability is opt-in and can be
adopted independently — most teams start with whichever load is hurting most, which is usually
history.

Requirements: Java 21+, Spring Boot 3.x, NATS 2.10+, and PostgreSQL for the projection and
content-addressed store.

See the [User Guide](user/USER_GUIDE.md) and [Quick Start](user/QUICK_START.md).

---

## 8. About

3eAI Labs Ltd is a UK company working on telecommunications and process orchestration systems.
`nats-bpm-channels` is developed in the open under the Apache License 2.0 and is used in production
in the company's own systems.

Repository: <https://github.com/3eAI-Labs/nats-bpm-channels>
Contact: oss@3eai-labs.com

---

*Copyright 2026 3eAI Labs Ltd. Camunda is a trademark of Camunda Services GmbH. Flowable is a
trademark of Flowable AG. CIBSeven is a project of CIB software GmbH. Licence terms and pricing
referenced in section 1.1 are those published by their respective vendors and were accurate at the
time of writing.*
