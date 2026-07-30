# Your engine's ceiling is your database

*Part 2 of 3 — [Part 1: The licence wall](01-the-licence-wall.md) · [Part 3: From tables to a log](03-from-tables-to-a-log.md)*

---

Here is a pattern that looks completely reasonable and will quietly cap your throughput.

```xml
<serviceTask id="sendSms" camunda:delegateExpression="${natsRequestReply}">
  <extensionElements>
    <camunda:field name="subject" stringValue="task.send-sms" />
    <camunda:field name="timeout" stringValue="30s" />
  </extensionElements>
</serviceTask>
```

A service task calls out to a worker and waits for the reply. Clean, synchronous, easy to reason
about. We shipped it, people used it, and it works.

It also holds a database transaction open for the entire call.

The engine is inside a JDBC transaction when it reaches that service task. It stays there while the
request goes out, while the worker does its thing, and while the reply comes back. A worker that
takes two seconds holds a database transaction for two seconds. Run a hundred instances
concurrently and you have a hundred open transactions doing nothing but waiting on the network.

Your connection pool notices before you do.

## This is not a bug, it is the architecture

Camunda 7, CIBSeven, CadenzaFlow and Flowable are all database-bound by design, and that design is
why they are trustworthy. Every token movement is a write. Every wait state is a row. Every process
variable is a row. Every engine command gets its own transaction. When the process crashes, the
database tells you exactly where everything was.

The cost shows up at volume. Your bottleneck is not CPU on the engine nodes. It is the throughput of
one relational database that every engine instance shares. Adding engine nodes adds pressure to the
same database.

The usual advice does not remove the constraint:

- **Async continuations** move work between transactions. The number of transactions does not go
  down.
- **More engine nodes** all write to the same place.
- **External tasks** are usually described as *the* decoupling pattern — and workers poll the engine
  database for work. Scaling workers scales database load. You made it worse.

## What is actually in there

Not all of that traffic is equally necessary. That is the opening.

Orchestration state — where each token is, which instance waits on what — has to be durable and
transactionally consistent. That is the engine's job and it should stay exactly where it is.

But look at what shares the transaction with it:

- **History rows.** `ACT_HI_*` is audit and reporting data. In plenty of deployments it outweighs
  runtime data, and it is written inside the same transactions as your state changes.
- **Large variable payloads.** A PDF in a process variable goes into the variable table and gets
  copied into history.
- **Work dispatch.** Telling a worker there is a job is a messaging problem being solved with a
  polling loop against a relational table.
- **Outbound messages.** Sending an event to another system is integration — but the requirement
  "don't commit and lose the message" drags it into the database.

High-volume, all of it. Orchestration state, none of it.

## Four things moved off the transaction

Each of these is a separate release, independent, and opt-in.

**External task dispatch (v0.2.0).** Workers stop polling. Tasks go out over JetStream and get
pushed. On the happy path `fetchAndLock` is zero — no worker touches the database to find work.

```yaml
spring:
  nats:
    camunda:
      a2:
        topics: [ send-sms ]
        defaults:
          lock-duration-seconds: 300
```

**History offload (v0.3.0).** `ACT_HI_*` traffic leaves the engine database and gets projected into
a separate PostgreSQL instance. You choose per history class whether it goes through a
transactional outbox (at-least-once, one row insert in the transaction) or post-commit
(at-most-once, zero extra writes):

```yaml
        history:
          audit-critical-classes: [ PROCESS_INSTANCE, VARIABLE ]
```

**Large variable externalisation (v0.4.0).** Variables over 4 KB go to a content-addressed store
keyed by SHA-256, not the variable table. Identical content is stored once and reference-counted —
the same document on ten thousand instances is one row.

**Outbound handoff (v0.5.0).** Message-throw and send-task delivery, dual-write-safe, same explicit
choice per message type.

All four are built on the engines' public SPIs. No forked engine, no patched engine. Upgrading your
engine does not break the integration, which matters a lot for a lineage that already has a
maintenance story to manage.

## The part we are not going to overclaim

We have not published a throughput number, and we are not going to invent one.

What is measured, under real infrastructure:

| | |
|---|---|
| Audit-critical loss on relay failover | **RPO = 0** (real 3-replica JetStream KV failover) |
| Leader-lease split-brain | **0** (N-candidate race, real KV compare-and-swap) |
| Recovery time | **RTO ≤ 60 s** (bounded by lease TTL) |
| Test suite | **1,416 tests**, real Testcontainers and fault injection |

What is not measured: how much faster your system gets. That depends on your database, your load
shape and which capabilities you turn on. There is a benchmark in the repository that compares
native polling against JetStream push. Run it against your environment — that is what it is for.

What we *can* say without a benchmark is structural: work that required a database round trip no
longer does.

## The honest ceiling

Here is the limit, stated plainly.

The engine still owns its state machine. Token movements still get written relationally, in a
transaction. Completing an external task is still one short transaction. We did not remove that and
cannot remove it from behind an SPI — that is the boundary of the approach.

So: everything *around* the state moves. The state itself does not.

Removing that last constraint means a different execution core, one where the log is the source of
truth rather than a table. Which is [the next post](03-from-tables-to-a-log.md) — and, it turns out,
the reason the four increments above are shaped the way they are.

---

📖 [User Guide](../user/USER_GUIDE.md) · [White paper](../WHITE_PAPER.md) ·
[GitHub](https://github.com/3eAI-Labs/nats-bpm-channels)

*Next: [From tables to a log](03-from-tables-to-a-log.md)*
