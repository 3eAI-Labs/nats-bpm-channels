# Four BPM engines, one NATS layer

*Part 1 of 3 — [Part 2: Your engine's ceiling is your database](02-your-ceiling-is-your-database.md) · [Part 3: From tables to a log](03-from-tables-to-a-log.md)*

---

A lot of production BPMN runs on engines that persist through a relational database: Flowable, and
the Camunda 7 lineage — Camunda 7 itself plus the maintained forks. These are mature engines. Teams
that run them know them well, have years of process models in them, and have no particular reason to
move.

We build on them. This is what we found while doing it.

## The lineage is in better shape than people assume

Camunda 7 reached the end of its community lifecycle in October 2025. The lineage did not end there.

**CIBSeven** is an Apache-2.0 fork by CIB software GmbH, published on Maven Central and actively
maintained. The part that matters most in practice: it keeps the Camunda BPMN extension namespace.
Your `camunda:` prefixed BPMN files run unmodified:

```xml
<serviceTask id="notify" camunda:delegateExpression="${myDelegate}">
```

That is not a small detail. A fork that renames the namespace turns every BPMN file in your
repository into a migration task. CIBSeven renamed the Java packages (`org.camunda.*` →
`org.cibseven.*`) and left the BPMN dialect alone.

**CadenzaFlow** continues the same lineage with rebranded packages and ongoing security maintenance.

**Flowable** is a different thing entirely — not a fork, a separate Apache-2.0 engine with full
BPMN, CMMN and DMN.

Four engines, all open source, all actively developed.

## What we built on NATS

Each of these engines gives you a seam to plug messaging into. Flowable has the Event Registry, a
channel abstraction for getting events in and out. The Camunda 7 lineage has public extension
points. We wrote the NATS binding for both — once, with the same behaviour across all four engines,
so a team running more than one does not maintain more than one integration.

The part that takes the time is not the happy path. It is deciding what happens when a consumer
nak-s, when a message exhausts its retries, and where a DLQ publish goes when JetStream is the thing
that is down. Those decisions are made once here, and they are the same on every engine:

```xml
<dependency>
    <groupId>com.3eai-labs</groupId>
    <artifactId>cibseven-nats-channel</artifactId>
    <version>0.8.0</version>
</dependency>
```

```yaml
spring:
  nats:
    url: nats://localhost:4222
    cibseven:
      subscriptions:
        - subject: order.new
          messageName: OrderReceived
```

Publish to `order.new`, a waiting process instance gets correlated. Add `jetstream: true` and you
get durable delivery with retries and a dead-letter queue. Point a service task at
`${natsRequestReply}` and a worker in Go or Python picks the work up.

Every adapter declares its engine as a `provided` dependency, so adding one pulls no engine into
your build and cannot override the version you already run.

## Then we started taking load off the database

Messaging was the starting point, not the interesting part.

Once teams had NATS working, the questions changed shape. Nobody asked for more message patterns.
They asked why the engine slowed down under load while the engine nodes were barely warm.

The answer was the database. These engines persist through it, and *everything* goes through it —
every token move, every wait state, every variable, each in its own JDBC transaction. Add engine
nodes and they all push into the same tables. That is also what makes them trustworthy: durability
and the throughput ceiling come from the same property, and you do not get to keep one and drop the
other.

So we asked a narrower question. Orchestration state has to stay in the engine database — but how
much of the work merely *sharing* that transaction actually has to be there?

Less than we expected. Four things have moved out, for the Camunda-lineage adapters:

**External task dispatch.** Workers used to poll the engine database to find work. Now the adapter
publishes the job to JetStream once the engine transaction commits, and workers consume from NATS.
The engine still does the final token move in a short transaction; the repeated polling is gone.

**History.** `ACT_HI_*` traffic is usually one of the largest write volumes in a BPM installation,
and by default it is written inside the same transactions as runtime state. We route history events
to JetStream and project them into a separate PostgreSQL database. Audit-critical classes go through
a transactional outbox for at-least-once delivery; bulk classes take a cheaper post-commit path. The
projection ships with its own query API, reconciliation, cutover, retention and erasure support.

**Large variables.** Payloads above a threshold move out of the engine's variable table into a
content-addressed store keyed by SHA-256. Identical content is stored once and reference-counted,
and the payload is deletable once the last reference goes. That matters for storage and for erasure
— you cannot honestly claim to have erased personal data if a large payload still sits outside the
row you anonymised.

**Outbound messages.** Message-throw and send-task delivery hit the classic dual-write problem: the
process transaction commits and the message to the outside world is lost. We made the trade explicit
per message type — critical types through a transaction-local outbox and a leader-elected relay,
best-effort types published after commit with no extra database write.

Each one is independent and opt-in. All four are built on the engines' public extension points, so
nothing is forked or patched and your engine vendor's security updates apply normally. Flowable
currently ships the messaging foundation; bringing it to parity on these four is a tracked roadmap
item.

How each of those works, and what it costs, is [the next post](02-your-ceiling-is-your-database.md).

## If you just want the messaging part

It works on its own, and most people start there.

```bash
docker run -p 4222:4222 nats:2.10 --jetstream
```

Add the adapter for your engine, set `spring.nats.url`, declare a subscription. The offload
capabilities are separate and opt-in — you do not adopt them by accident, and you do not need them
to get value from the messaging layer.

Everything is on Maven Central under `com.3eai-labs`, GPG-signed, Apache-2.0.

📖 [Quick Start](../user/QUICK_START.md) · [User Guide](../user/USER_GUIDE.md) ·
[GitHub](https://github.com/3eAI-Labs/nats-bpm-channels)

---

*Next: [Your engine's ceiling is your database](02-your-ceiling-is-your-database.md)*
