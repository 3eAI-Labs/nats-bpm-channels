# From tables to a log

*Part 3 of 3 — [Part 1: The licence wall](01-the-licence-wall.md) · [Part 2: Your ceiling is your database](02-your-ceiling-is-your-database.md)*

---

Describe the four offload increments one at a time and they sound like four unrelated optimisations.
History goes to a projection. Dispatch goes over JetStream. Big variables go to a content store.
Outbound messages go through an outbox.

Line them up and something else shows up.

| Workload | Was | Is |
|---|---|---|
| History events | rows written in the engine transaction | a stream, projected into a separate store |
| Task dispatch | polling a relational table | a stream, pushed to workers |
| Outbound messages | rows plus a dual-write problem | a stream, with an explicit delivery guarantee |
| Large payloads | rows in the variable table | content-addressed objects, referenced from a stream |

Every one of them moved a workload from *relational storage with per-command ACID transactions* to
*a partitioned, replayable log*.

That was not a coincidence, and it was not only about making the current engine faster.

## The question underneath

The design note this project works from asks it directly. Not "how do we remove persistence" —
you cannot, the engine is stateful and orchestration state has to be durable somewhere. The real
question is narrower and more useful:

> Is that persistence *relational with per-command ACID transactions*, or *a partitioned replayable
> log*? And is the high-volume work — dispatch, fan-out, coordination, history — loading the
> database transaction at all?

Those are two different questions and they have two different answers. The second one you can
attack today, from behind a public SPI, without forking anything. That is what the four increments
did.

The first one you cannot. It is the engine's state machine, and changing it means changing the
engine.

## What a log-structured core actually buys

This is well-trodden ground. Zeebe made the choice for Camunda 8: the log is the source of truth,
state is a materialised projection of the log, partitioning is the scaling axis. Kafka Streams works
the same way. So does every event-sourced system you have read about.

You get horizontal scale — partitions are independent, so throughput grows by adding them. You get
replay: state is derivable, so recovery is re-reading rather than restoring. You get an append-only
write path, which is the cheapest thing a storage system can do.

You give up things that matter. Arbitrary relational queries against live state are gone —
you query projections, and projections lag. Cross-partition transactions are gone. Operationally it
is a different animal: partition rebalancing, log compaction, retention.

It is a real trade, not an upgrade. Which is exactly why doing it as a big-bang rewrite is a bad
idea.

## Why the offload work is the on-ramp

Here is the part that ties the series together.

Every workload already moved onto the log is a problem already solved once, under real load. The
history projection reads from a stream, keys by process instance, and handles out-of-order arrival
with merge-upsert on stream sequence. Those are design problems with known answers now, rather than
open questions. Same for task dispatch, same for outbound delivery, same for the content-addressed
payload store.

To be clear about what that is and is not: it is accumulated design knowledge and reusable
material, not a compatibility guarantee. A native core would be an independent project, free to
define its own wire format, and nothing here promises that today's workers or streams plug into it
unchanged.

And the semantics are already proven under real conditions rather than assumed:

- Leader election on a NATS KV lease with compare-and-swap — split-brain measured at zero against
  an N-candidate race
- Relay failover — RPO of zero across a real 3-replica JetStream KV failover
- Custody transfer: acknowledge only once persistence has actually changed hands, in every role
- At-least-once everywhere, with `Nats-Msg-Id` dedup and idempotent completion

None of that is engine-specific. It is the substrate a native core would sit on, and it is already
running in production systems.

## What is deliberately not claimed

`nats-bpm-channels` is not a log-structured engine. It is a NATS messaging and offload layer for
engines that are relational and will stay relational.

There is separate work on a NATS-native execution core — the log as source of truth, no RDBMS in the
engine itself. It is early and this post is not announcing it. What it *is* doing is explaining why
the four increments look the way they do: they were shaped so that the eventual step is an
evolution, not a rewrite, and so that the boring, dangerous parts — delivery guarantees, leader
election, failover semantics — are settled before anything depends on them.

## Where this leaves you

If you are on an open-source BPM engine and your database is the thing that hurts, you can take the
offload capabilities today, one at a time, and stop wherever the pain stops. Most teams start with
history, because that is usually the biggest share of write volume and the least transactionally
interesting.

Nothing about that decision commits you to a log-structured future, and nothing here promises you a
free ride into one. What it does mean is that the work is pointed in a coherent direction rather
than accumulating in a corner.

```xml
<dependency>
    <groupId>com.3eai-labs</groupId>
    <artifactId>camunda-nats-channel</artifactId>
    <version>0.7.0</version>
</dependency>
```

Apache-2.0, on Maven Central, GPG-signed.

📖 [White paper](../WHITE_PAPER.md) · [Architecture overview](../ARCHITECTURE_OVERVIEW.md) ·
[User Guide](../user/USER_GUIDE.md) · [GitHub](https://github.com/3eAI-Labs/nats-bpm-channels)

---

*Series: [1 — The licence wall](01-the-licence-wall.md) · [2 — Your ceiling is your database](02-your-ceiling-is-your-database.md) · 3 — From tables to a log*
