# The licence wall, and what's actually behind it

*Part 1 of 3 — [Part 2: Your engine's ceiling is your database](02-your-ceiling-is-your-database.md) · [Part 3: From tables to a log](03-from-tables-to-a-log.md)*

---

If you run Camunda 7 in production, the last two years handed you a bill you did not ask for.

In October 2024, Camunda 8.6 moved every self-managed component — Zeebe included — behind a paid
enterprise licence. In October 2025, Camunda 7 reached end of life and the security patches stopped.
So the options narrowed to: pay for 8, run an engine nobody patches, or leave.

Plenty has been written about that. What gets less attention is what you find when you actually
start moving.

## The obvious gap: maintenance

This one has answers now, and good ones.

**CIBSeven** is an Apache-2.0 fork of Camunda 7 by CIB software GmbH. It is published on Maven
Central, actively maintained, and — the part that matters most — it keeps the Camunda BPMN extension
namespace. Your `camunda:` prefixed BPMN files run unmodified:

```xml
<serviceTask id="notify" camunda:delegateExpression="${myDelegate}">
```

That is not a small detail. A fork that renames the namespace makes every BPMN file in your
repository a migration task. CIBSeven renamed the Java packages (`org.camunda.*` → `org.cibseven.*`)
but left the BPMN dialect alone.

**Flowable** is the other direction entirely — not a fork, a separate Apache-2.0 engine with full
BPMN, CMMN and DMN. Different migration, different tradeoffs, but no licence.

So the maintenance gap has a fix. Good.

## The gap nobody mentions in the migration guides: messaging

Here is where it got interesting for us.

Flowable's Event Registry — its channel abstraction for getting events in and out — ships adapters
for Kafka, RabbitMQ and JMS. Not NATS. The Camunda 7 forks inherited a messaging story designed
before lightweight brokers were common.

If your architecture already runs on NATS, you are writing that integration yourself. And "writing
it yourself" means writing the easy 80% yourself and discovering the other 20% in production: what
happens when the consumer nak-s, what happens when a message exhausts its retries, what happens to
the DLQ publish when JetStream is the thing that is down.

That is what `nats-bpm-channels` started as. One messaging layer, four engine bindings — Flowable,
Camunda 7, CIBSeven, CadenzaFlow. Same features across all of them:

```xml
<dependency>
    <groupId>com.3eai-labs</groupId>
    <artifactId>cibseven-nats-channel</artifactId>
    <version>0.7.0</version>
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

Everything is Apache-2.0, top to bottom: the engines, NATS, the client, this library. The stack's
licence cost is zero, which was the point.

## The gap we did not expect to be the interesting one

Here is the thing that changed the project.

Once teams had NATS messaging working, the complaints did not stop. They changed shape. Nobody was
asking for more message patterns. They were asking why the engine slowed down under load when the
engine nodes were barely warm.

The answer, every time, was the database.

These engines persist through a relational database, and *everything* goes through it. Every token
movement is a write. Every wait state is a row. Every variable is a row. Every engine command runs
in its own JDBC transaction. Add engine nodes and they all push into the same database.

Swapping Camunda 8 for an open-source engine solves your licence problem. It does not touch this
one — if anything it makes it more visible, because the engine you moved to has the same
architecture and you no longer have Zeebe's partitioned log doing the heavy lifting.

So a messaging adapter turned into something else: how much engine work can you take off that
database transaction without forking the engine?

Quite a lot, it turns out. Four increments have shipped — external task dispatch, history events,
large variable payloads, outbound messages. All built on the engines' public extension points, no
forks, no patched engines.

That is [the next post](02-your-ceiling-is-your-database.md).

## If you just want the messaging part

It works on its own, and most people start there.

```bash
docker run -p 4222:4222 nats:2.10 --jetstream
```

Add the adapter for your engine, set `spring.nats.url`, declare a subscription. The offload
capabilities are separate and opt-in — you do not adopt them by accident, and you do not need them
to get value from the messaging layer.

Everything is on Maven Central under `com.3eai-labs`, GPG-signed.

📖 [Quick Start](../user/QUICK_START.md) · [User Guide](../user/USER_GUIDE.md) ·
[GitHub](https://github.com/3eAI-Labs/nats-bpm-channels)

---

*Next: [Your engine's ceiling is your database](02-your-ceiling-is-your-database.md)*
