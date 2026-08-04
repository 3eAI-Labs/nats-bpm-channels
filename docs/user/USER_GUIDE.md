# User Guide

**nats-bpm-channels** — NATS.io messaging for open-source BPM engines.

Applies to version **0.8.0**. All artifacts are published to Maven Central under the
`com.3eai-labs` namespace.

---

## 1. Introduction

### What this is

nats-bpm-channels connects a BPM engine to [NATS.io](https://nats.io). It gives four
open-source engines — Flowable, Camunda 7, CIBSeven and CadenzaFlow — the same messaging feature
set: publish and subscribe, durable JetStream delivery with retries and a dead-letter queue, and
request-reply for external workers written in any language.

Beyond messaging, it moves high-volume engine work **off the database transaction**. A BPM engine
persists every token move, every wait state and every variable through its relational database, so
at high load the database becomes the ceiling. This project shifts four categories of that work
onto NATS: external task dispatch, history events, large variable payloads and outbound messages.

### Who it is for

- Teams on **Camunda 7** past its End of Life who need a maintained messaging layer
- Teams on **Flowable** who want NATS in the Event Registry, which ships only Kafka, RabbitMQ and JMS
- Teams on **CIBSeven**, the Apache-2.0 Camunda 7 community fork
- Anyone whose BPM database is the throughput bottleneck rather than the engine itself

### What you need to know first

The engine remains stateful and authoritative. Orchestration state — where each token is, which
instance is waiting — still lives in the engine database. What moves to NATS is the high-volume
work *around* that state, not the state itself.

---

## 2. Getting Started

### Prerequisites

| Requirement | Version | Note |
|---|---|---|
| Java | 21+ | Virtual threads are used for non-blocking I/O |
| Spring Boot | 3.x | Auto-configuration is provided |
| NATS server | 2.10+ | Required for JetStream `nakWithDelay` |
| BPM engine | Flowable 7.x / Camunda 7.x / CIBSeven 2.x / CadenzaFlow 1.x | One of |
| PostgreSQL | 12+ | Only for history projection and large-variable externalization |

### Install

Add the adapter for your engine. Every module inherits the NATS connection layer from `nats-core`,
so you do not declare it separately.

```xml
<dependency>
    <groupId>com.3eai-labs</groupId>
    <artifactId>camunda-nats-channel</artifactId>
    <version>0.8.0</version>
</dependency>
```

Replace the artifact id with `flowable-nats-channel`, `cibseven-nats-channel` or
`cadenzaflow-nats-channel` as appropriate.

> `cadenzaflow-nats-channel` is published to Maven Central for the first time in **0.8.0**. It was
> excluded from 0.7.0 because the build pinned an engine version that was never published there; on
> 0.7.0 it had to be built from source.

History offload additionally requires:

```xml
<dependency>
    <groupId>com.3eai-labs</groupId>
    <artifactId>nats-history-projection</artifactId>
    <version>0.8.0</version>
</dependency>
```

### Verify the artifact

Every artifact is signed. To check a download against the published key:

```bash
gpg --keyserver keyserver.ubuntu.com --recv-keys E610505884534DB9
gpg --verify nats-core-0.8.0.jar.asc nats-core-0.8.0.jar
```

A valid result reports `Good signature from "oss@3eai-labs.com"`.

### Enable virtual threads

Recommended in `application.yml`:

```yaml
spring:
  threads:
    virtual:
      enabled: true
```

---

## 3. Quick Start

A minimal working setup: connect to NATS, receive a message, correlate it to a waiting process
instance.

### Step 1 — start NATS

```bash
docker run -p 4222:4222 nats:2.10 --jetstream
```

### Step 2 — configure the connection

```yaml
spring:
  nats:
    url: nats://localhost:4222
```

### Step 3 — subscribe a message to a process

For Camunda 7, CIBSeven and CadenzaFlow, subscriptions are declared in configuration. Substitute
`camunda` with `cibseven` or `cadenzaflow` for those engines.

```yaml
spring:
  nats:
    camunda:
      subscriptions:
        - subject: order.new
          messageName: OrderReceived
          businessKeyHeader: X-Business-Key
```

A message published to `order.new` is correlated through
`RuntimeService.createMessageCorrelation()`. The process instance receives two variables
automatically: `natsPayload` (the message body) and `natsSubject`.

For Flowable, define an Event Registry channel instead:

```json
{
  "key": "orderInboundChannel",
  "category": "channel",
  "name": "Order Inbound Channel",
  "channelType": "inbound",
  "type": "nats",
  "deserializerType": "json",
  "channelEventKeyDetection": { "fixedValue": "orderEvent" },
  "channelFields": [
    { "name": "subject", "stringValue": "order.new" },
    { "name": "queueGroup", "stringValue": "order-service" }
  ]
}
```

### Step 4 — send a test message

```bash
nats pub order.new '{"orderId":"A-1001"}' -H X-Business-Key:A-1001
```

---

## 4. Features and Usage

### 4.1 Core NATS publish and subscribe

Fire-and-forget messaging, no persistence. Suitable for events where losing a message under a
broker restart is acceptable.

Outbound from a BPMN service task (Camunda 7 / CIBSeven syntax; CadenzaFlow accepts both
`camunda:` and `cadenzaflow:` prefixes):

```xml
<serviceTask id="notifyOrder" camunda:delegateExpression="${natsPublishDelegate}">
  <extensionElements>
    <camunda:field name="subject" stringValue="order.completed" />
    <camunda:field name="payloadVariable" stringValue="orderPayload" />
  </extensionElements>
</serviceTask>
```

### 4.2 JetStream durable delivery

Persistent messaging with acknowledgement, retry with exponential backoff, and a dead-letter queue.
Delivery is at-least-once, so consumers must be idempotent.

Add JetStream fields to a subscription:

```yaml
spring:
  nats:
    camunda:
      subscriptions:
        - subject: payment.completed
          messageName: PaymentConfirmed
          jetstream: true
          durableName: payment-consumer
          maxDeliver: 5
          dlqSubject: dlq.payment.completed
          autoCreateStream: true
          streamName: PAYMENTS
```

| Field | Default | Meaning |
|---|---|---|
| `jetstream` | `false` | Enable durable mode |
| `durableName` | — | Durable consumer name; required for JetStream |
| `deliverPolicy` | `all` | `all`, `last` or `new` |
| `ackWait` | `30s` | How long the server waits for an ack before redelivering |
| `maxDeliver` | `5` | Attempts before the message is routed to the DLQ |
| `dlqSubject` | `dlq.{subject}` | Where exhausted messages go |
| `autoCreateStream` | `false` | Create the stream if it does not exist |
| `streamName` | — | Target stream |

When a message exhausts `maxDeliver`, it is published to the DLQ subject. The DLQ publish uses
JetStream when available and falls back to core NATS otherwise, so a DLQ route is never silently
lost because JetStream is unavailable.

### 4.3 Request-reply with external workers

A BPMN service task hands work to a worker over NATS and waits for the reply. The worker can be
written in any language with a NATS client.

```xml
<serviceTask id="sendSms" camunda:delegateExpression="${natsRequestReply}">
  <extensionElements>
    <camunda:field name="subject" stringValue="task.send-sms" />
    <camunda:field name="timeout" stringValue="30s" />
    <camunda:field name="resultVariable" stringValue="smsResult" />
    <camunda:field name="payloadVariable" stringValue="smsPayload" />
  </extensionElements>
</serviceTask>
```

A worker in Go:

```go
nc.QueueSubscribe("task.send-sms", "sms-workers", func(msg *nats.Msg) {
    result := processSMS(msg.Data)
    nc.Publish(msg.Reply, result)
})
```

In Python:

```python
async def handler(msg):
    result = process_sms(msg.data)
    await nc.publish(msg.reply, result)

await nc.subscribe("task.send-sms", queue="sms-workers", cb=handler)
```

> **Important.** A synchronous request-reply holds the engine's database transaction open for the
> duration of the call. For high-volume work, prefer the external-task bridge described next, which
> does not.

### 4.4 External task dispatch over JetStream

Instead of workers polling the engine database for external tasks, tasks are dispatched over
JetStream. This removes the polling load from the database.

```yaml
spring:
  nats:
    camunda:
      a2:
        topics:
          - send-sms
          - provision-line
        defaults:
          lock-duration-seconds: 300
          ack-wait-seconds: 30
          max-deliver: 4
          sweep-period-seconds: 120
```

`lock-duration-seconds` has no default and must be set. It must be long enough to cover the
worker's real processing time; the adapter rejects a lock duration it considers unsafe relative to
`ack-wait-seconds` unless `allow-unsafe-lock-duration` is set to `true`. Leave that flag alone
unless you have measured the interaction.

Use `variable-allowlist` to restrict which process variables are placed on the wire per topic. This
matters when variables carry personal data.

### 4.5 History offload

Engine history writes (`ACT_HI_*`) are a large share of database traffic and are rarely needed
transactionally. This capability routes history events to NATS and projects them into a separate
PostgreSQL database, so history load leaves the engine database.

Events are classified. Audit-critical classes go through a transactional outbox with a
leader-elected relay, giving at-least-once delivery with no loss. Everything else is published
after commit, which is cheaper but at-most-once.

```yaml
spring:
  nats:
    camunda:
      history:
        audit-critical-classes:
          - PROCESS_INSTANCE
          - VARIABLE
        outbox:
          relay-cycle-period-seconds: 30

history:
  projection:
    partition-count: 8
    datasource:
      jdbc-url: jdbc:postgresql://localhost:5432/history
      username: history
      password: ${HISTORY_DB_PASSWORD}
```

Retention and erasure are configured separately:

```yaml
history:
  retention:
    audit-critical-default-window: P7Y
    bulk-default-days: 90
```

### 4.6 Large variable externalization

Variables above a size threshold are written to a content-addressed store rather than into the
engine's variable table. Identical payloads are stored once and reference-counted, so duplicate
content does not multiply.

```yaml
history:
  large-variable:
    enabled: true
    threshold-bytes: 4096
    projection-datasource:
      jdbc-url: jdbc:postgresql://localhost:5432/history
      username: history
      password: ${HISTORY_DB_PASSWORD}
```

Externalization happens after commit, so it does not extend the engine transaction. Reference
counting is integrated with retention and erasure: when the last reference to a payload is
released, the payload itself is deleted.

### 4.7 Outbound message handoff

Outbound BPMN messages and events (message-throw, send-task) are delivered to NATS in a way that
survives the dual-write problem — the risk that the transaction commits but the message is never
sent, or vice versa.

Messages you classify as critical go through a transactional outbox with a leader relay
(at-least-once). Everything else is published after commit (at-most-once).

```yaml
spring:
  nats:
    outbound:
      critical-types:
        - PaymentInstruction
      outbox:
        relay-cycle-period-seconds: 30
```

### 4.8 Observability

Micrometer counters are published for consume, ack, nak, DLQ and publish, along with a processing
timer. Logs use SLF4J structured `kv()` format and propagate `trace_id` through MDC.

To expose metrics, add Spring Boot Actuator and a registry — for example
`micrometer-registry-prometheus` — to your application. The adapters do not bring an Actuator
dependency of their own.

---

## 5. Configuration Reference

All properties below are read from your Spring configuration. Engine-scoped properties use the
prefix of the engine you deployed: `spring.nats.camunda`, `spring.nats.cibseven` or
`spring.nats.cadenzaflow`. Flowable configures channels through the Event Registry instead.

### Connection — `spring.nats`

| Property | Type | Default |
|---|---|---|
| `url` | String | `nats://localhost:4222` |
| `username` | String | — |
| `password` | String | — |
| `token` | String | — |
| `credentials-file` | String | — |
| `nkey-file` | String | — |
| `connection-timeout` | Duration | `5s` |
| `max-reconnects` | Integer | `-1` (infinite) |
| `reconnect-wait` | Duration | `2s` |
| `tls.enabled` | Boolean | `false` |
| `tls.cert-file` | String | — |
| `tls.key-file` | String | — |
| `tls.ca-file` | String | — |

### External task bridge — `spring.nats.<engine>.a2`

| Property | Type | Default |
|---|---|---|
| `topics` | List&lt;String&gt; | — |
| `topic-overrides` | Map | — |
| `variable-allowlist` | Map&lt;String,List&lt;String&gt;&gt; | — |
| `sentinel-worker-id` | String | `a2-jetstream-bridge` |
| `allow-unsafe-lock-duration` | Boolean | `false` |
| `defaults.lock-duration-seconds` | Long | — (**must be set**) |
| `defaults.ack-wait-seconds` | Long | `30` |
| `defaults.max-deliver` | Integer | `4` |
| `defaults.epsilon-seconds` | Long | `60` |
| `defaults.retry-timeout-millis` | Long | `5000` |
| `defaults.sweep-period-seconds` | Long | `120` |

### History offload — `spring.nats.<engine>.history`

| Property | Type | Default |
|---|---|---|
| `audit-critical-classes` | List&lt;String&gt; | — |
| `pseudonymization-opt-in` | Boolean | `false` |
| `tenant-key-id` | String | — |
| `tenant-key-version` | Integer | `1` |
| `outbox.relay-cycle-period-seconds` | Long | `30` |
| `outbox.stuck-threshold-multiplier` | Integer | `5` |

### Outbound handoff — `spring.nats.outbound`

| Property | Type | Default |
|---|---|---|
| `critical-types` | List&lt;String&gt; | — |
| `variable-allowlist` | Map&lt;String,List&lt;String&gt;&gt; | — |
| `outbox.relay-cycle-period-seconds` | Long | `30` |
| `outbox.stuck-threshold-multiplier` | Integer | `5` |

### History projection — `history.projection`

| Property | Type | Default |
|---|---|---|
| `partition-count` | Integer | `8` |
| `partition-assignment` | List&lt;Integer&gt; | — |
| `datasource.jdbc-url` | String | — |
| `datasource.username` | String | — |
| `datasource.password` | String | — |

### Large variables — `history.large-variable`

| Property | Type | Default |
|---|---|---|
| `enabled` | Boolean | `true` |
| `threshold-bytes` | Integer | `4096` |
| `sweep-cycle-period-seconds` | Long | `60` |
| `projection-datasource.jdbc-url` | String | — |
| `projection-datasource.username` | String | — |
| `projection-datasource.password` | String | — |

### Retention — `history.retention`

| Property | Type | Default |
|---|---|---|
| `audit-critical-default-window` | String (ISO-8601 period) | `P7Y` |
| `bulk-default-days` | Integer | `90` |
| `per-class-overrides` | Map&lt;String,String&gt; | — |

### Reconciliation — `history.reconciliation`

| Property | Type | Default |
|---|---|---|
| `cron` | String | `0 0 3 * * *` |
| `audit-critical-classes` | List&lt;String&gt; | — |
| `clean-streak-target-default` | Integer | `7` |
| `clean-streak-target-overrides` | Map&lt;String,Integer&gt; | — |
| `bulk-epsilon-overrides` | Map&lt;String,Long&gt; | — |

### Cutover — `history.cutover`

| Property | Type | Default |
|---|---|---|
| `volume-priority-order` | List&lt;String&gt; | — |

### Pseudonym vault — `history.vault.datasource`

| Property | Type | Default |
|---|---|---|
| `jdbc-url` | String | — |
| `username` | String | — |
| `password` | String | — |
| `vault-column-encryption-key-ref` | String | — |

---

## 6. Troubleshooting

### Messages are not reaching the process

Check, in order:

1. Is the subject exactly right? NATS subjects are case-sensitive and dot-separated.
2. Is a process instance actually waiting on that message name? Correlation fails silently when no
   instance is subscribed — the engine returns rather than throwing.
3. If using JetStream, does the stream exist and does its subject filter cover your subject? Set
   `autoCreateStream: true` in development, but create streams deliberately in production.

### Messages loop and end up in the DLQ

The consumer is nak-ing or timing out. Look at `ackWait` first: if processing legitimately takes
longer than the ack window, the server redelivers while the first attempt is still running.
Increase `ackWait` rather than `maxDeliver`.

### `Could not resolve dependencies: cadenzaflow-nats-channel`

That artifact exists from **0.8.0** onward. It was excluded from the 0.7.0 release, so a 0.7.0
coordinate will not resolve — either upgrade to 0.8.0 or build 0.7.0 from source with `mvn install`.

### External tasks are locked but never completed

The lock duration is probably shorter than real processing time, so the engine reclaims the task
while the worker is still running. Raise `defaults.lock-duration-seconds`. Do not set
`allow-unsafe-lock-duration: true` to silence the validation — the validation is what tells you the
lock and ack windows are inconsistent.

### History rows are missing from the projection

Non-audit-critical history is published after commit and is at-most-once by design. If a class must
never be lost, add it to `audit-critical-classes` so it goes through the transactional outbox
instead.

### `NoClassDefFoundError` for a Micrometer class

The adapters publish Micrometer metrics but do not bring a registry. Add Actuator and a registry
implementation to your application.

---

## 7. FAQ

**Do I have to adopt the offload features?**
No. The messaging features work on their own. External task dispatch, history offload, large
variable externalization and outbound handoff are independent and each is opt-in.

**Does this replace my engine's database?**
No. Orchestration state stays in the engine database. What moves off it is high-volume work around
that state.

**Is delivery exactly-once?**
No. JetStream delivery is at-least-once, and the transactional-outbox paths are at-least-once.
Design workers to be idempotent.

**Can workers be written in a language other than Java?**
Yes. Workers speak plain NATS, so any language with a NATS client works.

**Can I run more than one engine adapter in one application?**
The adapters are engine-specific and each expects its own engine on the classpath. Running two
engines in one application is not a supported configuration.

**Which BPMN namespace does CIBSeven use?**
CIBSeven keeps the Camunda extension namespace (`http://camunda.org/schema/1.0/bpmn`, `camunda:`
prefix), so BPMN authored for Camunda 7 runs unmodified.

**Why is `nats-bpm-bench` not on Maven Central?**
It is a benchmark harness, not a library. It stays in the repository and is deliberately excluded
from publication.

---

## License

Apache License 2.0. Copyright 2026 [3eAI Labs Ltd](https://3eai-labs.com).
