# nats-bpm-channels

[![CI](https://github.com/3eAI-Labs/nats-bpm-channels/actions/workflows/ci.yml/badge.svg)](https://github.com/3eAI-Labs/nats-bpm-channels/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/com.3eai-labs/nats-core.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/com.3eai-labs/nats-core)
[![License](https://img.shields.io/badge/License-BSL_1.1-blue.svg)](LICENSE)

NATS.io channel adapters for BPM engines. Flowable gets Event Registry integration; Camunda-lineage
adapters add database offload paths for high-volume workloads.

**Apache-era releases (through 0.8.1) are on Maven Central** under the `com.3eai-labs`
namespace (GPG-signed; the public key is on `keyserver.ubuntu.com`). From 0.10.0 the project
is source-available under the BSL 1.1: build from source for development and testing;
production artifacts and support ship with the commercial license.

📖 [Quick Start](docs/user/QUICK_START.md) · [User Guide](docs/user/USER_GUIDE.md) · [White Paper](docs/WHITE_PAPER.md) · [all documentation](#documentation)

| Engine | Module | Maven artifact |
|---|---|---|
| [Flowable](https://www.flowable.com/open-source) 7.x | [`flowable-nats-channel`](./flowable-nats-channel) | `com.3eai-labs:flowable-nats-channel` |
| [Camunda 7](https://docs.camunda.org/manual/7.24/) | [`camunda-nats-channel`](./camunda-nats-channel) | `com.3eai-labs:camunda-nats-channel` |
| [CIBSeven](https://cibseven.org/) 2.x | [`cibseven-nats-channel`](./cibseven-nats-channel) | `com.3eai-labs:cibseven-nats-channel` |
| [CadenzaFlow](https://cadenzaflow.com/) 1.x | [`cadenzaflow-nats-channel`](./cadenzaflow-nats-channel) | `com.3eai-labs:cadenzaflow-nats-channel` |

Every adapter declares its engine as a `provided` dependency. Adding one therefore pulls no engine
into your build and cannot override the version you already run — put the adapter alongside the
engine dependency your application already declares, not in place of it.

## Why this project?

A large installed base runs BPMN on the Camunda 7 lineage and on Flowable. Those engines are
durable because they persist through a relational database — and past a certain load, that database
is the ceiling rather than the engine.

This project gives those engines a shared messaging layer built on [NATS.io](https://nats.io), under
Apache 2.0:

- **Flowable** — Apache 2.0, full BPMN/CMMN/DMN; integrates through the Event Registry channel
- **Camunda 7** — a shared NATS layer built on the engine's public extension points
- **CIBSeven** — Apache 2.0 Camunda 7 community fork (CIB software GmbH), published on Maven Central and actively maintained (v2.x)
- **CadenzaFlow** — Camunda 7 community fork (3eAI Labs), continues the Camunda 7 lineage with rebranded packages and ongoing security maintenance

All four engines share the same NATS foundation. Engine-specific capabilities vary by adapter.

## Features

### Messaging foundation

- **Core NATS** — Pub/sub inbound and outbound event channels
- **JetStream** — Persistent messaging with ack/nack, exponential backoff (`nakWithDelay`), dead letter queue (JetStream primary + Core NATS fallback)
- **Request-Reply** — BPMN service tasks delegate work to external workers via NATS request-reply
- **Virtual Threads** — Java 21 virtual thread offloading for non-blocking I/O
- **Micrometer Metrics** — Counters for consume/ack/nak/dlq/publish + processing Timer
- **Structured Logging** — SLF4J `kv()` format with MDC `trace_id` propagation
- **Spring Boot Auto-Configuration** — Zero-config with `spring.nats.*` properties
- **Auth** — Username/password, token, credentials file, NKey

### Database offload (Camunda 7, CIBSeven and CadenzaFlow)

A BPM engine persists every token move, every wait state and every variable through its relational
database, so at high load the database — not the engine — is the ceiling. These capabilities move
high-volume work off that transaction. Each is independent and opt-in; orchestration state stays in
the engine database.

- **External task dispatch over JetStream** (`spring.nats.<engine>.a2`) — workers receive tasks by push instead of polling the engine database
- **History offload** (`spring.nats.<engine>.history`) — `ACT_HI_*` traffic routed to NATS and projected into a separate PostgreSQL database; audit-critical classes go through a transactional outbox (at-least-once), the rest post-commit. Runs dual-write until you cut a history class over: until then the engine still writes its own `ACT_HI_*` rows, so the database sees the same writes *plus* the publish
- **Large variable externalization** (`history.large-variable`) — variables above a threshold move to a content-addressed store with SHA-256 deduplication and reference counting, instead of the engine's variable table
- **Outbound handoff** (`spring.nats.outbound`) — message-throw and send-task delivery that survives the dual-write problem: critical types via transactional outbox, the rest post-commit

See the [User Guide](docs/user/USER_GUIDE.md) for configuration of each.

**Flowable has none of the four offload paths above.** It ships the messaging foundation — Event
Registry channels, JetStream delivery and the DLQ — and its outbound publishing routes failures to a
DLQ rather than going through a transactional outbox, so it does not carry the at-least-once
guarantee the Camunda-lineage adapters do. Bringing Flowable to parity is on the
[roadmap](#roadmap).

## Requirements

- Java 21+
- Spring Boot 3.x
- Engine: Flowable 7.x / Camunda 7.x / CIBSeven 2.x / CadenzaFlow 1.x
- NATS 2.10+ (for JetStream `nakWithDelay`)
- `spring.threads.virtual.enabled: true` (recommended)

## Building from source

Releases through 0.8.1 (Apache 2.0) can be taken from Maven Central; from 0.10.0 build from
source (free for development and testing under the BSL 1.1). To build the repository itself:

```bash
git clone https://github.com/3eAI-Labs/nats-bpm-channels.git
cd nats-bpm-channels
mvn install
```

The full reactor builds with no profile flags — all four engines resolve from Maven Central. Java 21
is required; a newer JDK will fail the tests. Integration tests use
[Testcontainers](https://testcontainers.com), so a running Docker daemon is needed; `mvn install
-DskipTests` builds without one.

## Shared configuration (NATS connection)

```yaml
spring:
  nats:
    url: nats://localhost:4222
```

| Property | Default | Description |
|----------|---------|-------------|
| `spring.nats.url` | `nats://localhost:4222` | NATS server URL |
| `spring.nats.username` | — | Username auth |
| `spring.nats.password` | — | Password auth |
| `spring.nats.token` | — | Token auth |
| `spring.nats.credentials-file` | — | Credentials file path |
| `spring.nats.nkey-file` | — | NKey seed file |
| `spring.nats.connection-timeout` | `5s` | Connection timeout |
| `spring.nats.max-reconnects` | `-1` (infinite) | Max reconnection attempts |
| `spring.nats.reconnect-wait` | `2s` | Wait between reconnects |
| `spring.nats.tls.enabled` | `false` | Enable TLS |
| `spring.nats.tls.cert-file` | — | Client certificate |
| `spring.nats.tls.key-file` | — | Client private key |
| `spring.nats.tls.ca-file` | — | CA certificate |

---

## Flowable

### Dependency

```xml
<dependency>
    <groupId>com.3eai-labs</groupId>
    <artifactId>flowable-nats-channel</artifactId>
    <version>0.8.0</version>
</dependency>
```

### Define a channel (Flowable Event Registry)

```json
{
  "key": "orderInboundChannel",
  "category": "channel",
  "name": "Order Inbound Channel",
  "channelType": "inbound",
  "type": "nats",
  "deserializerType": "json",
  "channelEventKeyDetection": {
    "fixedValue": "orderEvent"
  },
  "channelFields": [
    { "name": "subject", "stringValue": "order.new" },
    { "name": "queueGroup", "stringValue": "order-service" }
  ]
}
```

### JetStream channel fields

| Field | Default | Description |
|-------|---------|-------------|
| `jetstream` | `false` | Enable JetStream mode |
| `durableName` | — | Durable consumer name |
| `deliverPolicy` | `all` | `all`, `last`, `new` |
| `ackWait` | `30s` | Ack timeout |
| `maxDeliver` | `5` | Max retries before DLQ |
| `dlqSubject` | `dlq.{subject}` | Dead letter queue subject |
| `autoCreateStream` | `false` | Create stream if missing |
| `streamName` | — | Target stream name |

### Request-Reply (external workers)

```xml
<serviceTask id="sendSms" name="Send SMS"
    flowable:delegateExpression="${natsRequestReply}">
  <extensionElements>
    <flowable:field name="subject" stringValue="task.send-sms" />
    <flowable:field name="timeout" stringValue="30s" />
    <flowable:field name="resultVariable" stringValue="smsResult" />
    <flowable:field name="payloadVariable" stringValue="smsPayload" />
  </extensionElements>
</serviceTask>
```

---

## Camunda 7

### Dependency

```xml
<dependency>
    <groupId>com.3eai-labs</groupId>
    <artifactId>camunda-nats-channel</artifactId>
    <version>0.8.0</version>
</dependency>
```

### Configure subscriptions

```yaml
spring:
  nats:
    url: nats://localhost:4222
    camunda:
      subscriptions:
        - subject: order.new
          messageName: OrderReceived
          businessKeyHeader: X-Business-Key
        - subject: payment.completed
          messageName: PaymentConfirmed
          jetstream: true
          durableName: payment-consumer
          maxDeliver: 5
          dlqSubject: dlq.payment.completed
          autoCreateStream: true
          streamName: PAYMENTS
```

Messages on configured subjects are correlated to waiting process instances via `RuntimeService.createMessageCorrelation()`. Process variables `natsPayload` and `natsSubject` are set automatically.

### Outbound delegates

**Core NATS Publish:**
```xml
<serviceTask id="notifyOrder" camunda:delegateExpression="${natsPublishDelegate}">
  <extensionElements>
    <camunda:field name="subject" stringValue="order.completed" />
    <camunda:field name="payloadVariable" stringValue="orderPayload" />
  </extensionElements>
</serviceTask>
```

**JetStream Publish:**
```xml
<serviceTask id="persistEvent" camunda:delegateExpression="${jetStreamPublishDelegate}">
  <extensionElements>
    <camunda:field name="subject" stringValue="audit.events" />
    <camunda:field name="payloadVariable" stringValue="auditPayload" />
  </extensionElements>
</serviceTask>
```

**Request-Reply:**
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

---

## CIBSeven

[CIBSeven](https://cibseven.org/) is an Apache-2.0 community fork of Camunda 7 by CIB software GmbH, published on Maven Central (`org.cibseven.bpm`) and actively maintained (v2.x). Java packages are rebranded `org.camunda.* → org.cibseven.*`, but the **BPMN extension namespace is unchanged** (`http://camunda.org/schema/1.0/bpmn`, `camunda:` prefix) — so BPMN files authored for Camunda 7 run unmodified.

### Dependency

```xml
<dependency>
    <groupId>com.3eai-labs</groupId>
    <artifactId>cibseven-nats-channel</artifactId>
    <version>0.8.0</version>
</dependency>
```

### Configure subscriptions

```yaml
spring:
  nats:
    url: nats://localhost:4222
    cibseven:
      subscriptions:
        - subject: order.new
          messageName: OrderReceived
          businessKeyHeader: X-Business-Key
        - subject: payment.completed
          messageName: PaymentConfirmed
          jetstream: true
          durableName: payment-consumer
          maxDeliver: 5
          dlqSubject: dlq.payment.completed
          autoCreateStream: true
          streamName: PAYMENTS
```

Messages on configured subjects are correlated to waiting process instances via `RuntimeService.createMessageCorrelation()`. Process variables `natsPayload` and `natsSubject` are set automatically.

### Outbound delegates

CIBSeven retains the `camunda:` extension prefix, so outbound delegates are declared exactly as on Camunda 7:

```xml
<serviceTask id="notifyOrder" camunda:delegateExpression="${natsPublishDelegate}">
  <extensionElements>
    <camunda:field name="subject" stringValue="order.completed" />
    <camunda:field name="payloadVariable" stringValue="orderPayload" />
  </extensionElements>
</serviceTask>
```

The same `${jetStreamPublishDelegate}` and `${natsRequestReply}` delegates from the Camunda 7 section apply unchanged.

---

## CadenzaFlow

CadenzaFlow is a community-maintained fork of Camunda 7 with `org.camunda.* → org.cadenzaflow.*` package rebranding and ongoing security maintenance after the upstream EOL.

### Dependency

> **New in 0.8.0.** This adapter is published to Maven Central for the first time. In 0.7.0 it was
> excluded from the release because the build pinned an engine version that was never published; if
> you are on 0.7.0 you had to build it from source.

```xml
<dependency>
    <groupId>com.3eai-labs</groupId>
    <artifactId>cadenzaflow-nats-channel</artifactId>
    <version>0.8.0</version>
</dependency>
```

### Configure subscriptions

```yaml
spring:
  nats:
    url: nats://localhost:4222
    cadenzaflow:
      subscriptions:
        - subject: order.new
          messageName: OrderReceived
          businessKeyHeader: X-Business-Key
        - subject: payment.completed
          messageName: PaymentConfirmed
          jetstream: true
          durableName: payment-consumer
          maxDeliver: 5
          dlqSubject: dlq.payment.completed
          autoCreateStream: true
          streamName: PAYMENTS
```

### Outbound delegates

CadenzaFlow's parser accepts both `cadenzaflow:` and `camunda:` extension prefixes (triple-namespace BPMN parser), so BPMN files migrated from Camunda 7 work without modification.

```xml
<serviceTask id="sendSms" cadenzaflow:delegateExpression="${natsRequestReply}">
  <extensionElements>
    <cadenzaflow:field name="subject" stringValue="task.send-sms" />
    <cadenzaflow:field name="timeout" stringValue="30s" />
    <cadenzaflow:field name="resultVariable" stringValue="smsResult" />
    <cadenzaflow:field name="payloadVariable" stringValue="smsPayload" />
  </extensionElements>
</serviceTask>
```

---

## Request-Reply: polyglot workers

Workers can be written in **any language** with a NATS client:

**Go:**
```go
nc.QueueSubscribe("task.send-sms", "sms-workers", func(msg *nats.Msg) {
    result := processSMS(msg.Data)
    nc.Publish(msg.Reply, result)
})
```

**Java:**
```java
connection.createDispatcher().subscribe("task.send-sms", "sms-workers", msg -> {
    byte[] result = processSMS(msg.getData());
    connection.publish(msg.getReplyTo(), result);
});
```

**Python:**
```python
async def handler(msg):
    result = process_sms(msg.data)
    await nc.publish(msg.reply, result)

await nc.subscribe("task.send-sms", queue="sms-workers", cb=handler)
```

## Roadmap

| Capability | Released | Status |
|---|---|---|
| Core NATS pub/sub | | :white_check_mark: Complete |
| JetStream (persistent, DLQ, backoff) | | :white_check_mark: Complete |
| Request-Reply (external workers) | | :white_check_mark: Complete |
| Flowable adapter | | :white_check_mark: Complete |
| Camunda 7 adapter | | :white_check_mark: Complete |
| CadenzaFlow adapter | | :white_check_mark: Complete |
| External task dispatch over JetStream | v0.2.0 | :white_check_mark: Complete |
| History offload (`ACT_HI_*` → NATS → projection) | v0.3.0 | :white_check_mark: Complete |
| Large variable externalization | v0.4.0 | :white_check_mark: Complete |
| Outbound handoff (dual-write safe) | v0.5.0 | :white_check_mark: Complete |
| CIBSeven adapter | v0.6.0 | :white_check_mark: Complete |
| Published to Maven Central | v0.7.0 | :white_check_mark: Complete |
| Flowable database-offload parity | | :crystal_ball: Planned |
| Sharding | | :crystal_ball: Planned |
| NATS-native execution core | | :crystal_ball: Planned |

## Documentation

**Using it**

| | |
|---|---|
| [Quick Start](docs/user/QUICK_START.md) | One page — install, connect, receive a message, durable delivery, external workers |
| [User Guide](docs/user/USER_GUIDE.md) | Full reference: every capability with examples, all 59 configuration properties with types and defaults, troubleshooting, FAQ |

**Understanding it**

| | |
|---|---|
| [White Paper](docs/WHITE_PAPER.md) | Why BPM engines hit a database ceiling, what the four offload increments do about it, and where the log-structured direction leads |
| [Architecture Overview](docs/ARCHITECTURE_OVERVIEW.md) | System diagram, data flow and delivery guarantee per capability, design decisions, deployment topology, metrics to alert on |
| [Product Datasheet](docs/PRODUCT_DATASHEET.md) | Artifacts, capability matrix, delivery guarantees, technology stack, prerequisites |

**Reading about it**

| | |
|---|---|
| [1 — Four BPM engines, one NATS layer](docs/blog/01-four-engines-one-nats-layer.md) | The four engines, the missing NATS binding, and the ceiling we found behind it |
| [2 — Your ceiling is your database](docs/blog/02-your-ceiling-is-your-database.md) | The synchronous request-reply trap, and four workloads moved off the transaction |
| [3 — From tables to a log](docs/blog/03-from-tables-to-a-log.md) | Why each offload increment is shaped the way it is |

**By sector**

| | |
|---|---|
| [Telecommunications](docs/solution-briefs/SOLUTION_BRIEF_TELCO.md) | Subscriber identifiers as PII, and the erasure vs. audit-retention conflict |
| [Financial Services](docs/solution-briefs/SOLUTION_BRIEF_FINTECH.md) | Dual-write integrity for payment instructions, and audit trail completeness |
| [Public Sector](docs/solution-briefs/SOLUTION_BRIEF_GOVERNMENT.md) | No runtime dependency on external services, and accountability that survives erasure |

**Releasing it** — [RELEASING.md](RELEASING.md) · [CHANGELOG.md](CHANGELOG.md)

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) for how to build, what the tests
need, and the one rule that is easy to miss: the three Camunda-lineage adapters are byte-mirrors of
each other, so a change to one belongs in all three. Please open an issue first for anything beyond
a small fix.

Found a security problem? Do not open a public issue — see [SECURITY.md](SECURITY.md).

> **About this repository.** Development happens in a private repository; this one is published as a
> full-tree snapshot per release, which is why the history is short and starts at 0.7.0. Issues and
> pull requests are read and answered here as normal.

## License

[Business Source License 1.1](LICENSE) — free for non-production use (development, testing,
evaluation); production use requires a commercial license from [3eAI Labs](https://3eai-labs.com).
Each version converts to the Apache License 2.0 four years after its publication. Versions up to
and including 0.8.1 were released under Apache 2.0 and remain so.

Copyright 2026 [3eAI Labs Ltd](https://3eai-labs.com)
