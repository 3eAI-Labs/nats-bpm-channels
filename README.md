# nats-bpm-channels

[![CI](https://github.com/3eAI-Labs/nats-bpm-channels/actions/workflows/ci.yml/badge.svg)](https://github.com/3eAI-Labs/nats-bpm-channels/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

NATS.io channel adapters for BPM engines. Same messaging foundation, four engine bindings — pick the one that matches your stack.

| Engine | Module | Maven artifact |
|---|---|---|
| [Flowable](https://www.flowable.com/open-source) 7.x | [`flowable-nats-channel`](./flowable-nats-channel) | `com.3eai:flowable-nats-channel` |
| [Camunda 7](https://docs.camunda.org/manual/7.24/) | [`camunda-nats-channel`](./camunda-nats-channel) | `com.3eai:camunda-nats-channel` |
| [CIBSeven](https://cibseven.org/) 2.x | [`cibseven-nats-channel`](./cibseven-nats-channel) | `com.3eai:cibseven-nats-channel` |
| [CadenzaFlow](https://cadenzaflow.com/) 1.x | [`cadenzaflow-nats-channel`](./cadenzaflow-nats-channel) | `com.3eai:cadenzaflow-nats-channel` |

## Why this project?

[Camunda 8](https://camunda.com/) (v8.6+, October 2024) moved all components — including Zeebe — to a paid enterprise license at **$50K+/year**. Camunda 7 reached End of Life with no more security patches (October 2025).

This project provides a **high-performance, zero-cost messaging layer** built on [NATS.io](https://nats.io) for four open-source BPM engines:

- **Flowable** — Apache 2.0, full BPMN/CMMN/DMN, Event Registry channel integration
- **Camunda 7** — community fork users on EOL versions can keep modern messaging
- **CIBSeven** — Apache 2.0 Camunda 7 community fork (CIB software GmbH), published on Maven Central and actively maintained (v2.x); the drop-in public successor for Camunda 7 EOL
- **CadenzaFlow** — Camunda 7 community fork (3eAI Labs), continues Camunda 7 lineage with rebranded packages and ongoing security maintenance

All four engines share the same NATS feature set: Core pub/sub, JetStream persistent delivery, and Request-Reply for external workers.

## Features (all engines)

- **Core NATS** — Pub/sub inbound and outbound event channels
- **JetStream** — Persistent messaging with ack/nack, exponential backoff (`nakWithDelay`), dead letter queue (JetStream primary + Core NATS fallback)
- **Request-Reply** — BPMN service tasks delegate work to external workers via NATS request-reply
- **Virtual Threads** — Java 21 virtual thread offloading for non-blocking I/O
- **Micrometer Metrics** — Counters for consume/ack/nak/dlq/publish + processing Timer
- **Structured Logging** — SLF4J `kv()` format with MDC `trace_id` propagation
- **Spring Boot Auto-Configuration** — Zero-config with `spring.nats.*` properties
- **Auth** — Username/password, token, credentials file, NKey

## Requirements

- Java 21+
- Spring Boot 3.x
- Engine: Flowable 7.x / Camunda 7.x / CIBSeven 2.x / CadenzaFlow 1.x
- NATS 2.10+ (for JetStream `nakWithDelay`)
- `spring.threads.virtual.enabled: true` (recommended)

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
    <groupId>com.3eai</groupId>
    <artifactId>flowable-nats-channel</artifactId>
    <version>0.1.0</version>
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
    <groupId>com.3eai</groupId>
    <artifactId>camunda-nats-channel</artifactId>
    <version>0.1.0</version>
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
    <groupId>com.3eai</groupId>
    <artifactId>cibseven-nats-channel</artifactId>
    <version>0.1.0</version>
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

```xml
<dependency>
    <groupId>com.3eai</groupId>
    <artifactId>cadenzaflow-nats-channel</artifactId>
    <version>0.1.0</version>
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

| Phase | Status |
|-------|--------|
| Core NATS pub/sub | :white_check_mark: Complete |
| JetStream (persistent, DLQ, backoff) | :white_check_mark: Complete |
| Request-Reply (external workers) | :white_check_mark: Complete |
| Flowable adapter | :white_check_mark: Complete |
| Camunda 7 adapter | :white_check_mark: Complete |
| CIBSeven adapter | :white_check_mark: Complete |
| CadenzaFlow adapter | :white_check_mark: Complete |
| Key-Value Store integration | :crystal_ball: Planned |
| Object Store integration | :crystal_ball: Planned |

## Contributing

Contributions are welcome. Please open an issue first to discuss what you would like to change.

## License

[Apache License 2.0](LICENSE)

Copyright 2026 [3eAI Labs Ltd](https://3eai.com)
