# Quick Start

**nats-bpm-channels 0.7.0** — NATS.io messaging for Flowable, Camunda 7, CIBSeven and CadenzaFlow.
Requires Java 21+, Spring Boot 3.x, NATS 2.10+.

## 1. Start NATS

```bash
docker run -p 4222:4222 nats:2.10 --jetstream
```

## 2. Add the dependency

```xml
<dependency>
    <groupId>com.3eai-labs</groupId>
    <artifactId>camunda-nats-channel</artifactId>   <!-- or flowable- / cibseven- -->
    <version>0.7.0</version>
</dependency>
```

## 3. Connect

```yaml
spring:
  threads:
    virtual:
      enabled: true
  nats:
    url: nats://localhost:4222
```

## 4. Receive a message into a process

```yaml
spring:
  nats:
    camunda:                          # or cibseven / cadenzaflow
      subscriptions:
        - subject: order.new
          messageName: OrderReceived
          businessKeyHeader: X-Business-Key
```

Sets `natsPayload` and `natsSubject` as process variables.

```bash
nats pub order.new '{"orderId":"A-1001"}' -H X-Business-Key:A-1001
```

**Flowable** uses an Event Registry channel instead:

```json
{ "key": "orderInboundChannel", "channelType": "inbound", "type": "nats",
  "deserializerType": "json",
  "channelEventKeyDetection": { "fixedValue": "orderEvent" },
  "channelFields": [ { "name": "subject", "stringValue": "order.new" } ] }
```

## 5. Durable delivery with a DLQ

```yaml
        - subject: payment.completed
          messageName: PaymentConfirmed
          jetstream: true
          durableName: payment-consumer
          ackWait: 30s                # raise if processing is slow
          maxDeliver: 5               # then routed to the DLQ
          dlqSubject: dlq.payment.completed
          autoCreateStream: true
          streamName: PAYMENTS
```

At-least-once. Make consumers idempotent.

## 6. Publish from a service task

```xml
<serviceTask id="notifyOrder" camunda:delegateExpression="${natsPublishDelegate}">
  <extensionElements>
    <camunda:field name="subject" stringValue="order.completed" />
    <camunda:field name="payloadVariable" stringValue="orderPayload" />
  </extensionElements>
</serviceTask>
```

Also available: `${jetStreamPublishDelegate}`, `${natsRequestReply}`.

## 7. Call an external worker

```xml
<serviceTask id="sendSms" camunda:delegateExpression="${natsRequestReply}">
  <extensionElements>
    <camunda:field name="subject"         stringValue="task.send-sms" />
    <camunda:field name="timeout"         stringValue="30s" />
    <camunda:field name="payloadVariable" stringValue="smsPayload" />
    <camunda:field name="resultVariable"  stringValue="smsResult" />
  </extensionElements>
</serviceTask>
```

```go
nc.QueueSubscribe("task.send-sms", "sms-workers", func(msg *nats.Msg) {
    nc.Publish(msg.Reply, processSMS(msg.Data))
})
```

Request-reply holds the engine DB transaction open. For high volume use the external-task bridge:

```yaml
spring:
  nats:
    camunda:
      a2:
        topics: [ send-sms ]
        defaults:
          lock-duration-seconds: 300   # required, no default
```

## 8. Move history off the engine database

```yaml
spring:
  nats:
    camunda:
      history:
        audit-critical-classes: [ PROCESS_INSTANCE, VARIABLE ]   # outbox, at-least-once
history:                                                          # everything else: post-commit
  projection:
    datasource:
      jdbc-url: jdbc:postgresql://localhost:5432/history
      username: history
      password: ${HISTORY_DB_PASSWORD}
  retention:
    bulk-default-days: 90
```

Needs `nats-history-projection` on the classpath.

## Verify a release

```bash
gpg --keyserver keyserver.ubuntu.com --recv-keys E610505884534DB9
gpg --verify nats-core-0.7.0.jar.asc nats-core-0.7.0.jar   # → Good signature: oss@3eai-labs.com
```

## Common fixes

| Symptom | Fix |
|---|---|
| Message never arrives | Check exact subject; confirm an instance waits on that message name |
| Redelivery loop → DLQ | Raise `ackWait`, not `maxDeliver` |
| External task reclaimed mid-run | Raise `lock-duration-seconds` |
| `NoClassDefFoundError` Micrometer | Add Actuator + a registry to your app |
| `cadenzaflow-engine` unresolved | Not on Central — build from source or use `-DskipCadenzaflow` |

Full reference: [USER_GUIDE.md](USER_GUIDE.md)
