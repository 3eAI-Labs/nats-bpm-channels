# Architecture Overview

**nats-bpm-channels 0.7.0** · Apache License 2.0 · 3eAI Labs Ltd

---

## System architecture

```
              ┌───────────────────────────────────────────────────────┐
              │                    BPM ENGINE                          │
              │   Flowable 7 · Camunda 7 · CIBSeven 2 · CadenzaFlow 1  │
              │                                                        │
              │   BPMN execution · DMN · human tasks                   │
              │   ORCHESTRATION STATE  ──────────────► engine database │
              └───┬────────────┬────────────┬────────────┬─────────────┘
                  │            │            │            │
           public SPI seams — no engine source is modified
                  │            │            │            │
         ┌────────▼──┐  ┌──────▼─────┐ ┌────▼──────┐ ┌───▼──────────┐
         │ external  │  │  history   │ │  large    │ │  outbound    │
         │ task      │  │  events    │ │ variables │ │  messages    │
         │ dispatch  │  │            │ │           │ │              │
         └────────┬──┘  └──────┬─────┘ └────┬──────┘ └───┬──────────┘
                  │            │            │            │
              ┌───▼────────────▼────────────▼────────────▼───┐
              │            nats-core                          │
              │  connection · auth · TLS · JetStream mgmt     │
              │  headers · metrics · structured logging       │
              └───┬──────────────────────────────────────┬────┘
                  │                                      │
         ┌────────▼──────────────────┐        ┌──────────▼─────────┐
         │  NATS.io + JetStream      │        │  content-addressed  │
         │  partitioned · replayable │        │  payload store      │
         │  at-least-once            │        │  SHA-256 · refcount │
         └────┬─────────────────┬────┘        └──────────┬─────────┘
              │                 │                        │
     ┌────────▼───────┐  ┌──────▼──────────┐  ┌──────────▼─────────┐
     │ workers        │  │ nats-history-   │  │  PostgreSQL         │
     │ (any language) │  │ projection      │──►  (separate from     │
     └────────────────┘  └─────────────────┘  │   engine database)  │
                                               └────────────────────┘
```

The engine database keeps orchestration state and nothing is taken from it. Everything with volume
around that state moves through NATS.

---

## Data flow by capability

| Capability | Seam | Path | Guarantee |
|---|---|---|---|
| **External task dispatch** | `BpmnParseListener` + post-commit publisher | engine → JetStream → worker → completion bridge → engine | at-least-once (post-commit at-most-once + orphan sweep at-least-once = net at-least-once) |
| **History offload** | history event handler | engine → outbox *or* post-commit → JetStream → projection → PostgreSQL | per class: outbox = at-least-once, post-commit = at-most-once |
| **Large variables** | variable serializer | engine → (deferred, post-commit) → content-addressed store | at-least-once with reference counting |
| **Outbound messages** | `ExecutionListener` | engine → outbox *or* post-commit → NATS | per type: outbox = at-least-once, post-commit = at-most-once |
| **Inbound correlation** | subscription config | NATS → `createMessageCorrelation()` → engine | at-least-once; DLQ after `maxDeliver` |

---

## Key design decisions

| Decision | Choice | Why |
|---|---|---|
| Integration point | Public SPI only — no fork | An engine upgrade must not invalidate the integration; the Camunda 7 lineage already has a maintenance gap and does not need another fork |
| Dual-write problem | Two explicit paths, configured per class | Atomicity across two systems is not free; the trade is surfaced rather than hidden behind a default |
| Critical delivery | Transactional outbox + leader relay | Message commits with the state change; relay publishes and marks done |
| Best-effort delivery | Post-commit publish | Zero extra database writes; loss window accepted deliberately |
| Leader election | NATS KV lease with compare-and-swap | Single writer without a separate coordinator; lease expires if the holder dies |
| DLQ detection | In-band (`maxDeliver + 1`) | Advisories are real but best-effort and need a sequence lookup |
| DLQ failure | nak + alert + circuit breaker | No dlq-of-dlq, and never ack-drop — a message is never discarded to keep the pipeline moving |
| Duplicate suppression | `Nats-Msg-Id` dedup + idempotent complete/correlate | At-least-once delivery requires idempotent consumers |
| History ordering | Keyed by process instance, merge-upsert on stream sequence | Ordering matters within an instance, not across instances |
| Payload deduplication | SHA-256 content addressing + reference count | Identical content stored once; last release deletes, which makes erasure real |
| Custody transfer | Ack only after persistence changes hands | Applies in every role: worker, inbound, DLQ bridge |

---

## Scaling properties

| Property | Behaviour |
|---|---|
| Worker scale-out | No database cost — dispatch is push over JetStream; `fetchAndLock` is **0** on the happy path |
| Orphan sweep | Leader-only, amortised ≤ 1 read per period per cluster |
| Task completion | **1 short transaction per task remains** — the token move is still the engine's, and this is the honest ceiling of the SPI approach |
| History write path | Leaves the engine database entirely; projection scales independently by partition (default 8) |
| Worker fan-out | NATS queue group — exactly one worker takes each message |

Target SLIs, **not yet measured**: dispatch p95 ≤ 200 ms, reduced HikariCP active connections,
lock-wait ≈ 0. A benchmark harness (`nats-bpm-bench`) compares native polling against JetStream push
and ships in the repository; its results have not been published.

Measured: RPO = 0 and split-brain = 0 under real 3-replica JetStream KV failover; RTO ≤ 60 s bounded
by lease TTL.

---

## Deployment topology

```
┌─────────────────────────────────────────────────────────┐
│  Application pod                                         │
│  ┌────────────────────────────────────────────────────┐ │
│  │  Spring Boot 3.x · Java 21 (virtual threads)        │ │
│  │  BPM engine (embedded) + nats-bpm-channels adapter  │ │
│  └────────────────────────────────────────────────────┘ │
└───────┬──────────────────┬────────────────────┬─────────┘
        │ JDBC             │ NATS               │ JDBC
┌───────▼────────┐ ┌───────▼────────┐ ┌─────────▼─────────┐
│ engine DB      │ │ NATS cluster   │ │ projection DB      │
│ (orchestration │ │ 3 nodes,       │ │ (history + large   │
│  state only)   │ │ JetStream R3   │ │  payloads)         │
└────────────────┘ └───────┬────────┘ └───────────────────┘
                           │
                  ┌────────▼─────────┐
                  │ worker pods      │
                  │ any language     │
                  │ scale × N        │
                  └──────────────────┘
```

The adapter is a library, not a service — it runs inside the application that hosts the engine.
Only NATS and the projection database are additional infrastructure. Workers are independent
processes and need nothing but a NATS client.

Replication factor 3 is what the reliability evidence was produced against; the KV lease that drives
leader election depends on it for the stated RPO.

---

## Observability

41 Micrometer metrics across five families:

| Family | Examples |
|---|---|
| Connection | `nats.connection.reconnects`, `nats.connection.slow.consumers` |
| Messaging | `nats.inbound.consumed`, `nats.jetstream.inbound.ack` / `.nak` / `.dlq`, `nats.outbound.published` |
| External task | `nats.a2.dispatch.latency`, `nats.a2.sweep.republish`, `nats.a2.sweep.oldest_orphan_age_seconds` |
| History | `nats.history.outbox.written` / `.relayed` / `.oldest_row_age_seconds`, `nats.history.projection.lag_seconds` |
| Governance | `nats.history.retention.deleted_rows`, `nats.history.erasure.processed`, `nats.history.vault.access` |

The two `oldest_row_age_seconds` gauges and `projection.lag_seconds` are the ones to alert on: they
expose relay stall and projection lag before either becomes data loss.

Logs use SLF4J structured `kv()` format with `trace_id` propagated through MDC. The adapters publish
metrics but do not bundle a registry — add Actuator and a registry to the host application.

---

*Copyright 2026 3eAI Labs Ltd · <https://github.com/3eAI-Labs/nats-bpm-channels>*
