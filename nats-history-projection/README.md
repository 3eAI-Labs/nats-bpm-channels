# nats-history-projection

Engine-neutral history-offload projection service for `nats-bpm-channels`.

The module consumes the `HISTORY` JetStream stream produced by the Camunda 7, CIBSeven and
CadenzaFlow adapters, then maintains a PostgreSQL projection outside the engine database. It also
contains the read-only history query API, reconciliation and cutover control plane, retention
enforcement, erasure pipeline and pseudonymization vault integration.

The engine database remains the source of orchestration state. This module moves high-volume
history and governance work off that database and into a separate projection store.

## What It Provides

| Capability | Purpose |
|---|---|
| `HistoryProjectionConsumer` | Consumes history events from JetStream and writes denormalized projection rows |
| `HistoryQueryApi` / `HistoryQueryController` | Read-only REST/JSON access to process, activity, task and variable history |
| `ReconciliationJob` | Compares engine history tables with projection rows before class cutover |
| `CutoverControlPlane` / `CutoverRollback` | Moves history classes through dual-run and projection-read states |
| `RetentionEnforcementJob` | Applies class-based retention in the projection store |
| `ErasurePipeline` | Handles right-to-erasure flows without deleting legally retained audit structure |
| `PseudonymizationVaultClient` | Stores identity-to-token mappings in an isolated vault datasource |

Overview documentation is in `docs/WHITE_PAPER.md`, `docs/ARCHITECTURE_OVERVIEW.md` and
`docs/user/USER_GUIDE.md`.

## Single-Engine Deployment

The default auto-configuration targets one engine at a time. It registers one
`ReconciliationJob` and one `RetentionEnforcementJob`, parameterized by:

```yaml
history:
  engine-id: camunda
```

Use `camunda`, `cibseven` or `cadenzaflow` to match the producing adapter. The projection tables
carry `engine_id` on every row, but the default Spring configuration intentionally avoids guessing
how many engine databases your application owns.

## Multi-Engine Deployment

If one deployment writes events from more than one engine family into the same projection store,
the stream and database schema already support it:

```text
history.<engineId>.<class>.<processInstanceId>
```

Rows are separated by the `engine_id` column. A single `HistoryProjectionConsumer` can project all
events because partitioning is based on `processInstanceId` and the event envelope carries the
engine id.

Reconciliation and retention are different: they are engine-scoped jobs. Each engine needs its own
job instance because reconciliation reads that engine's own `ACT_HI_*` tables.

```java
@Bean
public ReconciliationJob cibsevenReconciliationJob(
        @Qualifier("projectionDataSource") DataSource projectionDataSource,
        @Qualifier("cibsevenEngineDataSourceReadOnly") DataSource cibsevenEngineDataSourceReadOnly,
        ClassCutoverStateStore stateStore,
        NatsChannelMetrics metrics,
        ReconciliationProperties properties) {
    return new ReconciliationJob(projectionDataSource, cibsevenEngineDataSourceReadOnly,
            stateStore, metrics, properties, "cibseven");
}
```

Use the same pattern for `RetentionEnforcementJob`, passing the matching `engineId`. The
`CutoverControlPlane` and `CutoverRollback` APIs already take `engineId` per call, so they do not
need to be duplicated.

If two engines require different audit-critical class sets, do not share one global
`history.reconciliation.audit-critical-classes` property. Provide engine-specific
`ReconciliationProperties` beans instead.

## Query API Constraint

`HistoryQueryApi` and `HistoryQueryController` do not expose `engineId` as a request parameter. The
API was designed around the primary single-engine deployment mode and the fixed core query contract.

In a multi-engine deployment, `processInstanceId`, `taskId` or similar identifiers could
theoretically collide across engines. That risk is low with UUID-like ids and irrelevant for
single-engine deployments, but a `HistoryQueryApi` instance backed by one shared, unfiltered store
cannot safely disambiguate colliding ids.

Tenants with real cross-engine id collision risk should use separate projection stores per engine,
engine-filtered database views per API instance, or add an `engineId`-aware query contract before
exposing shared history queries. Routing above this module is safe only when it routes to a data
source that is already filtered by engine.
