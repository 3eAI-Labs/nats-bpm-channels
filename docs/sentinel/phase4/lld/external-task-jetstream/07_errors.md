# 07 — Hata Kodları (Köprü)

Tam kayıt (23 kod → Java sınıf/hiyerarşi, log formatı, MDC alanları, metrik bağlaması, alarm kanalı) tek doğruluk kaynağı olarak burada **DEĞİL**, ayrı Phase 4 teslimatında yaşar:

**→ `docs/sentinel/phase4/ERROR_REGISTRY.md`**

## Sınıf → kod üretim noktası (hızlı bakış — tam ayrıntı ERROR_REGISTRY.md'de)

| Sınıf (bu modül) | Ürettiği kod(lar) |
|---|---|
| `A2ExternalTaskBehavior` | (dolaylı — `03_classes/2_camunda_a2.md` §1, guard-test kanıtı gerektirir, kod fırlatmaz) |
| `A2PostCommitPublisher` | `EXT_JETSTREAM_PUBLISH_UNAVAILABLE` |
| `A2OrphanSweep` | `SYS_SWEEP_QUERY_FAILED`, `SYS_SWEEP_RELOCK_FAILED`, `SYS_SWEEP_REPUBLISH_FAILED`, `BUS_TASK_RETRIES_EXHAUSTED` (no-op, DEBUG), `RES_TASK_SUSPENDED` (no-op, DEBUG) |
| `A2CompletionBridge` | `RES_EXTERNAL_TASK_NOT_FOUND`, `SYS_SENTINEL_WORKER_CONFLICT`, `VAL_EMPTY_MESSAGE_BODY`, `BUS_REPLY_DELIVERY_BUDGET_EXCEEDED`, `SYS_DLQ_PUBLISH_FAILED` |
| `A2IncidentBridge` | `SYS_DLQ_BRIDGE_PROCESSING_FAILED`, `BUS_INCIDENT_ALREADY_CREATED` (no-op, DEBUG/INFO) |
| `FailureEventBridge` | `RES_FAILURE_EVENT_CORRELATION_MISS`, `SYS_DLQ_BRIDGE_PROCESSING_FAILED` |
| `JetStreamInboundEventChannelAdapter` (Flowable) | `VAL_EMPTY_MESSAGE_BODY`, `BUS_REPLY_DELIVERY_BUDGET_EXCEEDED`, `SYS_DLQ_PUBLISH_FAILED`, `BUS_EVENT_CORRELATION_NOT_FOUND` |
| `DlqPublisher` (nats-core) | `SYS_DLQ_PUBLISH_FAILED` (FAILED_* outcome'ları) |
| `UmbrellaLockValidator` (nats-core) | `VAL_UMBRELLA_LOCK_TOO_SHORT` |
| `NamespaceValidator` (nats-core) | `VAL_TOPIC_NAMESPACE_COLLISION` |
| `nats-bpm-bench` sınıfları | `SYS_BENCH_ENVIRONMENT_UNAVAILABLE`, `BUS_BENCH_METRIC_REGRESSION`, `SYS_BENCH_SLI_DRIFT` |
| Worker (repo dışı) | `BUS_WORKER_BUSINESS_ERROR`, `SYS_WORKER_TRANSIENT_FAILURE`, `BUS_JOB_DELIVERY_BUDGET_EXCEEDED` (bu repo bu kodları ÜRETMEZ, yalnız tüketir/dokümante eder) |
