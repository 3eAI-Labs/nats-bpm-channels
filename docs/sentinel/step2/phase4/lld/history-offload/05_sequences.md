# 05 — Sequence Diyagramları (Köprü)

Tam diyagram kayıtları (Mermaid, `mmdc` ile doğrulanmış) tek doğruluk kaynağı olarak burada **DEĞİL**, ayrı Phase 4 teslimatında yaşar:

**→ [`docs/sentinel/step2/phase4/SEQUENCE_DIAGRAMS.md`](../../SEQUENCE_DIAGRAMS.md)**

## Diyagram → sınıf bağlaması (hızlı bakış)

| Diyagram (SEQUENCE_DIAGRAMS.md §) | Kapsayan sınıflar (bu modül) |
|---|---|
| §1 Audit-kritik tx-içi outbox→relay→PubAck→delete | `NatsHistoryEventHandler`, `CompactHistoryOutboxWriter`, `HistoryOutboxRelay` |
| §2 Bulk post-commit publish | `NatsHistoryEventHandler`, `HistoryPostCommitPublisher` |
| §3 Projeksiyon consume + merge-upsert (stale discard) | `HistoryProjectionConsumer`, `ProjectionStore` |
| §4 History-DLQ (delivery-budget aşımı) | `HistoryProjectionConsumer`, `HistoryDlqConsumer`, `HistoryDlqInspectionConsumer` |
| §5 Reconciliation → cutover kapısı → rolling-restart flip | `ReconciliationJob`, `CutoverControlPlane`, `ClassCutoverStateRegistry` |
| §6 Erasure kapsam-onayı akışı (BA-Q6) | `ErasurePipeline`, `ErasureScopeResolver` |
| §7 Retention job + audit-log | `RetentionEnforcementJob` |
| §8 Pseudonymization: saf-hesap → downstream kasa-persist (BA-Q5) | `PseudonymTokenGenerator`, `CompactHistoryOutboxWriter`, `PseudonymizationVaultClient` |
