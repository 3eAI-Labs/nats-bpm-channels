# LLD Manifest — history-offload (Basamak-2)

**Owning modules:** `nats-core`, `camunda-nats-channel`, `cadenzaflow-nats-channel`, `nats-bpm-bench` (basamak-1 modülleri, genişler), **`nats-history-projection`** (yeni modül).
**Last touched:** 2026-07-20 (LLD-Q1…5 karar kaydı işlendi)
**Durum:** LLD-Q1…5 KARARA BAĞLANDI (2026-07-20, 5/5 önerilen — `01_overview.md` karar kaydı) — phase-review bekliyor.
**Sentinel fazı:** Phase 4 — Developer (LLD). Girdi: `docs/sentinel/step2/phase3/` (HLD, API_CONTRACTS, INTEGRATION_MAP, DATA_OWNERSHIP.yaml, 11 ADR — hepsi Kabul).

---

## File Index

| File | Topic | Touches (sınıf/karar) |
|---|---|---|
| `01_overview.md` | Modül genel bakış, okuma sırası, kapsam dışı, "Phase3'ün devrettiği doğrulamalar" kapanışı (5 madde, fork-kanıtlı), LLD-Q1…5 | HLD §3 tüm bileşenler, ADR-0009…0019 |
| `02_package_structure.md` | Maven modülleri (+1 yeni: `nats-history-projection`), paket kökleri, bağımlılık yönü | ADR-0007 deseni |
| `03_classes/1_handler_outbox.md` | `NatsHistoryEventHandler`, `CompactHistoryOutboxWriter`, `HistoryPostCommitPublisher`, `HistoryClassificationProperties` (pointer) | BR-HDL-001…007, ADR-0009/0010 |
| `03_classes/2_relay_projection.md` | `HistoryOutboxRelay`, `HistoryProjectionConsumer`, `ProjectionStore`, `HistoryDlqConsumer`/`HistoryDlqInspectionConsumer` | BR-REL-001…006, ADR-0010/0011/0012/0013/0019 |
| `03_classes/3_query_api.md` | `HistoryQueryApi`, `HistoryQueryAuthzSpi`, Cockpit-körleşme haritası | BR-QRY-001/002/003, ADR-0014 |
| `03_classes/4_cutover_reconciliation.md` | `ReconciliationJob`, `CutoverControlPlane`, `ClassCutoverStateRegistry`, `CutoverRollback` | BR-CUT-001…004, BR-HDL-005, ADR-0015/0009 |
| `03_classes/5_bench.md` | `HistoryBenchScenario`, `HistoryDbWriteOpReport`, `HistoryStreamProvisioner`, `RelayFailoverBenchScenario` | BR-OBS-001…003, BR-DBT-001…003, ADR-0015/0013/0019 |
| `03_classes/6_governance.md` | `RetentionEnforcementJob`, `ErasurePipeline`, `ErasureScopeResolver`, `PseudonymTokenGenerator`, `PseudonymizationVaultClient` | BR-PII-001…005, ADR-0016/0017/0018 |
| `04_interfaces/1_wire_contract_refs.md` | Köprü → `api/asyncapi.yaml`/`api/openapi.yaml`, sınıf↔kanal eşlemesi | ADR-0013/0014 |
| `04_interfaces/2_projection_dtos.md` | `HistoryEventEnvelope`, `EntityHistoryRecord`, `LogHistoryRecord`, `UpsertOutcome` | ADR-0011/0012 |
| `05_sequences.md` | Köprü → `../../SEQUENCE_DIAGRAMS.md` | — |
| `06_state_machines.md` | 4 durum makinesi (`BUSINESS_LOGIC.md §2.1…2.4`) → sınıf bağlaması | BUSINESS_LOGIC §2.1-2.4, ADR-0015/0016/0017/0018 |
| `07_errors.md` | Köprü → `../../ERROR_REGISTRY.md` + sınıf→kod hızlı bakış | EXCEPTION_CODES.md (42 kod) |
| `08_config.md` | `HistoryClassificationProperties`, `HistoryOutboxProperties`, KV bucket'lar (`history-cutover-state`, `history-relay-leader`), `HistoryProjectionProperties`, `ReconciliationProperties`, `HistoryCutoverProperties`, `RetentionProperties`, `PseudonymVaultDataSourceProperties` | ADR-0010/0011/0015/0016/0018, LLD-Q1…5 |
| `09_security/1_transport_authz.md` | History subject-ACL genişlemesi (ADR-0008→0019) | ADR-0019, NFR-S4/S7 |
| `09_security/2_pii_protection.md` | At-rest şifreleme, `PiiMaskingService`, log/metrik PII sızdırmazlığı, pseudonymization güvenlik modeli | DP-9…DP-16, NFR-S1…S8 |
| `10_metrics.md` | `NatsChannelMetrics` genişlemesi, 5 alarm tanımı | NFR-O1…O3 |
| `99_deployment.md` | `application.yml` örnekleri, stream/KV provisioning, partition-rebalance + rolling-restart flip runbook'ları, kiracı RTO/RPO şablonu | ADR-0015/0019, ARCH-Q3/Q5 |

---

## Cross-References

| From | To | Reason |
|---|---|---|
| `03_classes/1_handler_outbox.md` | `03_classes/4_cutover_reconciliation.md` §2.2 | `ClassCutoverStateRegistry` kullanır (per-class routing) |
| `03_classes/1_handler_outbox.md` | `01_overview.md` "Hot-reconfigure" | Fork-kanıtlı gerekçe (`internalDbDelegate` neden bizim elimizde) |
| `03_classes/2_relay_projection.md` | `03_classes/1_handler_outbox.md` §2 | `compact_history_outbox` satırını okur (relay) |
| `03_classes/2_relay_projection.md` §1 | basamak-1 `SweepLeaderLease` | Yeniden kullanım, farklı bucket/key parametreleri |
| `03_classes/2_relay_projection.md` §4 | basamak-1 `DlqPublisher` | Yeniden kullanım (byte-mirror + custody-transfer) |
| `03_classes/3_query_api.md` | `03_classes/2_relay_projection.md` §3 | `ProjectionStore`'dan okur |
| `03_classes/3_query_api.md` | `03_classes/4_cutover_reconciliation.md` §1 | `class_cutover_state` (`cutoverState` bilgilendirici alan) |
| `03_classes/4_cutover_reconciliation.md` §2 | `08_config.md` §2 | `history-cutover-state` KV bucket şeması |
| `03_classes/5_bench.md` §1 | `03_classes/1_handler_outbox.md` §2, `03_classes/2_relay_projection.md` §1 | `compactOutboxRowCount`/`compactOutboxPayloadRowCount` ölçer (LLD-Q1) |
| `03_classes/6_governance.md` §3 | `03_classes/1_handler_outbox.md` §2 | `PseudonymTokenGenerator` paylaşımı (`nats-core`, tx-içi + downstream aynı algoritma) |
| `04_interfaces/1_wire_contract_refs.md` | `docs/sentinel/step2/phase3/api/asyncapi.yaml` + `api/openapi.yaml` | Tek doğruluk kaynağı (inline spec YASAK) |
| `05_sequences.md` | `docs/sentinel/step2/phase4/SEQUENCE_DIAGRAMS.md` | Diyagramlar orada yaşar |
| `06_state_machines.md` | `docs/sentinel/step2/phase2/BUSINESS_LOGIC.md §2.1…2.4` | Durum makinesi kaynağı, DEĞİŞTİRİLMEZ |
| `07_errors.md` | `docs/sentinel/step2/phase4/ERROR_REGISTRY.md` | Tam kayıt orada yaşar (42 kod) |
| `08_config.md` | `docs/sentinel/step2/phase4/DB_ACCESS_MAP.md` §4 | KV bucket şeması + DB erişimi çapraz-referans |
| `08_config.md` | `docs/sentinel/step2/phase4/DB_SCHEMA.md` | `RetentionProperties`/`PseudonymVaultDataSourceProperties` şema karşılığı |
| `09_security/1_transport_authz.md` | `docs/sentinel/phase4/lld/external-task-jetstream/09_security/1_transport_authz.md` (basamak-1) | Genişletilen taban (tekrar YOK, yalnız yeni ACL satırları) |
| `09_security/2_pii_protection.md` | `docs/sentinel/step2/phase1/DATA_CLASSIFICATION.md` DP-9…DP-16 | Kaynak sınıflandırma |
| `10_metrics.md` | `docs/sentinel/step2/phase4/ERROR_REGISTRY.md` §4 | Alarm ↔ kod eşlemesi |
| `99_deployment.md` §3 | `01_overview.md` LLD-Q2/LLD-Q3 | Rebalance + rolling-restart flip runbook'larının karar dayanağı |
| `99_deployment.md` §4 | `docs/sentinel/step2/phase1/TENANT_PII_CHECKLIST_TEMPLATE.md` | RTO/RPO kiracı-parametresi (Phase 5'te şablona eklenecek) |

---

## Open Questions / Drift Log

**LLD-Q1…5:** tam metin + öneri `01_overview.md` sonunda. Özet: (1) compact-outbox companion-satır NFR-P2 sayımı, (2) partition N=8 default, (3) cutover-flip KV mekanizması, (4) `class_cutover_state` projeksiyon DB'de yaşar, (5) BA-Q7 çarpanı=5×. Hiçbiri D-A…G/PO-Q1…7/BA-Q1…8/ARCH-Q1…5'i yeniden AÇMAZ.

**Drift kontrolü:** Bu manifest, klasördeki her `.md` dosyasını listeler (18/18); tüm cross-reference hedefleri bu fazda elle doğrulandı (dosyalar mevcut veya aynı commit'te yaratıldı). Basamak-1 modülüne (`docs/sentinel/phase4/lld/external-task-jetstream/`) yapılan tek referans (`09_security/1_transport_authz.md`) o modülün DEĞİŞMEDİĞİNİ teyit eder — basamak-1 LLD bu fazda DÜZENLENMEDİ.

**Rev-1, 2026-07-19T04:38:33Z, ilk yazım — modüler yerleşim doğrudan uygulandı (görev talimatı: basamak-1 şablonunu ŞABLON al, standart tek-dosya eşiği testi gerekmez; 21 bileşen + DB + config + security + metrics tek dosyada kesinlikle 500 satır/30KB/10 bölüm sınırını aşardı).**
