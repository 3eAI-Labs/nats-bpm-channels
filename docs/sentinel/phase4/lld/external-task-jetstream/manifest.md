# LLD Manifest — external-task-jetstream (Basamak-1)

**Owning modules:** `nats-core`, `camunda-nats-channel`, `cadenzaflow-nats-channel`, `flowable-nats-channel`, `nats-bpm-bench` (yeni).
**Last touched:** 2026-07-14T20:32:24Z
**Sentinel fazı:** Phase 4 — Developer (LLD). Girdi: `docs/sentinel/phase3/` (HLD, API_CONTRACTS, INTEGRATION_MAP, 8 ADR — hepsi Kabul).

---

## File Index

| File | Topic | Touches (sınıf/karar) |
|---|---|---|
| `01_overview.md` | Modül genel bakış, okuma sırası, kapsam dışı | HLD §2 tüm bileşenler |
| `02_package_structure.md` | Maven modülleri, paket kökleri, bağımlılık yönü, silinen sınıflar | ADR-0007, BR-MIG-001 |
| `03_classes/1_nats_core_common.md` | BpmHeaders/DlqHeaders, DlqPublisher, SweepLeaderLease/JetStreamKvManager, DlqBridgeCircuitBreakerFactory | BR-SUB-001/002/003/007/008, ADR-0002/0004/0006/0007 |
| `03_classes/2_camunda_a2.md` | A2ExternalTaskBehavior, A2BpmnParseListener, A2PostCommitPublisher, A2OrphanSweep, A2CompletionBridge, A2IncidentBridge | BR-A2-001…013, FR-A1…A13, US-A1…A8, ADR-0002/0003/0004/0005 |
| `03_classes/3_cadenzaflow_a2_mirror.md` | Camunda↔CadenzaFlow ayna eşleme, upgrade prosedürü | ADR-0005/0007, NFR-M1/M2 |
| `03_classes/4_flowable.md` | FailureEventBridge, JetStreamInboundEventChannelAdapter değişiklikleri, namespace validasyon, delegate phase-out | BR-FLW-001…005, BR-SUB-004, BR-MIG-001, FR-B1…B5/E1, US-B1…B5/E1 |
| `03_classes/5_bench.md` | `nats-bpm-bench` sınıf iskeleti | BR-OBS-001/002/003, FR-D1…D3, US-D1…D3, ADR-0007 |
| `04_interfaces/1_contract_fixes.md` | 5 kontrat-fix'in file:line değişiklik noktaları | BR-SUB-001/002/003/006/007, ADR-0006 |
| `05_sequences.md` | Köprü → `../../SEQUENCE_DIAGRAMS.md` | — |
| `06_state_machines.md` | A2/custody-transfer/umbrella-lock durum makinesi → sınıf bağlaması | BUSINESS_LOGIC §2.1/§2.2, ADR-0001/0003 |
| `07_errors.md` | Köprü → `../../ERROR_REGISTRY.md` + sınıf→kod hızlı bakış | EXCEPTION_CODES.md (23 kod) |
| `08_config.md` | UmbrellaLockProperties/Calculator/Validator, NamespaceValidator, KV config, CB eşikleri | ADR-0001/0002/0004, BAQ-3/BAQ-4 |
| `09_security/1_transport_authz.md` | NatsTransportSecurityGuard, subject-ACL şeması, ikincil savunma | ADR-0008, NFR-S3/S4, DP-4/DP-5 |
| `10_metrics.md` | NatsChannelMetrics genişlemesi, CB metrikleri, infra-metrik ayrımı | FR-D2, NFR-O1, US-D2 |
| `99_deployment.md` | application.yml örneği, stream/KV provisioning, upgrade runbook, silinen-dosya checklist | ADR-0005/0007 |

---

## Cross-References

| From | To | Reason |
|---|---|---|
| `03_classes/2_camunda_a2.md` | `03_classes/1_nats_core_common.md` | `DlqPublisher`, `SweepLeaderLease`, `DlqBridgeCircuitBreakerFactory` kullanır |
| `03_classes/3_cadenzaflow_a2_mirror.md` | `03_classes/2_camunda_a2.md` | Birebir ayna — davranış tanımı orada |
| `03_classes/4_flowable.md` | `03_classes/1_nats_core_common.md` | `DlqPublisher`, `BpmHeaders.extractTraceIdWithFallback` kullanır |
| `03_classes/4_flowable.md` | `08_config.md` | `NamespaceValidator` çağrısı |
| `04_interfaces/1_contract_fixes.md` | `03_classes/1_nats_core_common.md` §2 | Fix'lerin ortak uygulama sınıfı |
| `05_sequences.md` | `docs/sentinel/phase4/SEQUENCE_DIAGRAMS.md` | Diyagramlar orada yaşar (tek doğruluk kaynağı) |
| `06_state_machines.md` | `BUSINESS_LOGIC.md` §2.1/§2.2 (phase2) | Durum makinesi kaynağı, DEĞİŞTİRİLMEZ |
| `07_errors.md` | `docs/sentinel/phase4/ERROR_REGISTRY.md` | Tam kayıt orada yaşar |
| `08_config.md` | `docs/sentinel/phase4/DB_ACCESS_MAP.md` §3 | KV bucket şeması orada detaylı |
| `09_security/1_transport_authz.md` | `99_deployment.md` §2 | Deploy artefaktı pointer'ı |
| `10_metrics.md` | `docs/sentinel/phase4/ERROR_REGISTRY.md` §4 | Alarm tanımları orada |

---

## Open Questions / Drift Log

Toplanmış LLD-QUESTIONS (Levent onayına — tam metin ilgili dosyalarda):

1. **Çapraz-motor sweep-leader paylaşımı** (`03_classes/3_cadenzaflow_a2_mirror.md` sonu) — tek clusterda hem Camunda hem CadenzaFlow varsa `a2-sweep-leader` KV bucket'ının paylaşımı/ayrımı.
2. **`eventReceived` no-match davranışı belirsizliği** (`03_classes/4_flowable.md` sonu) — `FailureEventBridge`'in exception-yakalama varsayımı, test'le doğrulanana kadar geçici (bkz. `TEST_SPECIFICATIONS.md` (d)).
3. **`failure_event_correlation_miss` eşik-alarmı sayıları** (`docs/sentinel/phase4/ERROR_REGISTRY.md` §4.2) — BAQ-8'in "süreklilik/eşik-bazlı alarm" ilkesini somutlaştıran sayısal eşikler (10dk'da ≥3 → WARN, 60dk'da ≥10 → CRITICAL) bu LLD'nin **önerisidir**, formülden türetilmiş değildir (L gibi) — on-call sayfalama yüküne doğrudan etkisi olduğundan Levent onayı istenir.

Bu iki madde HARİCİNDE, LLD hiçbir kilitli kararı (25 karar + 8 ADR) değiştirmemiştir; her yeni LLD-düzeyi netleştirme (§1.4 marj formülü, §1.3 literal-topic kapsamı, vb.) ilgili dosyada "kilitli karara aykırı DEĞİL" gerekçesiyle işaretlenmiştir.

**Drift kontrolü:** Bu manifest, klasördeki her `.md` dosyasını listeler; hiçbir cross-reference kırık değildir (bu fazda elle doğrulandı — tüm hedef dosyalar mevcut).

**Rev-1, 2026-07-14T20:32:24Z, ilk yazım (modüler yerleşim — LLD_GUIDELINE §2.10.1 eşiği proaktif olarak aşılmadan uygulandı: 12 bileşen + 5 kontrat-fix + config + security + metrics tek dosyada 500 satır/30KB/10 bölüm sınırını kesinlikle aşacaktı).**
