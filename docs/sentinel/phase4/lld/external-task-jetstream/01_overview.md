# 01 — Genel Bakış (Module Overview)

**Kapsam:** Basamak-1 "External Task / Event-Driven Work Offload over JetStream" — `nats-bpm-channels`.
**Girdi:** `docs/sentinel/phase3/HLD.md` §2 (bileşen mimarisi), §3 (modül/paket yerleşimi), §9 (izlenebilirlik), §11 (doğrulama kapanışı); `docs/sentinel/phase3/API_CONTRACTS.md` + `api/asyncapi.yaml`; `docs/sentinel/phase2/*`; `docs/sentinel/phase1/*`; 8 ADR (hepsi Kabul).
**Rol:** Developer (Phase 4 — LLD). HLD onaylı, ADR'ler kilitli; bu belge onları **değiştirmez**, somutlaştırır.

---

## Neden bu belge var

HLD (§1-§2), basamak-1'in mimarisini bileşen düzeyinde tanımladı: iki motor idiomu (A2 = Camunda7/CadenzaFlow external-task-over-JetStream; Event Registry = Flowable) ortak JetStream substratında buluşuyor. Bu LLD, HLD'nin 12 bileşenini (§9 izlenebilirlik tablosu) **sınıf/metot/config-alan düzeyinde** somutlaştırır: hangi sınıf, hangi paket, hangi metot imzası, hangi mevcut kod noktasına (file:line) bağlanıyor, hangi BR/FR/US'yi kapatıyor.

**Bu belge LLD'dir, kod değildir.** Pseudo-code ve metot imzaları düzeyinde durur (LLD_GUIDELINE §2.3); gerçek implementasyon Phase 5'tedir.

---

## Kapsanan bileşenler (HLD §2 → bu modülün dosyaları)

| HLD §2 bileşeni | Bu modüldeki dosya | Modül (Maven) |
|---|---|---|
| A2ExternalTaskBehavior + BpmnParseListener (§2.1) | `03_classes/2_camunda_a2.md` §1 | `camunda-nats-channel` |
| A2PostCommitPublisher (§2.2) | `03_classes/2_camunda_a2.md` §2 | `camunda-nats-channel` |
| A2OrphanSweep + SweepLeaderLease (§2.3) | `03_classes/2_camunda_a2.md` §3 + `03_classes/1_nats_core_common.md` §3 | `camunda-nats-channel` + `nats-core` |
| A2CompletionBridge (§2.4) | `03_classes/2_camunda_a2.md` §4 | `camunda-nats-channel` |
| A2IncidentBridge (§2.5) | `03_classes/2_camunda_a2.md` §5 | `camunda-nats-channel` |
| FailureEventBridge (§2.6) | `03_classes/4_flowable.md` §2 | `flowable-nats-channel` |
| Ortak substrat + 5 kontrat-fix (§2.7) | `03_classes/1_nats_core_common.md` §2, `04_interfaces/1_contract_fixes.md` | `nats-core` |
| Flowable outbound/inbound olgunluk + delegate phase-out (§2.8) | `03_classes/4_flowable.md` §1/§3 | `flowable-nats-channel` (+ 3 modül silme) |
| Testcontainers bench (§2.9) | `03_classes/5_bench.md` | **`nats-bpm-bench`** (yeni) |
| A2 aynası (CadenzaFlow, §3) | `03_classes/3_cadenzaflow_a2_mirror.md` | `cadenzaflow-nats-channel` |
| Umbrella-lock config + validasyon (ADR-0001) | `08_config.md` §1 | `nats-core` |
| Transport + subject-level authz (ADR-0008) | `09_security/1_transport_authz.md` | `nats-core` (config) + deploy |

---

## Okuma sırası (önerilen)

1. `02_package_structure.md` — hangi sınıf hangi Maven modülüne/paketine gider (ADR-0007).
2. `03_classes/1_nats_core_common.md` — engine-nötr temel (herkes buna bağımlı: `publishToDlq`, `SweepLeaderLease`, config, metrik).
3. `03_classes/2_camunda_a2.md` — A2'nin beş bileşeni (behavior, publisher, sweep, completion-bridge, incident-bridge).
4. `03_classes/3_cadenzaflow_a2_mirror.md` — ayna farkları (yalnız import paketi, ADR-0007).
5. `03_classes/4_flowable.md` — FailureEventBridge + olgunluk fix'leri + delegate phase-out.
6. `03_classes/5_bench.md` — `nats-bpm-bench` modülü.
7. `04_interfaces/1_contract_fixes.md` — 5 kontrat-fix'in somut file:line değişiklik noktaları.
8. `05_sequences.md`, `06_state_machines.md`, `07_errors.md`, `08_config.md`, `09_security/1_transport_authz.md`, `10_metrics.md`, `99_deployment.md`.

Sequence diyagramları ve tam error registry, LLD-modülü dışında ayrı Phase 4 teslimatlarıdır (`docs/sentinel/phase4/SEQUENCE_DIAGRAMS.md`, `docs/sentinel/phase4/ERROR_REGISTRY.md`); `05_sequences.md`/`07_errors.md` bunlara **köprü** (cross-reference) dosyalarıdır, içerik tekrarlamaz (Sentinel MASTER_WORKFLOW §0.6: "delta/tekrar" yasağı — tek doğruluk kaynağı ilkesi).

---

## Kapsam dışı / reddedilen (bu LLD yeniden AÇMAZ)

Phase1-3'ün kilitlediği ve bu LLD'nin **değiştirmediği** kararlar: hot-poll (D-A), timer-only escalation (D-D), advisory-tabanlı DLQ tespiti (D-E), heartbeat/`msg.inProgress()` (D-H), gRPC ön kapısı (D-G), `a2-core` ortak soyutlama modülü (ARCH-Q4, basamak-6'ya ertelendi), token-move/completion tx'in kaldırılması (P2, basamak-6), basamak 2/3/5 kapsamı (history offload, büyük değişken externalization, DB sharding).

---

## Sürüm / kod tabanı referansları

- Motor fork kanıtları: `~/Workspaces/cadenzaflow/cadenzaflow-bpm-platform/engine` (bu LLD'de her iddia bu ağaçtan bizzat okunarak yeniden doğrulandı — phase2/3'ün `[BA-VERIFIED]` etiketli bulgularının üstüne).
- Mevcut repo kodu: `nats-core`, `camunda-nats-channel`, `cadenzaflow-nats-channel`, `flowable-nats-channel` (bu dört modül LLD'nin zeminidir; `cadenzaflow-nats-channel` ve `camunda-nats-channel` byte-aynen ayna — bu fazda `diff` ile doğrulandı, bkz. `03_classes/3_cadenzaflow_a2_mirror.md`).
- Build: Java 21 gerektirir (memory `build-requires-java21` — sistem default Java 25 Mockito'yu kırar).

**Rev-1, 2026-07-14, ilk yazım.**
