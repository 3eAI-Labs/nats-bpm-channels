# 03.3 — `cadenzaflow-nats-channel`: A2 Ayna (Mirror)

**Kaynak karar:** ADR-0007 §2 (ayna-tekrar ONAYLANDI, ARCH-Q4) + ADR-0005 §4 (Camunda ↔ CadenzaFlow ayrımı: yalnız paket importu farkı). NFR-M2: birebir taşınabilirlik.

**Kanıt (bu fazda `diff` ile doğrulandı):** mevcut `camunda-nats-channel/.../config/CamundaNatsAutoConfiguration.java` ↔ `cadenzaflow-nats-channel/.../config/CadenzaFlowNatsAutoConfiguration.java` arasında isim-normalize edilmiş `diff` **sıfır fark** verdi (`camunda`→`XXXX`, `cadenzaflow`→`XXXX` substitüsyonu sonrası byte-aynen). Bu basamağın yeni A2 kodu da **aynı disiplinle** üretilir: `03_classes/2_camunda_a2.md`'deki her sınıf, aşağıdaki tabloya göre satır satır aynalanır — **içerik/algoritma farkı YOK**, yalnız import kökü değişir.

---

## 1. Paket/import eşleme tablosu

| Camunda (`com.threeai.nats.camunda.a2`) | CadenzaFlow (`com.threeai.nats.cadenzaflow.a2`) |
|---|---|
| `org.camunda.bpm.engine.RuntimeService` | `org.cadenzaflow.bpm.engine.RuntimeService` |
| `org.camunda.bpm.engine.ExternalTaskService` | `org.cadenzaflow.bpm.engine.ExternalTaskService` |
| `org.camunda.bpm.engine.impl.persistence.entity.ExternalTaskEntity` | `org.cadenzaflow.bpm.engine.impl.persistence.entity.ExternalTaskEntity` |
| `org.camunda.bpm.engine.impl.bpmn.behavior.ExternalTaskActivityBehavior` | `org.cadenzaflow.bpm.engine.impl.bpmn.behavior.ExternalTaskActivityBehavior` |
| `org.camunda.bpm.engine.impl.bpmn.parser.{BpmnParseListener,AbstractBpmnParseListener}` | `org.cadenzaflow.bpm.engine.impl.bpmn.parser.{BpmnParseListener,AbstractBpmnParseListener}` |
| `org.camunda.bpm.engine.impl.cfg.{ProcessEngineConfigurationImpl,ProcessEnginePlugin,AbstractProcessEnginePlugin,TransactionState}` | `org.cadenzaflow.bpm.engine.impl.cfg.{...}` (aynı sınıf adları) |
| `org.camunda.bpm.engine.impl.cmd.{LockExternalTaskCmd,UnlockExternalTaskCmd,HandleExternalTaskCmd}` | `org.cadenzaflow.bpm.engine.impl.cmd.{...}` — **kanıt:** `UnlockExternalTaskCmd.java:37-39` (`execute` → `externalTask.unlock()`) fork'ta bizzat doğrulandı, upstream ile birebir |
| `org.camunda.bpm.engine.impl.core.variable.mapping.value.ConstantValueProvider` | `org.cadenzaflow.bpm.engine.impl.core.variable.mapping.value.ConstantValueProvider` |
| `org.camunda.bpm.engine.BadUserRequestException`, `.exception.NotFoundException` | `org.cadenzaflow.bpm.engine.{BadUserRequestException}`, `.exception.NotFoundException` |

`com.threeai.nats.core.*` importları (nats-core: `DlqPublisher`, `SweepLeaderLease`, `BpmHeaders`, `NatsChannelMetrics`, `UmbrellaLockResolver`, `DlqBridgeCircuitBreakerFactory`) **değişmez** — her iki motor modülü de aynı `nats-core` bağımlılığını paylaşır (`02_package_structure.md` §3).

---

## 2. Sınıf-eşleme (birebir davranış)

| Camunda sınıfı | CadenzaFlow sınıfı | Davranış farkı |
|---|---|---|
| `A2ExternalTaskBehavior` | `A2ExternalTaskBehavior` | YOK |
| `A2BpmnParseListener` | `A2BpmnParseListener` | YOK |
| `A2PostCommitPublisher` | `A2PostCommitPublisher` | YOK |
| `A2OrphanSweep` | `A2OrphanSweep` | YOK — `SweepLeaderLease` KV bucket'ı (`a2-sweep-leader`) her İKİ motor tarafından da **paylaşılır** (aynı NATS cluster, aynı bucket), ama her motor **kendi anahtarını** (`sweep-leader.camunda` / `sweep-leader.cadenzaflow`) kullanır — bkz. paylaşım notu aşağıda (**LLD-Q1 kararı, 2026-07-15**) |
| `A2CompletionBridge` | `A2CompletionBridge` | YOK |
| `A2IncidentBridge` | `A2IncidentBridge` | YOK — yalnız CB instance adı farklı: `cb-incident-bridge-cadenzaflow` (izolasyon, ADR-0004); `ignoreExceptions(NotFoundException.class, ...)` her iki tarafta da aynı (MAJOR-1a, `03_classes/1_nats_core_common.md` §4.2) |

**Paylaşım notu (leader-lease çapraz-motor — LLD-Q1 kararı, Levent onayı 2026-07-15):** Bir Camunda node'u VE bir CadenzaFlow node'u aynı clusterda çalışıyorsa (nadir, ama mimari olarak mümkün — iki farklı motor ailesi aynı NATS/JetStream substratını paylaşır, HLD §1), `a2-sweep-leader` KV bucket'ı **paylaşılır** ama her motor ailesi **kendi anahtar namespace'inde** (`sweep-leader.<engineId>`) bağımsız lider seçer (`03_classes/1_nats_core_common.md` §3.2). Bu, ilk taslakta tespit edilen "tek `leader` anahtarı için iki motor ailesi yarışır, yalnız biri lider olur, diğerinin orphan'ları taranmaz" riskini **yapısal olarak ortadan kaldırır** — `sweep-leader.camunda` yalnız Camunda node'ları arasında, `sweep-leader.cadenzaflow` yalnız CadenzaFlow node'ları arasında bir leader-election'dır; iki namespace birbirinden tamamen izoledir. Her motorun kendi `A2OrphanSweep` instance'ı kendi `ProcessEngine`sine bağlıdır ve artık kendi bağımsız leader-lease'ine sahiptir.

**Bağımlılık:** ADR-0005 §4, ADR-0007 §2, ADR-0002, NFR-M1/M2, LLD-Q1.

---

## 3. Migrasyon/upgrade prosedürü (ADR-0005 §3 — her iki motor için ayrı ayrı uygulanır)

Camunda 7.24+ / CadenzaFlow 1.2+ upgrade'inde her iki modül için de:
1. Beş kanca noktasının imza/davranışı değişmedi mi doğrula (§1 tablosundaki sınıflar).
2. Guard testler koşturulur (TEST_SPECIFICATIONS.md (a) — her iki motor için ayrı test sınıfı, `CamundaA2GuardTest` / `CadenzaFlowA2GuardTest`, mevcut `CamundaInboundIntegrationTest`/`CadenzaFlowInboundIntegrationTest` desenine ek).
3. `complete`'in lock-expiry kontrol etmediği invariant'ı yeniden teyit edilir.
4. Herhangi biri kırılırsa **fail-closed** — A2 aktivasyonu bloklanır (bootstrap-time guard, `08_config.md` §1.4).

---

## LLD-QUESTIONS (bu dosyaya özgü)

Bu dosyaya özgü açık soru **YOK** — çapraz-motor sweep-leader paylaşımı sorusu **LLD-Q1** ile kararlaştırıldı (Levent onayı, 2026-07-15; motor-başına lease key, bkz. §2 paylaşım notu). Karar kaydı: `manifest.md` "Open Questions / Drift Log".
