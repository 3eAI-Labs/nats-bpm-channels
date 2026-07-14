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
| `org.camunda.bpm.engine.core.variable.mapping.value.ConstantValueProvider` | `org.cadenzaflow.bpm.engine.impl.core.variable.mapping.value.ConstantValueProvider` |
| `org.camunda.bpm.engine.BadUserRequestException`, `.exception.NotFoundException` | `org.cadenzaflow.bpm.engine.{BadUserRequestException}`, `.exception.NotFoundException` |

`com.threeai.nats.core.*` importları (nats-core: `DlqPublisher`, `SweepLeaderLease`, `BpmHeaders`, `NatsChannelMetrics`, `UmbrellaLockResolver`, `DlqBridgeCircuitBreakerFactory`) **değişmez** — her iki motor modülü de aynı `nats-core` bağımlılığını paylaşır (`02_package_structure.md` §3).

---

## 2. Sınıf-eşleme (birebir davranış)

| Camunda sınıfı | CadenzaFlow sınıfı | Davranış farkı |
|---|---|---|
| `A2ExternalTaskBehavior` | `A2ExternalTaskBehavior` | YOK |
| `A2BpmnParseListener` | `A2BpmnParseListener` | YOK |
| `A2PostCommitPublisher` | `A2PostCommitPublisher` | YOK |
| `A2OrphanSweep` | `A2OrphanSweep` | YOK — `SweepLeaderLease` KV bucket'ı (`a2-sweep-leader`) her İKİ motor tarafından da **paylaşılır** (aynı NATS cluster, aynı bucket) çünkü sweep koordinasyonu engine-nötr (ADR-0002: "hem A2 motorları paylaşır") |
| `A2CompletionBridge` | `A2CompletionBridge` | YOK |
| `A2IncidentBridge` | `A2IncidentBridge` | YOK — yalnız CB instance adı farklı: `cb-incident-bridge-cadenzaflow` (izolasyon, ADR-0004) |

**Önemli paylaşım notu (leader-lease çapraz-motor):** Bir Camunda node'u VE bir CadenzaFlow node'u aynı clusterda çalışıyorsa (nadir, ama mimari olarak mümkün — iki farklı motor ailesi aynı NATS/JetStream substratını paylaşır, HLD §1), `a2-sweep-leader` KV bucket'ındaki **tek** `leader` anahtarı için **her iki motorun** sweep instance'ları da yarışır — bu doğru davranıştır: sweep engine-nötr bir işlevdir (fetchable-parite sorgusu her motorun **kendi** DB'sine gider, ama leader-seçimi tek küme-geneli kaynaktır). Bu, ADR-0002'nin "hem A2 motorları (camunda/cadenzaflow) paylaşır" ifadesinin doğal sonucudur — çelişki YOK, çünkü her motorun kendi `A2OrphanSweep` instance'ı kendi `ProcessEngine`sine bağlıdır; yalnız *leader-olma hakkı* paylaşılır. **Pratik sonuç:** tek clusterda birden fazla motor ailesi varsa, yalnız BİR motorun sweep'i o an lider olur — diğer motorun orphan'ları o döngüde taranmaz. **Bu basamak-1'in tek-motor-ailesi deployment senaryosunda (tipik durum) gözlemlenmez**; çoklu-motor-ailesi tek-cluster deployment nadir/gelişmiş bir topoloji olduğundan LLD-QUESTIONS'a not düşülür (bkz. dosya sonu).

**Bağımlılık:** ADR-0005 §4, ADR-0007 §2, NFR-M1/M2.

---

## 3. Migrasyon/upgrade prosedürü (ADR-0005 §3 — her iki motor için ayrı ayrı uygulanır)

Camunda 7.24+ / CadenzaFlow 1.2+ upgrade'inde her iki modül için de:
1. Beş kanca noktasının imza/davranışı değişmedi mi doğrula (§1 tablosundaki sınıflar).
2. Guard testler koşturulur (TEST_SPECIFICATIONS.md (a) — her iki motor için ayrı test sınıfı, `CamundaA2GuardTest` / `CadenzaFlowA2GuardTest`, mevcut `CamundaInboundIntegrationTest`/`CadenzaFlowInboundIntegrationTest` desenine ek).
3. `complete`'in lock-expiry kontrol etmediği invariant'ı yeniden teyit edilir.
4. Herhangi biri kırılırsa **fail-closed** — A2 aktivasyonu bloklanır (bootstrap-time guard, `08_config.md` §1.4).

---

## LLD-QUESTIONS (bu dosyaya özgü)

- **Çapraz-motor sweep-leader paylaşımı (§2 not):** Tek NATS clusterda hem Camunda hem CadenzaFlow node'ları aynı anda çalışıyorsa, `a2-sweep-leader` KV bucket'ının tek anahtarı için iki motor ailesi yarışır ve yalnız biri lider olur — diğerinin orphan'ları o an taranmaz (NFR-R3 SLA'sı o motor için ≤L+2S'ye kadar esneyebilir). ADR-0002 bunu örtük olarak kabul ediyor gibi görünüyor ("hem A2 motorları paylaşır") ama **açıkça bir karar kaydı YOK** bu çok-motor-ailesi senaryosu için. Bu basamak-1'in birincil hedef topolojisinde (tek motor ailesi) gözlemlenmez; çoklu-motor-ailesi deployment planlanıyorsa ayrı KV bucket'lar (`a2-sweep-leader-camunda`, `a2-sweep-leader-cadenzaflow`) mı yoksa paylaşılan tek bucket mi tercih edileceği **Levent onayına** bırakılır.
