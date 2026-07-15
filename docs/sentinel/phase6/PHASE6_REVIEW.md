# Phase 6 — Code Review Report

**Rol:** Reviewer + DevOps (AI performs, Human approves). **Kapsam:** basamak-1 "External Task /
Event-Driven Work Offload over JetStream" — `main` merge öncesi son kapı.
**Branch:** `feature/step1-a2-implementation` (main'e göre 22 commit, 153 dosya, +10799/-2096).
**Tarih:** 2026-07-15. **Reviewer:** Sentinel Phase 6 subagent (Sonnet), tam LLD/ADR/asyncapi/
DATA_CLASSIFICATION okunarak.

---

## 0. Hüküm (özet — detay §8)

**KOŞULLU ONAY** — main'e merge için onay öneriyorum, 2 koşul listesiyle (§8). Hiçbir BLOCKING
bulgu yok; build tam yeşil, 311/311 test geçiyor, mirror bütünlüğü doğrulandı, güvenlik/DP
taraması temiz. 2 MAJOR bulgu (JSON-parser kırılganlığı, WorkQueue/Limits retention drift) merge'i
durdurmaz ama üretim öncesi/sonrası hızlı takip gerektirir.

> **[GÜNCELLEME — koşullar karşılandı (2026-07-15)]** Her iki koşul (§8 madde 1 ve 2) merge
> öncesi bu turda kapatıldı: F-1 (Jackson tabanlı, üst-düzey-yalnız JSON parse) ve F-2
> (`ensureStream`'in `jobs.*` default'u `WorkQueue`) düzeltildi, sürüm `0.2.0` KESİNLEŞTİ, tam
> reactor yeşil (323/323 test — 311 taban + bu turun 12 yeni testi). Detay: CHANGELOG.md
> "Fixed" bölümü, `RELEASE_NOTES.md` §1/§4/§6.

---

## 1. Build & Test Doğrulaması

```
export JAVA_HOME=/usr/lib/jvm/temurin-21-jdk-amd64; export PATH=$JAVA_HOME/bin:$PATH
mvn clean test
```

**Sonuç: BUILD SUCCESS**, reactor 6 modül (root, nats-core, flowable, camunda, cadenzaflow, bench):

| Modül | Test | Sonuç |
|---|---|---|
| nats-core | 62 | ✅ 0 fail/error |
| flowable-nats-channel | 59 | ✅ 0 fail/error |
| camunda-nats-channel | 91 | ✅ 0 fail/error |
| cadenzaflow-nats-channel | 99 | ✅ 0 fail/error |
| nats-bpm-bench | 0 (bench-tag hariç tutuldu, beklenen) | ✅ |
| **Toplam** | **311** | **✅ 311/311 yeşil** |

Beklenen "311+ test yeşil" eşiği **birebir karşılandı**. `@Disabled`/`@Ignore` etiketli **hiçbir**
test dosyası bulunamadı (repo-geneli grep, target/ hariç) — commit `8621d6a`'nın eklediği
`A2OrphanSweepFetchableParityIntegrationTest` (o an disabled/BLOCKING olarak commit edilmiş)
sonraki JPMS-fix commit'i (`2560cf9`) ile düzeltilip **aktif ve geçen** bir teste dönüştürülmüş —
regresyon güvencesi gerçek (mock'suz embedded engine).

JaCoCo satır kapsaması (bench hariç, ölçüldü): **~%74.0** (camunda ~71%, cadenzaflow ~71.4%,
nats-core ~73%, flowable ~86%) — görevde belirtilen "~%75, %80 hedefinin altı" iddiasıyla
**tutarlı**, kabul edilmiş borç olarak teyit edildi (yeni bulgu değil).

---

## 2. LLD-Sadakat + Ayna Bütünlüğü

### 2.1 Camunda ↔ CadenzaFlow A2 paket ayna doğrulaması (mekanik diff)

Tüm 14 `a2` paket sınıfı + `A2*Test`/`CadenzaFlowA2GuardTest` çiftleri, motor adı
(`camunda`/`cadenzaflow`, tüm case varyantları) normalize edilip `diff` ile karşılaştırıldı.
**Sonuç: davranışsal fark SIFIR** — tüm gözlenen diff'ler yalnız Javadoc'taki dosya-yolu
cross-reference'ları (`03_classes/2_camunda_a2.md` her iki tarafta da doğru şekilde bu **tek**
kanonik LLD dosyasına işaret ediyor, kendisi kopyalanmıyor). `A2BpmnParseListener`'daki
`BpmnParse.CAMUNDA_BPMN_EXTENSIONS_NS` referansı **her iki motorda da aynı isim** — gerçek
CadenzaFlow fork kaynağından (`~/Workspaces/cadenzaflow/cadenzaflow-bpm-platform/engine/.../
BpmnParse.java:258`) doğrulandı: fork bu sabiti bilerek yeniden adlandırmamış (üç-namespace'li
`Namespace` nesnesi, geriye-uyumluluk) — **hata değil**, LLD'nin "fork kanıtı bizzat okunarak
doğrulandı" iddiasını destekleyen ek bir kanıt noktası.

`CamundaNatsAutoConfiguration` ↔ `CadenzaFlowNatsAutoConfiguration`, `NatsSubscriptionRegistrar`:
**birebir aynı** (normalize-diff sıfır).

**Bulgu (NIT, pre-existing, bloklamaz):** `JetStreamMessageCorrelationSubscriber` (mevcut,
A2-paketi DIŞI, bu PR'da DLQ/BpmHeaders entegrasyonu için dokunulmuş) camunda/cadenzaflow
kopyaları arasında iki küçük yorum-satırı farkı taşıyor ("Check if max deliveries exceeded"
camunda'da var, cadenzaflow'da yok) — bu drift **main'de zaten mevcuttu** (bu PR'ın
tanıttığı bir şey değil), davranış etkilenmiyor. LLD'nin "byte-aynen ayna" iddiası özellikle
**A2 paketleri** için doğrulanmıştı ve bu doğru — bu bulgu yalnız o iddianın kapsamı dışındaki,
önceden var olan bir dosyaya ilişkin.

### 2.2 LLD'den sapmalar — CODER-NOTE disiplini

`A2OrphanSweep` ve `A2ExternalTaskBehavior`'daki üç sapma, kodda **açıkça belgelenmiş**
CODER-NOTE'larla işaretli ve gerçek motor API'siyle (7.24.0 derlenmiş engine) doğrulanmış:
1. `selectExternalTasksForTopics(...)` LLD pseudo-code'unda 4 argüman alıyordu; gerçek imza 3
   argüman alıyor — kod gerçek imzaya uyarlanmış, kanıt kod-içi yorumda.
2. `ManagementService.executeCommand(...)` public API'sinde yok; `ProcessEngineConfigurationImpl.
   getCommandExecutorTxRequired()` üzerinden aynı `CommandExecutor`'a erişiliyor.
3. `instructions.values()` (canlı `Map.values()` view) MyBatis/OGNL JPMS-reddi üretiyordu —
   `ArrayList` materyalize edilerek düzeltildi (bkz. §1, JPMS fix).

**Bulgu (MINOR):** Bu üç sapma kod-içi CODER-NOTE'larla mükemmel belgelenmiş, ama
`docs/sentinel/phase4/lld/external-task-jetstream/03_classes/2_camunda_a2.md`'nin ilgili
pseudo-code blokları **rev-note'suz** eski (yanlış) imzaları hâlâ gösteriyor — MASTER_WORKFLOW
§0.6 "her değişiklik etkilediği belgeyi aynı commit'te günceller" disiplinine tam uymuyor. Kod
doğru, davranış doğru, yalnız LLD↔kod senkronu eksik. Merge'i bloklamaz; sonraki bir doküman
commit'inde 3 satırlık bir düzeltme yeterli.

---

## 3. Kontrat Bütünlüğü (kod ↔ `asyncapi.yaml`)

- **`type` discriminator:** `ReplyType` enum'u (`SUCCESS`/`BPMN_ERROR`/`TRANSIENT`) asyncapi
  `ReplyTypeDiscriminator` enum'uyla birebir. `A2ReplyPayloadDecoder.classify(...)` eksik/bilinmeyen
  değeri `Optional.empty()` ile işaretliyor → `A2CompletionBridge` bunu `DlqReason.
  INVALID_REPLY_TYPE` (**kod 24, `VAL_INVALID_REPLY_TYPE`**) ile DLQ'ya yönlendiriyor — asyncapi
  ve `ERROR_REGISTRY.md` §3 satır 24 ile tutarlı.
- **`variables` alanı:** `A2JobRequestPayload.variables` (opt-in, allowlist-tabanlı, default boş)
  — `A2ExternalTaskBehavior.captureAllowlistedVariables` + `A2JobMessageFactory.build(task, vars)`
  asyncapi'nin "yalnız allowlist konfigüreliyse VE execution'da mevcutsa eklenir" kuralını birebir
  uyguluyor (`hasVariable` kontrolü, atlanan-yok-hata-yok semantiği).
- **DLQ header seti** (`DlqHeaders`) — asyncapi `DlqHeaders` şemasının 5 alanı (ORIGINAL_SUBJECT/
  DELIVERY_COUNT/REASON/TIMESTAMP + Nats-Msg-Id) birebir üretiliyor (`DlqPublisher.
  appendMetaHeaders`).
- **`JobSuccessReply.contentType` → `application/json`** değişikliği kodda yansıtılmış
  (`A2ReplyPayloadDecoder` her zaman JSON-metin varsayıyor, `bodyAsString` + basit alan-çıkarımı).

### 🟠 F-1 [MAJOR] — El-yapımı JSON alan-çıkarımı, wire-kritik `type` discriminator'ı için kırılgan

`A2ReplyPayloadDecoder.extractJsonField(...)` (camunda + cadenzaflow, birebir ayna) derinlik
farkındalığı OLMAYAN bir string-arama parser'ıdır: `json.indexOf("\"" + fieldName + "\"")` ilk
eşleşmeyi alır. Asyncapi kontratı `variables`/`errorDetails` gibi **iç içe (nested) obje**
alanlarına izin verir (`additionalProperties: true`). Eğer bir worker'ın payload'ı, top-level
`type` alanından **önce**, iç içe bir obje içinde (ör. `variables: {"type": "sozlesme"}` gibi
gayet makul bir iş alanı adı) literal `"type"` anahtarı taşıyan bir alan yerleştirirse,
`extractJsonField` bu iç içe değeri top-level discriminator sanıp **yanlış sınıflandırma**
yapabilir (`complete`/`handleBpmnError`/`handleFailure` üçünden yanlışı tetiklenebilir, ya da
geçerli bir `type` varken yanlışlıkla `VAL_INVALID_REPLY_TYPE`'a düşebilir).

- **Kanıt:** `A2ReplyPayloadDecoderTest` yalnız düz (nested-obje içermeyen) payload'ları test
  ediyor — bu senaryo test-kapsamı DIŞINDA (dosya okunarak doğrulandı, 11 test, hiçbiri iç içe
  obje içermiyor).
- **Bağlam (ağırlaştırıcı DEĞİL, azaltıcı):** bu, bu PR'ın icat ettiği bir desen değil — repo'nun
  önceden var olan tarzı (`NatsMessageCorrelationSubscriber`'da "basit JSON alan çıkarımı" olarak
  zaten yorumlanmış) ile **tutarlı**; yeni Jackson bağımlılığı eklemeden (CODING_GUIDELINES
  §15.3 — onay gerektirir) bu tarza sadık kalınmış.
- **Öneri (KOŞULLU ONAY şartı DEĞİL, hızlı-takip):** ya derinlik-farkında (brace-depth-tracking)
  bir çıkarım yazılmalı, ya da worker-payload'larının `type` alanını her zaman **en dış seviyede
  ilk alan** olarak serileştirmesi bir wire-kontrat kuralı olarak asyncapi'ye eklenmeli
  (mekanik olarak doğrulanamaz, disiplin gerektirir) — ya da hafif bir JSON kütüphanesi
  (mevcut bağımlılıklarda zaten olan `logstash-logback-encoder` Jackson'a transitively bağımlı
  olabilir, kontrol edilip onay istenebilir).

### 🟠 F-2 [MAJOR, pre-existing] — `asyncapi.yaml`'ın `streamRetention: WorkQueue` beyanı hiçbir kod yolunda uygulanmıyor

`a2JobDispatch`/`a2JobReply`/`flowableEventChannel` kanalları `x-jetstream.streamRetention:
WorkQueue` deklare ediyor. Ancak repo'daki **tek** stream-oluşturma kodu
(`JetStreamStreamManager.ensureStream`, `nats-bpm-bench/BenchEnvironment.ensureStreams`,
`JobReplySameStreamDedupRegressionTest`) **her zaman** `RetentionPolicy.Limits` kullanıyor —
`WorkQueue` hiçbir yerde (main koddan bench'e, yeni regresyon testine kadar) fiilen
uygulanmıyor. Ayrıca DLQ-dışı subject'ler için `maxAge`/`maxMsgs` da verilmiyor (yalnız `dlq.`
önekli subject'ler 14 gün default alıyor) — bu, dev/test auto-create yolunun prod'da yanlışlıkla
kullanılması halinde **sınırsız büyüyen** bir stream riskine işaret eder.

- **Azaltıcı:** `99_deployment.md` bu auto-create yolunun **yalnız dev/test/preflight** için
  olduğunu, prod stream'lerinin **PR'lı YAML** ile ayrı provision edilmesi gerektiğini açıkça
  belirtiyor (`NATS_JETSTREAM.md` §5 kural 4/5). Ama bu disiplin insan-sürecine dayanıyor, kodun
  kendisi `WorkQueue`'yu **hiçbir yerde doğrulamıyor/uygulamıyor**.
  - **Pre-existing:** `RetentionPolicy.Limits` seçimi bu PR'dan ÖNCE de böyleydi (bu PR yalnız
    `maxAge` overload'ı ekledi, retention-policy tipini değiştirmedi) — bu PR'ın tanıttığı yeni bir
    regresyon değil, ama tam bu metoda dokunurken düzeltilebilecek bir fırsat kaçırılmış.
- **Öneri:** RELEASE_NOTES §4 son maddesi + RELEASE-QUESTIONS #4'e taşındı — prod NATS stream
  provisioning'inin gerçekten `WorkQueue` kullandığı ayrı bir DevOps doğrulama maddesi olarak
  takip edilmeli.

---

## 4. Güvenlik / DP Taraması (DP-1…8 son tarama)

- **DP-1 (log'da PII):** `NatsMessageCorrelationSubscriber`'ın ham `businessKey` logu **düzeltildi**
  (commit `b30097d`), gerçek Logback `ListAppender` regresyon testiyle kanıtlandı (3 modülde
  mirror). Repo-geneli ek grep (`kv("business_key"|"businessKey"` ham değer loglama deseni için)
  başka bir örnek bulmadı.
- **DP-4/NFR-S3 (transport):** `NatsTransportSecurityGuard` üç motor autoconfigürasyonunda da
  bean olarak kayıtlı (mekanik doğrulandı, grep). Production profilinde TLS+kimlik zorunlu.
- **DP-6 (DLQ reason header):** `DlqReason.headerValue()` yalnız kod/sınıf string'i döndürüyor
  (`"BUS_REPLY_DELIVERY_BUDGET_EXCEEDED"`, `"VAL_EMPTY_MESSAGE_BODY"`, vb.) — payload/PII değeri
  YOK, kaynak kod okunarak doğrulandı.
- **Secrets taraması:** `password\s*=\s*"..."`/`secret=`/`api-key=` desenleri için repo-geneli grep
  (test hariç) — **temiz**, hardcoded secret bulunamadı.
- **Bağımlılık taraması:** yeni bağımlılıklar (Resilience4j 2.2.0, datasource-proxy 1.10,
  H2 2.3.232, JaCoCo 0.8.12) hepsi `dependencyManagement`/BOM üzerinden version-pinned; hiçbiri
  hallüsinasyon değil (hepsi gerçek, bilinen kütüphaneler, Apache-2.0/EPL-MPL uyumlu lisanslar).
- **ADR-0008 ikincil savunma hattı:** `A2CompletionBridge`'in `NotFoundException`/
  `BadUserRequestException` dallarının (sahte-reply enjeksiyonuna karşı) kodda **aynen** LLD'nin
  belirttiği gibi mevcut olduğu doğrulandı.

**Bulgu yok (bu eksende) — 0 BLOCKING, 0 MAJOR.**

---

## 5. Kod Kalitesi / Hata Yutma / Thread-Safety / Kaynak Yönetimi

- **Hata yutma / sessiz-başarısızlık:** taranan tüm `catch` blokları (DlqPublisher, A2CompletionBridge,
  A2IncidentBridge, FailureEventBridge, JetStreamInboundEventChannelAdapter) ya loglayıp
  metrik artırıyor ya da açıkça yorumlanmış bir invaryant/idempotent-yut kararı taşıyor — boş
  catch blok **YOK** (grep + okuma ile doğrulandı).
- **Thread-safety:** `SweepLeaderLease.heldRevision` → `volatile Long` (doğru). Her bridge/adapter
  kendi `Executors.newVirtualThreadPerTaskExecutor()`'ını kullanıyor (görev-başına yeni sanal
  thread) — `FailureEventCorrelationMissConsumer`'ın `ThreadLocal` hand-off deseni bu modelde
  güvenli (her mesaj kendi thread'inde, sızıntı riski yok; `bindChannelKeyForCurrentThread`/
  `clearChannelKeyForCurrentThread` `try/finally` ile eşleştirilmiş — doğrulandı).
- **Kaynak sızıntısı:** her `subscribe()`/`unsubscribe()` çifti `dispatcher.drain(...)` +
  `executor.shutdown()` → `awaitTermination(10s)` → `shutdownNow()` deseniyle simetrik (4 sınıfta
  aynı iskelet — mevcut `JetStreamMessageCorrelationSubscriber` deseninin doğru bir genellemesi).
- **Null-güvenliği:** `metrics != null` guard'ları tutarlı (opsiyonel `MeterRegistry` bean'i yokken
  NPE yok). `msg.getHeaders() == null` kontrolleri `extractExternalTaskId`/`originalSubjectOf`'ta
  mevcut.

### 🟡 F-3 [MINOR] — `A2SubscriptionRegistrar.destroy()` sweep scheduler'ın bitmesini beklemiyor

`sweepScheduler.shutdownNow()` çağrılıyor ama `awaitTermination(...)` yok — hâlihazırda çalışan
bir `sweepCycle()` yarıda kesilebilir (muhtemelen zararsız, `execute(Command)` transaction-scoped
çağrılar; en kötü durum bir sonraki cycle'ın normal telafi mekanizmalarıyla düzelir). Diğer üç
sınıfın (`A2CompletionBridge`/`A2IncidentBridge`/`FailureEventBridge`) `unsubscribe()`'ı
`awaitTermination` kullanıyor; bu sınıf tutarsız. Kozmetik/dayanıklılık iyileştirmesi, bloklamaz.

### 🟡 F-4 [MINOR] — `FailureEventBridge` host uygulamasının önceden var olan `NonMatchingEventConsumer`'ını sessizce ez­iyor

`eventRegistryEngineConfiguration.setNonMatchingEventConsumer(correlationMissConsumer)` —
bean `null` ise WARN logluyor (doğru), ama bean **var VE zaten başka bir consumer'a sahipse**,
onu sessizce değiştiriyor, ne bir log ne bir delegasyon zinciri var. Bu basamak-1 kapsamında
muhtemelen zararsız (host uygulaması muhtemelen başka bir consumer kaydetmiyor), ama gelecekte
bir host uygulaması kendi SPI'ini kaydetmek isterse sessizce kaybolur. Bloklamaz, ileri-iş notu.

**Diğer bulgular yok** — Manager Pattern (DlqPublisher/JetStreamKvManager kaynak yönetimi
merkezi), Circuit Breaker (Resilience4j, ADR-0004 eşikleriyle birebir), DI (constructor injection
her yerde, hardcoded instantiation yok — bootstrap/autoconfig sınıfları hariç, ki onlar zaten
DI kompozisyon köküdür) — hepsi REVIEWER_GUIDELINE §1/§4 ile uyumlu.

---

## 6. Test Bütünlüğü

- **Kritik davranışlar testle sabitlenmiş:** JPMS-fix (`A2OrphanSweepFetchableParityIntegrationTest`,
  gerçek embedded engine), miss-SPI (`FailureEventCorrelationMissConsumerTest` +
  `...IntegrationTest`), custody-transfer (`A2CompletionBridgeTest` 13 test, DLQ-publish-failure
  nak yolu dahil), şemsiye-kilit (`UmbrellaLockCalculatorTest`/`UmbrellaLockValidatorTest`/
  `UmbrellaLockResolverTest`), tek-INSERT guard (`CamundaA2GuardTest`/`CadenzaFlowA2GuardTest`,
  gerçek H2+datasource-proxy), job/reply same-stream dedup tehlikesi
  (`JobReplySameStreamDedupRegressionTest`, gerçek Testcontainers NATS).
- **Test kalitesi:** okunan test dosyalarında (`A2ReplyPayloadDecoderTest`,
  `EventReceivedNoMatchBehaviorTest`, `JobReplySameStreamDedupRegressionTest`,
  `NatsMessageCorrelationSubscriberTest`) `assertTrue(true)` tipi anlamsız assertion **yok**;
  her test somut, davranış-odaklı bir iddia taşıyor. `EventReceivedNoMatchBehaviorTest` özellikle
  gerçek motor + bytecode-kanıtlı bir karakterizasyon testi — AI-halüsinasyon riskine karşı örnek
  bir titizlik seviyesi.
- **Kabul edilmiş açık:** `A2SubscriptionRegistrar` doğrudan birim-testsiz (görevde önceden kabul
  edilmiş borç, doğrulandı — dosya yok); `BoundaryTimerCostTest` (TEST_SPEC c) henüz yazılmadı
  (doğrulandı — dosya yok).

**Bulgu yok (bu eksende, kabul edilmiş borçlar hariç) — 0 BLOCKING, 0 yeni MAJOR.**

---

## 7. Bulgu Özeti (severity sayımı)

| Severity | Sayı | Kalemler |
|---|---|---|
| 🔴 BLOCKING | **0** | — |
| 🟠 MAJOR | **2** | F-1 (JSON-parser kırılganlığı), F-2 (WorkQueue/Limits retention drift, pre-existing) |
| 🟡 MINOR | **2** | F-3 (scheduler shutdown await eksik), F-4 (NonMatchingEventConsumer sessiz ez-me) |
| ℹ️ NIT | **2** | LLD↔kod pseudo-code imza senkron eksikliği (§2.2), pre-existing yorum-drift'i (§2.1) |
| ✅ Kabul edilmiş borç (yeni bulgu DEĞİL, doğrulandı) | 5 | TEST_SPEC(c) açık, sweep captured-variables taşımıyor, A2SubscriptionRegistrar testsiz, ~%74 kapsama, bench nightly-only |

---

## 8. Merge Kararı

### **KOŞULLU ONAY** — koşullar karşılandı (2026-07-15, bkz. §0 güncelleme notu)

**Koşullar** (merge SONRASI hızlı-takip olarak kapatılabilir, merge'i BLOKLAMAZ):

1. **F-1'i bir takip ticket'ına bağla:** `A2ReplyPayloadDecoder`'ın nested-JSON `type`-anahtar
   çakışması riski için ya derinlik-farkında bir çıkarım yazılsın ya da worker-SDK dokümantasyonuna
   "`type` alanı JSON kökünde, iç içe obje İÇİNDE bir `type` alanı taşımayın" kuralı eklensin —
   RELEASE-QUESTIONS'a taşındı (RELEASE_NOTES.md §6, madde kapsamında değil, ayrıca not düşüldü).
2. **F-2'yi bir DevOps doğrulama maddesine bağla:** prod NATS stream provisioning'inin (PR'lı
   YAML) gerçekten `WorkQueue` retention kullandığı, bu repo'nun dev/test auto-create kodunun
   (`Limits`) **prod'da hiç çalıştırılmadığı** elle/CI-check ile doğrulanmalı — RELEASE_NOTES §6
   madde 4.

**Gerekçe:** Build tam yeşil (311/311), disabled/skipped test yok, A2 motor-ayna bütünlüğü
mekanik doğrulandı (sıfır davranışsal fark), 5 kontrat-fix + yeni `type`/`variables` alanları
asyncapi ile birebir, güvenlik/DP taraması temiz (0 bulgu), JavaDelegate kaldırımı CHANGELOG'da
açıkça BREAKING işaretli ve migrasyon notu mevcut. İki MAJOR bulgu gerçek ama (a) ikisi de
kanıtlanmış canlı bir arıza değil, latent bir kırılganlık/operasyonel-disiplin bağımlılığı, (b)
ikisi de mevcut kod tabanının önceden kabul ettiği desenlerle tutarlı (yeni bir anti-pattern icat
edilmiş değil), (c) ikisi de merge sonrası ayrı commit'lerle, mevcut mimariyi bozmadan
kapatılabilir. REVIEWER_GUIDELINE'ın BLOCKING kriterleri (build fail, kritik güvenlik açığı,
testsiz yeni işlevsellik, iş mantığı gereksinimle uyumsuz, geri-uyum kırılması migrasyon planı
olmadan) — bunların hiçbiri bu iki bulgu için karşılanmıyor.

**Human sign-off gerekli** (MASTER_WORKFLOW §0.2, Stop Rule) — bu belge insan onayının yerine
geçmez, yalnız onun girdisidir.

---

## Ekler

- `CHANGELOG.md` (repo kökü) — tam değişiklik listesi.
- `docs/sentinel/phase6/RELEASE_NOTES.md` — sürüm önerisi, ZORUNLU deployment şartları, bilinen
  sınırlar, migrasyon notları, RELEASE-QUESTIONS.
