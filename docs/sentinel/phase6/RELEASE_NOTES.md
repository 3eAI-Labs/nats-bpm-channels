# Release Notes — Basamak-1: External Task / Event-Driven Work Offload over JetStream

**Sentinel fazı:** Phase 6 — Reviewer & DevOps (koşullu onay, koşullar bu turda kapatıldı — §6).
**Branch:** `feature/step1-a2-implementation` (Phase 6 incelemesindeki 22 commit — 9 Phase 5
implementasyon + 5 Phase 5.5 QA test/karakterizasyon + 7 QA düzeltme-paketi + 1 doküman/registry
düzeltmesi — artı bu turun F-1/F-2 düzeltme + sürüm-kesinleştirme commit'leri).
**Tarih:** 2026-07-15 (Phase 6 inceleme tarihi; sürüm bu tarihte KESİNLEŞTİ).
**Kök `pom.xml` sürümü:** `0.2.0` (KESİNLEŞTİ — bu turdan önce `0.1.0-SNAPSHOT`, henüz hiç sürüm
yayımlanmamıştı).

---

## 1. Sürüm kararı — `0.2.0` (KESİNLEŞTİ, Levent karari 2026-07-15)

Gerekçe:
- SemVer §4 pre-1.0 kuralı gereği 0.x serisinde herhangi bir kırıcı değişiklik teknik olarak
  minor/patch'te de yapılabilir ("anything may change at any time"). Ancak proje henüz **hiç**
  sürüm yayımlamadığından (`0.1.0-SNAPSHOT` hâlâ geliştirme aşamasında) ve bu basamak (1) hem
  büyük bir **yeni özellik seti** (A2 pipeline × 2 motor + FailureEventBridge + bench modülü) hem
  de **kırıcı bir kaldırım** (7 JavaDelegate sınıfı, aşağıya bkz.) içerdiğinden, disiplin gereği
  **minor sürüm artışı** (`0.1.0` → `0.2.0`) önerilip kabul edildi — kaldırımın görünürlüğünü
  artırır, semver okuyan tüketiciler için "bu sürümde API yüzeyi değişti" sinyalini taşır. Tüm
  6 pom (`root` + 5 modül) `versions:set` ile `0.2.0`'a yükseltildi, tam reactor yeşil.
  - **1.0.0 önerilmedi:** proje kendi yol haritasında ("kademeli strangler omurga", basamak 1-6)
    henüz basamak-1'de; 1.0.0 tipik olarak "kararlı genel kullanım API'si" sinyali taşır — bu,
    basamak-6'ya (native state-core) kadar erken olur.
- **BREAKING olarak değerlendirme:** JavaDelegate kaldırımı gerçek bir API-yüzeyi kırılımıdır (BPMN
  modelleri `camunda:class="...Delegate"` referansı deploy/execute zamanı başarısız olur, sessiz
  bir uyumluluk katmanı yok). Bu, CHANGELOG'da "Removed — BREAKING" olarak açıkça işaretlendi;
  migrasyon notu §5'te. Doğrudan kaldırım (deprecation-window'suz) Levent tarafından onaylandı —
  bkz. §6 Q2.

---

## 2. Öne çıkanlar

- **A2 external-task-over-JetStream** — Camunda 7 ve CadenzaFlow için, motor DB transaction'ını
  hiçbir noktada dış bir I/O ile tutmayan, tam push-tabanlı bir external-task dispatch/completion
  hattı: doğumda sıfır-ek-yazı sentinel-kilit, post-commit DB-sorgusuz publish, lider-seçimli soğuk
  orphan-sweep (crash-kurtarma), custody-transfer semantiğiyle completion/incident köprüleri.
- **FailureEventBridge (Flowable)** — paylaşılan DLQ'yu Flowable Event Registry'ye geri
  besleyen escalation köprüsü; gerçek "eşleşme-yok" tetikleyicisi ampirik olarak (bytecode +
  gerçek-motor testiyle) doğrulanıp SPI-tabanlı doğru mekanizmaya taşındı.
- **`nats-bpm-bench`** — iki motor + gerçek Postgres + gerçek NATS ile "A2-push, native-poll'a göre
  sıfır ek DB-poll/fetchAndLock üretir mi?" iddiasını (projenin ana tezi) somut, tekrarlanabilir bir
  build-kapısına (nightly) bağlayan yeni modül.
- **Kontrat sıkılaştırma** — zorunlu `type` (SUCCESS/BPMN_ERROR/TRANSIENT) discriminator'ı, opt-in
  `variables` alanı, 5 DLQ kontrat-fix'i tüm üç motor/adapter'da birebir uygulandı.
- **Güvenlik** — production'da TLS+NKey/JWT zorunlu kılan transport guard (ADR-0008), `jobs.*`
  namespace rezervasyon validasyonu, DLQ-bridge circuit-breaker'larında "iyi huylu" exception
  ayrımı (sahte CB-OPEN önleme).

---

## 3. ZORUNLU deployment şartları

Bu şartlar karşılanmadan basamak-1 üretimde **güvenle çalışmaz** — deployment runbook'una
(`99_deployment.md`) ve k8s manifest review checklist'ine eklenmelidir:

1. **[ZORUNLU] `jobs.<topic>` ve `jobs.<topic>.reply` AYRI JetStream stream'lerinde olmalı.**
   Aynı `Nats-Msg-Id` (=externalTaskId) her iki subject'te de taşınır; JetStream
   `duplicate_window` dedup'ı **stream-scoped'dur**. Tek bir birleşik stream'de her iki subject de
   varsa, worker'ın reply'ı kendi job'ının kopyası sanılıp **sessizce düşürülür** — external task,
   sweep onu yeniden yayınlayana kadar (≤L saniye) asılı kalır ve arıza "yavaş worker" gibi
   görünür. Bkz. `docs/sentinel/phase4/lld/external-task-jetstream/99_deployment.md` §2.1;
   regresyon kanıtı `JobReplySameStreamDedupRegressionTest` (nats-core).
2. **Subject-level ACL (broker config, bu repo dışı).** ADR-0008 §2 tablosu: worker hesabı yalnız
   kendi topic'inin `jobs.<topic>` / `jobs.<topic>.reply` subject'lerine pub/sub yetkili olmalı;
   `jobs.*.reply`'ı yalnız engine-inbound tüketmeli. Bu, sahte-reply enjeksiyonuna karşı **birincil**
   savunma hattıdır (uygulama-içi SENTINEL-kilit kontrolü yalnız ikincil hattır).
3. **L-floor validasyonu varsayılan olarak reject-startup'tır (BAQ-3).** `L < M·W + Σbackoff + S +
   ε` olan herhangi bir topic, `allow-unsafe-lock-duration=true` verilmedikçe bootstrap'ı
   durdurur. Bu flag'i prod'da açmadan önce riski değerlendirin — açıksa her sweep/dispatch
   döngüsünde kalıcı WARN loglanır (sessiz kabul yoktur).
4. **Production profile TLS + NKey/JWT zorunlu.** `spring.profiles.active=production` iken
   `NatsTransportSecurityGuard` `spring.nats.tls.enabled=true` VE
   (`spring.nats.credentials-file` VEYA `spring.nats.nkey-file`) olmadan bootstrap'ı reddeder.
5. **Java 21 zorunlu.** Sistem varsayılanı (Java 25 gözlemlendi bu ortamda) Mockito'yu kırıyor;
   `JAVA_HOME=temurin-21-jdk-amd64` ile build/test/deploy edin. `A2OrphanSweep`'in JPMS-fix'i de
   JDK16+ modül sınırlarına karşı yazıldı — farklı bir JDK dağıtımı/majör sürüm bu invaryantı
   yeniden test etmeden varsayılmamalı.
6. **`a2-sweep-leader` KV bucket + her iki motor idiomunun kendi `sweep-leader.<engineId>`
   anahtarı** bootstrap'ta idempotent oluşturulur (replikasyon 3 önerilir, prod). Aynı clusterda
   hem Camunda hem CadenzaFlow node'ları çalışıyorsa, bucket paylaşılır ama anahtar motor-ailesi
   başına izoledir — ek bir deploy adımı gerekmez, yalnız bilgi.
7. **DLQ stream retention — 14 gün varsayılan, kiracı-bazlı gözden geçirilmeli.** PII işleyen
   kiracılar için retention'ı kısaltmak/DLQ erişimini kısıtlamak operasyonel bir karardır
   (DATA_CLASSIFICATION.md DP-3) — kod bunu zorunlu kılmaz, yalnız varsayılanı sağlar.

---

## 4. Bilinen sınırlar (kabul edilmiş, release'i bloklamaz)

- **Literal-topic kısıtı:** A2 behavior-swap yalnız `camunda:topic` **literal** (sabit string)
  değerli aktivitelerde uygulanır; EL-expression topic'ler (`camunda:topic="${expr}"`) klasik
  external-task poller'ında kalır. PO tarafından kabul edildi (2026-07-15).
- **Boundary-timer maliyeti ölçülmedi (TEST_SPEC (c) açık):** opt-in boundary-timer'ın
  `ACT_RU_TIMER_JOB` maliyeti tasarlandı ama ölçüm testi (`BoundaryTimerCostTest`) henüz
  yazılmadı — modelleme rehberi (`99_deployment.md` §3) mevcut, ölçüm kanıtı değil.
- **Sweep re-publish captured-variables taşımıyor:** soğuk orphan-sweep'in yeniden yayınladığı
  job mesajı, sıcak-yol post-commit publish'in taşıdığı `variableAllowlist` değişkenlerini
  **taşımaz** (yalnız kimlik zarfı: externalTaskId/topic/businessKey) — sweep yalnız çıplak
  `ExternalTaskEntity` satırına erişir, execution/variable-scope context'i yok. Javadoc'ta
  belgelenmiş, kabul edilmiş bir sınırlamadır (`A2JobMessageFactory`).
- **`A2SubscriptionRegistrar` doğrudan birim-testsiz.** Bootstrap wiring sınıfı; davranışı
  entegrasyon testleri (`*IntegrationTest`, guard testleri) aracılığıyla dolaylı kapsanıyor, ama
  doğrudan bir `A2SubscriptionRegistrarTest` yok.
- **Satır kapsama ~%74-75** (JaCoCo, `nats-bpm-bench` hariç) — `CODING_GUIDELINES` %80 hedefinin
  altında. Kritik sınıflar (bridge'ler, sweep, DLQ publisher, calculator/validator) yoğun test
  edildi; eksik olan pay ağırlıklı olarak wiring/autoconfiguration sınıflarında (`*Registrar`,
  `*AutoConfiguration`).
- **Bench modülü yalnız nightly/manuel** (`@Tag("bench")`) — ana CI pipeline'ını bloklamaz;
  Docker/Testcontainers olmayan ortamlarda `Assumptions.abort(...)` ile atlanır (build kırılmaz).
- **[KAPANDI 2026-07-15, F-2] `ensureStream`'in `jobs.*` retention default'u artık `WorkQueue`.**
  Önceki sınır ("her zaman `RetentionPolicy.Limits`") bu turda düzeltildi:
  `JetStreamStreamManager.ensureStream` artık `jobs.`-prefixed subject'ler için `WorkQueue`,
  `dlq.`-prefixed ve diğer subject'ler için `Limits` varsayılıyor (bkz. CHANGELOG "Fixed" F-2).
  **Kalan kapsam-dışı madde (DevOps takibi, basamak-2 planlama girdisi — §6 Q4):**
  `nats-bpm-bench`'in kendi `BenchEnvironment.ensureStreams()`'i (bu fix'in kapsamındaki
  `ensureStream` çağrı yolunu KULLANMAZ, kendi `StreamConfiguration`'ını doğrudan `Limits` ile
  kurar) ve gerçek prod `nats stream add` provisioning'i (PR'lı YAML, bu repo dışı) bu fix'in
  kapsamı dışındadır — ikisinin de `WorkQueue` kullandığı ayrıca doğrulanmalıdır.

---

## 5. Migrasyon notları — JavaDelegate kullananlara

Basamak-1 ile **7 `JavaDelegate` sınıfı kaldırıldı** (BREAKING, bkz. CHANGELOG "Removed"):

| Kaldırılan sınıf | Modül(ler) |
|---|---|
| `NatsPublishDelegate` | camunda, cadenzaflow |
| `JetStreamPublishDelegate` | camunda, cadenzaflow |
| `NatsRequestReplyDelegate` | camunda, cadenzaflow, flowable |

**Etki:** bu sınıflara `camunda:class="..."` ile referans veren herhangi bir BPMN modeli,
deployment veya execution zamanında `ClassNotFoundException`/benzeri bir hata ile başarısız
olur. **Otomatik/sessiz bir uyumluluk katmanı yoktur.**

**Migrasyon yolu:**
1. `NatsPublishDelegate`/`JetStreamPublishDelegate` kullanan service task'lar → Flowable ise
   native `sendEvent` / Event Registry outbound channel'a; Camunda/CadenzaFlow ise A2 external-task
   desenine (`camunda:type="external" camunda:topic="<topic>"`, topic
   `spring.nats.{camunda,cadenzaflow}.a2.topics[]`'e eklenmeli) geçirin.
2. `NatsRequestReplyDelegate` (senkron, in-tx `connection.request(...)`, motor DB transaction'ını
   NATS round-trip süresince tutan desen — projenin temel tezinin ihlal ettiği kalıp) kullanan
   modeller → A2 external-task + reply pattern'ine geçirin (async, motor DB transaction'ı NATS'tan
   bağımsız).
3. Migrasyon öncesi mevcut BPMN modellerinde bu üç sınıf adı için bir envanter/tarama yapılması
   önerilir (repo-dışı, kiracı sorumluluğu).

**Geriye dönük uyumluluk hedeflenmedi** — bu basamak, projenin ana tezinin (senkron
request-reply'ın motor DB transaction'ını tuttuğu, dolayısıyla reddedildiği) doğrudan bir
sonucudur; ADR kayıtlarında (docs/06) zaten kilitli bir karardır.

---

## 6. RELEASE-DECISIONS (Levent karari, 2026-07-15 — eski RELEASE-QUESTIONS'ın karar kaydı)

Aşağıdaki dört madde Phase 6 incelemesinde soru olarak açılmıştı; Levent'in 2026-07-15 kararıyla
kapatıldı:

1. **Sürüm numarası — KARAR: `0.2.0` kabul edildi.** Farklı bir şema tercih edilmedi. Tüm 6 pom
   `0.2.0`'a yükseltildi, tam reactor `mvn clean test` ile doğrulandı (§1).
2. **JavaDelegate kaldırımının duyuru/geçiş süresi — KARAR: doğrudan kaldırım + migrasyon notları
   yeterli.** Deprecation-window/uyumluluk katmanı eklenmedi; CHANGELOG'daki "Removed — BREAKING"
   işareti ve bu belgenin §5 migrasyon yolu, duyuru/geçiş ihtiyacını karşılar kabul edildi. Kod
   zaten kaldırılmış durumdaydı (bu karar bir "geri al" kararı değildir).
3. **`allow-unsafe-lock-duration` runbook/on-call devri — KARAR: basamak-2 planlama girdisi,
   DevOps takımına devredildi.** Flag'in prod'da kapalı default'u kod-tarafında zaten zorunlu
   (§3 madde 3); operasyonel runbook/on-call eğitimi entegrasyonu bu basamağın (1) teslim
   kapsamında değil, basamak-2 DevOps planlamasına aktarıldı.
4. **WorkQueue/Limits retention drift'i — KARAR: F-2 kod-tarafı bu turda kapatıldı (§4), kalan
   DevOps doğrulaması basamak-2 planlama girdisi olarak takip edilecek.** `ensureStream`'in
   `jobs.*` default'u `WorkQueue`'ya düzeltildi (bu repo'nun kendi auto-create yolu artık asyncapi
   ile hizalı). Kapsam dışında kalan iki kalem — `nats-bpm-bench`'in kendi `ensureStreams()`'i ve
   gerçek prod `nats stream add` provisioning'i — DevOps takımına devredildi, basamak-2 planlama
   girdisi olarak izlenecek.

---

*Kaynaklar: `CHANGELOG.md` (repo kökü), `docs/sentinel/phase6/PHASE6_REVIEW.md`, `docs/sentinel/phase4/lld/external-task-jetstream/`, `docs/sentinel/phase3/ADR/0001…0008`, `docs/sentinel/phase3/api/asyncapi.yaml`, `docs/sentinel/phase1/DATA_CLASSIFICATION.md`.*
