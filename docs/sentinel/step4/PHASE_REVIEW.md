# Basamak-4 Konsolide Faz İncelemesi — Outbound Handoff

**İnceleme kapsamı:** basamak-4 (outbound message/event → NATS, dual-write güvenli) — YALIN yol, phase1/3/4 atlandı, `docs/09-outbound-handoff.md` tek SPEC (kod↔spec + gerçek-bağımlılık + güvenlik birleşik kapı)
**İnceleme tarihi:** 2026-07-22
**İnceleyen:** sentinel-phase-review (taze bağlam, Opus) — kodun yazımı görülmedi, yalnız artefaktlardan doğrulandı
**Branch:** `feature/step4-outbound-handoff` (`4f8983b..HEAD`, 5 commit: `db49832`,`34b9656`,`d27e1b5`,`463e990`,`6f58a37`)
**SPEC:** `docs/09` D-A'…D-G' kilitli (2026-07-22)
**Kanıt tabanı:** fork motoru `~/Workspaces/cadenzaflow/cadenzaflow-bpm-platform/engine` (file:line doğrulandı) + gerçek gömülü Camunda/CadenzaFlow motoru + gerçek Postgres 16 + gerçek NATS JetStream (Testcontainers, offline imajlar)

> **Not (loader / manifest disiplini):** Standart `load_phase_context.sh phase-review` `docs/01_product/GUIDELINES_MANIFEST.yaml` bulamadı ve EXIT 3 verdi. Bu, YALIN-yol + BESPOKE manifest yerleşiminin (`docs/sentinel/step2/phase1/GUIDELINES_MANIFEST.yaml`, `layout_deviation` ile bilinçli belgelenmiş) beklenen sonucudur — hata değil. Yöneten manifest (basamak-2, en güncel tam manifest) DOĞRUDAN okundu; enabled disiplinler manuel uygulandı: `evidence-based-analysis`, `traceability-chain`, `rejected-alternatives-locked`, `locked-decisions-immutable`, `turkish-docs-english-code`, `no-effort-estimates`; compliance `KVKK`/`GDPR` enabled, `PCI-DSS` disabled (kart verisi işlenmez — bu basamakta hiçbir deliverable'da "cardholder/credit card" izi YOK, doğrulandı). Basamak-4'e ait ayrı manifest yoktur (YALIN yol); inceleme görevde tanımlı denetim eksenleri + gerçek-PG/gerçek-motor kanıtı üzerinden yürütüldü — step3 emsaliyle aynı model.

---

## VERDICT: 🟠 HAS-CONCERNS-NEEDING-ACK

Outbound-handoff dikişi kilitli D-A'…D-G' kararlarına **sadık** ve uçtan uca **gerçek gömülü motor + gerçek Postgres + gerçek JetStream** üstünde kanıtlandı. **ADIM-0 (D-A' dikişinin temeli) BAĞIMSIZ doğrulandı** — hem fork kaynağından (CommandContext call-stack zinciri) hem ampirik olarak (`NatsOutboundHandoffIntegrationTest` KENDİM koştum: kritik message-throw tam 1 tx-içi outbox satırı yazıyor → relay publish+delete; best-effort send-task 0 satır, post-commit publish). **Custody-transfer sağlam** (publish-then-delete yalnız PubAck sonrası; basamak-2 F-001 dersi tekrarlanmamış), **güvenlik temiz** (dinamik-SQL yok, DP-1 PII log yok), **regresyon TAM** (756/756 kendi elimle yeşil), **ayna byte-özdeş**, **migration temiz apply**. Bloklayan bulgu YOK. Ancak **Flowable outbound (D-G') dual-write-GÜVENLİ DEĞİL** — yalnız transport-hatası custody'si (DLQ) eklendi, outbox yok; Camunda kritik yolunun at-least-once garantisiyle arasındaki asimetri PO tarafından yazılı kabul edilmeli (CODER-QUESTION #3'ün gerçek sonucu). Bu bir spec-sadık sınırlamadır (D-G' zaten "sağlamlaştırma" olarak kapsamlandı), kod kusuru değil — bu yüzden BLOCKER değil, ACK-gerektiren MAJOR.

**Bulgu sayıları:** 🔴 0 BLOCKER · 🟠 1 MAJOR (ACK) · 🟡 2 MINOR · 🟢 2 NIT

---

## Kategori Skorkartı

| # | Eksen | Durum | Not |
|---|---|---|---|
| 1 | Kod↔karar sadakati (D-A'…D-G') | ✅ | Hepsi korunuyor; subject `events.<engineId>.<type>.<processInstanceId>` birebir; `events.*`+`dlq.events.*` guard'a eklendi; `jobs.*` A2-rezerve, kullanılmamış |
| 2 | ADIM-0 dikiş temeli (tx-in + CommandContext) | ✅ | Fork kaynağı + ampirik entegrasyon testi (KENDİM koştum) — çift doğrulandı |
| 3 | Custody-transfer / silent-loss (basamak-2 F-001 dersi) | ✅ | Kritik: tx-içi writer + relay publish-then-delete (PubAck sonrası). Best-effort: post-commit at-most-once (WARN-only, tasarım) |
| 4 | Güvenlik (injection / DP-1 PII log) | ✅ | Sabit-identifier + parametreli SQL (tek interpolasyon `BATCH_SIZE` int sabit); payload/değişken değeri loglanmıyor (yalnız subject/type/id/hash) |
| 5 | Regresyon + migration | ✅ | 756/756 yeşil (bağımsız); `outbound_message_outbox` boş DB + history/vault üstüne temiz apply |
| 6 | Ayna + hijyen (byte-mirror, TODO=0) | ✅ | camunda↔cadenzaflow byte-özdeş (yalnız FQN + fork'un koruduğu `camunda:` BPMN-NS farkı); TODO/FIXME=0; kv() structured log |
| 7 | Flowable dual-write yeterliliği (D-G') | ⚠️ | Yalnız DLQ-hardening; outbox yok → kritik Flowable outbound dual-write-güvenli değil (FINDING-001, ACK) |
| 8 | Gözlemlenebilirlik (metrics) | ⚠️ | Flowable DLQ-routing metriği bağlanmamış (FINDING-002) |

Efsane: ✅ sorun yok · ⚠️ yalnız 🟡/🟠 · ❌ 🔴 içeriyor

---

## Kendi Elimle Koştuğum Testler (gerçek motor / gerçek PG / gerçek JetStream)

`JAVA_HOME=/usr/lib/jvm/temurin-21-jdk-amd64`, Testcontainers `postgres:16` + `nats:2.10-alpine` (imajlar lokalde, offline). Reactor önce `-DskipTests install` (BUILD OK), sonra hedefli + tam koşum.

| Test sınıfı | Modül | Sonuç | Kanıtladığı |
|---|---|---|---|
| **`NatsOutboundHandoffIntegrationTest`** | **camunda** | **2/2 ✅** | **ADIM-0 AMPİRİK**: gerçek gömülü Camunda + gerçek PG + gerçek JetStream; kritik message-throw → tam 1 tx-içi outbox satırı → relay publish+delete (`events.camunda.payment.requested.<pid>`); best-effort send-task → 0 satır, post-commit publish |
| `NatsOutboundHandoffIntegrationTest` | **cadenzaflow** | **2/2 ✅** | Aynı senaryo **gerçek CadenzaFlow fork motoru** üstünde (üretim hedefi) |
| `OutboundMessageOutboxWriterTest` | nats-core | 4/4 ✅ | tx-içi INSERT, çağıranın canlı Connection'ı (gerçek PG) |
| `OutboundMessageRelayTest` | nats-core | 4/4 ✅ | oldest-first fetch, publish-then-delete, stuck-age gauge (gerçek PG+NATS) |
| `OutboundClassificationPropertiesTest` | nats-core | 4/4 ✅ | D-C' kritik/best-effort ayrımı + değişken allowlist |
| `OutboundSubjectBuilderTest` | nats-core | 5/5 ✅ | D-E' subject şeması + blank-guard |
| `OutboundWireMessageFactoryTest` | nats-core | 5/5 ✅ | wire-envelope + dependency-free JSON escape |
| `OutboundPostCommitPublisherTest` | nats-core | 2/2 ✅ | best-effort at-most-once, WARN-only loss |
| `NamespaceValidatorTest` | nats-core | 9/9 ✅ | `events.*`/`dlq.events.*`/`jobs.*` rezervasyonu |
| `NatsOutboundPublisherTest` (camunda+cadenzaflow) | 2× | 6/6 ✅ | classify→outbox/post-commit dallanması, unsupported-element fail-safe skip |
| `OutboundMessageTypeResolverTest` (camunda+cadenzaflow) | 2× | 7/7 ✅ | message-throw/send-task tip türetimi; diğer element→empty |
| `NatsOutboundEventChannelAdapterTest` | flowable | 6/6 ✅ | D-G' core-NATS publish + DLQ-on-failure custody |
| `JetStreamOutboundEventChannelAdapterTest` | flowable | 5/5 ✅ | D-G' JS publish+PubAck + DLQ-on-failure |
| `NatsChannelDefinitionProcessorTest` | flowable | 16/16 ✅ | DLQ subject çözümü + `events.*` guard entegrasyonu |

**TAM REACTOR REGRESYON: 756 test, 0 failure, 0 error, 0 skip** (148 surefire raporundan toplandı) — coder'ın **756/756 iddiası birebir doğrulandı**, KENDİM koştum. Basamak-1/2/3 (A2/history/variable) davranışı kırılmamış.

**Bağımsız DB probe (disposable `postgres:16`, Testcontainers dışı):**
- `outbound_message_outbox` boş DB'ye **temiz apply** (pgcrypto ext + tablo + 2 index).
- history (`compact_history_outbox`) + vault (`pseudonym_map`) migration'larının **ÜSTÜNE katmanlı apply** — çakışma yok.
- Tablo şekli D-E'/D-F' ile uyumlu (subject VARCHAR(600), payload BYTEA, created_at index, engine_id index). Re-apply hata verir (Flyway-versiyonlu, `IF NOT EXISTS` değil — `compact_history_outbox` emsaliyle aynı; Flyway versiyonlaması tekrar-koşumu engeller).
- Probe konteynerı force-removed; Testcontainers Ryuk ile temizlendi; **leftover review konteynerı yok** (kalan postgres'ler 3 haftalık ilgisiz altyapı).

---

## ADIM-0 Bağımsız Teyidi (D-A' dikişinin temeli — KİLİT)

Coder "`ExecutionListener.notify` non-null `CommandContext` ile aynı tx call-stack'inde koşar" dedi. **İki yönden bağımsız doğruladım:**

**(a) Fork kaynağı (file:line, KENDİM okudum):**
- `CommandContextInterceptor.java` (`org.cadenzaflow.bpm.engine.impl.interceptor`): `Context.setCommandContext(context)` **`next.execute(command)`'dan ÖNCE** set edilir (satır ~106), `finally`'de `Context.removeCommandContext()` (satır ~126). → Komut yürütme boyunca CommandContext garanti mevcut.
- `AbstractEventAtomicOperation.java:59-63` (`impl.core.operation`): `execution.invokeListener(listener)` — listener `notify()`'ı **aynı call frame içinde senkron** çağırır (`eventNotificationsStarted` → `invokeListener`), komut bitene dek frame'den çıkmaz. → `Context.getCommandContext()` burada non-null.
- Tx-içi bağlantı: `NatsOutboundPublisher.currentTransactionConnection()` = `Context.getCommandContext().getDbSqlSession().getSqlSession().getConnection()` — MyBatis SqlSession'ın motor-tx JDBC bağlantısı, basamak-2 `NatsHistoryEventHandler` emsaliyle birebir.

**(b) Ampirik (KENDİM koştum):** `NatsOutboundHandoffIntegrationTest.criticalMessageThrow_writesOutboxRow_thenRelayPublishesAndDeletes` — `startProcessInstanceByKey` DÖNDÜKTEN sonra (üst komut commit'lendikten sonra) `outbound_message_outbox`'ta **tam 1 satır** görülüyor; bu, yazımın motorun kendi tx'i içinde (aynı bağlantı) gerçekleştiğinin kanıtı. Relay sonrası satır 0, mesaj JetStream'de `events.camunda.payment.requested.<pid>` üstünde.

**Sonuç: ADIM-0 SAĞLAM.** D-A' dikişi hem kaynak hem davranış seviyesinde teyitli. (Docs/09 §2.2'deki "⚠️ bu turda çapraz-doğrulanmadı" uyarısı artık kapandı — bu incelemede çapraz-doğrulandı.)

---

## Bulgular

### 🟠 FINDING-001 [MAJOR — ACK gerekli] — [Kod↔karar sadakati (D-G') + Açık risk] Flowable outbound dual-write-GÜVENLİ değil; yalnız transport-hatası custody'si (DLQ), outbox yok

**Ne:** Basamak-4'ün tek-cümlelik tezi (docs/09 §0) **dual-write riskini çözmek**. Camunda yolu bunu yapısal olarak başarıyor (tx-içi outbox + relay = at-least-once, commit'e bağlı). Flowable yolu (D-G') ise happy-path'i **değiştirmeden** yalnız FAILURE-path'e DLQ ekliyor:
- `NatsOutboundEventChannelAdapter.sendEvent` happy-path hâlâ çıplak `connection.publish` (core-NATS, ack yok, jetstream=false tenant tercihi).
- `JetStreamOutboundEventChannelAdapter.sendEvent` JS publish+PubAck yapıyor ama yine motor-tx'e bağlı bir outbox YOK.
- Her iki adapter da publish HATASINDA DLQ'ya custody-transfer ediyor (`OUTBOUND_PUBLISH_FAILED`) — bu transport-kaybını önler, ama **dual-write'ı çözmez**: `sendEvent` motor-tx'i İÇİNDE koşuyorsa ve publish başarılı ama tx rollback olursa → **phantom mesaj**; tx DIŞINDA koşuyorsa commit-sonrası publish-hatası DLQ'ya düşer (custody OK) ama commit'e-atomik at-least-once garantisi yok.

**CODER-QUESTION #3 değerlendirmesi (meşru-erteleme mi, açık-risk mi):** **İKİSİ birden.** tx-boundary'nin *doğrulanması* meşru-ertelemedir — Flowable ENGINE kaynağı workspace'te YOK (teyit ettim: yalnızca ilgisiz `robusta/flowable5-engine` var, farklı codebase), event-registry API jar'ları çağıranı izlemeye yetmiyor. Ama tx-boundary ne olursa olsun *sonuç* bir açık-risktir: Flowable kritik outbound Camunda ile aynı at-least-once kontratına GETİRİLMEDİ. Kod, kilitli D-G''ye SADIK ("sağlamlaştırma"=DLQ; erteleme-4b REDDEDİLDİ) — yani spec ihlali değil; risk, spec'in kendi tezinin Flowable için tam karşılanmamasıdır.

**Nerede:** `flowable-.../NatsOutboundEventChannelAdapter.java:64-93` (happy-path değişmemiş, DLQ yalnız catch/disconnected); `JetStreamOutboundEventChannelAdapter.java:55-86`; Javadoc CODER-QUESTION (`NatsOutboundEventChannelAdapter.java:34-39`).

**Neden önemli:** Carrier-grade tier'da "kritik outbound" için Camunda ↔ Flowable garanti asimetrisi operasyonel bir sürpriz olur (aynı iş mesajı Camunda'dan at-least-once, Flowable'dan at-most-once+DLQ). PO/mimar bunu açıkça bilmeli.

**Önerilen çözüm:** (1) Flowable outbound'un **at-most-once + DLQ-on-failure** (dual-write-güvenli DEĞİL) olduğunu docs/09 §5'e açık-risk olarak yaz; (2) kritik Flowable outbound gereken tenant için outbox-eşdeğeri takip işi (basamak-4b/5) olarak izle; (3) Flowable ENGINE kaynağı erişilebilir olduğunda `sendEvent` tx-boundary'sini file:line teyit et (docs/09 §4 item 3). Bu, açık yazılı ACK ile ilerleyebilir — bloklamaz.

---

### 🟡 FINDING-002 [MINOR] — [Gözlemlenebilirlik] İki Flowable outbound metrik metodu bildirilmiş ama HİÇ bağlanmamış → DLQ-routing yalnız log'da görünür

**Ne:** `NatsChannelMetrics.flowableOutboundPublishedCount(subject,channel)` ve `flowableOutboundDlqRoutedCount(subject,channel)` eklenmiş fakat **hiçbir yerde çağrılmıyor** (grep: 0 kullanım). Flowable adapter'ları önceden var olan `jsPublishCount`/`jsPublishErrorCount`'u kullanıyor; `routeToDlq()` yalnızca WARN log basıyor, DLQ-metriği artırmıyor. Sonuç: Flowable outbound **DLQ-routing hacmi metrik ile gözlemlenemiyor/alarmlanamıyor** (yalnız log). Camunda outbox yolu ise tam metrikli (written/relayed/oldest-row-age gauge/post-commit).

**Nerede:** `nats-core/.../metrics/NatsChannelMetrics.java` (yeni 2 metod, ölü); `flowable-.../NatsOutboundEventChannelAdapter.java:96-106` + `JetStreamOutboundEventChannelAdapter.java:89-99` (`routeToDlq` metrik artırmıyor).

**Neden önemli:** Carrier-grade'de custody-transfer (DLQ-routing) metrik-görünür olmalı — DLQ hacmi bir sağlık sinyalidir.

**Önerilen çözüm:** `flowableOutboundDlqRoutedCount`'u her iki `routeToDlq()`'ya bağla (ve happy-path'te `flowableOutboundPublishedCount` gerekliyse — yoksa `jsPublishCount` yeterli, o zaman ölü metodu sil ve "DLQ-routing log-only" duruşunu belgeleyin).

---

### 🟡 FINDING-003 [MINOR] — [Açık risk / girdi-hijyeni] `messageType` NATS-subject token'ına karşı doğrulanmıyor → tenant mesaj adı `*`/`>`/boşluk içerirse bozuk/wildcard subject

**Ne:** `OutboundSubjectBuilder.build` yalnız blank-guard yapıyor; `messageType` doğrudan `events.<engineId>.<type>.<processInstanceId>` içine ve outbox `subject` kolonuna interpolate ediliyor. Tenant BPMN `<message name="...">` bir NATS wildcard token'ı (`*`, `>`) ya da boşluk içerirse subject bozulur/yanlış-eşleşir. Noktalı tipler (D-E' örnekleri `order.created`, `payment.requested`) **kasıtlı** ve çalışıyor (test kanıtladı), ama wildcard/boşluk korumasız.

**Nerede:** `nats-core/.../outbound/OutboundSubjectBuilder.java:18-23` (yalnız `requireNonBlank`).

**Neden önemli:** Girdi tenant-config (dış-saldırgan değil) olduğundan MINOR; ama namespace-guard disiplininin (BAQ-4) doğal uzantısı — bozuk subject sessiz yanlış-routing/publish-hatası doğurur.

**Önerilen çözüm:** `messageType`'ı `^[A-Za-z0-9._-]+$` gibi bir allowlist regex'ten geçir (veya NATS wildcard token'larını reddet) — namespace-guard emsaliyle tutarlı.

---

### 🟢 FINDING-004 [NIT] — [Açık risk] Outbound mesaj `traceId`'si taze UUID (inbound-trace propagasyonu yok) → dağıtık-trace sürekliliği kopuk; ama basamak-1 A2 emsaliyle TUTARLI

**Ne:** `NatsOutboundPublisher.notify` her mesaj için `traceId = UUID.randomUUID()` üretiyor — process'in başlatan inbound trace bağlamından türetmiyor. Outbound mesaj, process'in nedensel zincirine korele olmaz.

**Nerede:** `NatsOutboundPublisher.java:82`.

**Değerlendirme (regresyon DEĞİL):** Bu, basamak-1 `A2JobMessageFactory.java:54`'ün **birebir emsali** (`BpmHeaders.build(UUID.randomUUID()...)`; motor traceId'yi first-class alan olarak sunmuyor — o sınıfın yorumu bunu açıkça söylüyor). Yani proje-geneli bilinen sınırlama, basamak-4'e özgü yeni kusur değil. Gelecekteki çapraz-kesen trace-propagation işi için not.

---

### 🟢 FINDING-005 [NIT] — [Test tamlığı] Entegrasyon testi tx-içi yazımı ve custody-transfer'i kanıtlıyor ama açık rollback-atomiklik assert'i içermiyor

**Ne:** `NatsOutboundHandoffIntegrationTest` outbox satırının tx-içi YAZILDIĞINI (commit sonrası görünür) ve relay'in publish+delete ettiğini kanıtlıyor, ancak **motor tx'i rollback olduğunda satırın YAZILMADIĞINI** davranışsal olarak assert etmiyor. Atomiklik yapısal olarak garanti (yazım motorun kendi SqlSession JDBC bağlantısını kullanıyor — aynı tx, birlikte rollback), bu yüzden kusur değil, kapsam-tamlığı notu.

**Önerilen çözüm:** Opsiyonel — aynı tx'te sonradan patlayan bir adım (ör. hata fırlatan bir sonraki delegate) ile bir rollback-atomiklik testi ekleyip garantiyi davranışsal kilitleyin.

---

## Coder Beyanlarının Değerlendirmesi (görevde istenen)

| # | Coder beyanı | Değerlendirme |
|---|---|---|
| **ADIM-0** (ExecutionListener non-null CommandContext, tx-içi) | **DOĞRU ✅ — bağımsız teyitli** | Fork kaynağı (CommandContextInterceptor set-before/remove-in-finally + AbstractEventAtomicOperation senkron invokeListener) + ampirik integration test (KENDİM koştum). |
| **Custody-transfer / silent-loss** (basamak-2 F-001) | **DOĞRU ✅** | Kritik relay yalnız PubAck (senkron `jetStream.publish` throws-unless-ack) SONRASI delete; publish-then-forget/delete-before-ack YOK. Writer tx-içi (çağıranın Connection'ı, kendi pool açmıyor). Best-effort post-commit at-most-once (WARN-only, bilinçli). `HistoryOutboxRelay` deseniyle birebir. |
| **CODER-QUESTION #3** (Flowable tx-boundary) | **Meşru-erteleme (doğrulama) + açık-risk (sonuç)** 🟠 | Flowable ENGINE kaynağı workspace'te yok (teyit ettim) → tx-boundary doğrulaması meşru ertelenmiş. AMA Flowable outbound dual-write-güvenli değil (yalnız DLQ) → FINDING-001, açık ACK gerekli. |
| **CODER-NOTE-1** (migration engine modülünde, nats-core'da değil) | **DOĞRU ✅** | `compact_history_outbox` da `camunda-nats-channel/.../db/migration/history/`'de — emsal teyitli; nats-core'un kendi DataSource'u yok (shared lib). Byte-özdeş cadenzaflow aynası. |
| **CODER-NOTE-2** (writer/relay/post-commit nats-core'da TEK, per-engine değil) | **DOĞRU ✅** | Bu 3 sınıf imzasında fork-tipi taşımıyor (yalnız `Connection`+`OutboundMessageDraft`) → tek yerde yaşayıp iki motor modülünce kullanılabilir; yalnız fork-tipine dokunan `NatsOutboundPublisher` (ExecutionListener) aynalanmış. Yapısal olarak doğru. |
| **CODER-NOTE-3** (tip BPMN-element'ten türetiliyor, field-injection değil) | **DOĞRU ✅** | `OutboundMessageTypeResolver.resolve` model element'ten okuyor; singleton bean'de field-injection instance-state clobber/race doğururdu. Stateless+thread-safe. |
| **CODER-NOTE-4** (Flowable jetstream=true/false iki-adapter split korundu) | **DOĞRU ✅** | İki ayrı adapter; her ikisine DLQ opt-in eklendi, transport değişmedi. |
| **CODER-NOTE-5** (criticalTypes default boş → best-effort) | **DOĞRU ✅** | Outbound tipleri %100 tenant-tanımlı; motor-bilinen seed yok → boş default tek savunulabilir seçim. `classify()` bilinmeyen tip→BEST_EFFORT. |

---

## Güvenlik Ekseni (ayrı özet)

- **SQL injection — TEMİZ ✅.** `OutboundMessageOutboxWriter.INSERT_SQL` sabit + 8 parametreli bind (`setObject`/`setString`/`setBytes`). `OutboundMessageRelay` SELECT/DELETE/AGE sabit; tek interpolasyon `BATCH_SIZE` (private static final int). Dinamik-identifier/kullanıcı-girdisi interpolasyonu YOK. (basamak-2/3 allowlist dersi zaten uygulanmış — burada interpolate edilen identifier hiç yok.)
- **PII loglama (DP-1) — TEMİZ ✅.** Grep: hiçbir `log.*` satırı payload/getVariable/rawEvent/businessKey **değerini** yazmıyor. Yalnız `subject`, `message_type`, `engine_id`, `process_instance_id`, `outbox_row_id`, yaş/eşik. Outbox `business_key` kolonu CONFIDENTIAL işaretli, subject'e KOYULMUYOR. `DlqReason.headerValue()` yalnız kod string'i (DP-6).
- **Namespace-guard — SAĞLAM ✅.** `events.*` + `dlq.events.*` rezerve (spesifik `dlq.events.` önce kontrol); `jobs.*` A2-rezerve dokunulmadı. Flowable channel processor bu guard'ı çağırıyor (16/16 test).
- **Metrik kardinalitesi — TEMİZ ✅.** Outbound-handoff metrikleri `message_type`/`engine_id`/`outcome` ile etiketli (sınırlı); `processInstanceId` etiket olarak YOK (yüksek-kardinalite PII kaçağı yok).

---

## Şeffaflık — Ne Kontrol Ettim

- **Okunan production dosyaları (tam):** nats-core outbound — `OutboundMessageOutboxWriter`(79), `OutboundMessageRelay`(191), `OutboundMessageRelayScheduler`(62), `OutboundPostCommitPublisher`(52), `OutboundWireMessageFactory`(122), `OutboundSubjectBuilder`(30), `OutboundClassification`(13), `OutboundClassificationProperties`(75), `OutboundMessageDraft`(18), `OutboundMessageOutboxProperties`(35), `OutboundHeaders`(26); `NamespaceValidator`(diff), `DlqReason`(diff), `TopicNamespaceCollisionException`(diff), `NatsChannelMetrics`(diff). camunda — `NatsOutboundPublisher`(145), `OutboundMessageTypeResolver`(59), `V1__outbound_message_outbox.sql`(43), auto-config diff. Flowable — `NatsOutboundEventChannelAdapter`(107), `JetStreamOutboundEventChannelAdapter`(100), `NatsChannelDefinitionProcessor`/`NatsOutboundChannelModel` diff. bench — `OutboundBenchScenario`(header), `OutboundDbWriteOpReport`(31). cadenzaflow auto-config diff.
- **Fork-kanıt (KENDİM okudum):** `CommandContextInterceptor.java` (set/remove CommandContext), `AbstractEventAtomicOperation.java:40-80` (senkron invokeListener), `BpmnParse.java:4436` (`CAMUNDA_BPMN_EXTENSIONS_NS` — fork'un `camunda:` NS koruması).
- **Bağımsız koşulan testler:** yukarıdaki tablo + **TAM reactor 756/756** (KENDİM, gerçek PG+NATS).
- **Bağımsız DB probe:** outbound migration boş + katmanlı apply (disposable PG); tablo/index doğrulama; re-apply davranışı.
- **Grep pass'leri:** TODO/FIXME (0 prod); dinamik-SQL (yok); PII-değer loglama (0); metrik-metod kullanımı (2 ölü Flowable metod); ayna byte-diff (camunda↔cadenzaflow); `@Scheduled` double-drive (history emsaliyle tutarlı, bulgu değil); `@EnableScheduling` (prod'da yok).
- **Ayna doğrulaması:** `NatsOutboundPublisher`/`OutboundMessageTypeResolver`/migration SQL — FQN + fork'un koruduğu `camunda:` BPMN-NS dışında byte-özdeş; SQL birebir.

## Dürüstlük — Ne Kontrol ETMEDİM

- **Flowable `sendEvent` tx-boundary'si** — Flowable ENGINE kaynağı workspace'te yok (yalnız ilgisiz `robusta/flowable5-engine`); tx-içi/dışı olduğu file:line izlenemedi (FINDING-001; docs/09 §4 item 3, meşru erteleme).
- **Rollback-atomiklik davranışsal testi** — yapısal olarak garanti (engine SqlSession bağlantısı) ama açık test yok (FINDING-005).
- **Yük/soak/throughput** — `OutboundBenchScenario` var (DB-write-op profil: best-effort=0 ek yazım, kritik ≤1 satır/tx hard-gate) ama bench KOŞULMADI (faz-5.5 kapsamı; ekip işi).
- **Message END-event (yalnız intermediate throw değil)** — docs/09 §4 item 2; `OutboundMessageTypeResolver` `ThrowEvent`'i (end dahil `ThrowEvent` alt-tipi) kapsıyor ama end-event message dalının CAM-436 parse denkliği ampirik izlenmedi.
- **Çok-node leader yarışı / relay lease devri gerçek stres** — kod + tek-leader test ile değerlendirildi; çok-node kaos koşulmadı.
- **NATS wildcard'lı tenant message-adı gerçek publish davranışı** — FINDING-003 kod-incelemesiyle; canlı bozuk-subject publish denenmedi.

---

## İnsan İçin Sıradaki Aksiyon

**HAS-CONCERNS-NEEDING-ACK:** Bloklayan bulgu YOK; faz teknik olarak ilerlemeye hazır. Ancak:
1. **FINDING-001 (🟠, ACK zorunlu):** Flowable outbound'un dual-write-güvenli DEĞİL (at-most-once + DLQ) olduğunu docs/09 §5'e açık-risk yaz + PO/mimar yazılı kabul; Flowable ENGINE kaynağı erişilince tx-boundary teyidi (docs/09 §4-3) + gerekirse outbox-eşdeğeri takip işi.
2. **FINDING-002/003 (🟡):** çöz (DLQ metriğini bağla; messageType allowlist regex) veya gerekçeli-kabul.
3. **FINDING-004/005 (🟢):** bilgi amaçlı; trace-propagation ve rollback-testi gelecekte.

Çözüm/ACK sonrası `/sentinel` faz-review yeniden koşulabilir. **Kodu değiştirmedim; commit/push yapmadım; tüm disposable konteynerlar temizlendi.**

---

## Bulgu Kapanış Kaydı (2026-07-22, review sonrası)

| Bulgu | Kapanış |
|---|---|
| 🟠 F-001 (Flowable outbound dual-write asimetri) | **YAZILI ACK** — kilitli D-G' kapsamı (sağlamlaştırma=DLQ); Flowable at-least-once outbox = izlenen borç, Flowable engine-source tx-boundary teyidi önkoşul. Camunda/CadenzaFlow kritik-outbound at-least-once garantili. |
| 🟡 F-002 (metrik bağlama) | **DÜZELTİLDİ** `2499014`. |
| 🟡 F-003 (messageType subject-token doğrulama) | **DÜZELTİLDİ + BAĞIMSIZ DOĞRULANDI** `2499014`; OutboundSubjectBuilderTest 18/18 (wildcard/bozuk red). |
| 🟢 F-004/F-005 | İZLENİR (NIT). |

**Sonuç:** BLOCKER yok; 1 MAJOR yazılı-ACK, 2 MINOR düzeltildi, 2 NIT izlenir. Reactor 778/778. Faz kapısı için tek bekleyen: Levent go/no-go (v0.5.0).
