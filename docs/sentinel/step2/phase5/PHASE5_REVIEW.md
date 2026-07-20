# Phase 5 (Implementation) Review Raporu — basamak-2 History Offload — **YENİDEN-KOŞU (fix sonrası)**

**İncelenen faz:** 5 (Implementation / kod) — RE-REVIEW (önceki verdict HAS-BLOCKERS'ın fix'leri doğrulandı)
**İnceleme tarihi:** 2026-07-21
**Reviewer:** sentinel-phase-review (taze bağlam, Opus 4.8 — fix kodunun yazımı görülmedi, yalnız artefaktlardan + bağımsız test koşumundan doğrulandı)
**Branch:** `feature/step2-history-offload` (main'e göre +18 commit; son 4'ü fix: `f8f3b57`, `5ca7987`, `f7724b4`, `a2869da`)
**İncelenen modüller:** `nats-core`, `camunda-nats-channel`, `cadenzaflow-nats-channel`, `nats-history-projection`, `nats-bpm-bench`
**Kontrat kaynakları:** phase3 `api/{asyncapi,openapi}.yaml` + ADR 0009–0019; phase4 `lld/history-offload/*`, `db/migrations/*`, `ERROR_REGISTRY.md`, `DB_ACCESS_MAP.md`; phase1 `GUIDELINES_MANIFEST.yaml` (BESPOKE)
**Manifest durumu:** enabled core-disiplin:7, compliance:KVKK+GDPR (v-pinli), stack:NATS_JETSTREAM/POSTGRES/JAVA, disabled:PCI-DSS/HIPAA/KAFKA/CLICKHOUSE/ui-ux/frontend-security; spot_check_minimum:5; phase_gate: MAJOR çözülmeden/yazılı-kabul olmadan faz geçilmez
**NOT (BESPOKE sapma):** Standart `load_phase_context.sh` bu repoda `docs/01_product/` beklediği için exit-3 verir; bu manifest'in bilinçli BESPOKE tercihidir (PO-Q1 2026-07-17, `layout_deviation`). phase-3.5 / RUNTIME_CONTRACT / docs/01_product YOKLUĞU **eksiklik sayılmamıştır**. Manifest doğrudan okundu, disiplinler manuel denetlendi.

---

## Verdict

**HAS-CONCERNS-NEEDING-ACK**

Önceki review'ın **bloklayan MAJOR'ı (FINDING-001, append-log idempotency)** ve **3 MINOR'ı (FINDING-002/003/004)** hepsi **GERÇEKTEN KAPANDI** — bağımsız kanıtla (asyncapi yeniden-validate temiz + gerçek PG/NATS Testcontainers ile 4/4 hedef test yeşil, redelivery testi motor-event-time + doğru dated-partition'ı uçtan uca kanıtlıyor). Fix'ler yeni bir **veri/denetim-bütünlüğü** regresyonu doğurmadı; mimari ayna disiplini (camunda↔cadenzaflow) 5/5 değişen dosyada korundu; güvenlik ekseni (SQL-injection allowlist, ham-PII log yokluğu) fix'lerde de temiz. **Ancak fix sürecinde yüzeye çıkan bir 🟡 concern var:** `SweepLeaderLease.heldRevision` renew-fail sonrası null'lanmadığı için `isLeader()` bayat-true kalır → FINDING-004'ün yeni `SYS_OUTBOX_RELAY_LEADER_LOST` WARN'ı, commit'in iddia ettiği gibi "yalnız gerçek geçişte bir kez" değil, **geçiş sonrası her non-leader döngüsünde tekrar** üretilir. Veri/denetim etkisi YOK (relay leader-kapısı `tryAcquireOrRenew()` dönüşünü kullanır, `isLeader()`'ı değil), yalnız gözlemlenebilirlik/alarm sinyali sadakati bozulur. WARN-seviye, MAJOR barajını aşmaz → bloklamaz; ama yazılı ack (veya küçük takip-fix) gerektirir. Yazar bunu zaten "kapsam-dışı bilinen latent" olarak işaretlemiş.

---

## Önceki Bulguların Kapanış Durumu

| Eski bulgu | Ciddiyet | Fix commit | Bağımsız doğrulama | Durum |
|---|---|---|---|---|
| FINDING-001 append-log dedup `event_time=now()` | 🟠 MAJOR | `f8f3b57` | asyncapi validate temiz + redelivery testi yeşil (tek satır + motor-zamanı + doğru partition) | ✅ **KAPANDI** |
| FINDING-002 erasure verify yalnız ACTINST assignee | 🟡 MINOR | `5ca7987` | ErasurePipelineTest 8/8 (3 yeni: VARINST/COMMENT fail + out-of-scope pass) | ✅ **KAPANDI** |
| FINDING-003 vault Javadoc bayat + accessor_identity | 🟡 MINOR | `f7724b4` | Javadoc motor-side'a düzeltildi; `accessor_identity="system:compact-history-outbox-writer"`; vault test 5/5 | ✅ **KAPANDI** |
| FINDING-004 8 kod kaynakta izlenemez | 🟡 MINOR | `a2869da` | 8/8 kod artık kaynakta izlenebilir; DLQ-fail nak gerçek davranış (test 2/2) | ✅ **KAPANDI** (leader-lost emit'i NEW-001 ile etkileşir) |
| FINDING-005 DETAIL byte-array düşürülüyor | 🟢 NIT | — (meşru erteleme) | `HistoryProjectionConsumer.java:185-197` hâlâ dokümante-boşluk | ⏸️ AÇIK (NIT, izlenir) |
| FINDING-006 attachment kolonları anonim-dışı | 🟢 NIT | — (meşru erteleme) | CODER-NOTE korunuyor | ⏸️ AÇIK (NIT, izlenir) |

---

## Bulgular

### 🔴 Kritik (bloklar)

_(yok)_

### 🟠 MAJOR (bloklar — çöz veya yazılı kabul)

_(yok — önceki tek MAJOR olan FINDING-001 kapandı)_

### 🟡 Concern (yazılı ack gerektirir; faz-6 ile paralel kapatılabilir)

**NEW-001** — [kategori: Internal Consistency / gözlemlenebilirlik sinyal sadakati]
- **Ne:** `SweepLeaderLease.heldRevision` yalnız başarılı acquire/renew'de set edilir; renew-fail'in HİÇBİR yolunda (KV handle yok / lookup fail / başka node tuttu / update-race) null'lanmaz. Sonuç: bir node liderliği bir kez aldıysa (`heldRevision != null`), sonradan lider olmaktan çıksa bile `isLeader()` **bayat-true** döner. FINDING-004 fix'i `relayCycle()`'da `boolean wasLeader = leaderLease.isLeader()` okuyup `!tryAcquireOrRenew() && wasLeader` iken `SYS_OUTBOX_RELAY_LEADER_LOST` WARN basar → geçiş sonrası **her** non-leader döngüsünde WARN tekrarlar (bir kez değil).
- **Nerede:**
  - `nats-core/.../jetstream/SweepLeaderLease.java:44` (`private volatile Long heldRevision`), `:90,:107,:110,:117` (dört renew-fail return-false yolu, hiçbiri `heldRevision=null` yapmıyor), `:121-122` (`isLeader() { return heldRevision != null; }`).
  - `camunda-nats-channel/.../history/HistoryOutboxRelay.java:77-86` (+`cadenzaflow` ayna) — `wasLeader=isLeader()` → WARN.
- **Kanıt:** `a2869da` commit mesajı: *"logs WARN only on a genuine leader->non-leader TRANSITION (not on every routine 'not currently leader' cycle, which stays silent)"* — bu iddia gerçek işletimde YANLIŞ: bayat `heldRevision` yüzünden `wasLeader` geçiş sonrası kalıcı-true. `HistoryOutboxRelayLeaderTransitionTest` mock `isLeader()` (`thenReturn(true/false)`) kullandığı için gerçek staleness'i **maskeler**; `SweepLeaderLeaseTest`'in `isLeader()==false` assert'leri yalnız **hiç-acquire-edilmemiş** taze lease üzerinde koşar (acquire→kaybet senaryosu test edilmez — edilseydi FAIL ederdi). `ERROR_REGISTRY.md:62` bu kodu **WARN / "rutin"** sınıflandırır.
- **Neden önemli:** Carrier-grade backend'de her non-leader döngüsünde tekrarlayan yanlış "leadership lost" WARN → alarm yorgunluğu / sahte "flapping leader" sinyali. **Ancak veri/denetim-bütünlüğü etkisi YOK:** relay'in gerçek lider-kapısı `if (!tryAcquireOrRenew()) return;` ile sürülür (`isLeader()` yalnız log bayrağı); non-leader döngü DB okumaz, mesaj kaybı/mükerrer denetim satırı doğmaz. Basamak-1'de `isLeader()` kontrol-akışı/log için HİÇ tüketilmiyordu (yalnız `tryAcquireOrRenew()` dönüşü davranışı sürüyordu) — bu latent-benign hata ilk kez FINDING-004 fix'iyle görünür oldu.
- **Önerilen çözüm:** `tryAcquireOrRenew()`'in renew-fail dallarında (özellikle `:110` başka-node-tuttu ve `:117` update-race) `this.heldRevision = null;` set et; ya da `HistoryOutboxRelayLeaderTransitionTest`'e gerçek acquire→lose sekansı ekleyip staleness'i yakala. Alternatif: yazılı ack ile takip-basamağa devret (yazar zaten latent olarak işaretledi).

### 🟢 NIT (bilgilendirici — meşru erteleme / doküman-drift)

**NEW-002** — Fix-sonrası artık doküman-drift: engine outbox `event_time` DDL yorumu hâlâ "display only, ADR-0012" (`camunda`/`cadenzaflow`-nats-channel `.../resources/db/migration/history/V1__compact_history_outbox.sql:36`; ayna `docs/sentinel/step2/phase4/db/migrations/engine-outbox/001_compact_history_outbox.sql:36`). FINDING-001 bu değeri append-log sınıfları için projeksiyon-tarafında **dedup unique-key + range-partition ANCHOR** olarak yeniden-tanımladı (`V2__append_log_tables.sql:7,11,37`). "display only" ifadesi entity-lifecycle için doğru (orada stream_sequence otorite) ama audit-kritik append-log (OP_LOG/EXT_TASK_LOG) için artık yanıltıcı — ileride "event_time serbestçe değişebilir" yanılgısına yol açar. Yorumu güncelle (davranış doğru; yalnız yorum).

**NEW-003 (gözlem)** — `HistoryEventFieldExtractor.eventTimeOf` (`:157-179`) tanınmayan alt-tip / null timestamp'te `Instant.now()`'a düşer. Bu değer **publish/tx-write anında bir kez** hesaplanıp mesaja/outbox kolonuna pişirildiği için redelivery-idempotency'yi BOZMAZ (redelivery aynı saklı değeri taşır) — F-001 çözümü sağlam. Yalnız null-timestamp'li nadir olayda partition-anchor gerçek-event-zamanı yerine write-zamanı olur; kabul edilebilir kenar durum, bulgu değil.

**NEW-004 (gözlem)** — `HistoryDlqConsumer.routeToDlq` (`:32-36`) `historyDlqRoutedCount` metriğini publish outcome'dan BAĞIMSIZ artırır; FAILED_BOTH_PUBLISH'te bile "routed" sayacı artar. FINDING-004 fix'i consumer'ın ack/nak'ını düzeltti ama bu metrik-sayımı fix-öncesi de böyleydi (kapsam-dışı, pre-existing). "routed vs dlq-publish-failed" oranına dayalı alarmı hafif çarpıtır; trivial.

---

## Kategori Scorecard

| # | Kategori | Durum | Not |
|---|---|---|---|
| 1 | Completeness | ✅ | 8/8 error-registry kodu artık izlenebilir; CQ-1 bayat Javadoc + accessor_identity düzeltildi; kalan NIT'ler meşru-erteleme |
| 2 | Alignment (kontrat sadakati) | ✅ | asyncapi'ye EVENT_TIME header eklendi (required) + `@asyncapi/cli validate` TEMİZ; iki publish yolu da header'ı set eder; consumer header'dan okur (now() kalktı) |
| 3 | Internal consistency | ⚠️ | FINDING-001 iç-tutarsızlığı GİDERİLDİ (redelivery testi kanıt); yeni 🟡 NEW-001 (SweepLeaderLease staleness → leader-lost WARN tekrarı) — gözlemlenebilirlik, veri değil |
| 4 | Manifest discipline | ✅ | Disabled (KAFKA/CLICKHOUSE/PCI-DSS/HIPAA/ui-ux) içerik sızıntısı YOK; enabled stack ADR-yetkili; kilitli D-A…G ve CQ-1/CQ-3 korunmuş; ayna disiplini fix'lerde de tam |
| 5 | Open risks | ⚠️ | NEW-001 yazılı-ack bekliyor; NEW-002 doküman-drift; FINDING-005/006 NIT ertelemeleri izlenir |

Legend: ✅ = sorun yok · ⚠️ = yalnız MINOR/concern/NIT · ❌ = MAJOR/BLOKER

---

## Fix Doğrulama Detayı (bağımsız)

### FINDING-001 [MAJOR] → KAPANDI
- **Wire kontratı:** `asyncapi.yaml` `HistoryHeaders` şemasına `X-Cadenzaflow-History-Event-Time` eklendi + `required` listesine kondu (epoch-millis, UTC, string). `npx @asyncapi/cli validate` → *"valid! ... don't have governance issues."* (0 error).
- **Consumer (now() KALKTI):** `HistoryProjectionConsumer.java:152` `requireEventTimeHeader(msg)` → `:175-183` header'dan `Instant.ofEpochMilli(Long.parseLong(raw))`; eksik/bozuk header = WIRE-CONTRACT VIOLATION → mevcut schema-drift → DLQ (sessiz `now()` fallback YOK). `Instant.now()` kaynakta artık yalnız `eventTimeOf` fallback'inde (publish-anında pişer, redelivery-sabit).
- **Audit-kritik yol (relay):** `HistoryOutboxRelay.buildMessage` `:225` `EVENT_TIME = row.eventTime().toEpochMilli()` — `compact_history_outbox.event_time` kolonundan AYNEN taşınır (yeniden hesaplanmaz). Kolon `NOT NULL` (`V1__compact_history_outbox.sql:36`) → relay'de NPE riski yok. Kolonu yazan: `CompactHistoryOutboxWriter.java:111` (`eventTimeOf`) + `:131` (`setTimestamp`), tx-içi.
- **Bulk yol (post-commit) DA set ediyor:** `HistoryPostCommitPublisher.java:41` `eventTime = HistoryEventFieldExtractor.eventTimeOf(historyEvent)` → `:43-45` `WireMessageFactory.build(..., eventTime)`; factory `:57` header'ı set eder. Yani hem audit-kritik hem bulk yolda header HER ZAMAN set (build imzası `eventTime` non-null zorunlu — eksik olsa derleme kırılırdı; BUILD SUCCESS).
- **Dedup ↔ partition tutarlılığı:** Redelivery testi (aşağıda) tek satır (ON CONFLICT tuttu) + `event_time == motor-zamanı` + doğru dated-partition (`operation_log_history_2020_01`, `_default` DEĞİL) — üçünü uçtan uca yeşil kanıtlıyor.
- **Ayna:** camunda↔cadenzaflow için 5/5 değişen dosya `sed engine→ENGINE` normalizasyonu altında BYTE-AYNI.

### FINDING-002 [MINOR] → KAPANDI
`verifyErasure(requestId, confirmedScope, bulkClasses)` iki-katmanlı: (1) mevcut HistoryQueryApi-yüzeyi ACTINST assignee (ACTINST kapsamdaysa), (2) YENİ `firstStillPopulatedColumn` — `bulkClasses`'taki HER sınıfın HER `ANONYMIZATION_COLUMNS` kolonu (+ VARINST'te `variable_value_ref`) için doğrudan-SQL raw-kolon kontrolü. Kolonlar `allowlistedAnonymizationColumns` (allowlist re-validate) + tablo `ProjectionStore.tableNameFor` (sabit map) → **enjeksiyon regresyonu yok**. Kapsam-dışı sınıf denetlenmez (yanlış-pozitif fail önlenir). 3 yeni test (VARINST/COMMENT still-populated → throw+FAILED audit; out-of-scope → PASS) yeşil.

### FINDING-003 [MINOR] → KAPANDI
`PseudonymizationVaultClient` sınıf-Javadoc'u CQ-1 sonrası mimariyi doğru anlatır (persistMapping = ENGINE-side `CompactHistoryOutboxWriter`; projection consumer vault-habersiz; nats-history-projection yalnız delete/reidentify). Ek: gerçek doğruluk hatası düzeltildi — `persistMapping`'in `vault_access_audit` WRITE satırı `accessor_identity` artık `"system:compact-history-outbox-writer"` (eski yanlış `"system:history-projection-consumer"` değil). Vault test 5/5.

### FINDING-004 [MINOR] → KAPANDI (leader-lost emit'i NEW-001 ile etkileşiyor)
8 kod artık kaynakta izlenebilir: 5 gerçek emit (`BUS_PROJECTION_STALE_EVENT_DISCARDED` yorum+mevcut DEBUG; `BUS_PSEUDONYMIZATION_APPLIED` INFO — ham userId/token loglamaz; `BUS_OUTBOX_DUPLICATE_RELAY_DELIVERY` PubAck.isDuplicate INFO; `SYS_OUTBOX_RELAY_LEADER_LOST` transition WARN; `SYS_HISTORY_DLQ_PUBLISH_FAILED` nak) + 1 bench düzeltme (`SYS_BENCH_HISTORY_ENVIRONMENT_UNAVAILABLE`) + 2 traceable bench CODER-NOTE (`BUS_BENCH_BASELINE_MISSING`, `SYS_BENCH_HISTORY_SLI_DRIFT`).
- **`SYS_HISTORY_DLQ_PUBLISH_FAILED` — GERÇEK davranış (uydurma-emit DEĞİL):** `DlqPublishOutcome` gerçekten `FAILED_NO_DLQ_SUBJECT`/`FAILED_BOTH_PUBLISH` döndürür (`DlqPublishOutcome.java`); `HistoryDlqConsumer.routeToDlq` bu outcome'u yayar; **fix-öncesi** `routeToDlqThenAck` KOŞULSUZ ack'liyordu → DLQ publish gerçekten iki-yolda-da fail ederse mesaj SESSİZCE kaybolurdu. Fix (`HistoryProjectionConsumer.java:129-141`) yalnız PUBLISHED_*'ta ack, FAILED_*'ta `nak()`+ERROR → mesaj korunur, redelivery'de DLQ-route tekrar denenir. `HistoryProjectionConsumerDlqRoutingTest` 2/2 (success→ack, fail→nak+never-ack).
- **`SYS_OUTBOX_RELAY_LEADER_LOST` — GERÇEK davranış ama misfire (bkz. NEW-001):** emit gerçek geçişte ateşlenir; ancak `SweepLeaderLease.isLeader()` staleness'i yüzünden geçiş sonrası her döngü tekrar eder. Uydurma-emit değil; sinyal-sadakati kusuru.

---

## Bağımsız Test Çalıştırma Sonuçları (JAVA_HOME=temurin-21, Docker OK, gerçek PG16+NATS2.10 Testcontainers)

| Modül | Test sınıfı | Sonuç | İlgili bulgu |
|---|---|---|---|
| nats-history-projection | HistoryProjectionConsumerTest | ✅ **4/4** (yeni redelivery testi dahil) | FINDING-001 |
| nats-history-projection | ErasurePipelineTest | ✅ **8/8** (3 yeni verify testi dahil) | FINDING-002 |
| nats-history-projection | HistoryProjectionConsumerDlqRoutingTest | ✅ **2/2** (YENİ) | FINDING-004 (DLQ-fail nak) |
| nats-history-projection | ProjectionStoreTest | ✅ 9/9 | merge-upsert/dedup regresyon-yok |
| nats-core | SweepLeaderLeaseTest | ✅ 8/8 | (staleness'i test ETMİYOR — NEW-001) |
| nats-core | PseudonymizationVaultClientTest | ✅ 5/5 | FINDING-003 / CQ-1 |
| camunda-nats-channel | HistoryOutboxRelayTest | ✅ 4/4 (dedup-redelivery dahil) | FINDING-004 (duplicate-relay) |
| camunda-nats-channel | HistoryOutboxRelayLeaderTransitionTest | ✅ 2/2 (mock isLeader — staleness maskeli) | FINDING-004 / NEW-001 |
| camunda-nats-channel | CompactHistoryOutboxWriterTest / …VaultIntegrationTest | ✅ 4/4, 2/2 | FINDING-001 outbox event_time / CQ-1 |
| camunda-nats-channel | HistoryOffloadAuditCriticalFlowIntegrationTest | ✅ 1/1 | e2e audit yol |

**Reactor:** nats-history-projection alt-küme **BUILD SUCCESS (23 test)**; nats-core+camunda alt-küme **BUILD SUCCESS (13+13 test)**. Tüm migration'lar (`V1/V2/V3` + vault + outbox) gerçek `postgres:16`'ya BOŞ DB üstünde `SqlMigrationRunner` ile uygulandı ve yeşil geçti (empirik DDL doğrulaması). Redelivery testi manuel bir dated-partition (`operation_log_history_2020_01`) kurup satırın oraya (m-`_default`'a değil) düştüğünü asserte ederek partition-anchor fix'ini kanıtlar.

---

## Güvenlik Ekseni (fix'lerde regresyon yok)

- **SQL injection:** FINDING-002'nin yeni `firstStillPopulatedColumn` doğrudan-SQL'i kolonları `allowlistedAnonymizationColumns` (allowlist+regex re-validate) + sabit `variable_value_ref` literal'inden, tabloyu `ProjectionStore.tableNameFor` sabit map'inden alır; WHERE `?` bind. Yeni enjeksiyon yüzeyi YOK.
- **Ham-PII log:** Yeni log satırları (`BUS_PSEUDONYMIZATION_APPLIED`, leader-lost, DLQ-fail) yalnız `history_class`/`engine_id`/`tenant_key_version`/`subject`/`outcome` loglar — ham `userId`/token/assignee/değer YAZILMAZ (mesaj metnindeki "userId"/"token" kelimeleri alan-etiketi, değer değil). DP-1 temiz.
- **Kasa CRITICAL + accessor doğruluğu:** `AUTH_PSEUDONYM_VAULT_ACCESS_DENIED` yolu korunur; WRITE audit accessor_identity artık gerçek çağıranı (motor-side writer) yansıtır.

---

## Spot-Check Haritası (≥5 file:line, bağımsız doğrulandı)

1. `HistoryProjectionConsumer.java:152,175-183` — event_time header'dan; `Instant.now()` consume-anı KALKTI; eksik/bozuk → DLQ. ✔ (F-001)
2. `HistoryWireMessageFactory.java:57` (camunda+cadenzaflow) — `EVENT_TIME = eventTime.toEpochMilli()`; imza `eventTime` zorunlu. ✔ (F-001 bulk)
3. `HistoryOutboxRelay.java:225` (camunda+cadenzaflow) — `EVENT_TIME = row.eventTime()`; outbox kolonu `NOT NULL` (`V1__compact_history_outbox.sql:36`). ✔ (F-001 audit)
4. `asyncapi.yaml` `HistoryHeaders`: EVENT_TIME şema + `required` (satır ~215/~245) → `@asyncapi/cli validate` 0-error. ✔ (F-001 kontrat)
5. `ErasurePipeline.java:216-266` — `verifyErasure(+bulkClasses)` → `firstStillPopulatedColumn` allowlist-revalidate direct-SQL; 3 yeni test yeşil. ✔ (F-002)
6. `PseudonymizationVaultClient.java` sınıf-Javadoc + `persistMapping` `accessor_identity="system:compact-history-outbox-writer"`. ✔ (F-003)
7. `SweepLeaderLease.java:44,90,107,110,117,121-122` — `heldRevision` renew-fail'de null'lanmıyor → `isLeader()` bayat. ✘ (NEW-001)
8. `HistoryProjectionConsumer.java:129-141` + `DlqPublishOutcome.java` — FAILED_* outcome'da `nak()` (sessiz-ack DEĞİL); routeToDlq gerçek FAILED döndürür. ✔ (F-004)

---

## Şeffaflık — Ne Kontrol Ettim

- **Okunan fix kodu:** HistoryProjectionConsumer, HistoryWireMessageFactory (2 motor), HistoryOutboxRelay (2 motor), HistoryPostCommitPublisher, HistoryEventFieldExtractor, CompactHistoryOutboxWriter, HistoryHeaders, SweepLeaderLease, ErasurePipeline, PseudonymizationVaultClient, HistoryDlqConsumer, DlqPublishOutcome, ProjectionStore (+ testler: HistoryProjectionConsumerTest redelivery, ErasurePipelineTest, HistoryProjectionConsumerDlqRoutingTest, HistoryOutboxRelayLeaderTransitionTest, SweepLeaderLeaseTest).
- **Empirik:** `@asyncapi/cli validate` (temiz); 4 hedef fix-testi + destekleyici sınıflar gerçek PG16+NATS2.10 Testcontainers'ta yeşil; camunda↔cadenzaflow ayna diff (5/5 byte-aynı, engine-adı hariç); 8 error-kod grep izlenebilirlik; yeni-kod ham-PII/enjeksiyon grep.
- **Yeni-bulgu avı:** SweepLeaderLease staleness (NEW-001 doğrulandı); bulk-yol event-time set (doğrulandı — set ediliyor); asyncapi required-header eski-publisher kırar mı (kırmıyor — tek publisher-çifti, ikisi de header set eder, BUILD SUCCESS); publisher-bypass grep (yalnız 2 yol, ikisi de EVENT_TIME set).

## Dürüstlük — Ne Kontrol ETMEDİM

- **Tam reactor + performans/yük/soak:** yalnız fix-etkili + güvenlik/uyum-kritik altküme koşuldu (görev talimatı). NFR-P* throughput / relay-failover latency ölçülmedi (Phase 5.5). Basamak-1 `ExternalTaskLifecycleBenchTest` @bench önceden-var timeout regresyon SAYILMADI (talimat).
- **Canlı çok-motorlu KV leader-transition:** NEW-001 staleness'i KOD-OKUMASIYLA doğruladı; canlı KV-lease-expiry yarışını gerçek kümede koşmadı (mevcut testler de mock/taze-lease).
- **Regülasyon yorumu:** KVKK/GDPR yasal-madde yorumu DPO alanı; yalnız kod↔DATA_CLASSIFICATION/ADR izlenebilirliği.
- **Fork motor iç davranışı:** `eventTimeOf`'un fork `HistoryEvent` alt-tip zaman-alanı eşlemesi kod-kanıtına dayanılarak kabul edildi; ikili-fork'a karşı yeniden doğrulanmadı.

---

## İnsan İçin Sıradaki Aksiyon

**HAS-CONCERNS-NEEDING-ACK** — Bloklayan MAJOR (FINDING-001) ve tüm MINOR'lar (FINDING-002/003/004) bağımsız kanıtla **KAPANDI**; faz-6'ya geçiş önünde **veri/denetim-bütünlüğü bloğu yok**. Tek 🟡 concern **NEW-001** (SweepLeaderLease `heldRevision` staleness → SYS_OUTBOX_RELAY_LEADER_LOST WARN tekrarı): ya küçük takip-fix (`tryAcquireOrRenew` renew-fail dallarında `heldRevision=null` + gerçek acquire→lose testi), ya da mimar/PO'dan **yazılı-gerekçeli kabul** (WARN-seviye, veri etkisi yok, yazar zaten latent işaretledi). NIT'ler (NEW-002 doküman-drift, NEW-003/004 gözlem, FINDING-005/006) izlenir. Karar sonrası faz-6 açılabilir; kod tekrar değişirse `/sentinel:phase-review 5` yeniden koştur.

---

## Bulgu Kapanış Kaydı (2026-07-21, re-review sonrası)

**Verdict yörüngesi:** 1. koşu HAS-BLOCKERS (1 MAJOR) → fix turu → 2. koşu HAS-CONCERNS-NEEDING-ACK (0 MAJOR, 1 MINOR + 3 NIT) → NEW-001/002 fix → **tüm bulgular kapandı.**

| Bulgu | Kapanış |
|---|---|
| FINDING-001 🟠 MAJOR | **KAPANDI** (re-review'da bağımsız doğrulandı) — motor event_time tele taşınıyor (`X-Cadenzaflow-History-Event-Time`, asyncapi required + validate-temiz), `Instant.now()` kalktı, her iki yayın yolu set eder, redelivery e2e idempotent + doğru dated-partition. Commit `f8f3b57`. |
| FINDING-002 🟡 MINOR | **KAPANDI** — erasure-verify birincil PII yüzeylerini (VARINST/DETAIL/TASKINST-name/COMMENT) allowlist-revalidate direct-SQL ile denetler. Commit `5ca7987`. |
| FINDING-003 🟡 MINOR | **KAPANDI** — vault Javadoc engine-side'a düzeltildi + maskelenen gerçek bug (`vault_access_audit` accessor_identity) giderildi. Commit `f7724b4`. |
| FINDING-004 🟡 MINOR | **KAPANDI** — 8 kod izlenebilir; `SYS_HISTORY_DLQ_PUBLISH_FAILED` gerçek silent-loss fix'i. Commit `a2869da`. |
| NEW-001 🟡 MINOR | **KAPANDI** — `SweepLeaderLease.heldRevision` her acquire/renew-fail'de null'lanır → `isLeader()` gerçek durumu yansıtır, leader-lost WARN artık yalnız geçişte; mock-maskeli transition testi gerçek-lease/mock-NATS-I/O'ya çevrildi. Basamak-1 sweep regresyonu YOK (SweepLeaderLeaseTest 12/12, A2OrphanSweepTest 7/7 iki motor). Commit `52de8d8`. |
| NEW-002 🟢 NIT | **KAPANDI** — outbox `event_time` DDL yorumu düzeltildi (append-log için load-bearing: dedup-key + partition-anchor; entity-lifecycle merge-upsert için stream_sequence tie-break authority kalır). 3 dosya senkron. Commit `3981ae7`. |
| NEW-003 🟢 NIT | **AÇIK/İZLENİR** — `eventTimeOf` `Instant.now()` fallback yalnız null-timestamp kenar durumu; redelivery-idempotency'yi bozmaz (F-001 sağlam). |
| NEW-004 🟢 NIT | **AÇIK/İZLENİR** — `routeToDlq` metriği FAILED'de "routed" sayar (pre-existing, trivial). |
| FINDING-005/006 🟢 NIT | **AÇIK/İZLENİR** — DETAIL byte-payload düşürme + attachment_name/url anonim-dışı (kodda belgeli meşru-erteleme). |

**Tam reactor son durum (JAVA_HOME=temurin-21):** 564 test, 0 failure/0 error (nats-core 108, camunda 148, cadenzaflow 156, flowable 59, projection 88, bench 5). Güvenlik ekseni temiz (SQL-inj kapalı, PII-log yok, kasa CRITICAL/L4). Ayna mekanik-diff.

**Sonuç:** 1 MAJOR + 4 MINOR düzeltme yoluyla kapatıldı; 4 NIT belgeli/izlenir erteleme. Faz-5 kapısı için tek bekleyen: Levent'in phase-5.5 onayı.
