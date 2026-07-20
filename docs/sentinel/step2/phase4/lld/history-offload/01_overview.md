# 01 — Genel Bakış (Module Overview)

**Kapsam:** Basamak-2 "History Offload" — `ACT_HI_*` yazımını NATS/JetStream üstünden async projeksiyona taşıyan tüm bileşenler (EPIC-A…G).
**Girdi:** `docs/sentinel/step2/phase3/HLD.md` (§2 mimari, §3 bileşenler, §9 izlenebilirlik, §11 doğrulama kapanışı, §12 ARCH-Q1…5), `api/asyncapi.yaml` + `api/openapi.yaml`, `docs/sentinel/step2/phase2/*` (31 BR, 42 kod), `docs/sentinel/step2/phase1/*` (26 FR/30 NFR/7 IR, DP-1…16), 11 ADR (0009…0019, hepsi Kabul).
**Rol:** Developer (Phase 4 — LLD). HLD onaylı, ADR'ler kilitli; bu belge onları **değiştirmez**, somutlaştırır.
**Durum:** LLD-Q1…5 KARARA BAĞLANDI (2026-07-20, 5/5 önerilen seçenek — aşağıda karar kaydı) — phase-review bekliyor.

---

## Neden bu belge var

HLD §3, basamak-2'nin 21 bileşenini (§9 izlenebilirlik tablosu) EPIC-A…G altında tanımladı. Bu LLD onları **sınıf/metot-imzası/config-alan/DB-tablo** düzeyinde somutlaştırır. Basamak-1'den farklı olarak basamak-2 (a) **motor-içi** (engine node'a gömülü: handler+outbox+relay) VE (b) **motor-dışı** (ayrı deploy edilebilir: projeksiyon servisi — consumer+query-API+retention+erasure+kasa) iki farklı çalışma-zamanı yüzeyi açar — HLD §2 mimari diyagramındaki `EngineNode`/`Proj`/`Ops` alt-grafikleriyle birebir.

**Bu belge LLD'dir, kod değildir.** Sınıf listeleri + public metot imzaları + sorumluluk düzeyinde durur (görev talimatı: yüksek-ripple kontratlara odaklan, iç gövde pseudo-kodu YAZMA — LLD_GUIDELINE §2.3'ün "detay" beklentisi burada **kontrat detayı** olarak yorumlanır, algoritma detayı Phase 5'tir).

---

## Kapsanan bileşenler (HLD §3 → bu modülün dosyaları)

| HLD §3 bileşeni | Bu modüldeki dosya | Maven modülü / deploy birimi |
|---|---|---|
| NatsHistoryEventHandler (§3.1.1) | `03_classes/1_handler_outbox.md` §1 | `camunda-nats-channel` + `cadenzaflow-nats-channel` (ayna) |
| CompactHistoryOutbox (§3.1.2) | `03_classes/1_handler_outbox.md` §2 | aynı (yazar) |
| HistoryPostCommitPublisher (§3.1.3) | `03_classes/1_handler_outbox.md` §3 | aynı |
| History publish şeması (§3.1.4) | `04_interfaces/1_wire_contract_refs.md` | `nats-core` (paylaşılan) |
| HistoryOutboxRelay (§3.2.1) | `03_classes/2_relay_projection.md` §1 | `camunda-nats-channel` + ayna (engine node'a gömülü, leader-elected) |
| HistoryProjectionConsumer (§3.2.2) | `03_classes/2_relay_projection.md` §2 | **`nats-history-projection`** (yeni modül, motor-dışı) |
| ProjectionStore (§3.2.3) | `03_classes/2_relay_projection.md` §3 | aynı |
| History wire-contract (§3.2.4) | `04_interfaces/1_wire_contract_refs.md` | `api/asyncapi.yaml` (köprü, tekrar YOK) |
| HistoryDlq (§3.2.5) | `03_classes/2_relay_projection.md` §4 | `nats-core` (`DlqPublisher` yeniden kullanım) + `nats-history-projection` |
| HistoryQueryApi (§3.3.1) | `03_classes/3_query_api.md` §1 | `nats-history-projection` (gömülebilir + opsiyonel standalone, ARCH-Q4) |
| Cockpit-körleşme dok. (§3.3.2) | `03_classes/3_query_api.md` §2 | dokümantasyon, kod değil |
| ReconciliationJob (§3.4.1) | `03_classes/4_cutover_reconciliation.md` §1 | `nats-history-projection` |
| CutoverControlPlane (§3.4.2) | `03_classes/4_cutover_reconciliation.md` §2 | `nats-history-projection` (yazar) + engine node (okuyucu, bkz. LLD-Q3) |
| CutoverRollback (§3.4.3) | `03_classes/4_cutover_reconciliation.md` §3 | `nats-history-projection` |
| NormalizeWriteMetric + bench (§3.5.1) | `03_classes/5_bench.md` §1 | `nats-bpm-bench` (basamak-1 modülü genişler) |
| Destekleyici SLI'lar (§3.5.2) | `10_metrics.md` | `nats-core` + `nats-history-projection` |
| Bench İLK KOŞU / stream provisioning (§3.5.3/4) | `03_classes/5_bench.md` §2 | `nats-bpm-bench` |
| RetentionEnforcementJob (§3.6.1) | `03_classes/6_governance.md` §1 | `nats-history-projection` |
| ErasurePipeline (§3.6.2) | `03_classes/6_governance.md` §2 | `nats-history-projection` |
| PseudonymizationVault (§3.6.3) | `03_classes/6_governance.md` §3 | **`nats-history-projection`** (vault CLIENT) — vault DB ayrı, kod aynı modülde (küçük yüzey) |

---

## Okuma sırası (önerilen)

1. `02_package_structure.md` — hangi sınıf hangi Maven modülüne gider (basamak-1 ADR-0007 desenine uygun + yeni `nats-history-projection` modülü).
2. `03_classes/1_handler_outbox.md` — EPIC-A (handler + hibrit yol).
3. `03_classes/2_relay_projection.md` — EPIC-B (relay + projeksiyon consumer + DLQ).
4. `03_classes/3_query_api.md` — EPIC-C (sorgu-API + Cockpit-körleşme).
5. `03_classes/4_cutover_reconciliation.md` — EPIC-D (reconciliation + cutover + rollback).
6. `03_classes/5_bench.md` — EPIC-E/F (metrik/bench genişlemesi).
7. `03_classes/6_governance.md` — EPIC-G (retention/erasure/pseudonymization).
8. `04_interfaces/`, `05_sequences.md`, `06_state_machines.md`, `07_errors.md`, `08_config.md`, `09_security/`, `10_metrics.md`, `99_deployment.md`.

DB şeması/erişim haritası bu LLD-modülünün DIŞINDA, `docs/sentinel/step2/phase4/DB_SCHEMA.md` + `DB_ACCESS_MAP.md`'dedir (migration'lar `db/migrations/`); sequence diyagramları ve tam error registry de aynı şekilde ayrı Phase 4 teslimatlarıdır (`SEQUENCE_DIAGRAMS.md`, `ERROR_REGISTRY.md`) — `05_sequences.md`/`07_errors.md` bunlara **köprü**dür, içerik tekrarlamaz (MASTER_WORKFLOW §0.6).

---

## Kapsam dışı / reddedilen (bu LLD yeniden AÇMAZ)

D-A…G, PO-Q1…7, BA-Q1…8, ARCH-Q1…5 (38 karar, kilitli — HLD §12 kapanış kaydı). Ayrıca: Flowable history offload (basamak-2b, D-G), token-move/completion tx kaldırılması (basamak-6), büyük değişken externalization (basamak-3), DB sharding (basamak-5), sorgu-API agregasyon/analitik (PO-Q3), üç-motor-birlikte (D-G).

---

## Sürüm / kod tabanı referansları

- Fork kanıtları: `~/Workspaces/cadenzaflow/cadenzaflow-bpm-platform/engine` (bu LLD'de her hot-reconfigure/partition/config iddiası bu ağaçtan bizzat okunarak doğrulandı — bkz. §"Phase3'ün devrettiği doğrulamalar" ve `03_classes/1_handler_outbox.md` §1.4).
- Mevcut repo kodu: `nats-core`, `camunda-nats-channel`, `cadenzaflow-nats-channel`, `nats-bpm-bench` (basamak-1 modülleri, bu basamakta genişler); **`nats-history-projection`** basamak-2'nin tek yeni Maven modülü.
- Build: Java 21 gerektirir (basamak-1 memory `build-requires-java21` aynen geçerli).
- DB migration-proof: `docs/sentinel/step2/phase4/DB_SCHEMA.md §5` (Docker v29.6.0, `postgres:16`, üç hedef bizzat uygulandı ve fonksiyonel doğrulandı).

---

## Phase3'ün devrettiği doğrulamalar — kapanış kaydı (görev talimatı §"Phase3'ün devrettiği doğrulamalar")

### 1. Hot-reconfigure desteği (ARCH-Q5 açık kapısı) — **KAPANDI (fork-kanıtlı), mekanizma tasarlandı**

**Fork kanıtı (bu fazda bizzat okundu):**
- `ProcessEngineConfigurationImpl.java:1134-1141` — `init()` (dolayısıyla `initHistoryEventHandler()`, `:2788-2796`) yalnız `buildProcessEngine()` (`:1124-1130`) içinde, **tek sefer** çağrılır.
- `initHistoryEventHandler()` (`:2788-2796`): `if (historyEventHandler == null)` guard'ı — `enableDefaultDbHistoryEventHandler`'ı post-boot değiştirmenin (`setEnableDefaultDbHistoryEventHandler(...)`) **hiçbir etkisi yoktur** (composite zaten kurulmuş). Bu, fork'un HAM `enableDefaultDbHistoryEventHandler` bayrağının **motor-genelinde, tek-seferlik boot-zamanı** bir karar olduğunu kanıtlar — sınıf-BAŞINA değil (`ProcessEngineConfigurationImpl.java:768` alan tanımı: `protected boolean enableDefaultDbHistoryEventHandler`, tek boolean).
- `CompositeDbHistoryEventHandler.java:70-72` (`addDefaultDbHistoryEventHandler()`) `DbHistoryEventHandler`'ı **koşulsuz** delege listesine ekler — sınıf-bazlı filtre YOK; `DbHistoryEventHandler.java:39` public no-arg constructor'a sahiptir (bizzat instantiate edilebilir).
- **Kritik ters-kanıt:** `HistoryEventProcessor.java:74-75` (`processHistoryEvents`) her event'te `Context.getProcessEngineConfiguration().getHistoryEventHandler()`'ı **CANLI okur** (cache YOK) — `historyEventHandler` alanı runtime'da swap edilirse bir sonraki event bunu ANINDA görür.

**Sonuç (tasarım kararı, ARCH-Q5'i DEĞİŞTİRMEZ):** Ham fork mekanizması (`enableDefaultDbHistoryEventHandler`) sınıf-başına granülerlik SAĞLAMAZ ve tek-seferlik boot kararıdır → bu ekseni kullansaydık rolling-restart ZORUNLU olurdu (ARCH-Q5'in "rolling-restart" seçimini bu ekseninde DOĞRULAR). Ancak basamak-2 bu ham mekanizmaya HİÇ dayanmaz: `NatsHistoryEventHandler` (kendi composite'imiz) `customHistoryEventHandlers`'a EKLENİR ve `enableDefaultDbHistoryEventHandler` **DAİMA `false`** sabitlenir (fork'un otomatik `DbHistoryEventHandler` eklemesi hiç tetiklenmez); `NatsHistoryEventHandler` kendi İÇİNDE bir `DbHistoryEventHandler` örneği TUTAR ve her `handleEvent` çağrısında sınıf-bazlı bir routing tablosuna (`ClassCutoverStateRegistry`, `08_config.md` §2) bakarak bu iç örneğe delege edip etmeyeceğine karar verir. `HistoryEventProcessor`'ın canlı-okuma davranışı sayesinde bu routing tablosu teorik olarak **restart'sız** güncellenebilir — ama ARCH-Q5 KİLİTLİ kararı gereği v1'de bu tablo yalnız **bootstrap'ta bir kez** okunur (KV'den, LLD-Q3), restart tetikleyicisi = config-flip uygulama mekanizması. **İleri-uyumluluk:** routing tablosunu "boot'ta oku"dan "canlı izle (KV watch)"e geçirmek, fork'a HİÇBİR dokunuş gerektirmez — yalnız bizim `NatsHistoryEventHandler` içi bir değişikliktir (gelecekteki bir basamak/iyileştirme, ARCH-Q5'in bıraktığı kapı). Ayrıntı: `03_classes/1_handler_outbox.md` §1.4, `03_classes/4_cutover_reconciliation.md` §2.

### 2. Partition sayısı + rebalance (ARCH-Q3 detayı) — **KAPANDI**

**N (partition sayısı) default = 8** (2'nin kuvveti, config `history.projection.partitionCount`, kiracı override). Gerekçe: NFR-P5 yatay-ölçek hedefine somut bir başlangıç tavanı verir (8 consumer instance'a kadar lineer ölçek), aşırı-parçalanmayı önler (JetStream stream/consumer sayısı operasyonel yüktür). Mekanizma: `HISTORY` stream'i `SubjectTransform` ile `history.<engineId>.<class>.<processInstanceId>` subject'ini yayın-ANINDA `history.<engineId>.<class>.<processInstanceId>.part.{{Partition(8,3)}}` biçimine dönüştürür (`processInstanceId` = 3. wildcard token'dan hash — [NATS docs Subject Mapping and Partitioning]); `N` adet durable, `filter_subject: history.>.part.<i>` ile bağlı consumer, her biri `HistoryProjectionConsumer`'ın bir instance'ı/thread'i. **Consumer-partition eşlemesi:** statik, `i = replicaOrdinal % N` (K8s StatefulSet ordinal veya config-verilen `partitionAssignment` listesi) — basamak-1'in queue-group yaklaşımından FARKLI, deterministik atama (partition'ın SIRA garantisi queue-group'ta yoktur). **N değişiminin etkisi:** `processInstanceId`'nin hash'i N'e göre değiştiğinden, N değişimi AYNI instance'ın gelecekteki event'lerini FARKLI bir partition'a yönlendirir (geçmiş mesajlar eski partition'da kalır) — bu, "aynı instance hep aynı partition" (D-E per-instance sıra şartı) invariant'ını N-değişim ANINDA GEÇİCİ olarak ihlal edebilir. **Rebalance = bakım-penceresi prosedürü** (canlı/otomatik DEĞİL): (1) yeni publish'leri durdur/pause (veya kısa bir donma penceresi kabul et), (2) tüm consumer'ların stream'i N_eski partition sınırına kadar tükettiğini doğrula (consumer lag=0), (3) stream `SubjectTransform`'unu yeni N'e güncelle, (4) `N_yeni` consumer'ı yeni `filter_subject`'lerle yeniden-provision et, (5) publish'i devam ettir. `99_deployment.md` §3'te runbook.

### 3. Somut RTO/RPO sayıları (phase3-review F-004) — **KAPANDI**

- **Relay RTO (audit-kritik yol):** `history-relay-leader` KV lease TTL = `2·relayCyclePeriod` (basamak-1 `SweepLeaderLease` deseni birebir), `relayCyclePeriod` default **30s** → **TTL=60s**. Lider kaybı → devir penceresi **≤60s**; bu pencerede outbox satırları relay'siz kalır ama **kaybolmaz** (durable, PubAck-öncesi-delete YASAK) → **RPO=0** audit-kritik için (kayıp yok, yalnız gecikme). Bkz. `08_config.md` §3.
- **Relay RPO (bulk yol):** post-commit publish penceresinde çökme = kalıcı kayıp, D-A'nın **bilinçli kabulü** — RPO burada "0 değil, reconciliation'da tespit edilir" (mimari-kabul-edilen kayıp, restore edilecek bir "nokta" yok).
- **Projeksiyon PG RTO/RPO:** **kiracı-owned zarf** (NFR-R8, gömülebilir-kütüphane duruşu) — bu LLD'nin somut karşılığı: `99_deployment.md` §4 bir runbook-şablonu sağlar (önerilen: RPO ≤15dk via `pg_basebackup`/WAL-archiving sıklığı, RTO ≤1h via standby/restore) + `TENANT_PII_CHECKLIST_TEMPLATE.md`'ye (basamak-2 genişletmesi, phase1) bu iki alanın **doldurulması zorunlu** kiracı-parametresi olduğu notu düşülür. Store kesintisi audit KAYBI üretmez (outbox/relay dayanıklılığı sayesinde) — yalnız projeksiyon-lag büyür (NFR-P3 SLI).

### 4. BA-Q7 çarpan başlangıç değeri — **KAPANDI**

**Default = 5×** relay-döngü-gecikmesi (`relayCyclePeriod=30s` → `SYS_OUTBOX_ROW_STUCK` eşiği = **150s**), config `history.outbox.stuckThresholdMultiplier` (basamak-1 `08_config.md` §4 "ADR-0004 eşikleri sabit, konfigüre edilmez" deseninden BİLİNÇLİ FARK — burada tenant/bench kalibrasyonuna AÇIK bırakılır, PO-Q4 "kalibre edilebilir başlangıç" deseni). Kalibrasyon prosedürü: `TEST_SPECIFICATIONS.md` (e).

### 5. Relay failover ölçüm tasarımı — **KAPANDI (tasarım); ölçüm phase5.5**

Bench senaryosu (`RelayFailoverBenchScenario`) tasarlandı — `TEST_SPECIFICATIONS.md` (f). Gerçek ölçüm (leader-kill → devir-süresi) Testcontainers + gerçek NATS cluster gerektirir, phase5.5 kapsamına bırakıldı (görev talimatının izin verdiği açık devir).

---

## LLD-QUESTIONS — Karar Kaydı (KARARA BAĞLANDI 2026-07-20)

> **Levent 2026-07-20'de tek tek karar verdi — 5/5 önerilen seçenek KABUL:** Q1=companion satır NFR-P2 metriğine dahil değil (bench'te ayrı raporlanır); Q2=N=8; Q3=NATS KV `history-cutover-state` + boot-read; Q4=`class_cutover_state` projeksiyon DB'de; Q5=çarpan 5×. Tasarım karar metniyle birebir — değişiklik gerekmedi.

| # | Soru | Seçenekler | ÖNERİ (gerekçeli) |
|---|---|---|---|
| **LLD-Q1** | `compact_history_outbox`'ta büyük byte-array payload varsa (`payload_large_ref` dolu, `compact_history_outbox_payload`'a 2. bir INSERT) — bu NFR-P2'nin "≤1 kompakt outbox satırı/tx" hedefini İHLAL eder mi? | (a) Companion-satır metriğe DAHİL değildir (yalnız `compact_history_outbox` sayılır — nadir/opsiyonel bir yardımcı yazı); (b) audit-kritik bütçesi büyük-payload'lı event'lerde ≤2 satır/tx'e genişler | **(a).** `compact_history_outbox` METADATA/kimlik satırıdır ve NFR-P2'nin ölçtüğü budur; companion yalnız EXT_TASK_LOG.errorDetails gibi NADİR büyük-payload durumlarında yazılır (OP_LOG/INCIDENT'te genelde gerekmez) — `pg_stat_statements` fingerprint'i zaten ayrı bir queryid alır, bench raporunda AYRI satır olarak görünür (D-F ölçüm şeffaflığı korunur), sert kapıyı YANLIŞLIKLA kırmaz. |
| **LLD-Q2** | Projeksiyon partition sayısı N'in default değeri? | (a) N=4 (muhafazakâr, az operasyonel yük); (b) N=8 (orta); (c) N=16 (yüksek-ölçek varsayımı) | **(b) N=8** — `01_overview.md` "Phase3'ün devrettiği doğrulamalar #2" gerekçesi; kiracı override edebilir (`08_config.md` §5). |
| **LLD-Q3** | Cutover config-flip'in engine node'a ULAŞTIRILMA mekanizması? | (a) Engine node projeksiyon DB'sini DOĞRUDAN okur (`class_cutover_state`); (b) yeni NATS KV bucket (`history-cutover-state`), engine node yalnız BOOT'ta okur; (c) yalnız `application.yml` (operatör elle günceller, KV YOK) | **(b).** (a) bounded-context ihlali yaratır (engine node projeksiyon DB'sinin tüketicisi DEĞİL, `DATA_OWNERSHIP.yaml`'da böyle listelenmedi) — REDDEDİLİR. (c) basamak-1'in "koordinasyon NATS KV'de, DB'de değil" tezini terk eder VE gelecekteki hot-reconfigure'a kapıyı kapatır. (b) her ikisini de korur: KV = koordinasyon (basamak-1 tez), boot-only-read = ARCH-Q5 rolling-restart kararına UYUMLU, ileride "watch"a çevrilebilir (kod değişikliği yalnız bizim tarafımızda, fork'a dokunmaz). |
| **LLD-Q4** | `class_cutover_state` hangi veritabanında yaşamalı? | (a) Projeksiyon DB (`history-projection-service` bounded-context); (b) Engine DB | **(a).** `ReconciliationJob`/`CutoverControlPlane` zaten `nats-history-projection` modülünde çalışıyor (motor-dışı); engine DB'ye yeni bir okuma/yazma bağımlılığı eklemek DP-9/DATA_OWNERSHIP tek-sahiplik ilkesini ihlal eder. Engine node yalnız LLD-Q3'ün KV yansımasını okur — DB'yi hiç görmez. |
| **LLD-Q5** | BA-Q7 outbox-stuck çarpanının somut default'u? | (a) 3×; (b) 5×; (c) 10× | **(b) 5×** — ADR-0015 metninin kendi örneği ("örn. 5×") ile birebir; ilk bench koşusuyla kalibre edilecek başlangıç (PO-Q4 deseni). |

**Not:** LLD-Q1…5'in hiçbiri D-A…G/PO-Q1…7/BA-Q1…8/ARCH-Q1…5'i yeniden AÇMAZ — hepsi o kararların İÇİNDEKİ, LLD-düzeyinde somutlaştırma gerektiren parametre boşluklarıdır (basamak-1'in LLD-Q1…3 desenine paralel). Basamak-2 kilitli karar seti bu kayıtla **32** olur: D-A…G (7) + PO-Q1…7 (7) + BA-Q1…8 (8) + ARCH-Q1…5 (5) + LLD-Q1…5 (5).
