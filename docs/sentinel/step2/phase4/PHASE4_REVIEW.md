# Phase 4 Review Raporu — Basamak-2: History Offload

**İncelenen faz:** 4 (LLD — Developer)
**İnceleme tarihi:** 2026-07-20
**İnceleyen:** sentinel-phase-review (taze bağlam, Opus — adversarial)
**Kapsam (bizzat okunan dosyalar):** `lld/history-offload/` (19 dosya, manifest.md dahil), `DB_SCHEMA.md`, `DB_ACCESS_MAP.md`, `ERROR_REGISTRY.md`, `SEQUENCE_DIAGRAMS.md`, `TEST_SPECIFICATIONS.md`, `db/migrations/*` (5 SQL); upstream: phase1 (SRS/DATA_CLASSIFICATION/manifest), phase2 (BUSINESS_LOGIC/EXCEPTION_CODES), phase3 (HLD §3/§9/§12, ADR 0009–0019, api/*.yaml, DATA_OWNERSHIP.yaml); fork `~/Workspaces/cadenzaflow/cadenzaflow-bpm-platform/engine`.
**Manifest durumu (BESPOKE):** guidelines enabled:7 disabled:2 · compliance enabled:2 (KVKK, GDPR) disabled:2 (PCI-DSS, HIPAA) · stack enabled:3 (NATS_JETSTREAM, POSTGRES, JAVA) disabled:2 (KAFKA, CLICKHOUSE) · system_tier: carrier-grade-backend · spot_check_minimum:5

> **Loader notu:** Standart `load_phase_context.sh` bu projede exit 3 verir — bu, manifest'in `layout_deviation` bölümünde **bilinçle belgelenmiş BESPOKE sapmadır** (docs/sentinel/step2/phase1/ yerleşimi, guideline adları plugin dosyalarına çözülmez). Manifest doğrudan okundu, disiplinler elle denetlendi. Bu bir FINDING-000 DEĞİLDİR (tasarım gereği). phase-3.5/RUNTIME_CONTRACT yokluğu da BESPOKE desendir (basamak-1 devralınır) — eksiklik sayılmadı.

---

## Verdict

**HAS-CONCERNS-NEEDING-ACK**

Basamak-2 LLD teslimatları **olağanüstü yüksek kalitede**: migration-proof bağımsız olarak tekrarlandı (5/5 migration `postgres:16.14` üzerinde hatasız uygulandı; 3-adımlı merge-upsert, partition-DROP retention, pgcrypto kasa round-trip + hard-delete, CHECK-kısıtı ve legal_hold default'ları FONKSİYONEL olarak doğrulandı), 8 fork file:line iddiasının tümü kaynak ağaçta teyit edildi, manifest disiplini temiz (sıfır disabled-guideline sızması), 42/42 hata kodu izlenebilir, tüm çapraz-referanslar çözülüyor, kilitli 32 karara (D-A…G/PO/BA/ARCH/LLD-Q) sadık. **Bloklayıcı (🔴) bulgu YOK.** İki MINOR (🟡) tutarlılık/izlenebilirlik bulgusu yazılı kabul (veya phase5-paralel düzeltme) gerektirir; ikisi de mekanik ve tasarımı değiştirmez.

Manifest `phase_gate` kuralı gereği: KOŞULLU ONAY'da bu MINOR'lar phase5 ile paralel kapatılabilir.

---

## Bulgular

### 🔴 Kritik (bloklayıcı)

_(yok — bloklayıcı sorun bulunmadı)_

### 🟡 Concern (açık yazılı kabul gerektirir)

**FINDING-001** — [kategori: Consistency]
- **Ne:** Basamak-2'nin en kritik entegrasyon dikişi olan `NatsHistoryEventHandler`, `camunda-nats-channel` modülünde (paket `com.threeai.nats.camunda.history`) tanımlanmış ama YANLIŞ motor paketinin arayüzünü implemente ediyor: `implements org.cadenzaflow.bpm.engine.impl.history.handler.HistoryEventHandler`.
- **Nerede:** `lld/history-offload/03_classes/1_handler_outbox.md` satır 18.
- **Kanıt:** Mevcut `camunda-nats-channel` modülü doğrulanarak `org.camunda.bpm.*` hedefler (`grep`: 12× `import org.camunda.bpm.engine`, `pom.xml` bağımlılığı `org.camunda.bpm:camunda-engine`); `cadenzaflow-nats-channel` ise `org.cadenzaflow.bpm.*` kullanır. Bu, LLD'nin KENDİ ayna-kuralıyla çelişir (`02_package_structure.md` §2 satır 48: "cadenzaflow-nats-channel … yalnız `org.cadenzaflow.bpm.*` importları — ADR-0007"). Camunda modülü için doğru FQN `org.camunda.bpm.engine.impl.history.handler.HistoryEventHandler` olmalı; `org.cadenzaflow` karşılığı ayna (`cadenzaflow-nats-channel`) sınıfına aittir. (Kaynak: tüm fork-kanıtı cadenzaflow ağacından okunduğu için örnek snippet'e cadenzaflow FQN'i sızmış görünüyor.)
- **Neden önemli:** Bu snippet'ten kod yazan bir geliştirici (soğuk okuyucu) motorun SPI dikişinde yanlış paketi hedefler; camunda modülü org.cadenzaflow.* arayüzünü göremez (derlenmez). Tasarım niyeti belirsiz DEĞİL (modül yerleşimi + ayna-kuralı açık), ama illüstrasyon hatası düzeltilmeli.
- **Önerilen çözüm:** `03_classes/1_handler_outbox.md` snippet'inde camunda örneği için FQN'i `org.camunda.bpm.engine.impl.history.handler.HistoryEventHandler`'a çevir; ayna (cadenzaflow) için `org.cadenzaflow.*` olduğunu bir cümleyle belirt. Phase 5 uygulaması öncesi veya paralel.

**FINDING-002** — [kategori: Alignment]
- **Ne:** Bileşen sayım/kapsama-tablosu tutarsızlığı: `01_overview.md` HLD §3'ün "21 bileşen" tanımladığını yazar ve "Kapsanan bileşenler" tablosu 21 bileşeni haritalar; ancak HLD §9 yetkili izlenebilirlik tablosu **22 bileşen** listeler.
- **Nerede:** `lld/history-offload/01_overview.md` satır 12 + kapsama tablosu (satır 20–41); karşı: `phase3/HLD.md §9` (satır 282+).
- **Kanıt:** HLD §9 bileşen satırları sayarak: 22 (NatsHistoryEventHandler … PseudonymizationVault, "Devreden borç triyajı" §3.5.5 DAHİL). Overview kapsama tablosu §3.5.3/4'ü tek satırda birleştirir VE §3.5.5 "Devreden borç triyajı" için AYRI eşleme satırı içermez. Bileşenin kendisi `03_classes/5_bench.md §4`'te (US-F3/BR-DBT-003, dokümantasyon) SUBSTANTİF olarak KARŞILANIR — yani HLD'de olup LLD'de karşılıksız bir bileşen YOKTUR; yalnız overview'ın kendi kapsama-indeksi ve öz-sayımı (21) bir eksiktir.
- **Neden önemli:** Overview'ı kapsama-indeksi olarak kullanan okuyucu §3.5.5'in karşılandığını göremez; öz-sayım HLD §9'un yetkili 22'siyle uyuşmaz.
- **Önerilen çözüm:** Overview kapsama tablosuna §3.5.5 için `03_classes/5_bench.md §4`'e işaret eden bir satır ekle; "21 bileşen" → "22 bileşen (§3.5.3/4 tek dosya, §3.5.5 dokümantasyon)" olarak düzelt.

### 🟢 Observation (bilgilendirici)

**FINDING-003** — [kategori: Consistency]
- **Ne:** `ERROR_REGISTRY.md §4` "Bulgu" notu (satır 161) `EXCEPTION_CODES.md §12`'nin `RES_=3` yazdığını ("1-birimlik drift") iddia eder; ancak güncel `EXCEPTION_CODES.md §12` **`RES_=4`** yazar (toplam 7+15+4+14+2=42) — drift kaynakta ZATEN düzeltilmiş.
- **Nerede:** `ERROR_REGISTRY.md §4` (satır 161) vs `phase2/EXCEPTION_CODES.md §12` (satır 136–150).
- **Kanıt:** §12 tablosu satırı `| RES_ | 4 |` + toplam metni "42". ERROR_REGISTRY'nin KENDİ sayıları doğru (RES_=4, toplam 42); yalnız kaynağa dair meta-yorum bayat. Her iki belge de artık RES_=4 / 42 üzerinde hemfikir — kozmetik.
- **Önerilen çözüm:** İsteğe bağlı — ERROR_REGISTRY §4 notunu "§12 zaten RES_=4'e düzeltildi" olacak şekilde güncelle (bloklamaz).

**FINDING-004** — [kategori: Alignment (kanıt-precision)]
- **Ne:** İki minör fork-atıf imprecision'ı: (a) `01_overview.md` satır 81 `enableDefaultDbHistoryEventHandler` alanı için `ProcessEngineConfigurationImpl.java:768` der, alan gerçekte satır **769**'dadır (768 = kapanış javadoc `*/`); (b) aynı paragraf `DbHistoryEventHandler.java:39`'u "public no-arg constructor" olarak anar, satır 39 sınıf-bildirimidir — constructor implicit default'tur (`CompositeDbHistoryEventHandler.java:71`'deki `new DbHistoryEventHandler()` ile teyitli).
- **Kanıt:** `grep -n "boolean enableDefaultDbHistoryEventHandler"` → 769. Her iki durumda da ÖZ doğrudur; atıflar ≤1 satır kayık / çıkarım-satırına işaret eder.
- **Önerilen çözüm:** İsteğe bağlı — `:768`→`:769`, ve "(implicit no-arg ctor; `new DbHistoryEventHandler()` ile örneklenir)" notu.

---

## Kategori Scorecard

| # | Kategori | Durum | Kısa not |
|---|---|---|---|
| 1 | Completeness (Tamlık) | ✅ | Tüm zorunlu Phase 4 teslimatları mevcut/dolu; modüler LLD (18/18 içerik dosyası indekste), 5 migration, 42-kod registry, 8 sequence, 6 test-spec + 2 prosedür tam. |
| 2 | Alignment (İzlenebilirlik) | ⚠️ | Her sınıf → HLD §3/§9 + BR/FR/US + ADR izleniyor; ADR 0009–0019 mevcut. FINDING-002: overview öz-sayımı 21 vs HLD §9'un 22'si (§3.5.5 kapsanmış ama tabloda yok). |
| 3 | Internal consistency | ⚠️ | Migration↔DB_SCHEMA↔DB_ACCESS_MAP↔DATA_OWNERSHIP hizalı; sequence sınıf-adları 15/15 eşleşir. FINDING-001 (camunda modülünde org.cadenzaflow FQN) + FINDING-003 (bayat RES_ notu). |
| 4 | Manifest discipline | ✅ | Sıfır disabled-guideline sızması (PCI-DSS/HIPAA/KAFKA/CLICKHOUSE/UI — hepsi 0 hit). DDL yalnız migrations'ta (DB_SCHEMA inline DDL YOK); wire-spec yalnız api/*.yaml (LLD köprü). |
| 5 | Open risks | ✅ | phase5/5.5 devirleri (relay-failover ölçümü, kalibrasyon, TENANT RTO/RPO şablonu) meşru ve etiketli; CRITICAL compliance-invariant'lar (retention audit-log, erasure-verification, vault-access) alarmlı. |

Legend: ✅ = sorun yok · ⚠️ = yalnız 🟡 bulgu · ❌ = herhangi 🔴

---

## Kilitli-Karar Ekseni (32 karar — özel denetim)

| Karar seti | Denetim sonucu |
|---|---|
| **D-A** (hibrit outbox+post-commit) | ✅ `CompactHistoryOutboxWriter` (tx-içi, ≤1 satır) + `HistoryPostCommitPublisher` (sıfır DB, at-most-once, D-A bilinçli kayıp kabulü). Tam-outbox/tam-post-commit AÇILMADI. |
| **D-B** (ayrı Postgres projeksiyon, ClickHouse ertelendi) | ✅ Projeksiyon ayrı DB (ADR-0011); CLICKHOUSE 0 hit. |
| **D-E** (instance-anahtarlı sıra + merge-upsert) | ✅ `Partition(8,3)` instance-partition + `stream_sequence` tie-break; sırasız+salt-upsert / global tek-consumer AÇILMADI. |
| **D-G** (Camunda-önce, Flowable=2b) | ✅ `flowable-nats-channel` "basamak-2 DIŞI, HİÇ değişmez" (02_package §1). |
| **PO-Q5** (audit-kritik = OP_LOG/INCIDENT/EXT_TASK_LOG) | ✅ DDL'de legal_hold default TRUE **bizzat probe'landı** (incident/operation_log/ext_task_log = true; activity_instance = false). |
| **ARCH-Q1** (payload=referans) | ✅ Şemada GERÇEK referans: `payload_large_ref UUID` (soft pointer), `variable_value_ref`/`error_details_ref`/`content_ref` → `projection_large_payload` FK. Inline blob DEĞİL. |
| **ARCH-Q2** (ayrı Postgres kasa) | ✅ `pseudonym-vault/` ayrı migration + izole `DataSource` (`history.vault.datasource`); pgcrypto round-trip probe'landı. |
| **ARCH-Q5** (rolling-restart cutover) ↔ **LLD-Q3** (KV boot-read) | ✅ TUTARLI: `ClassCutoverStateRegistry.loadAtBootstrap()` yalnız boot'ta okur; rolling-restart = apply mekanizması; watch'a ileri-uyum fork'a dokunmaz. ARCH-Q5 DEĞİŞMEDİ. |
| **LLD-Q1** (companion-satır NFR-P2) ↔ **D-F** | ✅ Companion `pg_stat_statements`'te ayrı queryid; bench raporunda AYRI sayaç (`compactOutboxPayloadRowCount`) — D-F ölçüm şeffaflığı korunur, sert kapı yanlış kırılmaz. |
| **ADR-0012** (stream_sequence versiyon kolonu) | ✅ Her entity-lifecycle tablosunda `stream_sequence BIGINT NOT NULL`; 3-adım protokol **canlı probe'landı** (105 uygulanır, stale 50 → 0 satır). |
| **rejected-alternatives-locked** (kalıcı dual-run vb.) | ✅ `CutoverRollback` yalnız DUAL_RUN'a döner, "cutover kuyruğundan sil" API'si YOK (NFR-R5). Hiçbir reddedilen alternatif yeniden açılmadı. |

**Spekülatif iç-katman sızması (eksen #5):** YOK. Sınıf dosyaları public metot imzası + sorumluluk-Javadoc düzeyinde durur; metot gövdesi/pseudo-kod yok. `04_interfaces/2_projection_dtos.md` iç uygulamayı açıkça Phase 5'e devreder ("Phase 5 iç uygulaması bu şablonu doldurur").

---

## Migration-Proof — Bağımsız Tekrar (görev talimatı şartı)

**Ortam:** Docker v29.6.0 (LLD iddiası doğrulandı), tek-kullanımlık `postgres:16.14` konteyner (`sentinel-p4-pgproof`), 3 ayrı veritabanı (engine_outbox/projection/vault), `ON_ERROR_STOP=1`. Konteyner test sonrası `docker rm -f` ile TEMİZLENDİ.

| Doğrulama | Sonuç |
|---|---|
| 5 migration uygula (engine-outbox + projection 001–003 + vault) | ✅ Hepsi hatasız (`ON_ERROR_STOP=1` → hata olsa abort ederdi) |
| Sayım: engine=2, projeksiyon 20 mantıksal (15 sınıf + projection_large_payload + 4 kontrol) + 15 default partition, vault=2 → **39 fiziksel** | ✅ `pg_class`: 15 partitioned parent + 20 base (15 default partition + 5 standalone) + 2 engine + 2 vault = **39**; **20 mantıksal (projeksiyon)** — DB_SCHEMA §5 ile BİREBİR |
| **Probe 1** — merge-upsert 3-adım (`activity_instance_history`) | ✅ INSERT → UPDATE(ss=105, dur=4200 uygulandı) → UPDATE(ss=50 stale → **0 satır**, değişmez) |
| **Probe 2** — partition-DROP retention (`variable_detail_history`) | ✅ dated partition oluştur → satır ekle → `DROP TABLE` → parent'ta **0 satır** (bulk-DELETE/VACUUM yükü yok) |
| **Probe 3** — pgcrypto kasa (`pseudonym_map`) | ✅ `pgp_sym_encrypt('user-42')` → `pgp_sym_decrypt` = `user-42`; `DELETE` sonrası **0 satır** (tersinmez); `vault_access_audit`'te WRITE/REIDENTIFY_ATTEMPT(granted=f)/DELETE izi |
| **Bonus** — CHECK + FK invariant'ları | ✅ `chk_operation_log_history_pseudonym_consistency` (pseudonymized=true + NULL token) REDDEDİLDİ; legal_hold default'ları probe'landı; `variable_value_ref` FK mevcut |

**Sonuç:** LLD'nin "migration-proof" (DB_SCHEMA §5) iddiası bağımsız olarak TEKRARLANDI ve DOĞRULANDI — merge-upsert / partition-retention / pgcrypto-kasa mekanizmalarının üçü de gerçek Postgres 16'da bizzat koşuldu.

---

## Şeffaflık — Ne Kontrol Ettim

- **Okunan dosyalar:** 19 LLD dosyası (01_overview 119, manifest 69, 6 sınıf-dosyası, 2 interface, 05–10 + 99), DB_SCHEMA (157), DB_ACCESS_MAP (69), ERROR_REGISTRY (169), SEQUENCE_DIAGRAMS (293), TEST_SPECIFICATIONS (151), 5 migration SQL (794 satır toplam) — hepsi uçtan uca.
- **Fork spot-check'leri (5+ zorunlu → 8 yapıldı, hepsi ✅):** `ProcessEngineConfigurationImpl.java:1124-1141` (init tek-sefer/buildProcessEngine), `:2788-2796` (initHistoryEventHandler `if(==null)` guard), `:769` (enableDefaultDbHistoryEventHandler alanı — LLD `:768` demiş, off-by-one FINDING-004), `CompositeDbHistoryEventHandler.java:70-72` (addDefaultDbHistoryEventHandler koşulsuz), `DbHistoryEventHandler.java:39` (sınıf/implicit ctor), `:97` (ByteArrayEntity … ResourceTypes.HISTORY), `HistoryEventProcessor.java:75` (getHistoryEventHandler CANLI okuma), `CompositeHistoryEventHandler.java:101-105` (handleEvents → for-loop handleEvent).
- **Migration-proof:** yukarıdaki tablo (bağımsız tekrar + 4 fonksiyonel probe).
- **Sayım doğrulamaları:** 42/42 hata kodu (kategori: VAL7/BUS15/RES4/SYS14/AUTH2=42, ERROR_REGISTRY §3-4 SAYARAK teyit); 39 fiziksel/20 mantıksal tablo (bizzat SQL); 8 sequence diyagramı; HLD §9 = 22 bileşen (overview 21 → FINDING-002).
- **Çapraz-referanslar:** manifest.md 10 çapraz-ref hedefi + BUSINESS_LOGIC §2.1-2.4 + DP-9…16 + ADR 0009-0019 + DATA_OWNERSHIP 5 capability (tek-provider) — HEPSİ çözülüyor, kırık ref YOK.
- **Manifest disiplini grep'leri:** cardholder/credit-card/PAN/kart, HIPAA/PHI/patient/sağlık, kafka, clickhouse, React/Vue/frontend/admin-ui — **hepsi 0 hit** (temiz).
- **LLD modüler disiplin (§2.10):** modüler yerleşim (eşik-üstü doğru); manifest.md File Index = 18/18 içerik dosyası (bizzat `find` ile eşleştirildi); `Last touched: 2026-07-20` = en yeni mtime (16:33, taze); domain klasör adları kavramsal (03_classes/04_interfaces/09_security). LLD `manifest.md` (dosya indeksi) ≠ `GUIDELINES_MANIFEST.yaml` (guideline enable/disable) — karıştırılmadı.
- **Layer-freedom (§0.6):** supersede/amendment/delta token'ı YOK; "cutover" domain terimi (layering değil); Rev-1 ilk yazım.

---

## Dürüstlük — Ne Kontrol ETMEDİM

- **Kod derleme/çalışma zamanı davranışı** (Phase 5+ kapsamı) — sınıflar henüz kod DEĞİL; sadece FQN/paket tutarlılığını mevcut basamak-1 modüllerine karşı denetledim (FINDING-001).
- **Sequence diyagramlarının `mmdc` render'ı** — `@mermaid-js/mermaid-cli` kurulumunu bu oturumda çalıştırmadım; SEQUENCE_DIAGRAMS.md'nin kendi "Doğrulama Kaydı" (8/8 SVG, 11.16.0) iddiasını kabul ettim ancak bağımsız render YAPMADIM (ortam-eksikliği bulgu değildir). Diyagram↔sınıf-adı tutarlılığını metinsel olarak kontrol ettim (15/15 eşleşir).
- **Fiziksel DB izolasyonunun** (3 ayrı Postgres örneği/host) deployment-düzeyi ispatı — migration-proof'u tek konteyner + 3 DB ile yaptım (DDL-doğruluğu için yeterli); host-düzeyi izolasyon deployment (Phase 6) konusudur.
- **pg_stat_statements fingerprint / D-F ölçümü** — gerçek bench koşusu Phase 5.5 kapsamı; yalnız LLD-Q1'in ölçüm-şeffaflık argümanının mantıksal tutarlılığını denetledim.
- **KVKK/GDPR hukuki yorumları** — DPO/hukuk alanı; yalnız compliance-invariant'ların (audit-log, legal_hold, erasure-verification) yapısal varlığını denetledim.

---

## İnsan İçin Sonraki Adım

**HAS-CONCERNS-NEEDING-ACK:** İki 🟡 bulgu için ya düzelt ya da yazılı gerekçeli kabul ver (manifest `phase_gate`: KOŞULLU ONAY'da MINOR'lar phase5-paralel kapatılabilir):
- **FINDING-001** (camunda modülü FQN `org.cadenzaflow`→`org.camunda`) — en yüksek öncelikli 🟡, SPI dikişi; phase5 kod-yazımı öncesi düzeltilmesi önerilir.
- **FINDING-002** (overview kapsama tablosu §3.5.5 + "21→22" öz-sayım) — izlenebilirlik-bookkeeping; overview'a bir satır eklemesi.
- FINDING-003/004 (🟢) bilgilendiricidir; isteğe bağlı temizlik.

Bu iki 🟡 yazılı kabul edilir veya düzeltilirse **Phase 4 → Phase 5 (implementation)** onaylanabilir. Bloklayıcı yoktur.

---

## Bulgu Kapanış Kaydı (2026-07-20, review sonrası aynı gün)

| Bulgu | Kapanış |
|---|---|
| FINDING-001 🟡 | **DÜZELTİLDİ** — `NatsHistoryEventHandler` FQN'i `org.camunda.bpm.engine.impl.history.handler.HistoryEventHandler` olarak düzeltildi (`camunda-nats-channel` modülü `org.camunda.*` hedefler; `cadenzaflow-nats-channel` byte-aynası `org.cadenzaflow.*` — ADR-0007 ayna-kuralı nota bağlandı). |
| FINDING-002 🟡 | **DÜZELTİLDİ** — overview bileşen sayısı 21→22 (HLD §9 yetkili sayı); kapsama tablosuna §3.5.5 "Devreden borç triyajı" → `5_bench.md §4` satırı eklendi. |
| FINDING-003 🟢 | **DÜZELTİLDİ** — `ERROR_REGISTRY §4` bayat drift-notu "ÇÖZÜLDÜ" kaydına çevrildi (kaynak `EXCEPTION_CODES §12` zaten RES_=4'e düzeltilmişti; iki belge hemfikir). |
| FINDING-004 🟢 | **DÜZELTİLDİ** — fork atıfları düzeltildi: alan tanımı `:768`→`:769`; `DbHistoryEventHandler.java:39`→`:40` sınıf bildirimi + "public no-arg ctor" ifadesi "implicit public no-arg ctor" olarak netleştirildi (fork kaynağına karşı yeniden doğrulandı). |

**Sonuç:** 4/4 bulgu düzeltme yoluyla kapatıldı (yazılı-kabul yolu kullanılmadı). Levent'in faz-5 standing onayı (2026-07-20 "phase 5 ile devam") ile kapı geçildi.
