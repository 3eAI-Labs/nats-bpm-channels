# Phase 5.5 — Test Execution Report — basamak-2 History Offload

**Sentinel fazı:** 5.5 — Tester (QA). **Branch:** `feature/step2-history-offload`. **Ortam:** JAVA_HOME=temurin-21, Docker OK (Testcontainers gerçek Postgres 16 + NATS 2.10-alpine).
**Tarih:** 2026-07-21. **Girdi:** Phase 5 kapanışı (`PHASE5_REVIEW.md`, HAS-CONCERNS-NEEDING-ACK, 0 MAJOR/0 açık MINOR, sadece belgeli NIT'ler).

**Revizyon (phase-review turu):** FINDING-001 sonucu +1 test (`RetentionEnforcementJobTest`, retention audit-log atomikliği) eklendi — sayılar aşağıda güncellendi.

---

## 1. Özet — test sayıları (modül-başına, gerçek koşum)

Her modül **ayrı `mvn test` koşusuyla** doğrulandı (tam reactor'ı tek `mvn` oturumunda koşturmak bu sandbox'ta bellek baskısı yaratıp JVM'i sessizce düşürüyor — bkz. §4 "Sandbox notu"; modül-başına izole koşum güvenilir sonuç verdi).

| Modül | Önce (Phase5 kapanışı) | Sonra (Phase5.5) | Yeni test | Sonuç |
|---|---|---|---|---|
| `nats-core` | 108 | **111** | +3 (kasa/vault unreachable) | ✅ 111/111 |
| `camunda-nats-channel` | 148 | **151** | +3 (custody-transfer) | ✅ 151/151 |
| `cadenzaflow-nats-channel` | 156 | **159** | +3 (custody-transfer, ayna) | ✅ 159/159 |
| `flowable-nats-channel` | 59 | 59 | 0 | ✅ 59/59 |
| `nats-history-projection` | 88 | **97** | +9 (DLQ-inspection ×6, cutover-apply-failed ×2, retention-audit-atomicity ×1 [phase-review FINDING-001]) | ✅ 97/97 |
| `nats-bpm-bench` (varsayılan, `bench` hariç) | 5 | **4** | −1 (eski `RelayFailoverBenchScenarioTest` placeholder → `@Tag("bench")`'e taşındı) | ✅ 4/4 |
| **Varsayılan toplam** | **564** | **581** | **+17** | ✅ **581/581** |
| `nats-bpm-bench` (`@Tag("bench")`, nightly/manual, `-Dbench.excludedGroups=`) | 2 | **4** | +2 (relay-failover GERÇEK ölçüm) | ⚠️ 3/4 (bkz. §4) |
| **Genel toplam** | **566** | **585** | **+19** | **584/585** (1 pre-existing, bkz. §4) |

**Yeni eklenen test dosyaları (3):**
- `nats-history-projection/.../projection/HistoryDlqInspectionConsumerTest.java` (6 test)
- `camunda-nats-channel/.../history/HistoryOutboxCustodyTransferTest.java` (3 test)
- `cadenzaflow-nats-channel/.../history/HistoryOutboxCustodyTransferTest.java` (3 test, camunda↔cadenzaflow ayna — `diff` sonrası engine-adı hariç BYTE-AYNI, mekanik doğrulandı)

**Genişletilen mevcut test dosyaları (3):**
- `nats-core/.../vault/PseudonymizationVaultClientTest.java` (+3: `SYS_PSEUDONYM_VAULT_UNAVAILABLE` yolları)
- `nats-history-projection/.../cutover/CutoverControlPlaneTest.java` (+2: `SYS_CUTOVER_CONFIG_APPLY_FAILED` — KV-yazım ve state-store yolları ayrı ayrı)
- `nats-history-projection/.../governance/RetentionEnforcementJobTest.java` (+1: phase-review FINDING-001 — `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED` atomiklik fault-injection testi, bkz. §5)

**Yeniden yazılan (davranış GERÇEK ölçüme çevrildi, TEST_SPEC h):**
- `nats-bpm-bench/.../history/RelayFailoverBenchScenario.java` (production, `UnsupportedOperationException` placeholder → gerçek çok-replika KV-lease failover ölçümü)
- `nats-bpm-bench/.../history/RelayFailoverReport.java` (RPO alanları eklendi: `outboxRowsSeeded`, `outboxRowsUnrecoveredAfterFailover`, `zeroAuditLoss()`)
- `nats-bpm-bench/.../history/RelayFailoverBenchScenarioTest.java` (`@Tag("bench")` — artık gerçek Testcontainers koşumu, 2 test)

---

## 2. TEST_SPECIFICATIONS.md (a–h) durumu

| Kalem | Phase5'te durum | Phase5.5 doğrulaması |
|---|---|---|
| (a) bench history modu (D-F sert kapı) | ✅ yapıldı (`HistoryBenchScenarioTest`, `@Tag("bench")`) | ✅ yeniden koşuldu, yeşil (6.3s) |
| (b) custody-transfer 3 çökme-noktası | ⚠️ KISMİ — `HistoryOutboxRelayTest` happy-path+publish-fail'i kapsıyordu ama **adanmış `HistoryOutboxCustodyTransferTest` sınıfı ve broker-kesinti (container pause/unpause) senaryosu YOKTU** | ✅ **KAPANDI** — yeni `HistoryOutboxCustodyTransferTest` (camunda+cadenzaflow): (1) commit-öncesi rollback→satır yok, (2) relay-restart→satır hayatta sonra silinir, (3) **gerçek Docker `pauseContainerCmd`/`unpauseContainerCmd`** ile NATS'i publish ANINDA durdurup satırın SİLİNMEDİĞİNİ, sonra broker dönünce SİLİNDİĞİNİ kanıtlıyor (`PublishOptions.DEFAULT_TIMEOUT`=2s'e bağlı, sınırlı bekleme) |
| (c) merge-upsert çakışması | ✅ yapıldı (`ProjectionStoreTest` 9/9) | ✅ değişmedi, yeşil |
| (d) cutover geri-dönüş | ✅ yapıldı (`CutoverRollbackTest`) | ✅ değişmedi, yeşil |
| (e) erasure doğrulama | ✅ yapıldı (`ErasurePipelineTest` 8/8) | ✅ değişmedi, yeşil |
| (f) kasa izolasyonu | ⚠️ **`PseudonymVaultIsolationTest` sınıfı hiç yazılmamış** (CQ-1 sonrası mimari değişikliği — vault artık engine-side, projeksiyon vault-habersiz) | 🟡 **QA-FINDING** (aşağıda) — güvenlik ÖZELLİĞİ (fiziksel izolasyon) config-düzeyinde + `PseudonymizationVaultClientTest`'in kendi izole container'ıyla dolaylı doğrulandı, ama spesifikasyonun birebir sınıfı yok |
| (g) BA-Q7 kalibrasyon | N/A (runbook, kod değil) | değişmedi |
| (h) relay failover | ⏸️ **TASARIM-ONLY** — `RelayFailoverBenchScenario.run()` `UnsupportedOperationException` fırlatıyordu | ✅ **KAPANDI — GERÇEK ÖLÇÜM** (bkz. §3) |

---

## 3. Relay-Failover GERÇEK Ölçümü (TEST_SPEC h) — sonuç

**Yöntem:** `BenchEnvironment`'ın gerçek Postgres 16 + NATS 2.10 Testcontainers'ı üzerinde, `history-relay-leader` KV bucket'ı **üretim TTL'i ile** (`2×relayCyclePeriodSeconds` = 60s, `08_config.md §3`) provision edildi. 3 engine-node replikası (`SweepLeaderLease`+`HistoryOutboxRelay` çifti, aynı bucket/key'e karşı yarışan) kuruldu. node-0 lider oldu, 5 audit-kritik outbox satırı (drenajdan ÖNCE) yazıldı, node-0 SERT kapatıldı (bir daha `relayCycle()` çağrılmadı — `docker kill` semantiği, graceful değil). Standby node'lar hızlı poll (500ms) ile `relayCycle()` çağrılarak lease'in ne zaman devraldığı ölçüldü.

### Sonuç (gerçek koşum, `RelayFailoverBenchScenarioTest`, JAVA_HOME=temurin-21)

| Metrik | Değer |
|---|---|
| **RTO (timeToRecover)** | **60.376 saniye** (`PT1M0.37553615S`) |
| **RTO bound (leaseTtl)** | 60s (`2×30s`, üretim default'u) |
| **RPO (audit-kritik kayıp)** | **0** — 5/5 seed edilmiş satır, failover sonrası yeni lider tarafından tam drenaj edildi (`outboxRowsUnrecoveredAfterFailover=0`) |
| **Sonuç** | ✅ RTO≤60s+poll-payı sınırı içinde, RPO=0 |

### 🟡 QA-FINDING (ölçüm-türevi gözlem, kod hatası DEĞİL) — "RTO≤60s" bound'unun gerçek anlamı

TTL-expiry temelli devir mekanizması NEDENİYLE bir standby, çökmüş liderin SON yenilemesinden itibaren **TAM 60 saniye geçmeden** lease'i asla devralamaz (NATS KV bucket TTL'i budur — daha erken devralma yapısal olarak İMKANSIZ). Ölçülen 60.376s bu yapısal alt-sınırı + bu test-harness'in kendi poll-granülerlerliğini (500ms) yansıtıyor. Yani **"RTO≤60s" iddiası "genelde çok daha hızlı, en kötü durumda 60s" değil — "yaklaşık HER ZAMAN ~60s" anlamına gelir** (üretimde gerçek standby'lar kendi 30s döngüsünde poll ettiği için gerçek-dünya RTO'su 60-90s aralığına da çıkabilir, döngü hizalanmasına bağlı). Bu, mimari/PO'nun on-call runbook beklentisini kalibre etmesi için önemli bir netleştirme — kod DEĞİŞİKLİĞİ gerektirmiyor (tasarım kasıtlı ve DP/NFR-R1 açısından güvenli — RPO=0 korunuyor, yalnız RTO'nun "iyimser" okunmaması gerekiyor). Detay: `RelayFailoverBenchScenario.java` sınıf Javadoc'unda belgelendi.

**Koşum notu:** bu test `@Tag("bench")` (nightly/manual, `BR-OBS-003` deseni) — gerçek ~67s sürüyor (env boot + 60s TTL bekleme + drenaj), varsayılan `mvn test`'i yavaşlatmamak için.

---

## 5. Retention audit-log atomikliği — GERÇEK fault-injection testi (phase-review FINDING-001)

Faz5.5 ilk turunda `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED` (CRITICAL, on-call-page) hiçbir testle örneklenmiyordu — retention-silme ↔ audit-log yazımı atomikliği (`DATA_GOVERNANCE.md §4.4`) yalnız `RetentionEnforcementJob`'ın tasarım-argümanıyla (CODER-NOTE) varsayılıyordu. Phase-review turunda gerçek-PG + fault-injection testi eklendi.

**Sonuç: ATOMİKLİK TUTTU — production bug bulunmadı.** Audit-log yazımı (bilinçli olarak ulaşılamaz bir `DataSource`'a bağlanarak) fault-inject edildiğinde: (a) `RetentionAuditLogWriteFailedException` bozulmadan yükseliyor, (b) `DROP TABLE` (henüz commit edilmemiş, aynı işlemin kendi connection'ında) `rollback()` ile geri alınıyor — partition test-ortamında GERÇEKTEN hayatta kalıyor, öksüz-silme oluşmuyor. Detaylı mekanizma analizi + bir 🟢 NIT (CODER-NOTE'un "aynı transaction" iddiası literal olarak yanlış, gerçek mekanizma farklı ama sonuç aynı) — bkz. `SECURITY_SCAN.md §6` ve `QA_FINDINGS.md` (FINDING-001 çözümü + QA-F6).

---

## 4. Sandbox / bilinen ortam kısıtları

- **Tam-reactor tek-oturum `mvn clean test` bellek baskısı:** tüm 6 modülü TEK `mvn` sürecinde art arda koşturmak (Testcontainers+Camunda engine boot'larının kümülatif belleği) JVM'i sessizce düşürüyor (log ortada kesiliyor, `BUILD SUCCESS/FAILURE` asla basılmıyor). Bu YENİ bir regresyon değil — talimatta önceden işaretlenen basamak-1 `ExternalTaskLifecycleBenchTest` bellek-baskısı deseniyle AYNI kök neden (sandbox kaynak sınırı). **Güvenilir metodoloji:** modül-başına izole `mvn test -pl <modül>` (yukarıdaki §1 tablosu bu şekilde üretildi) — her biri temiz BUILD SUCCESS verdi.
- **`ExternalTaskLifecycleBenchTest` (basamak-1, `@Tag("bench")`) — ÖNCEDEN-VAR timeout:** `a2PushProducesZeroPollAndZeroFetchAndLock` bu sandbox'ta `expected: 20L but was: 0L` ile FAIL ediyor (65.6s, tüm 20 instance zaman aşımına uğruyor). Görev talimatınca bilinen sorun ("git stash ile temiz HEAD'de tekrarlanıyor") — **YENİ bulgu SAYILMADI**, bu QA turunda tekrar üretildi (aynı hata deseni) ve NOT düşüldü. Kod DEĞİŞTİRİLMEDİ.
- **OWASP dependency-check NVD senkronizasyonu tamamlanamadı** (bkz. `SECURITY_SCAN.md` §3) — API anahtarsız 367.875 kaydı indirmek bu oturum bütçesini aşıyordu (~8 dakikada yalnız %8).
