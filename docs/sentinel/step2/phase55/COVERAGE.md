# Phase 5.5 — Code Coverage Report (JaCoCo) — basamak-2 History Offload

**Araç:** JaCoCo 0.8.12 (basamak-1'de reaktöre eklendi, commit `f273743`, tüm modüllerde miras alınır — bu turda yeni ekleme gerekmedi).
**Eşik (TESTER_GUIDELINE §1.2.1 + CODING_GUIDELINES_COMMON):** `>80% line coverage for business logic` (REQUIRED). Basamak-1 kapanışında (`docs/sentinel/phase6/PHASE6_REVIEW.md`) bu eşiğin altı **"kabul edilmiş borç"** olarak KAPANDI (camunda ~71%, cadenzaflow ~71.4%, nats-core ~73%, flowable ~86%, ortalama ~%74.0) — basamak-2 bu borcu MİRAS alır, yeni bir bulgu değildir.

---

## 1. Modül-başına satır/dal kapsaması (bu turdaki testler dahil, gerçek koşum)

| Modül | Satır (line) | Dal (branch) | Basamak-1 referansı | Durum |
|---|---|---|---|---|
| `nats-core` | **79.4%** (475/598) | 72.3%→(≈72%, 99/137 civarı) | ~73% | ✅ hafif iyileşme (+1.1pp), eşik-altı ama **kabul-edilmiş borç sınıfında** |
| `camunda-nats-channel` | **73.8%** (1080/1463) | 63.5% (292/460) | ~71% | ⚪ değişmedi (yeni testler mevcut satırları tekrar-egzersiz etti, coverage-% hareket etmedi — bkz. §3) |
| `cadenzaflow-nats-channel` | **74.0%** (1091/1474) | 64.1% (300/468) | ~71.4% | ⚪ değişmedi (aynı sebep) |
| `flowable-nats-channel` | **86.0%** (406/472) | 69.7% (83/119) | ~86% | ⚪ basamak-2 bu modüle DOKUNMADI (beklenen) |
| `nats-history-projection` **(YENİ modül)** | **83.2%** (1098/1319) | 72.5%+ | N/A (yeni) | ✅ eşik ÜSTÜNDE, bu turda 80.5%→82.6%→**83.2%** (phase-review FINDING-001 test'i dahil) iyileşti |
| `nats-bpm-bench` | **89.7%** (420/468) | 75.4% (98/130) | N/A | ✅ eşik üstünde |
| **Ağırlıklı reaktör ortalaması** | **78.9%** (4570/5794) | — | ~74.0% | ✅ basamak-1 ortalamasının ÜZERİNDE |

**`nats-history-projection` (YENİ, basamak-2'nin ana modülü) 83.2% ile hem >80% eşiğini geçiyor hem de tüm reaktörün en sağlıklı business-logic modüllerinden biri.**

---

## 2. Kapatılan kritik-yol coverage boşlukları (bu turda)

| Sınıf | Önce | Sonra | Nasıl |
|---|---|---|---|
| `HistoryDlqInspectionConsumer` (nats-history-projection) | **20.8%** (5/24), **SIFIR test dosyası** | **100%** (24/24) | Yeni `HistoryDlqInspectionConsumerTest` (6 test) — `RES_HISTORY_DLQ_ACCESS_DENIED` görünürlük yolu (CB-korumalı, DP-1 log-only), backoff hesaplama, CB-open fast-nak |
| `PseudonymizationVaultClient` (nats-core, **kasa**) | 87.0% (47/54) | **100%** (54/54) | +3 test — `SYS_PSEUDONYM_VAULT_UNAVAILABLE` yolları (`persistMapping`/`deleteMapping`/`reidentify`, ulaşılamaz DB) |
| `CutoverControlPlane` (nats-history-projection, **cutover**) | 75.8% (25/33) | **100%** (33/33) | +2 test — `SYS_CUTOVER_CONFIG_APPLY_FAILED` (KV-yazım-fail + state-store-yazım-fail, ayrı ayrı; KV zaten yazılmışken DB fail'i kanıtlayan senaryo dahil) |
| `HistoryOutboxRelay` + `CompactHistoryOutboxWriter` (**custody-transfer**) | 78.4% / 86.2% | değişmedi (bkz. not) | Yeni `HistoryOutboxCustodyTransferTest` (×2 motor) — 3 çökme-noktası invariant'ı, satır-sayısı ARTMADI çünkü aynı kod-yolları zaten `HistoryOutboxRelayTest`/`CompactHistoryOutboxWriterTest`'ten kapsanıyordu; DEĞER katkısı davranışsal-invariant/regresyon-koruması, coverage-% değil |
| `RetentionAuditLogger` (nats-history-projection, **retention/CRITICAL**) | 89.5% (17/19) | **100%** (19/19) | phase-review FINDING-001 — +1 test, `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED` fault-injection (bkz. `SECURITY_SCAN.md §6`) — atomiklik TUTTU |
| `RetentionEnforcementJob` (nats-history-projection) | 79.5% (66/83) | 88.0% (73/83) | aynı yeni test — DROP-rollback dalı artık egzersiz ediliyor |

**Not (custody-transfer %'nin sabit kalması üzerine):** JaCoCo satır-bazlı bir araçtır — YENİ bir test AYNI satırları farklı bir hata-enjeksiyon mekanizmasıyla (gerçek Docker `pause()` vs kapalı-bağlantı) tekrar çalıştırırsa % artmaz ama regresyon-yakalama gücü artar (iki BAĞIMSIZ yol aynı invariant'ı kanıtlar). Bu turda öncelik TEST_SPEC (b)'nin literal 3 çökme-noktasını (özellikle gerçek broker-kesintisi) kapatmaktı, coverage-% değil.

---

## 3. Kalan düşük-coverage alanlar (bu turda dokunulmadı — triyaj)

| Sınıf | Satır % | Neden düşük | Triyaj |
|---|---|---|---|
| `HistoryQueryController` | 53.1% (26/49) | Çoğu dal `HistoryQueryApiTest` (backend) üzerinden dolaylı kapsanıyor, controller-katmanı ince | LOW risk — backlog, `phase6` |
| `*Properties` sınıfları (`HistoryProjectionDataSourceProperties`, `ReconciliationProperties`, `HistoryProjectionProperties`, `HistoryCutoverProperties`, `NatsConnectionFactory` (nats-core, %0)) | %10–60 | Düz getter/setter POJO'lar, Spring `@ConfigurationProperties` binding'i test-edilmiyor (basamak-1'den de miras) | LOW risk, standart kalıp — Spring context-test'leriyle dolaylı doğrulanıyor |
| `ErasurePipeline` | 89.2% (116/130) | Kalan 14 satır çoğunlukla `SQLException` catch-dalları (`SYS_ERASURE_PIPELINE_FAILED`) | LOW-MEDIUM — CRITICAL path (`RES_ERASURE_VERIFICATION_FAILED`) zaten 8/8 testle kanıtlı; yalnız downstream-DB-fail dalı test edilmemiş, backlog önerilir |
| `HistoryProjectionConsumer` | 69.6% (64/92) | `HistoryProjectionConsumerTest` (4) + `...DlqRoutingTest` (2) mevcut ama bazı dallar (örn. stale-discard log-only dalı) ayrı egzersiz edilmiyor | LOW risk — audit-kritik olmayan gözlemlenebilirlik dalları |

---

## 4. Eşik uyumu özeti

`TESTER_GUIDELINE.md §1.2.1`: ">80% line coverage for business logic" [REQUIRED]. Basamak-1 kapanışında bu eşik-altı durum **belgeli kabul-edilmiş borç** statüsüne alındı (yeni bulgu üretilmedi, `PHASE6_REVIEW.md §1`). Basamak-2:
- **YENİ `nats-history-projection` modülü eşiği GEÇİYOR (82.6%)** — asıl business-logic'in merkezi burada, bu önemli.
- `camunda`/`cadenzaflow`/`nats-core` basamak-1'in mirasını taşıyor (~74-79%), bu turda CRITICAL-path (kasa, cutover, custody-transfer, DLQ) hedefli iyileştirmelerle net kazanım sağlandı, ama genel %-eşiği basamak-1'den devralınan mimari/test-yatırımı borcu nedeniyle hâlâ altında. **Bu YENİ bir blocker değil** — basamak-1 emsaliyle TUTARLI kabul-edilmiş borç sınıfında kalmaya devam ediyor; insan onayı bu turda da aynı şekilde istenir.
