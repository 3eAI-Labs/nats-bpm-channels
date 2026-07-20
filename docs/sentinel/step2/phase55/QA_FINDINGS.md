# Phase 5.5 — QA Findings + İzlenen-Borç Triyajı — basamak-2 History Offload

Severity ölçeği: 🔴 CRITICAL (bloklar) · 🟠 HIGH (bloklar) · 🟡 MEDIUM/gözlem (yazılı-ack veya backlog) · 🟢 LOW/NIT (bilgilendirici).
**🔴/🟠 sayısı: 0.** Tüm bulgular 🟡/🟢 — faz geçişini bloklamıyor.

---

## Yeni QA-FINDINGS (bu turda üretildi)

### 🟡 QA-F1 — Relay-failover "RTO≤60s" bound'u iyimser okunmamalı (ölçüm-türevi netleştirme, kod hatası DEĞİL)
- **Kanıt:** `TEST_REPORT.md §3` — gerçek koşum `timeToRecover=60.376s`, `leaseTtl=60s`. TTL-expiry mekanizması nedeniyle bir standby SON yenilemeden itibaren TAM TTL geçmeden ASLA devralamaz — bu yapısal bir ALT-SINIR, "en kötü durum" değil.
- **Etki:** Veri/audit etkisi YOK (RPO=0 doğrulandı). Yalnız operasyonel beklenti-yönetimi: on-call runbook'ları "RTO tipik olarak çok daha hızlı, 60s yalnız en kötü durum" varsayımıyla YAZILMAMALI.
- **Öneri:** Mimar/PO bilgilendirilsin; kod değişikliği gerekmiyor, `RelayFailoverBenchScenario.java` Javadoc'unda belgelendi.

### 🟡 QA-F2 — TEST_SPEC (f) `PseudonymVaultIsolationTest` hiç yazılmamış
- **Kanıt:** `SECURITY_SCAN.md §4`. Güvenlik ÖZELLİĞİ (fiziksel izolasyon, ayrı DataSource/bean, `pseudonym_map` projeksiyon-tarafında hiç referanslanmıyor) bağımsız doğrulandı — eksik olan yalnız TEK-YERDE-TOPLANMIŞ adanmış regresyon testi.
- **Test-edilebilir mi:** Evet, kolay (2× Postgres Testcontainers).
- **Gerçek risk:** DÜŞÜK.
- **Öneri:** phase6/backlog.

### 🟡 QA-F3 — Testcontainers sürüm-tutarsızlığı (2.0.4 core + 1.19.8 alt-modüller)
- **Kanıt:** `SECURITY_SCAN.md §5.2` — `dependency:tree` her modülde aynı deseni gösteriyor.
- **Gerçek risk:** DÜŞÜK bugün (testler yeşil), ORTA-vadeli bakım-kırılganlığı.
- **Öneri:** kök pom'da explicit version pin, phase6/backlog.

### 🟡 QA-F4 — SpotBugs pom'a kalıcı bağlanmamış (CI-gate yok)
- **Kanıt:** `SECURITY_SCAN.md §1`. Basamak-1'den miras — YENİ değil.
- **Öneri:** DevOps/phase6, `verify` fazına bağlama önerilir.

### 🟢 QA-F5 (NIT) — `task_instance_history.task_description` kolonu asla doldurulmuyor
- **Kanıt:** DB migration'da kolon var (`docs/.../db/migrations/projection/001_entity_lifecycle_tables.sql:162`), allowlist'te var (`HistoryClassColumnMapping.java:82`, `"task_description"`), ama `HistoryEventFieldExtractor`'ın `HistoricTaskInstanceEventEntity` dalı (`camunda-nats-channel/.../HistoryEventFieldExtractor.java:64-69`) `e.getDescription()`'ı hiç çağırmıyor VE `COLUMN_OVERRIDES`'ta `"description"→"task_description"` eşlemesi yok (yalnız `"name"→"task_name"` var, satır 111). İki-satırlık eksiklik: extractor'a `fields.put("description", e.getDescription())` + override-map'e giriş.
- **Test-edilebilir mi:** Evet (extractor birim-testi).
- **Gerçek risk:** DÜŞÜK — TASKINST audit-kritik SINIF DEĞİL (bulk), veri-kaybı/güvenlik etkisi yok, yalnız tamlık boşluğu (task açıklamaları history sorgu API'sinde hiç görünmüyor).
- **Öneri:** phase6/backlog — küçük, iyi-sınırlı 2-satırlık production fix + 1 birim-test, coder tarafından ele alınmalı (QA davranış-değişikliği yapmıyor, kural gereği).

---

## İzlenen-borç triyajı (görev talimatınca — PHASE5_REVIEW.md İZLENEN BORÇLAR)

| Borç | Test-edilebilir mi | Gerçek risk | Karar |
|---|---|---|---|
| **NEW-003** — `eventTimeOf` null-ts → `Instant.now()` fallback | Zaten test edilmiş | Faz5 review: "redelivery-idempotency'yi BOZMAZ, kabul edilebilir kenar durum" | ✅ **KAPALI** — `HistoryEventFieldExtractorTest.eventTimeOf_unknownType_fallsBackToNow` (satır 88) bu davranışı ZATEN karakterize-test ediyor. Aksiyon gerekmiyor. |
| **NEW-004** — `routeToDlq` metriği `FAILED_BOTH_PUBLISH`'te de "routed" sayıyor | Evet, kolay (adanmış `HistoryDlqConsumer` birim-testi yok) | Faz5 review: "trivial, pre-existing" — kozmetik alarm-oranı sapması, veri/güvenlik etkisi yok | 🟢 **phase6/backlog** — adanmış test + metrik-semantiği düzeltmesi küçük kapsamlı, coder işi |
| **F-005** — DETAIL byte-payload düşürülüyor | N/A (belgeli meşru erteleme) | Faz5 review: "hâlâ dokümante-boşluk" | ⏸️ **AÇIK/İZLENİR** (değişmedi) — QA aksiyon almadı, phase4/phase5 kararına saygı |
| **F-006** — attachment kolonları anonim-dışı | N/A (belgeli meşru erteleme) | Faz5 review: "CODER-NOTE korunuyor" | ⏸️ **AÇIK/İZLENİR** (değişmedi) |
| **`task_description` extractor'da doldurulmuyor** | Evet | DÜŞÜK (bkz. QA-F5 yukarıda) | 🟢 **QA-F5 olarak kayda geçti, phase6/backlog** |

---

## Kapatılan test-spesifikasyon boşlukları (bu turda QA tarafından KAPATILDI, artık bulgu DEĞİL)

- TEST_SPEC (b) custody-transfer — 3 çökme-noktasının HEPSİ artık adanmış, gerçek-Docker-pause içeren bir sınıfla kanıtlı (camunda+cadenzaflow ayna).
- TEST_SPEC (h) relay-failover — GERÇEK ölçüme çevrildi, RTO/RPO sayıları üretildi.
- `HistoryDlqInspectionConsumer` — sıfır coverage'dan %100'e (güvenlik-ilgili DLQ görünürlük sınıfı).
- Kasa `SYS_PSEUDONYM_VAULT_UNAVAILABLE` yolu — hiç test edilmemiş 3 hata-dalı artık kanıtlı.
- Cutover `SYS_CUTOVER_CONFIG_APPLY_FAILED` — hiç test edilmemiş 2 hata-dalı (KV-yazım + state-store-yazım, ayrı ayrı) artık kanıtlı.

---

## Onay için insan aksiyonu

Bu QA turunda **0 CRITICAL, 0 HIGH** bulgu. 4× 🟡 (QA-F1…F4) yazılı-ack veya backlog kararı bekliyor (hiçbiri bloklayıcı değil, hepsi ya gözlem-türü netleştirme ya da düşük-riskli bağımlılık/altyapı borcu). 1× 🟢 (QA-F5) backlog önerisi.

**"Phase 5.5 deliverables are ready. Do you approve to proceed to Phase 6?"**
