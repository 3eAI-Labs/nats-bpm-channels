# Test Specifications — Basamak-2: History Offload

**Sentinel fazı:** Phase 4 — Developer (LLD). **Kaynak:** görev talimatı ("phase5/5.5 için kritik test spesifikasyonları"), `docs/sentinel/step2/phase4/lld/history-offload/01_overview.md` "Phase3'ün devrettiği doğrulamalar" (#4/#5), ADR-0015 (D-F), ADR-0010 (custody-transfer), ADR-0012 (merge-upsert), ADR-0015 (rollback), ADR-0017 (erasure), ADR-0016 (kasa izolasyonu).
**Durum:** Taslak — Levent faz-4 onayına sunuluyor. **Kod YAZILMAZ** (Phase 5 kapsamı) — yalnız test tasarımı (sınıf adı, kurulum, adım, assertion, geçme kriteri).

---

## (a) Bench history modu — normalize DB yazım-op sert kapı (D-F, BR-OBS-001)

**Doğrulanacak:** cutover'lanan sınıflarda `ACT_HI` yazım bileşeni **0**; audit-kritik outbox bileşeni **≤1 satır/tx** (LLD-Q1 companion-satır istisnası hariç).

**Test sınıfı:** `HistoryBenchScenarioTest` (`nats-bpm-bench/src/test/.../history/`) — basamak-1 `ExternalTaskLifecycleBenchTest` deseninin genişlemesi.

**Kurulum:** Testcontainers `PostgreSQLContainer` (`pg_stat_statements`, engine DB + projeksiyon DB İKİ AYRI container) + embedded `ProcessEngine` (`NatsHistoryEventHandler` aktif, tüm audit-kritik+bulk sınıflar konfigüre) + NATS JetStream (HISTORY + DLQ_HISTORY stream'leri).

**Adımlar:**
1. `HISTORY_OFFLOAD` modunda çalıştır: bir alt-küme sınıf ÖNCEDEN cutover'lanmış (`ClassCutoverStateRegistry` test-fixture ile `isCutOver=true` sabitlenmiş).
2. 1000 process instance tam yaşam döngüsü (start→activities→variables→complete) çalıştırılır.
3. `PgStatStatementsSnapshotter.capture(engineDataSource)` — `ACT_HI_*` tablolarına karşı INSERT sayısı.
4. `compact_history_outbox`/`compact_history_outbox_payload` INSERT sayısı ayrı ayrı sayılır.

**Assertion (geçme kriteri):**
- Cutover'lanmış sınıflar için `ACT_HI_<X>` INSERT sayısı = **0**.
- Audit-kritik sınıflarda `compact_history_outbox` INSERT sayısı = event sayısı (≤1/tx); `compact_history_outbox_payload` INSERT sayısı AYRI raporlanır, hard-gate'e DAHİL EDİLMEZ (LLD-Q1 KARAR 2026-07-20; test iki sayacı da ayrı raporlamaya devam eder — şeffaflık).
- Hedef kaçarsa `BUS_BENCH_HISTORY_METRIC_REGRESSION` → `passesHardGate()==false` → build-fail.

**Regresyon rolü:** basamak-1 D-F metodolojisiyle AYNI (`pg_stat_statements` fingerprint, IN-list arity uyarısı history basit-INSERT'lerde geçerli değil — HLD §11 kalem 3 doğrulaması).

---

## (b) Custody-transfer — outbox→relay→PubAck→delete, çökme-noktası testi

**Doğrulanacak:** BR-REL-001'in ÜÇ çökme-noktası (commit-öncesi, publish-öncesi, PubAck-öncesi) invariant'ları korur — hiçbiri kalıcı audit kaybı üretmez.

**Test sınıfı:** `HistoryOutboxCustodyTransferTest` (`camunda-nats-channel/src/test/.../history/`), gerçek NATS Testcontainers + embedded engine+DB.

**Adımlar:**
1. **Commit-öncesi çökme simülasyonu:** tx rollback zorla (test-injected exception, `CompactHistoryOutboxWriter.write(...)` sonrası, engine commit ÖNCESİ) → outbox tablosunda satır **YOK** olmalı.
2. **Publish-öncesi çökme (relay node restart) simülasyonu:** outbox satırı yaz, relay'i BAŞLATMADAN önce durum kontrol et → satır HAYATTA; relay'i başlat → satır PubAck sonrası silinir.
3. **PubAck-öncesi broker-kesintisi simülasyonu:** NATS container'ı relay publish ANINDA durdur (Testcontainers `pause()`), relay'in retry/backoff'a girdiğini ve satırın SİLİNMEDİĞİNİ doğrula; container'ı devam ettir (`unpause()`), satırın nihayetinde silindiğini doğrula.

**Assertion:** 3 senaryoda da NATS'a yayınlanmış+PubAck-onaylı bir event kaybolmaz; satır yalnız PubAck-sonrası silinir (asla önce).

---

## (c) Merge-upsert çakışması — stream-sequence tie-break + belirsizlik

**Doğrulanacak:** ADR-0012 tie-break'in doğru çalıştığı (BUS_PROJECTION_STALE_EVENT_DISCARDED) VE partition-güvenli protokolün (`DB_SCHEMA.md §2.3`) doğru davrandığı.

**Test sınıfı:** `ProjectionMergeUpsertConflictTest` (`nats-history-projection/src/test/.../projection/`), gerçek Postgres 16 Testcontainers (bu LLD'nin migration'larıyla provision edilir, `DB_SCHEMA.md §5` deseninin genişlemesi — CI'da tekrarlanabilir).

**Adımlar:**
1. `activity_instance_history`'e `stream_sequence=100` ile bir satır yaz (ilk event).
2. `stream_sequence=105` ile ikinci bir event gönder (`ProjectionStore.upsertEntity(...)`) → APPLIED beklenir.
3. `stream_sequence=50` ile GEÇ gelen bir event gönder (relay retry senaryosu) → STALE_DISCARDED beklenir, satır DEĞİŞMEMELİ.
4. İKİ eşzamanlı event'i (`stream_sequence=110` aynı anda iki thread'den) gönder — dedup zaten stream-düzeyinde bunu önler ama uygulama-katmanı race koşulunu (aynı anda iki `SELECT...UPDATE` çağrısı) test eder → **yalnız biri** APPLIED, DB-düzeyi UPDATE'in `stream_sequence < ?` koşulu ikinci çağrıyı doğal olarak STALE yapmalı (race-safe).

**Assertion:** Nihai satır durumu her zaman EN YÜKSEK `stream_sequence`'i yansıtır; hiçbir ara-durum kalıcı hale gelmez (Docker-doğrulanmış temel protokol, `DB_SCHEMA.md §5` madde 1'in genişlemesi — burada eşzamanlılık EKLENİR).

---

## (d) Cutover geri-dönüş (rollback) — streak sıfırlama + Cockpit-history geri gelişi

**Doğrulanacak:** BR-CUT-003 — geri-dönüş sonrası dual-run yeniden başlar, streak SIFIRDAN, kalıcı dual-run YASAK (yeniden cutover kuyruğuna girer).

**Test sınıfı:** `CutoverRollbackTest` (`nats-history-projection/src/test/.../cutover/`), gerçek Postgres (projeksiyon) + gerçek NATS KV (`history-cutover-state`).

**Adımlar:**
1. Bir sınıfı `CUTOVERLANMIS` durumuna getir (test-fixture: `class_cutover_state` satırı + KV `cutover.<engine>.<class>=true`).
2. `CutoverRollback.rollback(engineId, class, operatorId, reason)` çağır.
3. `class_cutover_state` satırını oku: `state=DUAL_RUN`, `clean_streak_days=0`, `rollback_count=1`.
4. KV değerini oku: `cutover.<engine>.<class>=false`.
5. (Engine-side, ayrı entegrasyon testi) `ClassCutoverStateRegistry.loadAtBootstrap()` yeni KV değerini okuduğunda `isCutOver(class)==false` döner → `NatsHistoryEventHandler` o sınıf için `internalDbDelegate`'i TEKRAR çağırır (ACT_HI yazımı geri döner).
6. Aynı sınıf için `ReconciliationJob` bir sonraki döngüde çalışır — streak 0'dan başlar (önceki geçmiş SAYILMAZ).

**Assertion:** `BUS_CUTOVER_ROLLBACK_TRIGGERED` audit-logged (INFO); `rollback_count` artar; kalıcı dual-run'a "sabitleme" API'si YOKTUR (yalnız `DUAL_RUN`'a döner, sınıf otomatik olarak yeniden cutover kuyruğuna girer — BR-HDL-005).

---

## (e) Erasure doğrulama — `RES_ERASURE_VERIFICATION_FAILED` yolu

**Doğrulanacak:** ADR-0017'nin "doğrulanabilir tamlık" şartı — anonimleştirme YARIM kaldığında CRITICAL sinyal üretilir.

**Test sınıfı:** `ErasureVerificationFailureTest` (`nats-history-projection/src/test/.../governance/`).

**Adımlar:**
1. Bulk sınıfta (örn. `variable_instance_history`) bir subject'e ait satırlar yaz.
2. `ErasurePipeline.requestErasure(subjectKey, scope)` çağır — net kapsam (tek instance kümesi) → doğrudan `executeAnonymization(...)`.
3. **Kasıtlı yarım-bırakma simülasyonu:** `executeAnonymization(...)`'ın SQL adımını test-double ile KISMEN uygula (örn. `variable_instance_history` anonimleştirilir ama ilişkili `projection_large_payload` satırı KASITLI atlanır — gerçekçi bir "yarım pipeline" senaryosu).
4. Doğrulama sorgusunu (`HistoryQueryApi` üzerinden) çalıştır — hâlâ PII döndürüyor mu kontrol et.

**Assertion:** Doğrulama sorgusu PII döndürüyorsa `RES_ERASURE_VERIFICATION_FAILED` (`ErasureVerificationFailedException`) fırlar, `erasure_audit_log.verification_status='FAILED'` yazılır, CRITICAL alarm tetiklenir (`10_metrics.md §2.3`). Doğrulama TEMİZse `verification_status='PASSED'`.

---

## (f) Kasa izolasyonu — fiziksel ayrım + yetkisiz erişim reddi

**Doğrulanacak:** ARCH-Q2/ADR-0016 — kasa projeksiyon store'dan FİZİKSEL olarak izole; yetkisiz erişim CRITICAL.

**Test sınıfı:** `PseudonymVaultIsolationTest` (`nats-history-projection/src/test/.../vault/`), İKİ AYRI Postgres Testcontainers (projeksiyon + kasa).

**Adımlar:**
1. `PseudonymizationVaultClient`'ın `DataSource`'unun projeksiyon `DataSource`'undan **FARKLI bir JDBC URL/container** olduğunu doğrula (konfig-düzeyi assertion — aynı container'a işaret ediyorsa test FAIL).
2. `pseudonym_map`'te bir satır yaz, `operation_log_history.pseudonym_token`'ın kasadaki token'la eşleştiğini ama `operation_log_history`'de gerçek `user_id` DEĞERİNİN hiçbir yerde bulunmadığını doğrula (SQL `SELECT` ile projeksiyon DB'de gerçek değer ARANAMAZ).
3. `reidentify(...)`'ı `authorized=false` ile çağır → `AUTH_PSEUDONYM_VAULT_ACCESS_DENIED` fırlar, `vault_access_audit`'e `granted=false` satırı düşer.
4. `deleteMapping(...)` çağır → `pseudonym_map`'te satır **fiziksel olarak YOK** (soft-delete DEĞİL, `SELECT count(*)=0`).

**Assertion:** İki DB arasında FK/cross-database sorgu YOK (yapısal izolasyon); yetkisiz erişim her zaman `vault_access_audit`'e (granted=false) düşer; silme tersinmez.

---

## (g) BA-Q7 outbox-stuck çarpanı kalibrasyon prosedürü (LLD-Q5, `01_overview.md` "#4")

**Amaç:** default `stuckThresholdMultiplier=5×` (150s @ 30s döngü) değerinin ilk gerçek relay-döngü telemetrisiyle kalibre edilmesi.

**Prosedür (test DEĞİL, ölçüm-runbook — `nats-bpm-bench` çıktısına dayanır):**
1. `(a)` bench koşusundan `nats.history.outbox.relayed` timing dağılımını (p50/p95/p99 relay-cycle-to-publish süresi) topla.
2. p99 relay-döngü-gecikmesi ile `stuckThresholdMultiplier × relayCyclePeriod` (150s) karşılaştır — p99 150s'e YAKINSA (örn. >100s) çarpan YÜKSELTİLMELİ (yanlış-pozitif `SYS_OUTBOX_ROW_STUCK` riski); p99 ≪150s ise çarpan DÜŞÜRÜLEBİLİR (daha erken tespit).
3. İlk pilot deployment'tan sonra (gerçek trafik) aynı analiz TEKRARLANIR — `08_config.md §1` `stuckThresholdMultiplier` config'i güncellenir (kod değişikliği GEREKMEZ).

---

## (h) Relay failover bench senaryosu (tasarım — ölçüm phase5.5)

**Test sınıfı:** `RelayFailoverBenchScenario` (`03_classes/5_bench.md §3`) — `nats-bpm-bench`.

**Senaryo:**
1. N engine-node replikası (Testcontainers, her biri kendi `HistoryOutboxRelay` instance'ı) + gerçek NATS cluster + `history-relay-leader` KV bucket.
2. Sürekli audit-kritik event akışı üret (outbox satırları birikir).
3. Mevcut lider node'u SERT kapat (`docker kill`, graceful shutdown DEĞİL — gerçekçi çökme).
4. Zamanlayıcı başlat: leader-kaybından yeni liderin `sweepCycle()`'a eşdeğer `relayCycle()`'ı çalıştırdığı ana kadar geçen süreyi ölç.
5. Bu süreyi TTL bound'uyla (60s, `01_overview.md` "#3" RTO) karşılaştır.

**Beklenen:** ölçülen devir süresi ≤ TTL (60s); outbox satırlarının hiçbiri kaybolmaz (devir öncesi/sonrası satır sayısı KORUNUR, yalnız gecikir). **Gerçek koşum phase5.5 kapsamı** (Testcontainers + gerçek NATS cluster gerektirir — bu LLD yalnız senaryo TASARIMINI verir).

---

## Özet — test-spesifikasyon sayımı

| Kalem | Test sınıfı (önerilen) | Tür | Build'i kırar mı? |
|---|---|---|---|
| (a) bench history modu (D-F) | `HistoryBenchScenarioTest` | Testcontainers (PG×2+engine+NATS) | **Yalnız** `BUS_BENCH_HISTORY_METRIC_REGRESSION` |
| (b) custody-transfer | `HistoryOutboxCustodyTransferTest` | Entegrasyon (NATS Testcontainers + embedded engine) | Hayır — invariant doğrulama |
| (c) merge-upsert çakışması | `ProjectionMergeUpsertConflictTest` | Entegrasyon (Postgres 16 Testcontainers) | Hayır — invariant doğrulama |
| (d) cutover geri-dönüş | `CutoverRollbackTest` | Entegrasyon (Postgres + NATS KV) | Hayır — invariant doğrulama |
| (e) erasure doğrulama | `ErasureVerificationFailureTest` | Entegrasyon (Postgres) | Hayır — CRITICAL-path karakterizasyonu |
| (f) kasa izolasyonu | `PseudonymVaultIsolationTest` | Entegrasyon (Postgres×2 Testcontainers) | Hayır — izolasyon doğrulama |
| (g) BA-Q7 kalibrasyon | — (runbook, kod değil) | Ölçüm-prosedürü | N/A |
| (h) relay failover | `RelayFailoverBenchScenario` | Testcontainers (tasarım; ölçüm phase5.5) | Hayır — SLI raporu |

**Toplam: 6 test-spesifikasyonu (a-f) + 1 kalibrasyon-prosedürü (g) + 1 tasarım-yalnız senaryo (h, ölçüm phase5.5'e devredildi).**
