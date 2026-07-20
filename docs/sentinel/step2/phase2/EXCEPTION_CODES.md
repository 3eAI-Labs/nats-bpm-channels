# EXCEPTION CODES — Basamak-2: History Offload (ACT_HI → NATS → async query-store)

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Sentinel fazı:** Phase 2 — Business Analyst (basamak-2)
**İlgili:** `BUSINESS_LOGIC.md` (BR-XXX), `DECISION_MATRIX.md`, `ERROR_HANDLING_GUIDELINE.md` (kategori taksonomisi), basamak-1 `docs/sentinel/phase2/EXCEPTION_CODES.md` (**devralınan taksonomi**)
**Tarih:** 2026-07-17
**Durum:** BA-Q1…8 kararları işlendi (2026-07-17); phase-review KOŞULLU ONAY + bulgular düzeltildi (`PHASE2_REVIEW.md`) — Levent faz-3 onayı bekleniyor

> Bu katalog `ERROR_HANDLING_GUIDELINE.md §1.1` kategori formatını (`{CATEGORY}_{DESCRIPTION}`: `VAL_`, `BUS_`, `RES_`, `SYS_`, `EXT_`, `AUTH_`) **basamak-1'den aynen devralır** — asenkron mesajlaşma katmanı olduğundan "HTTP Status" yerine **"Statü"** (ack/nak/DLQ/retry/reddet/build-fail vb.) kullanılır; kategori semantiği (VAL_=girdi/config hatası, BUS_=iş kuralı sonucu, RES_=varlık-durum sorunu, SYS_=sistem arızası, AUTH_=kimlik/yetki sorunu, EXT_=harici bağımlılık) korunur. Guideline §6 ilkesi aynen uygulanır: **BUS_ kodları WARN'dır** (beklenen davranış), **SYS_ kodları ERROR'dır** (mühendislik dikkati gerektirir) — istisnalar açıkça işaretlenmiştir (basamak-1'deki `BUS_BENCH_METRIC_REGRESSION` deseni burada `BUS_BENCH_HISTORY_METRIC_REGRESSION` ve `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED`/`AUTH_PSEUDONYM_VAULT_ACCESS_DENIED` için tekrarlanır — invariant ihlalleri CRITICAL).
>
> **Basamak-1'in 23 kodu bu katalogla DEĞİŞTİRİLMEZ** — A2/Flowable dispatch akışı (EPIC A-E, basamak-1) için hâlâ geçerlidir ve aynı repoda yaşamaya devam eder. Bu belge basamak-2'nin **YENİ modülü** (history offload) için katalog **EKLER**. Her kod `[07§3]`/`[07§4]` (docs/07'de doğrulanmış/reusable taban) ya da `[BA-türetildi]` (bu fazda bulunan, `BUSINESS_LOGIC.md §9` BA-Q'ya bağlı) etiketlidir.

**Kaynak (source) sözlüğü (basamak-2, 11 grup):** **Handler/Config** (composite handler + bootstrap sınıflandırma), **Outbox/Relay** (audit-kritik tx-içi outbox + leader-elected relay), **Projeksiyon-Consumer** (instance-partition + merge-upsert), **History-DLQ** (dlq.history.> runtime), **Sorgu-API** (query-store read-only servis), **Reconciliation** (dual-run fark tespiti), **Cutover** (sınıf-bazlı kademeli geçiş), **Bench** (`nats-bpm-bench` history modu), **Retention** (EPIC-G scheduled job), **Erasure** (EPIC-G silme-hakkı pipeline'ı), **Pseudonymization** (EPIC-G kimlik↔takma-ad kasası).

---

## 1. Handler/Config kaynaklı (composite `HistoryEventHandler` + bootstrap sınıflandırma)

| Kod | Statü | Tetikleyen koşul | Davranış | Idempotency etkisi | Log seviyesi | BR/FR/US | Kanıt |
|---|---|---|---|---|---|---|---|
| `VAL_HISTORY_CLASS_UNCLASSIFIED` | fail-safe-bulk + WARN | Motor upgrade ile eklenen bir `ACT_HI` sınıfı, sınıflandırma haritasında (audit-kritik/bulk) yok | Fail-safe **bulk** default uygulanır (ACT_HI yazımı motor default'unda kalır, henüz cutover kapsamına girmez) + ops-alert; kalıcı bir sınıf-eksikliği DEĞİL, config güncelleme gerektiren geçici durum | N/A (config-zamanı) | WARN | BR-HDL-007 / FR-A3 / US-A2 | `[BA-türetildi]` (BA-Q4); Karar Matrisi 1 satır 6 |
| `VAL_HISTORY_LEVEL_AUDIT_CRITICAL_MISMATCH` | deployment-time WARN (BA-Q4 KARAR 2026-07-17 — hard-reject DEĞİL) | Audit-kritik konfigüre edilmiş bir sınıf, aktif `HistoryLevel` altında hiç ÜRETİLMİYOR (örn. NONE) | Bootstrap WARN — "audit kaybı imkansız" (NFR-R1) garantisi bu HistoryLevel altında anlamsızdır; motor bootstrap'ı BLOKLANMAZ (basamak-1'in `VAL_UMBRELLA_LOCK_TOO_SHORT` hard-reject deseninden BİLİNÇLİ farklı — gerekçe: HistoryLevel motor-genel bir ayardır, yalnız history-offload'a özgü değil) | N/A (deployment-zamanı) | WARN (kalıcı, her bootstrap'ta tekrar) | BR-HDL-007 / FR-A2, FR-A3 / US-A1, US-A2 | `[07§3]` `HistoryLevel.java:56-82`, `HistoryLevelNone.java:27-39`; `[BA-türetildi]` (BA-Q4) |
| `VAL_HISTORY_STREAM_PROVISIONING_MISSING` | deployment-time ERROR (prod) / WARN (bench) | `ensureStreams()`/prod provisioning history stream'ini veya `dlq.history.>`'i sağlamıyor | Prod'da deployment engellenir; bench'te warn-only (CI'ı bloklamaz) | N/A | ERROR (prod) / WARN (bench) | BR-DBT-002 / FR-F2 / US-F2 | `07§5` borç #2; `07§1` madde 5 (CQ-6) |

---

## 2. Outbox/Relay kaynaklı (audit-kritik tx-içi outbox + leader-elected relay)

| Kod | Statü | Tetikleyen koşul | Davranış | Idempotency etkisi | Log seviyesi | BR/FR/US | Kanıt |
|---|---|---|---|---|---|---|---|
| `SYS_OUTBOX_RELAY_PUBLISH_FAILED` | retry/backoff | Relay, outbox satırını NATS'a publish etmeye çalışır ama broker erişilemez | Outbox satırı SİLİNMEZ (PubAck-öncesi-delete YASAK); retry/backoff, satır hayatta | At-least-once korunur — satır kalıcılığı garanti | WARN (bütçe dahilinde) / ERROR (uzun süredir başarısız) | BR-REL-001 / FR-B1 / US-B1 | `[07§4]` custody-transfer ilkesi |
| `SYS_OUTBOX_RELAY_LEADER_LOST` | failover (transparent) | Relay leader lease'i (`SweepLeaderLease` [07§4]) kaybeder/süresi dolar | Yeni leader lease'i devralır, kaldığı yerden devam eder — outbox satırları hayatta (NFR-R8) | Devir sırasında çift-publish RİSKİ var ama dedup (`Nats-Msg-Id`) yutar | WARN (leader-devri, rutin) | BR-REL-001 / FR-B1 / US-B1 | `07§4` reusable `SweepLeaderLease`; NFR-R8 |
| `SYS_OUTBOX_ROW_STUCK` | ops-alert | Outbox satırı, normal relay-döngü gecikmesinin bir katı (eşik `[BA-türetildi]`, BA-Q7 — phase3/4'te kalibre) kadar bekliyor — relay/leader stuck şüphesi | Ops alert; DP-12 "kısa maruziyet" varsayımı ihlal ediliyor olabilir; audit veri KAYBOLMAZ (satır hâlâ orada) ama maruziyet penceresi UZAR | Zararsız (satır durumu değişmedi) — yalnız gözlemlenebilirlik sinyali | WARN → ERROR (eşik aşımı sürerse) | BR-REL-001 (BA-Q7) / FR-B1 / US-B1 | `[BA-türetildi]`; DP-12 |
| `BUS_OUTBOX_DUPLICATE_RELAY_DELIVERY` *(bilgilendirici, hata değil)* | no-op | Aynı outbox satırı relay retry (leader-devri veya PubAck-timeout) nedeniyle iki kez publish edilir | Downstream dedup (`Nats-Msg-Id`) yutar — projeksiyonda tek satır | At-least-once'in doğal/beklenen sonucu | DEBUG/INFO | BR-REL-001 / FR-B1 / US-B1 | `07§1` madde 5 (D-E dedup) |

---

## 3. Projeksiyon-Consumer kaynaklı (instance-partition + merge-upsert)

| Kod | Statü | Tetikleyen koşul | Davranış | Idempotency etkisi | Log seviyesi | BR/FR/US | Kanıt |
|---|---|---|---|---|---|---|---|
| `BUS_PROJECTION_STALE_EVENT_DISCARDED` *(beklenen durum, hata değil)* | no-op | Merge-upsert, gelen event'in sıra-anahtarının mevcut projeksiyon satırından ESKİ/EŞİT olduğunu tespit eder | Event DISCARD edilir, mevcut state EZİLMEZ — bu, NFR-R4'ün güvenlik ağının TASARLANMIŞ sonucudur | Güvenlik ağı doğru çalışıyor — hata değil | DEBUG (rutin) | BR-REL-002 / FR-B2 / US-B2 | `07§1` madde 5 (D-E merge-upsert) |
| `BUS_MERGE_UPSERT_CONFLICT_AMBIGUOUS` | WARN + tie-break | Gelen event'in sıra-anahtarı mevcut satırla EŞİT/belirsiz (aynı anda birden fazla kaynaktan çakışan yazım) | WARN + tie-break kuralı (NATS stream-sequence — BA-Q1 KARAR 2026-07-17) uygulanır; uygulama detayı phase3/4 LLD'de | Potansiyel çift-belirsizlik — izlenmeli | WARN | BR-REL-006 (BA-Q1) / FR-B2 / US-B2 | `[BA-türetildi]`; SRS §2.5 "merge-upsert çatışma-çözüm kenar durumları phase3'te doğrulanacak" |
| `SYS_PROJECTION_WRITE_FAILED` | nak, redelivery | Projeksiyon Postgres yazımı başarısız (DB down, deadlock, constraint ihlali) | JetStream redelivery; consumer idempotent olduğundan tekrar deneme güvenli | At-least-once — retry idempotent | ERROR | BR-REL-002 / FR-B2 / US-B2 | `07§1` madde 2 (D-B ayrı Postgres) |
| `SYS_PROJECTION_SCHEMA_DRIFT` | consumer durur + ERROR | Gelen mesaj, asyncapi-kontratına (ADR-0006 [07§4]) uymuyor (beklenmeyen alan/tip) | Consumer bu mesajı işlemeyi reddeder — DLQ'ya yönlenir (Matris 5); şema-drift kalıcıysa consumer geneli durur, deploy-uyumsuzluğu işaret eder | Redelivery işe yaramaz (sürekli aynı hata) — DLQ nihai varış | ERROR | BR-REL-002, BR-REL-004 / FR-B2, FR-B4 / US-B2, US-B4 | `07§4` reusable ADR-0006 (contract-first) |

---

## 4. History-DLQ kaynaklı (`dlq.history.>` runtime davranışı)

| Kod | Statü | Tetikleyen koşul | Davranış | Idempotency etkisi | Log seviyesi | BR/FR/US | Kanıt |
|---|---|---|---|---|---|---|---|
| `BUS_HISTORY_DELIVERY_BUDGET_EXCEEDED` | DLQ-route | `deliveryCount > maxDeliver` (projeksiyon consumer'ın in-band tespiti) | `dlq.history.<...>`'a header-korumalı byte-ayna kopya → orijinal mesaj ACK | Tek-seferlik geçiş (DLQ'ya bir kez düşer) | WARN | BR-REL-005 / FR-B5 / US-B5 | `07§1` madde 5 (D-E); `[07§4]` basamak-1 DLQ substratı |
| `SYS_HISTORY_DLQ_PUBLISH_FAILED` | nak + alert | DLQ publish (JetStream) başarısız | Custody-transfer korunur: nak + alert, **asla ack-drop** (basamak-1 "dlq-of-dlq YOK" ilkesi aynen) | Redelivery'de tekrar denenir | ERROR | BR-REL-005 / FR-B5 / US-B5 | `[07§4]` custody-transfer ilkesi |
| `RES_HISTORY_DLQ_ACCESS_DENIED` | reddedilir | DLQ payload'ına (PII yüzeyi, DP-13) yetkisiz erişim denemesi | Erişim reddedilir, security-log | N/A | WARN (güvenlik logu) | BR-REL-005 / FR-B5 / US-B5 | `DATA_CLASSIFICATION.md` DP-13 |

---

## 5. Sorgu-API kaynaklı (read-only query-store servisi)

| Kod | Statü | Tetikleyen koşul | Davranış | Idempotency etkisi | Log seviyesi | BR/FR/US | Kanıt |
|---|---|---|---|---|---|---|---|
| `VAL_QUERY_UNSUPPORTED_PATTERN` | reddedilir (400-eşdeğeri) | İstek çekirdek-4 okuma desenlerinden (PO-Q3) birine uymuyor (örn. agregasyon/analitik/raporlama) | İstek reddedilir; kapsam-dışı olduğu (SRS §7) mesajla belirtilir | N/A | WARN | BR-QRY-001 / FR-C1 / US-C1 | `07§1` madde 3 (D-C); PO-Q3; SRS §7 |
| `AUTH_QUERY_ACCESS_DENIED` | reddedilir (403-eşdeğeri) | Role-based erişim kontrolü başarısız | İstek reddedilir | N/A | WARN | BR-QRY-001 / FR-C1 / US-C1 | `DATA_CLASSIFICATION.md` DP-15 |
| `BUS_QUERY_PII_MASKED` *(bilgilendirici, hata değil)* | yanıt-maskeli | Yanıt RESTRICTED/PII alan içeriyor, istek sahibinin PII-görme izni yok | Alan maskelenir (DP-15); loglara PII DEĞERİ yazılmaz (DP-1 devralınan) | N/A | DEBUG/INFO | BR-QRY-001 / FR-C1 / US-C1 | `DATA_CLASSIFICATION.md` DP-1, DP-15 |
| `RES_HISTORY_INSTANCE_NOT_FOUND` *(beklenen durum, hata değil)* | bulunamadı (404-eşdeğeri) | Çekirdek-4 sorgusunun hedef instance/kaydı projeksiyonda yok (hiç yazılmadı, retention ile silindi ya da erasure sonrası) | Boş/404 yanıt; PII sızdırmayan mesaj. Retention/erasure sonrası "yokluk" MEŞRU sonuçtur (US-G1/G2) | N/A | DEBUG/INFO | BR-QRY-001 / FR-C1 / US-C1 | phase3 `api/openapi.yaml` 404 yanıtı (phase3-review F-001 ile registry'ye eklendi) |

---

## 6. Reconciliation kaynaklı (dual-run fark tespiti)

| Kod | Statü | Tetikleyen koşul | Davranış | Idempotency etkisi | Log seviyesi | BR/FR/US | Kanıt |
|---|---|---|---|---|---|---|---|
| `SYS_RECONCILIATION_JOB_FAILED` | log-only, döngü atlanır | Reconciliation job'ı DB okuma hatasıyla (projeksiyon veya ACT_HI tarafı) başarısız olur | Log ERROR; bir sonraki periyotta tekrar denenir; dual-run/cutover engellenmez ama o döngü için streak GÜNCELLENMEZ | Etkilenen sınıflar bir döngü "veri yok" sayılır (streak ilerlemez, geriye de gitmez) | ERROR | BR-CUT-001 / FR-D1 / US-D1 | `07§1` madde 3 (D-C reconciliation) |
| `BUS_RECONCILIATION_DIFF_DETECTED` | streak-reset | Sınıf-başına projeksiyon ↔ ACT_HI fark sayısı > 0 (audit-kritik) veya > epsilon/artan-trend (bulk, BA-Q2) | "N gün temiz" sayacı SIFIRLANIR; cutover kapısı kapalı kalır | Beklenen dual-run davranışı — hata değil, sinyal | WARN | BR-CUT-001, BR-CUT-004 (BA-Q2) / FR-D1 / US-D1 | `07§1` madde 7 (D-F fark sayacı) |
| `RES_RECONCILIATION_DIFF_THRESHOLD_EXCEEDED` | ops-alert | Fark sayısı/trend süreklilik gösteriyor (tek olay değil, tekrarlayan) | Ops alert — gerçek drift/config sorunu şüphesi (yalnız beklenen at-most-once kaybı değil) | N/A | WARN → ERROR (süreklilik) | BR-CUT-001 / FR-D1 / US-D1 | `07§1` madde 7 (D-F) |
| `VAL_RECONCILIATION_WINDOW_N_INVALID` | config reddedilir | Sınıf-başına konfigüre `N` (temiz-gün) ≤0 veya sağduyu-dışı bir değer | Config reddedilir, default (7g, PO-Q4) korunur | N/A (config-zamanı) | ERROR (config reddi) | BR-CUT-002 / FR-D1 / US-D1, US-D2 | PO-Q4 kararı |

---

## 7. Cutover kaynaklı (sınıf-bazlı kademeli geçiş)

| Kod | Statü | Tetikleyen koşul | Davranış | Idempotency etkisi | Log seviyesi | BR/FR/US | Kanıt |
|---|---|---|---|---|---|---|---|
| `BUS_CUTOVER_GATE_NOT_MET` | reddedilir | Cutover manuel talep edilir ama sınıf "N gün temiz" kriterini karşılamıyor | Talep reddedilir; reconciliation'ın devam etmesi gerektiği bildirilir | N/A | WARN | BR-CUT-002 / FR-D2 / US-D2 | `07§1` madde 3 (D-C kademeli) |
| `SYS_CUTOVER_CONFIG_APPLY_FAILED` | retry + alert | Kapı açık, config-flip (`enableDefaultDbHistoryEventHandler=false`) deployment/apply anında başarısız | Dual-run DEVAM eder (fail-safe — DB handler açık kalır, veri kaybı riski yok); alert, yeniden denenir | N/A | ERROR | BR-CUT-002 / FR-D2 / US-D2 | `07§1` madde 3 (D-C) |
| `BUS_CUTOVER_ROLLBACK_TRIGGERED` *(operasyonel, hata değil)* | audit-logged | Operatör cutover'lanmış bir sınıfı geri açar (`enableDefaultDbHistoryEventHandler=true`) | DB handler yeniden etkinleşir, dual-run yeniden başlar; Cockpit-history geri gelir (US-C2); reconciliation streak SIFIRDAN | Yeniden-cutover için streak geçmişi SAYILMAZ | INFO (operatör-tetikli, planlı) | BR-CUT-003 / FR-D3 / US-D3 | `07§1` madde 3 (D-C — geri-dönüş=konfig); NFR-R7 |

---

## 8. Bench kaynaklı (`nats-bpm-bench` history modu)

| Kod | Statü | Tetikleyen koşul | Davranış | Idempotency etkisi | Log seviyesi | BR/FR/US | Kanıt |
|---|---|---|---|---|---|---|---|
| `BUS_BENCH_HISTORY_METRIC_REGRESSION` | **build-fail (sert kapı)** | Normalize DB yazım-op metriği (US-E1) hedefi kaçırır: cutover'lanan sınıfta `ACT_HI` bileşeni > 0, VEYA audit-kritik outbox bileşeni > 1/tx | Bench raporu FAIL; **basamak-2 kapanışının TEK sert kabul kapısı** (D-F/PO-Q7 ilkesi — reconciliation-temizliği ayrı, cutover kapısıdır) | N/A (test-zamanı) | ERROR (bench raporunda) | BR-OBS-001 / FR-E1 / US-E1 | `07§1` madde 7 (D-F) |
| `SYS_BENCH_HISTORY_ENVIRONMENT_UNAVAILABLE` | build-warn-only (FAIL etmez) | Testcontainers (projeksiyon PG + engine + NATS) ayağa kalkamaz | Bench koşusu inconclusive; ana CI build'i BLOKLAMAZ (`@Tag("bench")`, nightly/manuel) | N/A | WARN | BR-OBS-003 / FR-E3 / US-E3 | `07§4` reusable `nats-bpm-bench` |
| `SYS_BENCH_HISTORY_SLI_DRIFT` | build-warn-only | Destekleyici SLI hedefi kaçırılır (projeksiyon gecikmesi p95, reconciliation fark sayacı) | Rapor edilir, regresyon işaretlenir; kapanışı BLOKLAMAZ (SLI, sert kapı değil — NFR-P3) | N/A | WARN | BR-OBS-002 / FR-E2 / US-E2 | `07§1` madde 7 (D-F) |
| `BUS_BENCH_BASELINE_MISSING` | build-warn-only | Basamak-1'in bench İLK GERÇEK KOŞU baseline'ı (US-F1) henüz üretilmemiş | History-modu regresyon oranı hesaplanamaz; bench inconclusive işaretlenir, US-F1'in önce koşması gerektiği bildirilir | N/A | WARN | BR-DBT-001, BR-OBS-003 / FR-F1, FR-E3 / US-F1, US-E3 | `07§5` borç #7 |

---

## 9. Retention kaynaklı (EPIC-G — scheduled job)

| Kod | Statü | Tetikleyen koşul | Davranış | Idempotency etkisi | Log seviyesi | BR/FR/US | Kanıt |
|---|---|---|---|---|---|---|---|
| `SYS_RETENTION_JOB_FAILED` | log-only, döngü atlanır | Scheduled retention job DB hatasıyla başarısız olur | Log ERROR; bir sonraki periyotta tekrar denenir | Zararsız — satırlar bir periyot gecikmeli silinir | ERROR | BR-PII-001 / FR-G1 / US-G1 | `DATA_GOVERNANCE v4.0 §4.4` |
| `BUS_RETENTION_WINDOW_BREACH_DETECTED` *(beklenen durum, hata değil)* | silme uygulanır | Satır yaşı, sınıf-başına retention penceresini (bulk 90g / audit-kritik yasal-saklama) aşıyor | Silme uygulanır + audit-log kaydı üretilir | Otomatik, periyodik — tekrar tetiklenmez (satır silindi) | INFO | BR-PII-001 / FR-G1 / US-G1 | PO-Q7 kararı |
| `VAL_RETENTION_OVERRIDE_BELOW_LEGAL_MINIMUM` | reddedilir | Kiracı, audit-kritik sınıf retention'ını yasal-saklama asgarisinin altına çekmeye çalışır | Config reddedilir; hukuki/DPO onayı olmadan uygulanmaz | N/A (config-zamanı) | ERROR (config reddi) | BR-PII-001 / FR-G1 / US-G1 | `DATA_GOVERNANCE v4.0 §4.2`; `KVKK v1.0 §4.2` |
| `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED` | **CRITICAL — on-call page** | Retention silmesi BAŞARILI oldu ama audit-log yazımı BAŞARISIZ | **Compliance-invariant ihlali:** silme oldu, izi yok. Guideline §4.4 "her silme için audit-log kaydı" şartını ihlal eder. Otomatik ack/devam YERİNE anında insan müdahalesi (on-call page) gerekir | N/A — kök nedene kadar tekrar tetiklenmemeli | **CRITICAL (ERROR + page)** — `SYS_SENTINEL_WORKER_CONFLICT` (basamak-1) ile aynı ciddiyet sınıfı | BR-PII-001 (`[BA-türetildi]`) / FR-G1 / US-G1 | `[BA-türetildi]`; `DATA_GOVERNANCE v4.0 §4.4` |

---

## 10. Erasure kaynaklı (EPIC-G — KVKK/GDPR silme-hakkı pipeline'ı)

| Kod | Statü | Tetikleyen koşul | Davranış | Idempotency etkisi | Log seviyesi | BR/FR/US | Kanıt |
|---|---|---|---|---|---|---|---|
| `BUS_ERASURE_REQUEST_ACCEPTED` *(bilgilendirici)* | pipeline tetiklenir | Data-subject silme-hakkı talebi bulk sınıf PII'sini hedefliyor, subject-key net | Soft-delete → anonimleştirme pipeline'ı başlar | N/A | INFO | BR-PII-002 / FR-G2 / US-G2 | PO-Q2 katman-2 |
| `BUS_ERASURE_REQUEST_LEGAL_HOLD_BLOCKED` *(beklenen davranış, hata değil)* | reddedilir | Talep audit-kritik sınıfı (OP_LOG/INCIDENT/EXT_TASK_LOG) hedefliyor | Erasure REDDEDİLİR; yasal-saklama istisnası (§6 katman-1) gerekçesiyle; pseudonymization alternatifi (opt-in ise) sunulur | N/A | WARN (talep sahibine bildirilir) | BR-PII-002, BR-PII-003 / FR-G2, FR-G3 / US-G2, US-G3 | `DATA_CLASSIFICATION.md §6` katman-1 |
| `VAL_ERASURE_SUBJECT_KEY_AMBIGUOUS` | kapsam-onayı istenir | Subject-key (businessKey/userId) birden fazla döneme/instance kümesine karşılık geliyor (telco kimlik-yeniden-kullanımı, BA-Q6) | Pipeline OTOMATİK tetiklenmez; aday instance/zaman-aralığı listesi talep sahibine sunulur, açık onay istenir | N/A | WARN | BR-PII-005 (BA-Q6) / FR-G2 / US-G2 | `[BA-türetildi]` |
| `SYS_ERASURE_PIPELINE_FAILED` | retry + alert | Anonimleştirme SQL/adımı teknik olarak başarısız (DB hatası) | Retry; audit-log(başarısızlık) üretilir; kalıcı başarısızlık alert'e yükseltilir | Adım tekrar denenebilir (idempotent SQL varsayılır — phase3/4 detayı) | ERROR | BR-PII-002 / FR-G2 / US-G2 | `DATA_GOVERNANCE v4.0 §4.4` |
| `RES_ERASURE_VERIFICATION_FAILED` | **CRITICAL — compliance-risk** | Erasure sonrası doğrulama sorgusu Sorgu-API'de HÂLÂ ilgili PII'yi döndürüyor | Pipeline eksik/yarım tamamlanmış — CRITICAL, KVKK 30 günlük SLA riski (`KVKK v1.0 §2.1`) | N/A — kök nedene kadar tekrar denenmeli | **CRITICAL (ERROR + alert)** | BR-PII-002 / FR-G2 / US-G2 | `KVKK v1.0 §2.1` (30g SLA) |

---

## 11. Pseudonymization kaynaklı (EPIC-G — kimlik↔takma-ad kasası)

| Kod | Statü | Tetikleyen koşul | Davranış | Idempotency etkisi | Log seviyesi | BR/FR/US | Kanıt |
|---|---|---|---|---|---|---|---|
| `BUS_PSEUDONYMIZATION_APPLIED` *(bilgilendirici)* | no-op (başarı) | Audit-kritik event, kiracı pseudonymization opt-in seçmiş; userId tx-içi saf hesapla takma-ada çevrilir (BA-Q5) | Projeksiyon satırında userId yerine takma-ad görünür; kasa-yazımı downstream/async (BR-PII-004) | N/A | INFO | BR-PII-003, BR-PII-004 (BA-Q5) / FR-G3 / US-G3 | PO-Q2 katman-3; `[BA-türetildi]` (BA-Q5) |
| `SYS_PSEUDONYM_VAULT_UNAVAILABLE` | downstream retry | Kasa store (ayrı depo, L4-bitişik) erişilemez, harita-satırı yazımı (downstream) başarısız | Downstream retry; **audit-kritik outbox/relay/NATS akışı ENGELLENMEZ** — pseudonym-değeri zaten tx-içi hesaplanmıştı, yalnız kasa-persist gecikir (D-A ilkesinin BA-genişlemesi, BA-Q5) | Retry idempotent (aynı harita satırı yeniden yazılabilir) | ERROR | BR-PII-004 (BA-Q5) / FR-G3 / US-G3 | `[BA-türetildi]` |
| `AUTH_PSEUDONYM_VAULT_ACCESS_DENIED` | **CRITICAL — security-page** | Kasaya (re-identification anahtarı taşıyan, L4-bitişik) yetkisiz erişim denemesi | Erişim reddedilir; **anında güvenlik müdahalesi** gerekir — kasa erişimi en-az-yetki + audit ilkesiyle korunur (DP-16) | N/A | **CRITICAL (ERROR + security-page)** — `SYS_SENTINEL_WORKER_CONFLICT` (basamak-1) ile aynı ciddiyet sınıfı | BR-PII-003 / FR-G3 / US-G3 | `DATA_CLASSIFICATION.md` DP-16 |
| `BUS_PSEUDONYM_MAP_ENTRY_DELETED` *(erasure-via-vault, bilgilendirici)* | audit-logged | Data-subject silme-hakkı talebi, pseudonymized audit-kritik kayda karşı, kasa harita-kaydının silinmesiyle karşılanır | Harita-kaydı silinir → takma-ad TERSİNMEZ olur; OP_LOG/INCIDENT satırının kendisi (denetim yapısı) KORUNUR, retention süresi DEĞİŞMEZ (BA-Q8) | Tek-seferlik, tersinmez | INFO (audit-log zorunlu) | BR-PII-003 / FR-G3 / US-G3 | PO-Q2 katman-3; DP-16 |

---

## 12. Kategori özeti (basamak-2 eklentisi)

| Kategori | Kod sayısı | Anlamı (guideline §1.1) |
|---|---|---|
| `VAL_` | 7 | Girdi/config biçim hatası (sınıflandırma eksik, HistoryLevel uyumsuzluğu, stream provisioning eksik, N-geçersiz, subject-key belirsiz, retention-asgari-altı, sorgu-deseni-desteklenmiyor) |
| `BUS_` | 15 | İş kuralı sonucu — **beklenen davranış**, çoğunlukla WARN/INFO (bench-baseline-missing, bench-history-metric-regression, cutover-gate-not-met, cutover-rollback-triggered, erasure-accepted, erasure-legal-hold-blocked, history-delivery-budget-exceeded, merge-conflict-ambiguous, duplicate-relay-delivery, stale-event-discarded, pseudonym-applied, pseudonym-map-deleted, query-pii-masked, reconciliation-diff-detected, retention-breach-detected — 15 kod, 9'u "hata değil, bilgilendirici" açıkça işaretli) — *not: `BUS_BENCH_HISTORY_METRIC_REGRESSION` basamak-1 deseniyle aynı: kabul-kapısı ihlali → ERROR + build-fail, "BUS_=WARN" kuralının bilinçli istisnası* |
| `RES_` | 4 | Varlık-durum sorunu (DLQ-erişim-reddi, reconciliation-eşik-aşımı, erasure-doğrulama-başarısız, history-instance-bulunamadı [F-001 eklentisi, bilgilendirici]) — *not: `RES_ERASURE_VERIFICATION_FAILED` CRITICAL, kategori RES_ olsa da compliance-risk nedeniyle yükseltilmiş* |
| `SYS_` | 14 | Sistem/altyapı arızası — çoğunlukla ERROR (relay-publish/leader/stuck ×3, projeksiyon-write/schema-drift ×2, history-dlq-publish, reconciliation-job, cutover-config-apply, retention-job, erasure-pipeline, pseudonym-vault-unavailable, bench-environment/sli-drift ×2) — *not: `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED` CRITICAL (compliance-invariant); `SYS_BENCH_HISTORY_ENVIRONMENT_UNAVAILABLE`/`SYS_BENCH_HISTORY_SLI_DRIFT`/`SYS_OUTBOX_RELAY_LEADER_LOST` WARN'dır, kategori SYS_ olsa da build/akışı bloklamaz* |
| `AUTH_` | 2 | Kimlik/yetki sorunu (sorgu-API erişim reddi, kasa erişim reddi) — *not: `AUTH_PSEUDONYM_VAULT_ACCESS_DENIED` CRITICAL + security-page, `SYS_SENTINEL_WORKER_CONFLICT` (basamak-1) ile aynı ciddiyet sınıfı* |

**Toplam: 42 exception/durum kodu** (11 kaynak grubu: Handler/Config=3, Outbox/Relay=4, Projeksiyon-Consumer=4, History-DLQ=3, Sorgu-API=4, Reconciliation=4, Cutover=3, Bench=4, Retention=4, Erasure=5, Pseudonymization=4 → 3+4+4+3+4+4+3+4+4+5+4=42; `RES_HISTORY_INSTANCE_NOT_FOUND` phase3-review F-001 ile eklendi). Bunlardan **10 kodu** açıkça **"beklenen durum/bilgilendirici, hata değil"** olarak etiketlenmiştir (`BUS_OUTBOX_DUPLICATE_RELAY_DELIVERY`, `BUS_PROJECTION_STALE_EVENT_DISCARDED`, `BUS_QUERY_PII_MASKED`, `BUS_CUTOVER_ROLLBACK_TRIGGERED`, `BUS_RETENTION_WINDOW_BREACH_DETECTED`, `BUS_ERASURE_REQUEST_ACCEPTED`, `BUS_ERASURE_REQUEST_LEGAL_HOLD_BLOCKED`, `BUS_PSEUDONYMIZATION_APPLIED`, `BUS_PSEUDONYM_MAP_ENTRY_DELETED`, `RES_HISTORY_INSTANCE_NOT_FOUND` — ERROR_HANDLING_GUIDELINE §6 ilkesi: "Errors are not exceptions to normal flow — they ARE normal flow"). **3 kod CRITICAL/security-page seviyesindedir** (`SYS_RETENTION_AUDIT_LOG_WRITE_FAILED`, `RES_ERASURE_VERIFICATION_FAILED`, `AUTH_PSEUDONYM_VAULT_ACCESS_DENIED`) — hepsi compliance-invariant ihlalleri, basamak-1'in `SYS_SENTINEL_WORKER_CONFLICT` (BAQ-7) ciddiyet sınıfıyla eşdeğer.

**Basamak-1 ile birlikte repo-geneli toplam: 23 (basamak-1) + 42 (basamak-2) = 65 exception/durum kodu.**

---

## 13. İzlenebilirlik özeti (Kod → BR → FR → US)

| Kod | BR | FR | US |
|---|---|---|---|
| VAL_HISTORY_CLASS_UNCLASSIFIED | BR-HDL-007 | FR-A3 | US-A2 |
| VAL_HISTORY_LEVEL_AUDIT_CRITICAL_MISMATCH | BR-HDL-007 | FR-A2, FR-A3 | US-A1, US-A2 |
| VAL_HISTORY_STREAM_PROVISIONING_MISSING | BR-DBT-002 | FR-F2 | US-F2 |
| SYS_OUTBOX_RELAY_PUBLISH_FAILED | BR-REL-001 | FR-B1 | US-B1 |
| SYS_OUTBOX_RELAY_LEADER_LOST | BR-REL-001 | FR-B1 | US-B1 |
| SYS_OUTBOX_ROW_STUCK | BR-REL-001 | FR-B1 | US-B1 |
| BUS_OUTBOX_DUPLICATE_RELAY_DELIVERY | BR-REL-001 | FR-B1 | US-B1 |
| BUS_PROJECTION_STALE_EVENT_DISCARDED | BR-REL-002 | FR-B2 | US-B2 |
| BUS_MERGE_UPSERT_CONFLICT_AMBIGUOUS | BR-REL-006 | FR-B2 | US-B2 |
| SYS_PROJECTION_WRITE_FAILED | BR-REL-002 | FR-B2 | US-B2 |
| SYS_PROJECTION_SCHEMA_DRIFT | BR-REL-002, BR-REL-004 | FR-B2, FR-B4 | US-B2, US-B4 |
| BUS_HISTORY_DELIVERY_BUDGET_EXCEEDED | BR-REL-005 | FR-B5 | US-B5 |
| SYS_HISTORY_DLQ_PUBLISH_FAILED | BR-REL-005 | FR-B5 | US-B5 |
| RES_HISTORY_DLQ_ACCESS_DENIED | BR-REL-005 | FR-B5 | US-B5 |
| VAL_QUERY_UNSUPPORTED_PATTERN | BR-QRY-001 | FR-C1 | US-C1 |
| AUTH_QUERY_ACCESS_DENIED | BR-QRY-001 | FR-C1 | US-C1 |
| BUS_QUERY_PII_MASKED | BR-QRY-001 | FR-C1 | US-C1 |
| SYS_RECONCILIATION_JOB_FAILED | BR-CUT-001 | FR-D1 | US-D1 |
| BUS_RECONCILIATION_DIFF_DETECTED | BR-CUT-001, BR-CUT-004 | FR-D1 | US-D1 |
| RES_RECONCILIATION_DIFF_THRESHOLD_EXCEEDED | BR-CUT-001 | FR-D1 | US-D1 |
| VAL_RECONCILIATION_WINDOW_N_INVALID | BR-CUT-002 | FR-D1 | US-D1, US-D2 |
| BUS_CUTOVER_GATE_NOT_MET | BR-CUT-002 | FR-D2 | US-D2 |
| SYS_CUTOVER_CONFIG_APPLY_FAILED | BR-CUT-002 | FR-D2 | US-D2 |
| BUS_CUTOVER_ROLLBACK_TRIGGERED | BR-CUT-003 | FR-D3 | US-D3 |
| BUS_BENCH_HISTORY_METRIC_REGRESSION | BR-OBS-001 | FR-E1 | US-E1 |
| SYS_BENCH_HISTORY_ENVIRONMENT_UNAVAILABLE | BR-OBS-003 | FR-E3 | US-E3 |
| SYS_BENCH_HISTORY_SLI_DRIFT | BR-OBS-002 | FR-E2 | US-E2 |
| BUS_BENCH_BASELINE_MISSING | BR-DBT-001, BR-OBS-003 | FR-F1, FR-E3 | US-F1, US-E3 |
| SYS_RETENTION_JOB_FAILED | BR-PII-001 | FR-G1 | US-G1 |
| BUS_RETENTION_WINDOW_BREACH_DETECTED | BR-PII-001 | FR-G1 | US-G1 |
| VAL_RETENTION_OVERRIDE_BELOW_LEGAL_MINIMUM | BR-PII-001 | FR-G1 | US-G1 |
| SYS_RETENTION_AUDIT_LOG_WRITE_FAILED | BR-PII-001 | FR-G1 | US-G1 |
| BUS_ERASURE_REQUEST_ACCEPTED | BR-PII-002 | FR-G2 | US-G2 |
| BUS_ERASURE_REQUEST_LEGAL_HOLD_BLOCKED | BR-PII-002, BR-PII-003 | FR-G2, FR-G3 | US-G2, US-G3 |
| VAL_ERASURE_SUBJECT_KEY_AMBIGUOUS | BR-PII-005 | FR-G2 | US-G2 |
| SYS_ERASURE_PIPELINE_FAILED | BR-PII-002 | FR-G2 | US-G2 |
| RES_ERASURE_VERIFICATION_FAILED | BR-PII-002 | FR-G2 | US-G2 |
| BUS_PSEUDONYMIZATION_APPLIED | BR-PII-003, BR-PII-004 | FR-G3 | US-G3 |
| SYS_PSEUDONYM_VAULT_UNAVAILABLE | BR-PII-004 | FR-G3 | US-G3 |
| AUTH_PSEUDONYM_VAULT_ACCESS_DENIED | BR-PII-003 | FR-G3 | US-G3 |
| BUS_PSEUDONYM_MAP_ENTRY_DELETED | BR-PII-003 | FR-G3 | US-G3 |

**Sonuç:** 42/42 kod, sıfır izlenebilirlik-dışı (traceless) kod yok — hepsi bir BR üzerinden bir US'ye bağlanır.

---

## 14. BA-QUESTION karar referansı (KARARA BAĞLANDI 2026-07-17)

Bu katalogdaki 9 kodun davranışı `BUSINESS_LOGIC.md §9`'daki BA-Q1…8 karar kaydına bağlıdır — **Levent 2026-07-17'de 8/8'ini önerilen seçenekle karara bağladı**; aşağıdaki davranışlar artık KARARDIR:

- `BUS_MERGE_UPSERT_CONFLICT_AMBIGUOUS` → **BA-Q1:** tie-break = NATS stream-sequence
- `BUS_RECONCILIATION_DIFF_DETECTED` (bulk sınıf yorumu) → **BA-Q2:** audit-kritik=mutlak-sıfır, bulk=epsilon+trend
- (Sorgu-API kapsam yorumu, ayrı kod değil, BR-QRY-003 davranışı) → **BA-Q3:** projeksiyondaki tüm sınıflar sunulsun (cutover-bağımsız)
- `VAL_HISTORY_LEVEL_AUDIT_CRITICAL_MISMATCH` → **BA-Q4:** WARN (hard-reject değil)
- `SYS_PSEUDONYM_VAULT_UNAVAILABLE`, `BUS_PSEUDONYMIZATION_APPLIED` → **BA-Q5:** pseudonym-değeri tx-içi saf hesap, kasa-persist downstream/async
- `VAL_ERASURE_SUBJECT_KEY_AMBIGUOUS` → **BA-Q6:** kapsam-onayı akışı (bare key-match değil)
- `SYS_OUTBOX_ROW_STUCK` → **BA-Q7:** eşik = relay-döngü-gecikmesinin katı (nicel değer phase3/4 kalibrasyonu)
- `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED` / `BUS_PSEUDONYM_MAP_ENTRY_DELETED` (retention süresi etkileşimi) → **BA-Q8:** pseudonymization retention süresini KISALTMAZ

Tam soru metinleri ve gerekçeler: `BUSINESS_LOGIC.md §9` (Q→öneri formatı).

---

*İlgili: `BUSINESS_LOGIC.md` (BR-XXX kataloğu, BA-QUESTIONS §9), `DECISION_MATRIX.md` (bu kodların üretildiği karar noktaları), basamak-1 `docs/sentinel/phase2/EXCEPTION_CODES.md` (devralınan taksonomi, 23 kod, DEĞİŞTİRİLMEDİ).*
