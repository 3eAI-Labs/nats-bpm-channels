# DECISION MATRIX — Basamak-2: History Offload (ACT_HI → NATS → async query-store)

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Sentinel fazı:** Phase 2 — Business Analyst (basamak-2)
**İlgili:** `BUSINESS_LOGIC.md` (BR-XXX kataloğu, süreç akışları, durum makineleri), `EXCEPTION_CODES.md`
**Tarih:** 2026-07-17
**Durum:** BA-Q1…8 kararları işlendi (2026-07-17) — phase-review bekliyor

> BA Guideline §2.2 gereği "if-this-then-that" mantığı burada tablo olarak sabitlenir — düz metin YASAK. Her satır bir BR/FR/US'ye bağlıdır. Kanıtlar `[07§3]` (docs/07 §3/§7'de DOĞRULANMIŞ, bu fazda taşındı), `[07§4]` (basamak-1'den reusable, docs/07 §4) ya da `[BA-türetildi]` (bu fazda bulunan kenar-durum, `BUSINESS_LOGIC.md §9` BA-Q'ya bağlı) etiketlidir. Görev talimatı gereği asgari altı ana matris + üç destekleyici matris üretilmiştir.

---

## Matris 1 — Event-sınıfı → tutarlılık yolu / yönlendirme (D-A/D-D)

| # | Girdi: ACT_HI sınıfı | Girdi: sınıflandırma config | Girdi: hacim | Aksiyon: tutarlılık yolu | Aksiyon: DB yazımı (cutover öncesi) | Aksiyon: DB yazımı (cutover sonrası) | BR/FR | Kanıt |
|---|---|---|---|---|---|---|---|---|
| 1 | OP_LOG, INCIDENT, EXT_TASK_LOG | audit-kritik (default, PO-Q5) | düşük | tx-içi kompakt outbox + relay (at-least-once) | tam ACT_HI satırı + ≤1 outbox satırı | yalnız ≤1 outbox satırı (ACT_HI=0) | BR-HDL-002/003 / FR-A3/A4 | `07§1` madde 1 (D-A); PO-Q5 |
| 2 | DETAIL, VARINST, ACTINST | bulk (default) | yüksek (~%90) | post-commit publish (at-most-once) | tam ACT_HI satırı + publish | publish yalnız (ACT_HI=0) | BR-HDL-002/004 / FR-A3/A5 | `07§1` madde 1 (D-A) |
| 3 | PROCINST, TASKINST, JOB_LOG, TASK LOG/DECINST/CASEINST/BATCH | bulk (default) | orta-yüksek | post-commit publish | tam ACT_HI satırı + publish | publish yalnız | BR-HDL-002/004 | `DATA_CLASSIFICATION.md §2.1` |
| 4 | IDENTITYLINK, COMMENT, ATTACHMENT | bulk (default, **PO-Q5 bilinçli kararı**) | düşük-orta, PII-yoğun | post-commit publish — **PII-yoğunluk TEK BAŞINA audit-kritik gerekçesi DEĞİL** | tam ACT_HI satırı + publish | publish yalnız; PII koruması EPIC-G retention/erasure'dan gelir | BR-HDL-002 / FR-A3 | PO-Q5 kararı (2026-07-17); `TENANT_PII_CHECKLIST §2.1` |
| 5 | Herhangi bir sınıf | Kiracı IDENTITYLINK/COMMENT/ATTACHMENT'ı audit-kritik'e taşımış (override) | — | tx-içi kompakt outbox + relay (satır 1 ile aynı) | — | — | BR-HDL-002 | `TENANT_PII_CHECKLIST §2.1` rehberi |
| 6 | **[BA-türetildi]** Motor upgrade ile eklenen, sınıflandırma haritasında OLMAYAN yeni sınıf | tanımsız | bilinmiyor | fail-safe **bulk** default + WARN + ops-alert | tam ACT_HI satırı (motor default) | — (henüz cutover kapsamı dışı, kapsamı-yok) | BR-HDL-007 (BA-Q4) | `[BA-türetildi]` |
| 7 | Herhangi bir audit-kritik sınıf | HistoryLevel event'i ÜRETMİYOR (örn. NONE) | — | Handler'a event hiç ULAŞMAZ — "kayıp imkansız" garantisi bu durumda anlamsızlaşır | yok (event üretilmedi) | yok | BR-HDL-007 (BA-Q4) | `[07§3]` `HistoryLevel.java:56-82`, `HistoryLevelNone.java:27-39` |

---

## Matris 2 — Cutover kapısı (reconciliation sonucu × sınıf → aç / bekle / geri-dön)

| # | Girdi: reconciliation durumu | Girdi: sınıf tipi | Girdi: sırada mı (hacim-öncelikli)? | Aksiyon | BR/FR | Kanıt |
|---|---|---|---|---|---|---|
| 1 | Fark = 0 (audit-kritik) / fark ≤ epsilon + trend-artmıyor (bulk) — **N gün ÜST ÜSTE** [`BA-türetildi`, BA-Q2] | Herhangi | Evet — sıradaki sınıf | **Kapı AÇIK** — cutover talep edilebilir | BR-CUT-001/002/004 | `07§1` madde 3 (D-C); PO-Q4 |
| 2 | Fark tespit edildi (herhangi bir gün) | Audit-kritik | — | **Streak SIFIRLANIR** (mutlak sıfır tolerans) — kapı KAPALI | BR-CUT-004 (BA-Q2) | `[BA-türetildi]` |
| 3 | Fark epsilon'u aşıyor VEYA artan trend | Bulk | — | **Streak SIFIRLANIR** — kapı KAPALI | BR-CUT-004 (BA-Q2) | `[BA-türetildi]` |
| 4 | Kapı AÇIK ama hacim-öncelikli sırada henüz DEĞİL | Herhangi | Hayır | **Bekle** — kapı teknik olarak açık ama sıra gelmedi (cutover talep edilmez) | BR-HDL-005/BR-CUT-002 | `07§1` madde 4 (D-D) |
| 5 | Kapı AÇIK, sıra geldi, config apply DENENDİ | Herhangi | Evet | Config apply başarılı → **CUTOVER'LANMIŞ**; başarısız → `SYS_CUTOVER_CONFIG_APPLY_FAILED`, kapı açık kalır, yeniden dene | BR-CUT-002 | `07§1` madde 3 (D-C) |
| 6 | Cutover'lanmış sınıfta operatör geri-dönüş talep eder | Herhangi | — | **Geri-dön**: DB handler yeniden açılır, dual-run yeniden başlar, streak SIFIRDAN | BR-CUT-003 | `07§1` madde 3 (D-C — "geri dönüş = konfig") |
| 7 | Big-bang cutover (tüm sınıflar tek seferde) talep edilirse | — | — | **REDDEDİLDİ** — yeniden açılmaz (D-C) | — | `07§1` madde 3 |
| 8 | Cutover'lanmış bir sınıf kalıcı olarak dual-run'a döndürülüp "hiç cutover edilmesin" denirse | Herhangi (audit-kritik dahil) | — | **REDDEDİLDİ** — kalıcı dual-run yasak (NFR-R5); sınıf sırayla YİNE cutover kuyruğuna girer | BR-HDL-005 | NFR-R5 |

---

## Matris 3 — Erasure talebi → sınıf × politika (erase / anonymize / legal-hold / pseudonym)

| # | Girdi: sınıf tipi | Girdi: kiracı opt-in pseudonymization? | Girdi: subject-key netliği | Aksiyon | Politika kodu (TENANT_PII_CHECKLIST §2.2) | BR/FR | Kanıt |
|---|---|---|---|---|---|---|---|
| 1 | Bulk (VARINST/DETAIL/TASKINST/COMMENT/ATTACHMENT/IDENTITYLINK) | — | Net (tek dönem/instance kümesi) | **Erase/anonymize** — soft-delete→anonymize pipeline çalışır | ANONYMIZE | BR-PII-002 | PO-Q2 katman-2; `KVKK v1.0 §4.3` |
| 2 | Bulk | — | **Belirsiz** (birden fazla dönem/instance — telco MSISDN churn) [`BA-türetildi`, BA-Q6] | **Kapsam-onayı iste** — aday liste sunulur, talep sahibi onaylamadan pipeline TETİKLENMEZ | ANONYMIZE (onay sonrası) | BR-PII-005 (BA-Q6) | `[BA-türetildi]` |
| 3 | Audit-kritik (OP_LOG/INCIDENT/EXT_TASK_LOG), pseudonymization OPT-IN DEĞİL | — | — | **Legal-hold istisnası** — erasure REDDEDİLİR; kayıt yasal-saklama süresince kalır | LEGAL-HOLD | BR-PII-003 | PO-Q2 katman-1; `DATA_CLASSIFICATION.md §6` |
| 4 | Audit-kritik, pseudonymization **OPT-IN** | Evet | — | **Pseudonymize + kasa-silme = etkin-erasure** — userId takma-ada döner, harita-kaydı silinince tersinmez | TOKEN/PSEUDONYM | BR-PII-003 | PO-Q2 katman-3; DP-16 |
| 5 | Audit-kritik, pseudonymize edilmiş kayıtta harita-kaydı SİLME talebi | Evet | — | **Harita-kaydı silinir** — takma-ad tersinmez olur, OP_LOG satırının kendisi (denetim yapısı) KORUNUR, retention süresi DEĞİŞMEZ [BA-Q8] | TOKEN/PSEUDONYM → MAP_DELETED | BR-PII-003, BR-PII-001 (BA-Q8) | `[BA-türetildi]` |
| 6 | Herhangi sınıf, pipeline adımı teknik olarak BAŞARISIZ | — | — | `SYS_ERASURE_PIPELINE_FAILED` — retry + alert + audit-log(başarısızlık) | — | BR-PII-002 | `DATA_GOVERNANCE v4.0 §4.4` |
| 7 | Erasure sonrası doğrulama sorgusu HÂLÂ PII döndürüyor | — | — | `RES_ERASURE_VERIFICATION_FAILED` — **CRITICAL** (KVKK 30g SLA riski) | — | BR-PII-002 | `KVKK v1.0 §2.1` (30g SLA) |

---

## Matris 4 — Mesaj işleme (dedup × sıra × merge-upsert çakışması → aksiyon)

| # | Girdi: dedup durumu (`Nats-Msg-Id`) | Girdi: sıra-anahtarı karşılaştırması | Girdi: partition | Aksiyon | BR/FR | Kanıt |
|---|---|---|---|---|---|---|
| 1 | İlk görülüyor | — | Doğru instance-partition | Normal işlem — merge-upsert uygulanır | BR-REL-002 | `07§1` madde 5 (D-E) |
| 2 | Daha önce görülmüş (stream-düzeyi dedup penceresi içinde) | — | — | **Yutulur** — consumer'a düşmez | BR-REL-002 | D-E dedup |
| 3 | Dedup penceresi dışında ama aynı olay (relay retry / at-least-once çift teslim) | Gelen sıra-anahtarı ≤ mevcut projeksiyon satırının sıra-anahtarı | Doğru | **`BUS_PROJECTION_STALE_EVENT_DISCARDED`** — ezilmez, no-op (güvenlik ağı) | BR-REL-002/006 | NFR-R4 |
| 4 | Yeni event | Gelen sıra-anahtarı > mevcut | Doğru | **Upsert uygulanır** | BR-REL-002 | NFR-R4 |
| 5 | Yeni event | Sıra-anahtarı **eşit/belirsiz** (örn. iki farklı outbox/post-commit kaynağından aynı anda) [`BA-türetildi`, BA-Q1] | Doğru | `BUS_MERGE_UPSERT_CONFLICT_AMBIGUOUS` — WARN + tie-break: **stream-sequence** [BA-Q1 KARAR 2026-07-17] | BR-REL-006 (BA-Q1) | `[BA-türetildi]` |
| 6 | — | — | **YANLIŞ** partition'a düşmüş (teorik, config hatası) | Reject/route-error — instance-anahtarlı partition şartı ihlali (D-E REDDEDİLEN "sırasız+salt-upsert" ile karıştırılmamalı) | BR-REL-002 | D-E |
| 7 | Sırasız + salt-upsert deseni (dedup'suz, sıra-kontrolsüz) talep edilirse | — | — | **REDDEDİLDİ** — yeniden açılmaz (D-E) | — | `07§1` madde 5 |

---

## Matris 5 — DLQ / redelivery (deliver-count × hata tipi → aksiyon)

| # | Girdi: kaynak | Girdi: deliverCount / hata tipi | Aksiyon | Custody-transfer anı | BR/FR | Kanıt |
|---|---|---|---|---|---|---|
| 1 | Relay (audit-kritik outbox→NATS) | Publish denemesi başarısız (broker down), `deliverCount ≤ maxDeliver` dahili değil (relay kendi retry/backoff'unu yönetir) | Retry/backoff — outbox satırı SİLİNMEZ | Transfer OLMADI (custody outbox'ta kalır) | BR-REL-001 | `07§4` custody-transfer |
| 2 | Projeksiyon consumer | `deliveryCount ≤ maxDeliver`, işlem sırasında transient hata (DB down) | `nakWithDelay` — redelivery | Transfer OLMADI | BR-REL-002 | `[07§4]` basamak-1 backoff deseni |
| 3 | Projeksiyon consumer | `deliveryCount > maxDeliver` | `dlq.history.<...>`'a header-korumalı byte-ayna kopya → orijinal mesaj ACK | DLQ-PubAck-sonrası-ack | BR-REL-005 | `07§1` madde 5 (D-E); `[07§4]` |
| 4 | DLQ publish (JetStream) | Başarılı | Orijinal mesaj ACK'lenir | DLQ-PubAck-sonrası-ack | BR-REL-005 | `[07§4]` basamak-1 ilkesi |
| 5 | DLQ publish (JetStream) | Başarısız | `nak + alert` — asla ack-drop | Transfer OLMADI | BR-REL-005 | `[07§4]` "dlq-of-dlq YOK" |
| 6 | Relay outbox satırı | Normal relay-döngü gecikmesinin katları kadar bekliyor (relay/leader stuck şüphesi) [`BA-türetildi`, BA-Q7] | `SYS_OUTBOX_ROW_STUCK` — ops-alert; satır DLQ'ya DÜŞMEZ (henüz publish edilmediği için DLQ akışı başlamadı) | Transfer henüz başlamadı | BR-REL-001 (BA-Q7) | `[BA-türetildi]` |
| 7 | History DLQ okuma | Yetkisiz erişim denemesi | `RES_HISTORY_DLQ_ACCESS_DENIED` | N/A (mesaj custody'si etkilenmez, erişim-kontrolü konusu) | BR-REL-005 | DP-13 |

---

## Matris 6 — Retention enforcement (sınıf × kiracı-override → silme + audit-log)

| # | Girdi: sınıf tipi | Girdi: kiracı override | Girdi: satır yaşı | Aksiyon | Audit-log | BR/FR | Kanıt |
|---|---|---|---|---|---|---|---|
| 1 | Bulk | Yok (default) | > 90 gün | Silinir (anonimleştirme pipeline ile veya doğrudan hard-delete — mekanizma phase3/4) | Zorunlu, her silme için | BR-PII-001 | PO-Q7 |
| 2 | Bulk | Kiracı 180 güne uzatmış | > 180 gün | Silinir | Zorunlu | BR-PII-001 | PO-Q7 (kiracı override) |
| 3 | Audit-kritik | Yok (default yasal-saklama, örn. 7y) | > yasal-saklama süresi | Silinir (LEGAL-HOLD süresi doldu) | Zorunlu | BR-PII-001 | `DATA_GOVERNANCE v4.0 §4.2` |
| 4 | Audit-kritik | Kiracı retention'ı yasal-asgarinin ALTINA çekmeye çalışıyor | — | `VAL_RETENTION_OVERRIDE_BELOW_LEGAL_MINIMUM` — **REDDEDİLİR**, hukuki/DPO onayı gerekir | N/A (config reddedildi) | BR-PII-001 | `KVKK v1.0 §4.2` |
| 5 | Audit-kritik, pseudonymized (opt-in) | Yok | > yasal-saklama süresi | Silinir — **retention süresi pseudonymization'dan ETKİLENMEZ** [BA-Q8] | Zorunlu | BR-PII-001/003 (BA-Q8) | `[BA-türetildi]` |
| 6 | Herhangi | — | Silme uygulandı ama audit-log yazımı BAŞARISIZ | `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED` — **CRITICAL** (silme oldu, iz yok — compliance-invariant ihlali) | Eksik (hatanın kendisi) | BR-PII-001 | `[BA-türetildi]`; `DATA_GOVERNANCE v4.0 §4.4` |
| 7 | Herhangi | — | Retention job'ın kendisi başarısız (DB hatası) | `SYS_RETENTION_JOB_FAILED` — sonraki periyotta tekrar | N/A | BR-PII-001 | Standart job-retry deseni |

---

## Ek Matris 7 — Dual-run/handler config guard (extension-point kurulumu + HistoryLevel önkoşulu)

| # | Girdi (operatör config) | Guard koşulu | Aksiyon | BR/FR |
|---|---|---|---|---|
| 1 | `customHistoryEventHandlers` + `enableDefaultDbHistoryEventHandler=true` | Her ikisi de set | Dual-run — kabul | BR-HDL-001 |
| 2 | Sınıf-başına `enableDefaultDbHistoryEventHandler=false` | Reconciliation kapısı AÇIK (Matris 2) | Cutover — kabul | BR-CUT-002 |
| 3 | Sınıf-başına `enableDefaultDbHistoryEventHandler=false` | Reconciliation kapısı KAPALI | `BUS_CUTOVER_GATE_NOT_MET` — reddedilir | BR-CUT-002 |
| 4 | Audit-kritik olarak işaretlenmiş sınıf | Aktif `HistoryLevel` bu sınıfın event'ini ÜRETMİYOR | `VAL_HISTORY_LEVEL_AUDIT_CRITICAL_MISMATCH` — WARN (BA-Q4 KARAR 2026-07-17: hard-reject değil) | BR-HDL-007 |
| 5 | Yeni/tanımsız ACT_HI sınıfı keşfedildi (motor upgrade) | Sınıflandırma haritasında yok | `VAL_HISTORY_CLASS_UNCLASSIFIED` — fail-safe bulk + WARN | BR-HDL-007 |
| 6 | `setHistoryEventHandler(...)` (tam ikame) kullanılmış | Dual-run/kademeli cutover AMACIYLA çakışıyor (tam ikame default handler'ı tamamen devre dışı bırakır) | Deployment-time uyarı — bu mod yalnız basamak-2 SONRASI tam-migrasyon senaryosu için, kademeli cutover akışını BYPASS eder | BR-HDL-001 |

---

## Ek Matris 8 — Sorgu-API erişim/maskeleme kararı (rol × alan-hassasiyeti → aksiyon)

| # | Girdi: istek deseni | Girdi: rol | Girdi: alan sınıfı (`DATA_CLASSIFICATION §1`) | Aksiyon | BR/FR |
|---|---|---|---|---|---|
| 1 | Çekirdek-4'ten biri | Yetkili (role-based) | INTERNAL/PUBLIC (activityId, historyEventId) | Tam görünür | BR-QRY-001 |
| 2 | Çekirdek-4'ten biri | Yetkili | CONFIDENTIAL (businessKey) | Kiracı politikasına göre maskelenebilir (DP-8 devralınan, normatif değil) | BR-QRY-001 |
| 3 | Çekirdek-4'ten biri | Yetkili, ama PII-görme izni YOK | RESTRICTED/PII (variable değeri, operatör kimliği) | **Maskelenir** (DP-15) | BR-QRY-001 |
| 4 | Çekirdek-4'ten biri | Yetkili, PII-görme izni VAR (örn. DPO rolü) | RESTRICTED/PII | Tam görünür — erişim audit-log'lanır | BR-QRY-001 |
| 5 | Çekirdek-4'ten biri | Yetkisiz (role-check başarısız) | — | `AUTH_QUERY_ACCESS_DENIED` | BR-QRY-001 |
| 6 | Agregasyon/analitik deseni | Herhangi | — | `VAL_QUERY_UNSUPPORTED_PATTERN` — kapsam dışı (PO-Q3) | BR-QRY-001 |
| 7 | Çekirdek-4'ten biri, hedef sınıf cutover OLMAMIŞ (dual-run'da) [BA-Q3] | Yetkili | — | Sunulur — projeksiyon dual-run'dan itibaren dolu (BR-QRY-003) | BR-QRY-003 |

---

## Ek Matris 9 — Pseudonymization/kasa erişim kararı (rol × operasyon → aksiyon)

| # | Girdi: operasyon | Girdi: rol/yetki | Aksiyon | BR/FR |
|---|---|---|---|---|
| 1 | Pseudonym-değeri hesaplama (tx-içi, saf) | Engine node (sistem-içi, insan değil) | İzinli — I/O gerektirmez (BA-Q5) | BR-PII-004 |
| 2 | Kasa YAZIMI (harita satırı ekleme, downstream) | Relay/projeksiyon boru hattı (sistem-içi) | İzinli — async, audit-logged | BR-PII-004 |
| 3 | Kasa OKUMA (re-identification, yasal/adli gerekçeli) | Yetkili + açık gerekçe + audit-log | İzinli, nadiren — en-az-yetki | BR-PII-003 |
| 4 | Kasa OKUMA | Yetkisiz / gerekçesiz | `AUTH_PSEUDONYM_VAULT_ACCESS_DENIED` — **CRITICAL, security-page** | BR-PII-003 |
| 5 | Kasa harita-kaydı SİLME (data-subject erasure talebi, pseudonymized audit-kritik kayıt) | Yetkili (erasure pipeline) | İzinli — silme sonrası takma-ad TERSİNMEZ | BR-PII-003 |
| 6 | Kasa store erişilemez (downstream yazım anında) | — | `SYS_PSEUDONYM_VAULT_UNAVAILABLE` — downstream retry; **audit-kritik outbox/relay/NATS akışı ENGELLENMEZ** (BA-Q5 ilkesi) | BR-PII-004 |

---

## İzlenebilirlik özeti (Matris → BR → FR → US)

| Matris | Kapsadığı BR | Kapsadığı FR | Kapsadığı US |
|---|---|---|---|
| Matris 1 (sınıf-yönlendirme) | BR-HDL-002, BR-HDL-003, BR-HDL-004, BR-HDL-007 | FR-A3, FR-A4, FR-A5 | US-A2, US-A3, US-A4 |
| Matris 2 (cutover kapısı) | BR-CUT-001, BR-CUT-002, BR-CUT-003, BR-CUT-004, BR-HDL-005 | FR-D1, FR-D2, FR-D3, FR-A6 | US-D1, US-D2, US-D3, US-A5 |
| Matris 3 (erasure politikası) | BR-PII-002, BR-PII-003, BR-PII-005 | FR-G2, FR-G3 | US-G2, US-G3 |
| Matris 4 (mesaj işleme) | BR-REL-002, BR-REL-006 | FR-B2 | US-B2 |
| Matris 5 (DLQ/redelivery) | BR-REL-001, BR-REL-002, BR-REL-005 | FR-B1, FR-B2, FR-B5 | US-B1, US-B2, US-B5 |
| Matris 6 (retention) | BR-PII-001, BR-PII-003 | FR-G1 | US-G1 |
| Ek Matris 7 (config guard) | BR-HDL-001, BR-HDL-007, BR-CUT-002 | FR-A1, FR-A2, FR-A3, FR-D2 | US-A1, US-A2, US-D2 |
| Ek Matris 8 (sorgu-API erişim) | BR-QRY-001, BR-QRY-003 | FR-C1 | US-C1 |
| Ek Matris 9 (kasa erişim) | BR-PII-003, BR-PII-004 | FR-G3 | US-G3 |

**Toplam:** 9 karar matrisi (6 ana + 3 destekleyici), toplam **62 karar satırı** (Matris 1=7, Matris 2=8, Matris 3=7, Matris 4=7, Matris 5=7, Matris 6=7, Ek Matris 7=6, Ek Matris 8=7, Ek Matris 9=6 → 7+8+7+7+7+7+6+7+6=62). BA-Q1…8 kararları (bkz. `BUSINESS_LOGIC.md §9` karar özeti) bu matrislerin ilgili satırlarına `[BA-türetildi]` etiketiyle işlenmiştir — **2026-07-17'de Levent 8/8'ini önerilen seçenekle karara bağladı**; satırlardaki öneri-aksiyonlar artık KARARDIR.

---

*İlgili: `BUSINESS_LOGIC.md` (BR-XXX tanımları, süreç akışları, durum makineleri, BA-QUESTIONS §9), `EXCEPTION_CODES.md` (bu matrislerdeki her "aksiyon" hücresinin exception-code karşılığı).*
