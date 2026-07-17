# Phase 2 (Business Analyst) Review Raporu — Basamak-2: History Offload

**İnceleme altındaki faz:** 2 (Business Analyst), basamak-2 (History Offload)
**İnceleme tarihi:** 2026-07-17
**İnceleyen:** sentinel-phase-review (taze bağlam, adversarial; Opus)
**Hedef artefaktlar:** `docs/sentinel/step2/phase2/BUSINESS_LOGIC.md` (1001 satır), `docs/sentinel/step2/phase2/DECISION_MATRIX.md` (156 satır), `docs/sentinel/step2/phase2/EXCEPTION_CODES.md` (218 satır)
**Upstream (karşı-doğrulama):** `step2/phase1/{USER_STORIES,SRS,DATA_CLASSIFICATION}.md`, `GUIDELINES_MANIFEST.yaml` (BESPOKE — doğrudan okundu), `docs/07-history-offload.md`, fork kaynağı `~/Workspaces/cadenzaflow/cadenzaflow-bpm-platform/engine`, yapısal taban `docs/sentinel/phase2/*` (basamak-1)
**Manifest durumu:** BESPOKE şema. disiplinler enabled:7 (evidence-based-analysis, no-effort-estimates, turkish-docs-english-code, traceability-chain, rejected-alternatives-locked, unverified-claims-tagged, locked-decisions-immutable) / disabled:2 (ui-ux-guidelines, frontend-security). compliance enabled:2 (KVKK v1.0, GDPR v1.0) / disabled:2 (PCI-DSS, HIPAA). Sürüm-pin: DATA_GOVERNANCE v4.0. `review.spot_check_minimum: 5`. phase_gate: MAJOR bloklar; MINOR paralel kapanır; kilitli D-A…D-G ihlali = MAJOR.

---

## Verdict

**HAS-CONCERNS-NEEDING-ACK** (KOŞULLU ONAY)

Üç teslimat da yapısal olarak eksiksiz, iç tutarlı ve izlenebilir; **BLOCKER veya MAJOR yok**, dolayısıyla manifest phase_gate faz-3'e geçişi engellemez. Kanıt disiplini örnek düzeyde: teslimatlara taşınan tüm `[07§3]` file:line iddiaları hem docs/07 §3 ile hem de gerçek fork kaynağıyla **birebir doğrulandı** (5/5 doğrudan fork spot-check geçti). Girdi-korpus sayımları (25 US / 26 FR / 30 NFR / 7 IR / DP-1…16) ve teslimat öz-sayımları (31 BR / 62 matris satırı / 41 kod) **bağımsız sayımla teyit edildi** — basamak-1 phase2 review'undaki miktar-hatası sınıfı bu kez YOK. İki MINOR bulgu (NFR-R1'in koşullanmış "imkansız" garantisi; BR-HDL-007'de kalmış bir "açık" ibaresi) yazılı PO/BA kabulü gerektirir; ikisi de faz-3 ile paralel kapatılabilir.

---

## Findings

### 🔴 BLOCKER (bloklar)

_(yok — bloklayıcı bulgu yok)_

### 🟠 MAJOR (çözülmeden faz geçilmez)

_(yok — kilitli D-A…D-G, PO-Q1…7, BA-Q1…8 sadakati ihlali bulunmadı; reddedilen alternatiflerin hiçbiri yeniden açılmamış)_

### 🟡 MINOR (yazılı kabul gerektirir; faz-3 ile paralel kapatılabilir)

**FINDING-001** — [kategori: OpenRisks / Consistency]
- **Ne:** NFR-R1 "audit-kritik sınıflar at-least-once; **kalıcı audit kaybı imkansız**" MUTLAK bir Must garantisidir; ancak BR-HDL-007 + BA-Q4 kararı bu garantinin **sessiz bir önkoşulu** olduğunu ortaya koyuyor — `HistoryLevel` ilgili event'i hiç üretmezse (örn. NONE) garanti boşa düşer ve bunu koruyan tek mekanizma **deployment-time WARN** (hard-reject değil). SRS NFR-R1 metni bu koşullanmayı taşımıyor.
- **Nerede:** `SRS.md` §4.2 NFR-R1 ↔ `BUSINESS_LOGIC.md` §3 BR-HDL-007 + §9 BA-Q4 ↔ `DECISION_MATRIX.md` Ek Matris 7 satır 4 ↔ `EXCEPTION_CODES.md` §1 `VAL_HISTORY_LEVEL_AUDIT_CRITICAL_MISMATCH`.
- **Kanıt:** BR-HDL-007 tanımı (BUSINESS_LOGIC:439): "'Audit kaybı imkansız' garantisi (NFR-R1) yalnız … `HistoryLevel`'in ilgili event'i ÜRETTİĞİ durumda geçerlidir … handler seviyesinde tespit edilemez." Fork kanıtı doğrulandı: `HistoryLevelNone.isHistoryEventProduced(...) { return false; }` (spot-check B) — NONE altında audit-kritik event handler'a hiç ulaşmaz.
- **Neden önemli:** Operatör `HistoryLevel=NONE` + bir sınıfı audit-kritik konfigüre ederse, yalnızca WARN üretilir ve **sessiz audit kaybı** oluşur — NFR-R1'in "imkansız" ifadesiyle çelişir. Risk bilinçle kabul edilmiş (WARN kararı gerekçeli: HistoryLevel motor-genel ayar, hard-reject motor bootstrap'ını basamak-2 ötesinde etkiler) ama SRS'in mutlak dili bu istisnayı yansıtmıyor.
- **Önerilen çözüm:** PO yazılı olarak (a) NFR-R1'in "imkansız"ının "geçerli HistoryLevel altında" biçiminde koşullu okunduğunu ve (b) WARN'ın (hard-reject değil) kabul edilen koruma olduğunu onaylasın; faz-3 HLD/deployment-guard bu önkoşulu taşısın (SRS NFR-R1'e §-çapraz-referans önerilir).

**FINDING-002** — [kategori: InternalConsistency]
- **Ne:** BA-Q4 kararı 2026-07-17'de **WARN olarak KİLİTLENDİ** (BUSINESS_LOGIC §9 karar özeti; DECISION_MATRIX Ek Matris 7 satır 4 "KARAR"; EXCEPTION_CODES §1 "KARAR — hard-reject DEĞİL"). Fakat BR-HDL-007'nin koşul tablosundaki 2. satır hâlâ karar-öncesi ibareyi taşıyor: "deployment-time WARN (bkz. BA-Q4, **hard-reject mi WARN mı açık**)". Aynı belge içinde karar "verildi" ile "açık" çelişiyor.
- **Nerede:** `BUSINESS_LOGIC.md` §3 BR-HDL-007 koşul-2 satırı (satır 447) ↔ aynı belge §9 (satır 978/991).
- **Kanıt:** Satır 447: "`VAL_HISTORY_LEVEL_AUDIT_CRITICAL_MISMATCH` — deployment-time WARN (bkz. BA-Q4, hard-reject mi WARN mı açık)". Karşıt: §9 KARAR ÖZETİ "BA-Q4 | … = **deployment-time WARN** (hard-reject DEĞİL …)".
- **Neden önemli:** Kilitli-karar sadakati bu manifest'in birincil kapısıdır; bir kararın "açık" olarak görünen artığı, soğuk okuyucuyu yanıltır ve gelecekte yanlış yeniden-açılmaya davet eder. Öz (WARN) her üç belgede aynıdır — bu yüzden MAJOR değil; yalnızca artık ibare dokümantasyon-hijyeni sorunudur.
- **Önerilen çözüm:** BR-HDL-007 koşul-2 hücresindeki "hard-reject mi WARN mı açık" ibaresini "BA-Q4 KARAR 2026-07-17: WARN, hard-reject değil" ile değiştir.

### 🟢 NIT (bilgilendirici; sonraki fazlar için)

**FINDING-003** — [kategori: Consistency]
- **Ne:** BR-PII-004 önceliği **Must (BA-Q5)** olarak işaretli; ebeveyn US-G3/FR-G3 ise **Should**. Bir Should-US'nin içindeki kenar-durum kuralının Must olması hafif bir öncelik-kalıtım tuhaflığıdır (pseudonymization hiç yapılmazsa kuralın Must'lığı devreye girmez).
- **Nerede:** `BUSINESS_LOGIC.md` BR-PII-004 (satır 825) ↔ US-G3 (S) / FR-G3 (S).
- **Neden önemli:** Anlamsal olarak savunulabilir (D-A'nın kilitli "tx-içi senkron I/O yasak" ilkesini koruduğu için: "pseudonymization yapılırsa bu zamanlama zorunludur"), ama öncelik-tutarlılığı okuyucuya "koşullu-Must" olarak açıklanmalı.
- **Önerilen çözüm:** BR-PII-004 önceliğini "Must-if-implemented (US-G3 Should'a bağlı)" biçiminde niteleyerek netleştir.

**FINDING-004** — [kategori: Alignment / Traceability]
- **Ne:** BA-Q3 kararı, FR-C1/US-C1'in "cutover'lanan sınıflar için" ifadesini "motivasyon çerçevesi, teknik filtre değil" olarak yeniden yorumlayıp sorgu-API kapsamını **projeksiyondaki tüm sınıflara** (cutover-bağımsız) genişletiyor. Bu, Levent tarafından onaylı meşru bir BA-netleştirmesidir; ancak faz-1 artefaktları (SRS FR-C1, US-C1) hâlâ dar "cutover'lanan sınıflar için" metnini taşıyor — yalnız SRS'i okuyan biri API'yi cutover-only sanır.
- **Nerede:** `BUSINESS_LOGIC.md` BR-QRY-003 (satır 592) + §9 BA-Q3 ↔ `SRS.md` FR-C1 (satır 118) / `USER_STORIES.md` US-C1 (satır 224).
- **Neden önemli:** Faz-2 tarafı doğru ele almış (BA-Q3 → US-C1/FR-C1 izlenebilir, PO-kararlı). Boşluk yalnızca faz-1 metninin geri-yazılmamış olması — bir faz-1 hijyen kalemi, faz-2 bloklayıcısı değil.
- **Önerilen çözüm:** Faz-1 FR-C1/US-C1'e "kapsam yorumu BA-Q3 ile netleşti (cutover-bağımsız)" çapraz-referansı düşülmesi düşünülsün.

**FINDING-005** — [kategori: Consistency — terminoloji]
- **Ne:** BA-Q1 kararı (tie-break = NATS stream-sequence) 2026-07-17'de KİLİTLENDİ, ama flow 1.4 (satır 100) ve `EXCEPTION_CODES` `BUS_MERGE_UPSERT_CONFLICT_AMBIGUOUS` hâlâ "**geçici** tie-break … phase3'te kesin mekanizma netleşir", BR-REL-006 ise "**Öneri**" başlığıyla anıyor. Öz (stream-sequence) tutarlı; yalnız "geçici/öneri" ifadeleri kilitli-karar diliyle uyumsuz.
- **Nerede:** `BUSINESS_LOGIC.md` flow 1.4 + BR-REL-006 ↔ `EXCEPTION_CODES.md` §3.
- **Önerilen çözüm:** "geçici/öneri" ibarelerini "BA-Q1 KARAR: stream-sequence; implementasyon detayı phase3" biçiminde sabitle.

**FINDING-006** — [kategori: pozitif gözlem — korunmaya değer]
- **Ne:** Basamak-1 phase2 review'unun üç hata sınıfı da bu teslimatta **bilinçle kapatılmış**: (1) girdi-korpus sayımları hatasız; (2) 6 kenar-durum kuralı sahte file:line ile şişirilmemiş, `[BA-türetildi]` etiketli ve SRS §2.5/ilgili NFR'ye bağlanmış; (3) taksonomi istisnaları (`BUS_=WARN` kuralının istisnası `BUS_BENCH_HISTORY_METRIC_REGRESSION`; `SYS_=ERROR`'un WARN istisnaları; CRITICAL yükseltmeler) EXCEPTION_CODES §12'de açıkça belgelenmiş. Korunmaya değer bir olgunluk sıçraması.

---

## Kategori Scorecard

| # | Kategori | Durum | Kısa not |
|---|---|---|---|
| 1 | Completeness | ✅ | 10 akış + 4 durum makinesi, 31 BR tam katalog, 9 matris/62 satır, 41 kod/11 grup + kategori özeti + izlenebilirlik; 25/25 US, 26/26 FR kapsandı; TODO/TBD-boş gövde yok |
| 2 | Alignment | ✅ | Her normatif iddia US/FR/D-A…D-G/PO-Q/BA-Q'ya izlenir; `[07§3]` file:line'lar fork kaynağıyla birebir doğrulandı (5/5); sürüm-pin (v4.0/v1.0) korundu |
| 3 | Internal consistency | ⚠️ | BA-Q1…8 üç belgede öz-tutarlı, öz-sayımlar hatasız; FINDING-002 (BR-HDL-007'de kalmış "açık") + FINDING-001 (NFR-R1 koşullanması) |
| 4 | Manifest discipline | ✅ | PCI/HIPAA/UI/frontend sızması 0; effort tahmini 0; reddedilenler yeniden açılmadı; taksonomi istisnaları belgeli |
| 5 | Open risks | ⚠️ | NFR-R1 WARN-guard'ı + BA-Q7 stuck-eşiği (doğru etiketli, phase3 kalibrasyonu) + BA-Q3 geri-yazım boşluğu; hepsi işaretli, ACK ile kabul edilebilir |

Açıklama: ✅ = sorun yok · ⚠️ = yalnız 🟡/🟢 · ❌ = 🔴/🟠 var

---

## Kilitli-Karar Sadakati Ekseni (manifest phase_gate özel kapısı)

| Eksen | Sonuç |
|---|---|
| D-A (hibrit tutarlılık: audit-kritik outbox / bulk post-commit) | ✅ Sadık — BR-HDL-002/003/004, Matris 1, flow 1.2/1.3'te modellendi; handler-içi senkron publish "YASAK" olarak korundu |
| D-B (ayrı Postgres projeksiyon; ClickHouse-şimdi ertelendi) | ✅ Sadık — BR-REL-002/003; ClickHouse yalnız "izole edilebilir evrim" notu, aktif kural değil |
| D-C (kademeli sınıf-bazlı cutover + sorgu-API; big-bang/kalıcı dual-run reddi) | ✅ Sadık — BR-CUT-001/002/003, durum makinesi 2.1; reddedilenler §7'de |
| D-D (tüm ACT_HI sınıfları, hacim-öncelikli sıra) | ✅ Sadık — BR-HDL-005, Matris 1 (16 sınıf tam) |
| D-E (instance-anahtarlı sıra + merge-upsert; DLQ basamak-1 aynen; sırasız+salt-upsert reddi) | ✅ Sadık — BR-REL-002/004/005/006, Matris 4/5 |
| D-F (normalize DB-yazım metriği = TEK sert kapı; iki-kapı ayrımı) | ✅ Sadık — BR-OBS-001, iki-kapı ayrımı flow 1.7 notunda korundu |
| D-G (Camunda 7 + CadenzaFlow; Flowable = basamak-2b) | ✅ Sadık — BR-HDL-001 koşul-3; Flowable her yerde kapsam-dışı |
| PO-Q1…7 | ✅ Sadık — PO-Q3 çekirdek-4, PO-Q4 N=7g, PO-Q5 audit-kritik liste, PO-Q6 S-dahil, PO-Q7 EPIC-G hepsi doğru işlendi |
| BA-Q1…8 (2026-07-17 karara bağlandı) | ✅ Öz üç belgede tutarlı; yalnız 2 ibare-artığı (FINDING-002 MINOR, FINDING-005 NIT) |
| Reddedilen alternatifler yeniden açıldı mı? | ❌ Hayır — hiçbiri yeniden açılmadı (§7 + SRS §7 ile tutarlı) |

**Sonuç:** Kilitli-karar sadakati kapısı **temiz** — MAJOR yok.

---

## Spot-check listesi (file:line — manifest gereği ≥5)

Tüm `[07§3]` iddiaları gerçek fork kaynağına (`~/Workspaces/cadenzaflow/cadenzaflow-bpm-platform/engine`) karşı doğrulandı — **5/5 GEÇTİ** (ayrıca docs/07 §3 birebir hizalama teyidi):

| # | İddia (teslimat) | Kaynak file:line | Sonuç |
|---|---|---|---|
| A | SPI `handleEvent`/`handleEvents` + Javadoc "async/MQ'ya açık kapı" (BR-HDL-001, flow 1.1) | `impl/history/handler/HistoryEventHandler.java:26-53` | ✅ GEÇTİ — Javadoc "message queue … asynchronously" + iki SPI metodu; paket `org.cadenzaflow.*` (fork rename teyidi) |
| B | `HistoryLevel=NONE → hiç event üretilmez` (BR-HDL-007, FINDING-001 temeli) | `impl/history/HistoryLevelNone.java:27-39` | ✅ GEÇTİ — `isHistoryEventProduced(...) { return false; }` |
| C | Genişletme noktası `initHistoryEventHandler()` yalnız null iken kurar; `enableDefaultDbHistoryEventHandler` dallanması (BR-HDL-001, BR-CUT-002 cutover mekanizması) | `ProcessEngineConfigurationImpl.java:2788-2796` | ✅ GEÇTİ — `if (historyEventHandler == null)` + Composite**Db**HistoryEventHandler vs CompositeHistoryEventHandler dallanması (dual-run/custom-only/cutover senaryolarını kanıtlar) |
| D | tx-içi senkron `handleEvent` çağrısı — in-handler publish riskinin kaynağı (BR-HDL-004, flow 1.2 notu, D-A gerekçesi) | `HistoryEventProcessor.java:73-85` | ✅ GEÇTİ — `historyEventHandler.handleEvent(singleEvent)` aynı thread, senkron |
| E | `ByteArrayEntity(..., ResourceTypes.HISTORY)` — variable byte payload (DATA_CLASSIFICATION §2.1, BR kanıtı) | `DbHistoryEventHandler.java:97-105` | ✅ GEÇTİ — `new ByteArrayEntity(..., ResourceTypes.HISTORY)` |

Ek hizalama teyidi (docs/07 §3 ↔ teslimat): default zincir `DbHistoryEventHandler.java:40` / `CompositeHistoryEventHandler.java:33,38` / `CompositeDbHistoryEventHandler.java:70-72` ve `CommandContext.java:186-197` (flushSessions→commit) — teslimattaki file:line'lar docs/07 §3 ile **birebir** (yeni/uydurma referans YOK).

**Spot-check sonucu: 5/5 doğrudan fork-kaynağı doğrulaması geçti** (manifest minimum 5 karşılandı ve aşıldı).

---

## Şeffaflık — Ne kontrol ettim

- **Okunan dosyalar (satır):** BUSINESS_LOGIC.md (1001), DECISION_MATRIX.md (156), EXCEPTION_CODES.md (218), USER_STORIES.md (469), SRS.md (294), DATA_CLASSIFICATION.md (229), GUIDELINES_MANIFEST.yaml (68), docs/07-history-offload.md (78), basamak-1 phase2 EXCEPTION_CODES.md (yapısal kıyas).
- **Bağımsız sayımlar (kendim saydım):** Girdi-korpus 25 US (EPIC A6+B5+C2+D3+E3+F3+G3) ✅ · 26 FR ✅ · 30 NFR (P5+R8+S8+O3+M5+L1) ✅ · 7 IR ✅ · DP-1…16 ✅. Teslimat: 31 BR (25×1:1 + 6 kenar-durum) ✅ · 62 matris satırı (7+8+7+7+7+7+6+7+6) ✅ · 41 kod (11 grup 3+4+4+3+3+4+3+4+4+5+4) ✅ · kategori dağılımı VAL 7/BUS 15/RES 3/SYS 14/AUTH 2 = 41 ✅ · 9 "bilgilendirici" + 3 CRITICAL etiketi ✅. **Tümü doğrulandı — sayım hatası yok.**
- **Çapraz-referans doğrulaması:** 25/25 US → BR → FR zinciri (BUSINESS_LOGIC §8); 41/41 kod → BR → FR → US (EXCEPTION_CODES §13); flow/matris'te anılan her exception kodunun katalogda var olması; BA-Q1/Q2/Q4/Q5/Q6/Q7/Q8'in üç belgede tutarlılığı; PO-Q3/Q4/Q5/Q6/Q7'nin US/SRS ile hizası; DP-9…16 ↔ NFR-S2…S8 ↔ BR-PII/BR-QRY bağları.
- **Anti-pattern greps:** disabled-guideline sızması (cardholder/PCI/HIPAA/PHI/health) → 0; UI/frontend (React/Vue/CSS/modal/admin-ui) → 0; effort tahmini (story-point/kişi-gün/hafta/sprint) → 0 (yalnız "Effort tahmini içermez" disclaimer'ı); layering token (supersedes/amendment/delta) → 0 ("cutover" domain terimi, layering değil); sürüm-pin DATA_GOVERNANCE v4.0 (11×) + KVKK v1.0 (8×) tutarlı.
- **Kanıt disiplini:** `[BA-türetildi]` etiketlerinin hiçbiri ham file:line kod-iddiası içermiyor (BR-HDL-007'nin file:line'ı ayrıca `[07§3]` etiketli); doğrulanmamış varsayımlar `[phase3'te doğrulanacak]` etiketli.
- **Basamak-1 kıyası:** 23 kod devralma iddiası doğrulandı (basamak-1 phase2'de 23 distinct kod + `SYS_SENTINEL_WORKER_CONFLICT` severity-peer mevcut).

---

## Dürüstlük — Ne kontrol ETMEDİM

- **Kod derleme/çalışma davranışı** — faz-5+ kapsamı; yalnız BA-katmanı mantık/izlenebilirlik denetlendi.
- **Faz-3 tasarım kararları** — kasa (pseudonymization vault) mimarisi, projeksiyon şeması DDL'i, subject-level authz, HistoryLevel deployment-guard mekanizması: hepsi bilinçle faz-3/4'e bırakılmış; bu review bunların doğru **ertelendiğini** teyit etti, tasarımlarını değerlendirmedi.
- **Nicel eşik kalibrasyonları** (BA-Q7 outbox-stuck çarpanı, projeksiyon p95 hedefi) — doğru biçimde phase3/4 bench'e işaretli; sayısal değer denetlenmedi.
- **Hukuki yorum** (KVKK yasal-saklama süresinin kesin gerekçesi, 7y taban) — DPO alanı; teslimat mekanizmayı verir, hukuki dayanağı DPO doğrulamasına işaretlemiş (uygun).
- **Bench baseline gerçek sayıları** (US-F1 ilk gerçek koşu) — henüz üretilmemiş; teslimat bunu doğru biçimde BUS_BENCH_BASELINE_MISSING ile modelliyor.

---

## İnsan için Sonraki Aksiyon

**HAS-CONCERNS-NEEDING-ACK:** İki 🟡 MINOR (FINDING-001 NFR-R1 koşullanması, FINDING-002 BR-HDL-007 "açık" artığı) için ya düzeltme yapılsın ya da yazılı gerekçeli PO/BA kabulü kaydedilsin — manifest phase_gate gereği bunlar faz-3 ile **paralel** kapatılabilir, faz geçişini bloklamaz. 🟢 NIT'ler (FINDING-003/004/005) düzeltme-fırsatı; FINDING-006 korunmaya değer olumlu gözlemdir. MAJOR/BLOCKER olmadığından **faz-3'e (Architecture) geçiş, iki MINOR'ın ACK'i alındıktan sonra onaylanabilir.**

---

*Tek-satır özet (orchestrator): `PHASE2_REVIEW: HAS-CONCERNS-NEEDING-ACK (🔴 0 BLOCKER / 🟠 0 MAJOR / 🟡 2 MINOR / 🟢 4 NIT) — spot-check 5/5 geçti`*

---

## Bulgu Kapanış Kaydı (2026-07-17, review sonrası aynı gün)

| Bulgu | Kapanış |
|---|---|
| FINDING-001 🟡 | **DÜZELTİLDİ** — `step2/phase1/SRS.md` NFR-R1'e önkoşul cümlesi eklendi (garanti aktif HistoryLevel'in event ÜRETTİĞİ konfigürasyonlar için; NONE → deployment-WARN, BR-HDL-007). BA-Q4 kararının SRS'e geri-yazımı. |
| FINDING-002 🟡 | **DÜZELTİLDİ** — BR-HDL-007 koşul-2 hücresindeki "hard-reject mi WARN mı açık" artığı "BA-Q4 KARAR 2026-07-17: hard-reject DEĞİL" ile değiştirildi. |
| FINDING-003 🟢 | **DÜZELTİLDİ** — BR-PII-004 önceliği "Koşullu-Must (US-G3 [S] kapsamda kaldıkça bağlayıcı)" olarak nitelendi. |
| FINDING-004 🟢 | **DÜZELTİLDİ** — BA-Q3 kararı faz-1'e geri-yazıldı: FR-C1 "sunulan sınıf kümesi = projeksiyondaki HER sınıf (cutover-bağımsız)"; US-C1'e kabul-kriteri eklendi. |
| FINDING-005 🟢 | **DÜZELTİLDİ** — akış 1.4, BR-REL-006/BR-CUT-004/BR-PII-004/BR-PII-005 "Öneri" başlıkları ve EXCEPTION_CODES `BUS_MERGE_UPSERT_CONFLICT_AMBIGUOUS` satırı "KARAR (BA-Qn, 2026-07-17)" diline çevrildi. |
| FINDING-006 🟢 | Pozitif gözlem — aksiyon gerekmez; basamak-1 hata sınıflarının bilinçli kapanışı phase3 prompt'una taşınacak. |

**Sonuç:** İki 🟡 MINOR dahil tüm bulgular düzeltme yoluyla kapatıldı (yazılı-kabul yolu kullanılmadı). Faz-3 geçişi için tek bekleyen: Levent'in onayı.
