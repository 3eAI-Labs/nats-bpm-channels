# ADR-0015 — Kademeli cutover kontrol düzlemi + reconciliation (iki-kapı)

- **Durum:** Kabul edildi (2026-07-17, Phase 3 — basamak-2). Alt-eksen KARARA BAĞLANDI: config-flip = **rolling-restart**, outbox-stuck eşiği = **relay-döngü-gecikmesi çarpanı** (bench-kalibre) (ARCH-Q5, 2026-07-18).
- **İzlenebilirlik:** US-D1/US-D2/US-D3/US-E1 → FR-D1/FR-D2/FR-D3/FR-E1 → BR-CUT-001/002/003/004/BR-OBS-001 → `BUS_RECONCILIATION_DIFF_DETECTED`/`BUS_CUTOVER_GATE_NOT_MET`/`SYS_CUTOVER_CONFIG_APPLY_FAILED`/`BUS_CUTOVER_ROLLBACK_TRIGGERED`/`BUS_BENCH_HISTORY_METRIC_REGRESSION` → D-C/D-D/D-F / PO-Q4 / BA-Q2 / NFR-R5/R7

## Bağlam

D-C/D-D (kilitli): kademeli, sınıf-bazlı, hacim-öncelikli cutover; reconciliation-kapılı; big-bang ve kalıcı dual-run REDDEDİLDİ. D-F: **iki ayrı kapı** — (1) reconciliation-temizliği = **cutover kapısı**; (2) normalize DB-yazım metriği = **yazım-azaltmanın TEK sert kapısı**. PO-Q4: N gün temiz default 7 (sınıf-başına konfig, kalibre edilebilir başlangıç). BA-Q2: "temiz" tanımı sınıf-tipine göre (audit-kritik mutlak sıfır; bulk epsilon+trend).

## Karar

### Reconciliation (cutover kapısı — yumuşak)
- Dual-run boyunca **ReconciliationJob** sınıf-başına projeksiyon satırları ↔ `ACT_HI` satırlarını periyodik karşılaştırır; **fark sayacı** SLI üretir; rapor **PII değeri sızdırmaz** (yalnız sayaç/id/hash — DP-14).
- **"N gün temiz" tanımı (BA-Q2):** audit-kritik sınıf → **mutlak sıfır** fark (fark → streak sıfırlanır, `BUS_RECONCILIATION_DIFF_DETECTED`); bulk sınıf → fark ≤ **konfigürable epsilon** (default 0, sınıf-başına override) **VE artan trend yok** (bulk at-most-once mimari-kabul-edilen kayıp — mutlak sıfır pratikte ulaşılamaz olabilir). Streak ≥ N (default 7g, PO-Q4) → cutover kapısı AÇIK. Süreklilik/eşik-aşımı → `RES_RECONCILIATION_DIFF_THRESHOLD_EXCEEDED` (drift/config şüphesi). Job hatası → `SYS_RECONCILIATION_JOB_FAILED` (döngü atlanır, streak ilerlemez).

### Cutover (config-flip)
- Kapı açık + hacim-öncelikli sırada (DETAIL→VARINST→ACTINST→…) sıra geldi → **CutoverControlPlane** o sınıf için `enableDefaultDbHistoryEventHandler=false` uygular (ADR-0009 senaryo 2). Kapı kapalıyken manuel zorlama → `BUS_CUTOVER_GATE_NOT_MET` (reddedilir). Config apply başarısız → `SYS_CUTOVER_CONFIG_APPLY_FAILED` (dual-run fail-safe devam eder, DB handler açık kalır — veri kaybı yok). Cutover sonrası o sınıfın ACT_HI yazım bileşeni = **0** (NFR-P1).
- **Geri-dönüş (NFR-R7):** operatör cutover'lanmış sınıfı yeniden açar (`enableDefaultDbHistoryEventHandler=true`, yalnız konfig) → `BUS_CUTOVER_ROLLBACK_TRIGGERED` (audit-logged); dual-run yeniden başlar, Cockpit-history geri gelir; yeniden-cutover öncesi reconciliation streak SIFIRDAN. **Kalıcı dual-run REDDEDİLDİ** (NFR-R5) — sınıf sırayla yine cutover kuyruğuna girer.

### Yazım-azaltma sert kapısı (ayrı — D-F)
- **BR-OBS-001 normalize DB yazım-op metriği** yazım-azaltmanın TEK sert kapısıdır (reconciliation-temizliğinden AYRI): cutover'lanan sınıfta ACT_HI bileşeni 0, audit-kritik outbox bileşeni ≤1/tx. Hedef kaçarsa `BUS_BENCH_HISTORY_METRIC_REGRESSION` → build-fail. Ölçüm `pg_stat_statements` fingerprint — history INSERT'leri ayrı queryid alır (parse-tree hash; literaller `$n`'e normalize) [PostgreSQL docs]; basamak-1 D-F metodolojisi yeniden kullanılır. **[phase3'te doğrulandı — resmi doküman; IN-list arity uyarısı history basit-INSERT'lerde geçerli değil.]** Bench iki mod (DB-history baseline ↔ offload) US-E1 metriğini üretir.

### Config-flip mekanizması + outbox-stuck eşiği (ARCH-Q5 — KARAR 2026-07-18)
- **KARAR (ARCH-Q5, 2026-07-18):** sınıf-başına `enableDefaultDbHistoryEventHandler` flip'i **rolling-restart** ile uygulanır (deterministik, en az motor-içi durum riski); hot-reconfigure **reddedilmedi, iyileştirme kapısı açık** — motor desteği phase4/5'te doğrulanırsa geçilebilir (doğrulanmamış varsayım üzerine mekanizma kurulmadı). **BA-Q7 outbox-stuck eşiği:** sabit sayı yerine **normal relay-döngü gecikmesinin çarpanı** (örn. 5×); ilk çarpan phase4/5 bench ölçümüyle kalibre edilir (`SYS_OUTBOX_ROW_STUCK`; PO-Q4 "kalibre edilebilir başlangıç" deseni).

## Sonuçlar

**Olumlu:** İki-kapı ayrımı reconciliation-güvenliğini yazım-azaltma-ispatından ayırır → her kapı kendi sinyalini korur. Fail-safe cutover (apply fail → dual-run devam) veri kaybını yapısal olarak engeller. Geri-dönüş yalnız konfig → düşük-riskli. Sınıf-tipine göre "temiz" tanımı bulk at-most-once gerçeğiyle cutover'ı imkânsız kılmadan sinyal kalitesini korur.

**Olumsuz / kabul edilen:** Reconciliation birincil DB'yi ve projeksiyonu okur (amortize, batch — NFR-P4). Rolling-restart, flip başına kısa bir rolling pencere gerektirir (ARCH-Q5 kabulü; hot-reconfigure phase4/5 doğrulamasına açık). Outbox-stuck eşiği ilk deploy'da kalibre edilmemiş bir başlangıç değeri taşır (PO-Q4 "kalibre edilebilir başlangıç" deseni).

## Reddedilenler
- **Big-bang cutover:** etki yüzeyi/geri-dönüş büyük. **Reddedildi** (D-C).
- **Kalıcı dual-run:** yazım hacmi kalkmıyor — §6.7 hedefiyle çelişir. **Reddedildi** (NFR-R5).
- **Tek-kapı (reconciliation = yazım-azaltma ispatı):** iki farklı garantiyi birleştirir, sinyal kalitesini bozar. **Reddedildi** (D-F iki-kapı).
- **Config-apply-fail'de cutover'a devam:** veri kaybı riski. **Reddedildi** (fail-safe: dual-run devam).
