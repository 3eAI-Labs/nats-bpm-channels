# ADR-0009 — Composite HistoryEventHandler plug-in stratejisi (fork değişmez, dual-run yetenekli)

- **Durum:** Kabul edildi (2026-07-17, Phase 3 — basamak-2)
- **İzlenebilirlik:** US-A1/US-A2/US-A5 → FR-A1/FR-A2/FR-A3/FR-A6 → BR-HDL-001/002/005/007 → `VAL_HISTORY_CLASS_UNCLASSIFIED`/`VAL_HISTORY_LEVEL_AUDIT_CRITICAL_MISMATCH` → D-A/D-D/D-G
- **Kesişim:** basamak-1 **ADR-0005** (engine impl-sınıf bağımlılığı + upgrade stratejisi) history yüzeyine genişler.

## Bağlam

Basamak-2, `ACT_HI` history yazımını motor-içi in-tx DB handler'dan motor-dışı push'a taşır. Fork history alanı upstream Camunda 7 ile birebir (tek commit: paket rename `org.cadenzaflow.*`); SPI `HistoryEventHandler.java:38-53` (`handleEvent`/`handleEvents`; Javadoc :26-29 async/MQ implementasyona açıkça kapı bırakır, fork'ta impl YOK) [07§3]. Motor üç resmi genişletme noktası sunar (`ProcessEngineConfigurationImpl.java:757-769` alanlar, `:2788-2796` `initHistoryEventHandler()` — yalnız null iken kurar, `:3876-3898` setter'lar) [07§3]:
1. `customHistoryEventHandlers` + `enableDefaultDbHistoryEventHandler=true` → dual-run (custom + default DB yan yana),
2. sınıf-başına `enableDefaultDbHistoryEventHandler=false` → yalnız custom (cutover),
3. `setHistoryEventHandler(...)` → tam ikame (default handler'ı tamamen devre dışı bırakır).

Soru: hangi mekanizma basamak-2'nin **kademeli, sınıf-bazlı cutover + reconciliation için dual-run** ihtiyacını karşılar; fork core'a dokunmadan; Camunda 7 ve CadenzaFlow tek adapter paylaşır mı?

## Karar

- **NatsHistoryEventHandler** custom composite handler, motor'un `CompositeHistoryEventHandler` deseni üstüne `customHistoryEventHandlers` + `enableDefaultDbHistoryEventHandler` genişletme noktalarıyla takılır — **fork motor kodu DEĞİŞMEZ** (NFR-M1). Dual-run (senaryo 1) reconciliation için varsayılan bootstrap durumudur; cutover = sınıf-başına `enableDefaultDbHistoryEventHandler=false` (senaryo 2).
- **`setHistoryEventHandler(...)` tam-ikame (senaryo 3) kademeli cutover için KULLANILMAZ** — default handler'ı tamamen devre dışı bırakır, dual-run/reconciliation kapısını BYPASS eder (Ek Matris 7 satır 6). Yalnız basamak-2 SONRASI tam-migrasyon için ayrılır; deployment-time uyarı verilir.
- **Camunda 7 ↔ CadenzaFlow tek adapter (byte-ayna)** paylaşır (NFR-M2; paket adı dışında fark yok). **Flowable KAPSAM DIŞI** (D-G — basamak-2b).
- **HistoryLevel farkındalığı:** handler yalnız konfigüre `HistoryLevel`'in ürettiği event'leri alır (`HistoryLevel.java:56-82`; `HistoryLevelNone.isHistoryEventProduced()→false`, `HistoryLevelNone.java:27-39`) [07§3]. Audit-kritik konfigüre edilmiş bir sınıf üretilmeyen bir seviyede ise `VAL_HISTORY_LEVEL_AUDIT_CRITICAL_MISMATCH` = **deployment-time WARN** (hard-reject DEĞİL — BA-Q4; HistoryLevel motor-genel ayar). Tanımsız yeni sınıf (motor upgrade) → `VAL_HISTORY_CLASS_UNCLASSIFIED` = fail-safe **bulk** + WARN (BR-HDL-007).
- **`handleEvents(List)` batch yolu:** fork'ta `CompositeHistoryEventHandler.handleEvents(List)` tek-tek `handleEvent`'e düşer (fork kaynak doğrulaması: `handleEvents(...) { for (e : events) handleEvent(e); }`, CompositeHistoryEventHandler.java:100-105). Custom handler da aynı deseni izler → batch-özel optimizasyon GEREKMEZ; sınıf-bazlı yönlendirme tek-event düzeyinde deterministiktir. **[phase3'te doğrulandı — fork kaynağı.]**
- **impl-sınıf upgrade yüzeyi (ADR-0005 genişlemesi):** `ProcessEngineConfigurationImpl` history alanları, `HistoryEventProcessor`, `CommandContext` (flushSessions→commit) yüzeyleri upgrade'de izlenecek bağımlılık olarak dokümante edilir; ADR-0005 guard-test + runbook deseni history handler'a genişler.

## Sonuçlar

**Olumlu:** Kademeli cutover ve reconciliation aynı bootstrap altyapısıyla (dual-run) desteklenir; fork'a sıfır dokunuş → upgrade maliyeti düşük; Camunda/CadenzaFlow ayna-adapter tek kontrat. HistoryLevel/sınıflandırma guard'ları sessiz garanti-boşluklarını görünür kılar.

**Olumsuz / kabul edilen:** Genişletme noktaları impl-sınıflara bağlıdır (public API değil) → motor upgrade'inde bu yüzey doğrulanmalı (ADR-0005 yüzeyi büyür). `VAL_HISTORY_LEVEL_AUDIT_CRITICAL_MISMATCH`'in WARN (hard-reject değil) olması, yanlış HistoryLevel konfigürasyonunda sessiz audit boşluğu riskini operatöre bırakır (NFR-R1 önkoşulu; bilinçli — BA-Q4).

## Reddedilenler
- **`setHistoryEventHandler` tam-ikame ile kademeli cutover:** default handler'ı komple kaldırır, dual-run/reconciliation kapısı çalışmaz. **Reddedildi** (yalnız post-basamak-2 tam-migrasyon).
- **Fork core'a async-history producer ekleme:** motor kodunu değiştirir (NFR-M1 ihlali). **Reddedildi** (SPI zaten yeterli kapı bırakıyor — Javadoc :26-29).
- **HistoryLevel uyumsuzluğunda hard-reject startup:** motor bootstrap'ını basamak-2 kapsamının ötesinde etkiler (HistoryLevel motor-genel). **Reddedildi** (BA-Q4 — WARN seçildi).
