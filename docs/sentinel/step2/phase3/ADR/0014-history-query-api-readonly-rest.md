# ADR-0014 — History sorgu-API: read-only REST/JSON, çekirdek-4, pluggable authz

- **Durum:** Kabul edildi (2026-07-17, Phase 3 — basamak-2; API kontratı sabit). Alt-eksen KARARA BAĞLANDI: dağıtım = **gömülebilir + opsiyonel standalone**, authz = **pluggable SPI** (ARCH-Q4, 2026-07-18).
- **İzlenebilirlik:** US-C1/US-C2 → FR-C1/FR-C2 → BR-QRY-001/002/003 → `VAL_QUERY_UNSUPPORTED_PATTERN`/`AUTH_QUERY_ACCESS_DENIED`/`BUS_QUERY_PII_MASKED` → D-C / PO-Q3 / BA-Q3 / NFR-S6

## Bağlam

D-C (kilitli): cutover'lanan sınıflarda Cockpit-history körleşir; karşılığında basamak-2 teslimatına projeksiyon store üstünde **minimal history sorgu-API'si** dahildir. PO-Q3: kapsam **çekirdek-4 okuma deseni** (processInstanceId→tam geçmiş; businessKey→instance listesi; zaman-aralığı+definition→liste; instance→task/activity/variable geçmişi); REST+JSON, sayfalamalı, read-only; agregasyon/analitik KAPSAM DIŞI. BA-Q3: sunulan sınıf kümesi = projeksiyonda var olan HER sınıf (dual-run dahil, cutover-bağımsız). SRS §4.7: bu bir gömülebilir OSS kütüphanedir, merkezi Admin-UI değildir; kimlik/authz pluggable (kiracı-sağlar).

**Cockpit `ACT_HI` bağımlılık yüzeyi [phase3'te doğrulandı]:** Camunda Cockpit'in history görünümleri (process-instance/definition history views) `ACT_HI_*` tablolarına bağlıdır ve **enterprise-only** özelliktir; history'yi başka bir DB örneğine yazmak Cockpit history görünümlerini bozar [resmi Camunda manual: webapps/cockpit/bpmn/process-history-views]. **Runtime Cockpit (`ACT_RU_*`) ETKİLENMEZ** (FR-C2). Cutover'lanan sınıfın history görünümü kararınca sorgu-API telafi eder; sınıf-başına körleşme haritası US-C2 dokümantasyonuyla verilir.

## Karar

- **HistoryQueryApi = read-only REST/JSON servis**, kontrat `docs/sentinel/step2/phase3/api/openapi.yaml` (OpenAPI 3.0.3; **redocly lint temiz** — bu ADR yazımında). Yalnız GET; sayfalamalı (`page`/`size` + `meta.total`); standart yanıt zarfı (`{success, message, code, data, meta}`, ARCHITECT_GUIDELINE §4). Okuma **projeksiyon Postgres'ten** (ADR-0011), engine DB'den DEĞİL.
- **Çekirdek-4 kapsamı sabit:** çekirdek-4 dışı istekler (agregasyon/analitik/filtresiz-tam-tarama) `VAL_QUERY_UNSUPPORTED_PATTERN` ile reddedilir (kapsam dışı, SRS §7).
- **Erişim + PII maskeleme:** her yanıt role-based erişim kontrolünden geçer (`AUTH_QUERY_ACCESS_DENIED`); RESTRICTED/PII alanlar (variable değeri, operatör kimliği, serbest metin) PII-görme izni yoksa maskelenir (`BUS_QUERY_PII_MASKED`; DP-15). Sorgu-API loglarına PII değeri YAZILMAZ (DP-1). Erişim-audit (kim-neyi-sorguladı) tutulabilir.
- **Cutover-bağımsız kapsam (BA-Q3):** API projeksiyonda var olan her sınıfı sunar; yanıt `cutoverState` alanı bilgilendiricidir, teknik filtre değil.
- **Dağıtım biçimi + authz (ARCH-Q4 — KARAR 2026-07-18):** gömülebilir kütüphane (kiracı gateway'ine gömülür) + **opsiyonel standalone** read-only servis; authz **pluggable SPI** (kiracı kendi Keycloak/APISIX/JWT bağlamasını verir). SRS §4.7 platform-compliance kapsam notuyla tutarlı (merkezi Keycloak/APISIX zorunluluğu bu gömülebilir kütüphaneye doğrudan uygulanmaz). Reddedilen: yalnız-gömülebilir (her tüketici JVM'e gömmek zorunda kalır), yalnız-standalone (hafif/gömülü senaryoyu keser). OpenAPI kontratı (çekirdek-4, zarf, maskeleme) karardan bağımsız sabit.

## Sonuçlar

**Olumlu:** Cockpit-körleşmesi telafi edilir (D-C); çekirdek-4 dar kapsam saldırı/karmaşıklık yüzeyini küçültür; standart zarf + pluggable authz kiracı stack'ine uyum sağlar. Makine-okunur OpenAPI kontratı codegen/doğrulanabilir.

**Olumsuz / kabul edilen:** Read-only kapsam agregasyon/raporlama ihtiyaçlarını karşılamaz (bilinçli — PO-Q3, gerekirse ayrı karar). Pluggable authz, somut kimlik bağlamasını kiracıya bırakır → deployment rehberi authz kurulumunu dokümante etmeli (ARCH-Q4). PII maskeleme role-modeli kiracı politikasına bağlıdır (DP-15).

## Reddedilenler
- **Agregasyon/analitik/raporlama görünümleri:** kapsam dışı (sorgu-API = çekirdek-4). **Reddedildi** (PO-Q3).
- **Engine DB'den okuma:** contention domain ayrımını bozar (D-B). **Reddedildi**.
- **Merkezi zorunlu Admin-UI + zorunlu Keycloak/APISIX:** gömülebilir OSS kütüphane doğasıyla çelişir (SRS §4.7). **Reddedildi** (pluggable authz seçildi; farklı zorunluluk isteniyorsa PO kararı).
- **Yazma/mutasyon uçları (read-write API):** history read-only bir projeksiyondur; erasure/retention ayrı kontrollü pipeline'dır (ADR-0017/0018), sorgu-API değil. **Reddedildi**.
