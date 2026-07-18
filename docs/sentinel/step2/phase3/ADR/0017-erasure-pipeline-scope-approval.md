# ADR-0017 — Bulk PII erasure pipeline + kapsam-onayı akışı

- **Durum:** Kabul edildi (2026-07-17, Phase 3 — basamak-2; BA-Q6 kararının mimari gerçekleştirmesi)
- **İzlenebilirlik:** US-G2 → FR-G2 → BR-PII-002/BR-PII-005 → `BUS_ERASURE_REQUEST_ACCEPTED`/`BUS_ERASURE_REQUEST_LEGAL_HOLD_BLOCKED`/`VAL_ERASURE_SUBJECT_KEY_AMBIGUOUS`/`SYS_ERASURE_PIPELINE_FAILED`/`RES_ERASURE_VERIFICATION_FAILED` → PO-Q2 katman-2 / BA-Q6 / DP-10 / NFR-S3
- **phase1-review F-002 ilk-sınıf tasarım kalemi.**

## Bağlam

PO-Q2 katman-2 (kilitli): bulk sınıflardaki PII (VARINST/DETAIL değerleri, TASKINST name/description, serbest metinler) projeksiyon-DB üstünde **erasure/anonimleştirme pipeline'ına** tabi (KVKK/GDPR silme-hakkı; `compliance/KVKK v1.0 §2.1` 30g SLA, §4.3 soft-delete→anonimleştirme). Audit-kritik sınıflar **yasal-saklama istisnası** (erasure REDDEDİLİR; pseudonymization alternatifi — ADR-0016). BA-Q6: telco MSISDN churn — aynı businessKey zamanla farklı gerçek kişilere ait olabilir → bare key-match riski.

## Karar

- **ErasurePipeline**, data-subject anahtarına (businessKey/userId) göre bulk sınıf PII'larını **soft-delete → anonymize** eder; **SQL-uygulanabilir** (ADR-0011 denormalize şema). Erasure audit-log'lanır (kim-neyi-ne-zaman-sildi).
- **Sınıf-yönlendirme:** bulk hedef → `BUS_ERASURE_REQUEST_ACCEPTED`; audit-kritik hedef → `BUS_ERASURE_REQUEST_LEGAL_HOLD_BLOCKED` (erasure reddedilir, pseudonymization alternatifi sunulur — ADR-0016).
- **Kapsam-onayı akışı (BA-Q6):** erasure bare businessKey eşleşmesiyle **doğrudan yürütülmez**. subject-key tek zaman-aralığında tek instance kümesine karşılık geliyorsa doğrudan; birden fazla döneme yayılıyorsa `VAL_ERASURE_SUBJECT_KEY_AMBIGUOUS` → aday instance/zaman-aralığı listesi talep sahibine sunulur, **açık kapsam onayı** olmadan pipeline TETİKLENMEZ (telco kimlik-yeniden-kullanımı koruması).
- **Doğrulanabilir tamlık:** pipeline sonrası doğrulama sorgusu Sorgu-API'de o PII'yi döndürmemeli; hâlâ dönüyorsa `RES_ERASURE_VERIFICATION_FAILED` = **CRITICAL** (KVKK 30g SLA riski). Pipeline adımı teknik başarısız → `SYS_ERASURE_PIPELINE_FAILED` (retry + alert + audit-log(başarısızlık); idempotent SQL varsayılır).
- **Retention ile ilişki:** retention-süresi dolan bulk satırlar zaten `DROP/DETACH PARTITION` ile silinir (ADR-0018); erasure bunu **talep-tetikli, süre-öncesi** ve **alan-düzeyi anonimleştirme** olarak tamamlar (aktif partition içinde UPDATE/anonymize).

## Sonuçlar

**Olumlu:** KVKK/GDPR silme-hakkı SQL-uygulanabilir ve doğrulanabilir; kapsam-onayı yanlış-kapsamlı silmeye karşı ucuz koruma (telco güveni); audit-log erasure'ın kendisini izlenebilir kılar. Bulk/audit-kritik ayrımı §6 katmanlı politikayı yapısal uygular.

**Olumsuz / kabul edilen:** Kapsam-onayı ek insan-adımı getirir (belirsiz subject-key'de gecikme) — 30g SLA içinde yönetilmeli. Soft-delete→anonymize mekanizması alan-düzeyi PII haritasına bağlıdır (kiracı-tanımlı, `TENANT_PII_CHECKLIST`). `RES_ERASURE_VERIFICATION_FAILED` CRITICAL → doğrulama otomasyonu gerektirir.

## Reddedilenler
- **Bare businessKey ile doğrudan erasure:** yanlış kişinin verisini silme/koruma riski (MSISDN churn). **Reddedildi** (BA-Q6 — kapsam-onayı).
- **Audit-kritik sınıfları da erasure'a tabi tutma:** denetim izinin bütünlüğünü bozar, yasal-saklama ihlali. **Reddedildi** (§6 katman-1; pseudonymization alternatifi — ADR-0016).
- **Hard-delete (soft-delete→anonymize yerine doğrudan silme):** doğrulama/geri-izleme penceresi bırakmaz. **Reddedildi** (`KVKK v1.0 §4.3` soft-delete→anonymize deseni; retention-expiry hard-drop ADR-0018'de ayrı).
