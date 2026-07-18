# ADR-0018 — Sınıf-bazlı retention enforcement + audit-log invariant

- **Durum:** Kabul edildi (2026-07-17, Phase 3 — basamak-2)
- **İzlenebilirlik:** US-G1 → FR-G1 → BR-PII-001 → `BUS_RETENTION_WINDOW_BREACH_DETECTED`/`VAL_RETENTION_OVERRIDE_BELOW_LEGAL_MINIMUM`/`SYS_RETENTION_JOB_FAILED`/`SYS_RETENTION_AUDIT_LOG_WRITE_FAILED` → PO-Q7 / BA-Q8 / DP-9 / NFR-S2/S3

## Bağlam

PO-Q7 (kilitli): projeksiyon retention **sınıf-bazlı**, otomatik enforcement'lı — bulk **default 90 gün** (kiracı override); audit-kritik **yasal-saklama süresi** (örn. 7y, kiracı override; hukuki dayanak DPO doğrulamasına işaretli). `DATA_GOVERNANCE v4.0 §4.4`: her silme için audit-log kaydı. BA-Q8: bir sınıf hem yasal-saklamaya hem opt-in pseudonymization'a tabiyse, pseudonymization retention süresini KISALTMAZ. History PII'nin uzun at-rest tutulduğu yerdir (projeksiyon L3, DP-9).

## Karar

- **RetentionEnforcementJob** (scheduled), projeksiyon satırlarını sınıf-başına retention penceresine göre tarar/siler. **Mekanizma:** projeksiyon tabloları zaman-bazlı **range-partition**'lıdır (ADR-0011) → süresi dolan bölümler `DROP/DETACH PARTITION` ile silinir (bulk-DELETE VACUUM yükü olmadan; PostgreSQL declarative range partitioning + pg_partman deseni [resmi doküman]).
- **Sınıf-bazlı pencere:** bulk → 90g (kiracı override); audit-kritik → yasal-saklama (kiracı override). Kiracı audit-kritik retention'ı yasal-asgarinin altına çekmeye çalışırsa `VAL_RETENTION_OVERRIDE_BELOW_LEGAL_MINIMUM` → **reddedilir** (hukuki/DPO onayı gerekir). Konfigürable `N`/pencere geçersizse config reddedilir.
- **Audit-log invariant (compliance-kritik):** her retention silmesi audit-log kaydı üretir (kim/ne/ne-zaman). Silme BAŞARILI ama audit-log yazımı BAŞARISIZ ise `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED` = **CRITICAL — on-call page** (silme oldu, izi yok = compliance-invariant ihlali; `SYS_SENTINEL_WORKER_CONFLICT` ciddiyet sınıfı). Job'ın kendisi DB hatasıyla düşerse `SYS_RETENTION_JOB_FAILED` (log-only, sonraki periyotta tekrar — satırlar bir periyot gecikmeli silinir, zararsız).
- **Pseudonymization etkileşimi (BA-Q8):** pseudonymized audit-kritik satır hâlâ tam yasal-saklama süresine tabidir — pseudonymization yalnız alan tersinebilirliğini değiştirir, kaydın (denetim izinin) yaşam döngüsünü değil (ADR-0016 harita-kaydı silme retention'ı KISALTMAZ).
- **Erasure ile ilişki:** retention = süre-tetikli, sınıf-bazlı (bu ADR); erasure = talep-tetikli, alan-düzeyi (ADR-0017). İki mekanizma aynı store'da (ADR-0011) yaşar, çakışmaz.

## Sonuçlar

**Olumlu:** KVKK/GDPR veri-minimizasyonu otomatik uygulanır (retention-expiry DROP PARTITION — verimli, VACUUM yükü yok); audit-log invariant her silmeyi izlenebilir kılar; yasal-asgari guard'ı compliance ihlalini config-zamanında engeller. Sınıf-bazlı pencere audit-kritik yasal-saklama ile bulk-minimizasyon gerilimini uzlaştırır.

**Olumsuz / kabul edilen:** Range-partition sınırları retention granülerliğini partition-boyutuna bağlar (gün-partition → gün-granülerlik). `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED` CRITICAL → audit-log yazımının kendisi yüksek-güvenilirlik gerektirir (silme-audit atomikliği phase4/5 LLD). Yasal-saklama süresinin kesin gerekçesi kiracı DPO onayına bağlı (`TENANT_PII_CHECKLIST` §4).

## Reddedilenler
- **Bulk-DELETE ile retention (partition yerine):** VACUUM yükü, hot-path etkisi. **Reddedildi** (DROP/DETACH PARTITION seçildi — resmi doküman deseni).
- **Silme sonrası audit-log'u best-effort (CRITICAL değil):** compliance-invariant ihlali sessiz kalır. **Reddedildi** (CRITICAL page — `DATA_GOVERNANCE v4.0 §4.4`).
- **Pseudonymization sonrası retention kısaltma:** denetim izinin bütünlüğünü bozar. **Reddedildi** (BA-Q8 — retention DEĞİŞMEZ).
- **Tek-tip retention (sınıf-bağımsız):** audit-kritik yasal-saklama ile bulk-minimizasyon çelişir. **Reddedildi** (PO-Q7 sınıf-bazlı).
