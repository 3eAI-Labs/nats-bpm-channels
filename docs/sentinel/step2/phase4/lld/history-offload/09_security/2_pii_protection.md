# 09.2 — PII Koruması (Projeksiyon + Kasa + Sorgu-API)

**Kaynak:** `DATA_CLASSIFICATION.md` DP-9…DP-16, ADR-0011/0014/0016/0017/0018. Basamak-1'de bu bölüm YOKTU (payload geçiciydi) — basamak-2'nin "en ağır" güvenlik yüzeyi (`DATA_CLASSIFICATION.md` girişi).

---

## 1. At-rest şifreleme (DP-9/NFR-S2)

| Depo | Şifreleme | Uygulama katmanı |
|---|---|---|
| Projeksiyon Postgres (15 sınıf tablosu) | Disk-düzeyi AES-256 (deploy-time, kiracı-owned altyapı) | `08_config.md` altyapı notu — kod değil |
| Pseudonym kasası `pseudonym_map.real_user_id_encrypted` | Disk-düzeyi AES-256 **+ pgcrypto column-level `pgp_sym_encrypt`** (savunma-derinliği, en yüksek hassasiyetli TEK kolon) | `PseudonymizationVaultClient` (`03_classes/6_governance.md` §3) — `pgcrypto` anahtarı KOD İÇİNDE YOK, `PseudonymVaultDataSourceProperties.vaultColumnEncryptionKeyRef` deploy-secret referansı |

---

## 2. Erişim kontrolü — role-based (DP-11/DP-15)

| Bileşen | Kontrol noktası | Reddedilirse |
|---|---|---|
| `HistoryQueryApi` | `HistoryQueryAuthzSpi.isAuthorized(...)` (pluggable, ARCH-Q4) | `AUTH_QUERY_ACCESS_DENIED` |
| `HistoryQueryApi` (PII alanları) | `HistoryQueryAuthzSpi.hasPiiViewPermission(...)` → `PiiMaskingService` | `BUS_QUERY_PII_MASKED` (bilgilendirici, reddetmez — maskeler) |
| `PseudonymizationVaultClient.reidentify(...)` | Çağıranın `authorized` parametresi (çağıran tarafın kendi authz katmanınca ÖNCEDEN doğrulanmış) | `AUTH_PSEUDONYM_VAULT_ACCESS_DENIED` (**CRITICAL, security-page**) |
| `HistoryDlqInspectionConsumer` | NATS subject-ACL (`09_security/1_transport_authz.md` §2, birincil) + kendi role-check (ikincil) | `RES_HISTORY_DLQ_ACCESS_DENIED` |

**`PiiMaskingService` (yeni, `com.threeai.nats.history.query`):**

```java
public class PiiMaskingService {
    /** Masks RESTRICTED/PII fields (variable value, operator identity, free text) per DP-15 when
     *  ctx.hasPiiViewPermission()==false. Never masks INTERNAL/PUBLIC fields (activityId,
     *  historyEventId). Masking is applied AFTER the DB read, BEFORE serialization -- masked values
     *  never appear in HistoryQueryApi's own logs (DP-1). */
    public <T> T mask(T responseDto, QueryContext ctx);
}
```

---

## 3. Log/metrik-tag PII sızdırmazlığı (DP-1/DP-14 — tüm bileşenlerde ortak kural)

Basamak-1'in `07_errors.md §2` MDC alan seti deseni AYNEN uygulanır: `trace_id`, `history_class`, `subject`, `engine_id`, `process_instance_id` (PSEUDONYMOUS, DP-1 gereği hâlâ loglanabilir — PII DEĞİL) MDC'ye YAZILIR; `business_key`, variable değeri, operatör kimliği, serbest metin ASLA. `ReconciliationJob` rapor çıktısı yalnız sayaç/id/hash taşır (DP-14) — bu, `ProjectionStore`'un `count(*)` sorgularıyla PII değeri hiç OKUMAMASI anlamına gelir (yapısal koruma, uygulama-katmanı maskeleme GEREKMEZ).

---

## 4. Pseudonymization'ın kendisi bir güvenlik kontrolüdür (BA-Q5, DP-16)

`PseudonymTokenGenerator.generate(...)` **tersinmez DEĞİLDİR** (deterministik keyed-hash — aynı `tenantKeyId`+`realValue` aynı token) — tersinmezlik `pseudonym_map` satırının SİLİNMESİYLE elde edilir (§`03_classes/6_governance.md` §3). `tenantKeyId` rotasyonu (deploy politikası) korelasyon riskini azaltır ama mevcut token'ları YENİDEN HESAPLANAMAZ hale getirir (eski token'lar eski anahtar-versiyonuyla kasada kalır, `tenant_key_version` kolonu bunu izler — `DB_SCHEMA.md §3`).

**Bağımlılık:** NFR-S1/S2/S3/S6/S8, DP-1/DP-9/DP-11/DP-14/DP-15/DP-16, ADR-0016/0017.
