# ADR-0016 — Pseudonymization kasası (kimlik↔takma-ad haritası, L4-bitişik)

- **Durum:** **Kabul edildi** (2026-07-18 — ARCH-Q2 KARAR: **ayrı Postgres örneği/şeması**; Önerildi 2026-07-17 → ARCH-Q2 ile Kabul'e geçti). Semantik (L4-bitişik izolasyon, tx-içi saf hesap + downstream async persist, silme=harita-kaydı) BA-Q5/PO-Q2 ile karara bağlıydı; depo teknolojisi de artık kilitli.
- **İzlenebilirlik:** US-G3 → FR-G3 → BR-PII-003/BR-PII-004 → `BUS_PSEUDONYMIZATION_APPLIED`/`SYS_PSEUDONYM_VAULT_UNAVAILABLE`/`AUTH_PSEUDONYM_VAULT_ACCESS_DENIED`/`BUS_PSEUDONYM_MAP_ENTRY_DELETED` → PO-Q2 katman-3 / BA-Q5 / DP-16 / NFR-S8
- **Kesişim:** basamak-1 **ADR-0002** (KV) ve platform **OpenBao** (ADR-0008 transport'un kimlik altyapısıyla aynı ailede) aday depolar arasında.
- **phase1-review F-002 ilk-sınıf tasarım kalemi** (yeni stateful yüzey, D-A…D-G kanıt tabanı dışında).

## Bağlam

PO-Q2 katman-3 (kilitli): audit-kritik kayıtlarda PII alanı (userId) tersinmez takma-ada çevrilir; kimlik↔takma-ad haritası **ayrı bir kasada** tutulur; **silme = harita kaydını silmek** → takma-ad tersinmez olur (denetim izinin yapısı korunur, re-identification imkânsız). DP-16: kasa **re-identification anahtarıdır** → **L4-bitişik** (`DATA_GOVERNANCE v4.0 §2.1` "encryption keys" seviyesi): ayrı depo, en-az-yetki + audit, projeksiyon store'dan izole. BA-Q5: D-A'nın "handler-içi senkron I/O yasak" ilkesi vault round-trip'ine de uygulanır.

## Karar

### Zamanlama (BA-Q5 — kilitli, uygulanır)
- Pseudonym **DEĞERİ** tx-içinde **saf/deterministik** hesaplanır (kiracı-anahtarlı keyed-hash — I/O gerektirmez); değer kompakt outbox akışına (ADR-0010 audit-kritik yol) eşlik eder.
- Kasaya **YAZIM** (kimlik↔takma-ad haritası satırı) **downstream/async** yapılır (relay/projeksiyon boru hattıyla aynı hat). Kasa erişilemezse `SYS_PSEUDONYM_VAULT_UNAVAILABLE` → downstream retry; **audit-kritik outbox/relay/NATS akışı ENGELLENMEZ** (pseudonym değeri zaten tx-içi hesaplandı, yalnız kasa-persist gecikir). D-A ilkesinin BA-genişlemesi.

### İzolasyon & erişim (DP-16 — kilitli, uygulanır)
- Kasa **projeksiyon store'dan izole** ayrı depoda; erişim **en-az-yetki + audit-log**. Re-identification (yasal/adli gerekçeli, nadiren) yetkili + açık gerekçe + audit ister. Yetkisiz erişim → `AUTH_PSEUDONYM_VAULT_ACCESS_DENIED` = **CRITICAL, security-page** (basamak-1 `SYS_SENTINEL_WORKER_CONFLICT` ciddiyet sınıfı).
- **Silme = harita-kaydı silme:** data-subject erasure (pseudonymized audit-kritik kayıt) → yalnız harita-kaydı silinir (`BUS_PSEUDONYM_MAP_ENTRY_DELETED`); OP_LOG/INCIDENT satırının kendisi (denetim yapısı) KORUNUR, retention süresi DEĞİŞMEZ (BA-Q8; ADR-0018).

### Kasa deposu (ARCH-Q2 — KARAR 2026-07-18: ayrı Postgres)
Adaylar:
| Depo | Artı | Eksi |
|---|---|---|
| **Ayrı Postgres örneği/şeması (öneri)** | SQL ile harita-kaydı silme (erasure=DELETE, ADR-0017 ile aynı ops); at-rest AES-256; izole instance kolay; projeksiyon Postgres ops bilgisi yeniden kullanılır | Yeni izole DB örneği/şema ops yükü |
| NATS JetStream KV | Substrat-hizalı, ek altyapı yok (KV zaten aktif — ADR-0002) | Rastgele-erişim/silme audit sorguları KV için zayıf; anahtar-izolasyonu L4 için yetersiz olabilir |
| OpenBao (LF, MPL-2.0) | Gerçek secret/key-management (L4), rotation/audit yerleşik; platform kimlik ailesiyle hizalı | Ağır bağımlılık; harita "secret" değil "pseudonym-map" — semantik uyum kısmi; NFR-L1 (MPL-2.0, Apache-uyumlu ama Apache değil) |

**KARAR (ARCH-Q2, 2026-07-18): Ayrı Postgres örneği/şeması** — silme=harita-kaydı semantiği SQL-DELETE ile birebir, erasure pipeline'ıyla (ADR-0017) tutarlı ops, L4-bitişik izolasyon (ayrı instance + AES-256 + en-az-yetki). Reddedilen: JetStream KV (izolasyon zayıf, denetimli silme dolaylı), OpenBao (yeni ops yüzeyi; yalnız kiracı halihazırda kullanıyorsa yeniden değerlendirilebilir — kiracı-deployment seçeneği, çekirdek tasarım değil).

## Sonuçlar

**Olumlu:** Denetim izinin yapısı korunurken silme-hakkı karşılanır (tersinmez takma-ad); audit akışı kasa kesintisinden etkilenmez (BA-Q5 ayrık downstream); re-identification anahtarı en yüksek koruma altında (DP-16). SQL-tabanlı depo erasure/retention ops'uyla tutarlı.

**Olumsuz / kabul edilen:** Yeni stateful yüzey (kasa) — ayrı HA/backup/erişim-kontrolü ops'u gerektirir (ARCH-Q2 kararıyla Postgres becerisi yeniden kullanılır). Pseudonym deterministik keyed-hash olduğundan aynı kiracı-anahtarıyla aynı girdi aynı takma-adı üretir (korelasyon yüzeyi) — kabul (denetim tutarlılığı için gerekli); kiracı-anahtarı rotasyonu re-identification'ı etkiler (deploy politikası).

## Reddedilenler
- **Tx-içi senkron kasa çağrısı:** D-A'nın reddettiği tam risk (latency + rollback). **Reddedildi** (BA-Q5).
- **Projeksiyon store'da harita (aynı DB):** L4-bitişik izolasyon ihlali (DP-16). **Reddedildi**.
- **Kaydın kendisini silme (harita yerine):** denetim izinin bütünlüğünü bozar; yasal-saklama istisnasını (§6 katman-1) ihlal eder. **Reddedildi** (silme=harita-kaydı).
