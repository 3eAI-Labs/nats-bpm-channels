# TENANT PII CHECKLIST — Alan-Düzeyi Sınıflandırma Şablonu
## Basamak-1: External Task / Event-Driven Work Offload over JetStream

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Sentinel fazı:** Phase 1 — Product Owner teslimatı (Q5 kararı, 2026-07-14)
**Tür:** **ŞABLON** — her kiracı/uygulama entegrasyonunda **doldurulacak**
**İlgili:** `DATA_CLASSIFICATION.md` (repo-düzeyi sınıflandırma), `SRS.md` (NFR-S1…S4, DP-1…DP-8)

> **Neden var:** `nats-bpm-channels` bir altyapı katmanıdır; job/reply/event **payload'ının alan içeriği kiracı-tanımlıdır** ve repo düzeyinde bilinemez (`DATA_CLASSIFICATION.md §6`). Bu şablon, kiracının **alan-düzeyi (field-level)** PII kararını yapılandırılmış biçimde kaydetmesi içindir. Doldurulmadan production'a PII taşıyan bir A2/Event-Registry akışı **açılmamalıdır**.

---

## 1. Nasıl doldurulur

1. Her BPMN modeli / A2 topic'i / Event Registry channel'ı için akışa giren **her payload alanını** ve mandatory header'ları §3 tablosuna satır olarak ekle.
2. Her alana bir **sınıf** ata (§2 şeması).
3. PII/koşullu-PII ise bir **masking kararı** ver (maskele / hashle / opak-token / olduğu-gibi + gerekçe).
4. Alanın DLQ'ya düşebileceğini unutma → **retention notu** yaz (DLQ default 14g; PII ise kısaltma/erişim kısıtlama gerekir — DP-3).
5. §4'te Veri Koruma Sorumlusu (DPO) / kiracı onayını al.

**Kural:** Boş bırakılan satır = "sınıflandırılmadı" = production-blocker.

---

## 2. Sınıflandırma şeması (referans — `DATA_CLASSIFICATION.md §1`)

| Sınıf | Tanım |
|---|---|
| **PUBLIC** | İfşası zarar üretmez |
| **INTERNAL** | Operasyonel; iş verisi değil |
| **PSEUDONYMOUS** | Teknik korelasyon kimliği; PII'ye bağlanabilir |
| **CONFIDENTIAL** | Hassas iş kimliği; bağlama göre PII |
| **RESTRICTED / PII** | Kişisel veri; en yüksek koruma |

**Masking kararı seçenekleri:** `NONE` (gerekçeli) · `MASK` (kısmi, örn. son 4 hane) · `HASH` (tek yönlü) · `TOKEN` (opak referans) · `DROP` (alanı taşıma).

---

## 3. Alan-düzeyi envanter (doldurulacak)

### 3.1 Mandatory header'lar (ön-doldurulmuş — kiracı masking/retention kararını verir)

| Alan | Kaynak | Sınıf (repo default) | PII? | Masking kararı | Retention notu | Sorumlu |
|---|---|---|---|---|---|---|
| `X-Cadenzaflow-Trace-Id` | header | PSEUDONYMOUS | Hayır | `NONE` | — | _(kiracı)_ |
| `X-Cadenzaflow-Business-Key` | header | CONFIDENTIAL (koşullu PII) | _(kiracı: E/H)_ | _(MASK/HASH/NONE öner: DP-8)_ | _(DLQ 14g — PII ise kısalt)_ | _(kiracı)_ |
| `X-Cadenzaflow-Idempotency-Key` | header | PSEUDONYMOUS | _(business-key'den türetiliyorsa E)_ | _(HASH öner: DP-7)_ | — | _(kiracı)_ |
| `X-Cadenzaflow-Correlation-Id` | header | PSEUDONYMOUS | _(kiracı)_ | _(kiracı)_ | — | _(kiracı)_ |
| `X-Cadenzaflow-Reply-Subject` | header | INTERNAL | Hayır | `NONE` | — | _(kiracı)_ |

### 3.2 Job / reply / event payload alanları (KİRACI EKLER — her alan bir satır)

| Alan (JSON path / değişken adı) | Kaynak (job/reply/event) | Sınıf | PII? | Masking kararı | Retention notu (DLQ 14g) | Sorumlu |
|---|---|---|---|---|---|---|
| _(örn. `customer.msisdn`)_ | _(job)_ | _(RESTRICTED/PII)_ | _(E)_ | _(MASK: son 4)_ | _(PII → DLQ retention kısalt + erişim kısıtla)_ | _(kiracı)_ |
| _(örn. `order.id`)_ | _(job)_ | _(CONFIDENTIAL)_ | _(H)_ | _(NONE)_ | _(—)_ | _(kiracı)_ |
| _(...)_ | | | | | | |

> **Hatırlatma:** DLQ mesajı orijinal payload'ı **byte-aynen** kopyalar (US-C1). Yani payload'daki her PII alanı, DLQ stream'inde retention süresince (default 14g) durur → §3.2'deki her RESTRICTED/PII satırı bir DLQ retention/erişim kararı gerektirir (DP-3).

---

## 4. Onay (kiracı entegrasyonu)

| Rol | Ad | Karar | Tarih | İmza/onay ref |
|---|---|---|---|---|
| Veri Koruma Sorumlusu (DPO) | _(...)_ | Onay / Ret | _(...)_ | _(...)_ |
| Kiracı teknik sahibi | _(...)_ | Onay / Ret | _(...)_ | _(...)_ |
| Platform/Ops | _(...)_ | Retention/erişim uygulandı | _(...)_ | _(...)_ |

**Kapanış koşulu:** §3'teki tüm satırlar sınıflandırıldı; RESTRICTED/PII satırlarının hepsi bir masking + retention kararına bağlandı; §4 onaylandı.

---

## 5. Referanslar
- `DATA_CLASSIFICATION.md` — repo-düzeyi sınıflandırma, DP-1…DP-8, trust boundary (§3), saklama (§5).
- `SRS.md` — NFR-S1…S4 (güvenlik/veri koruma), IR-2 (header kontratı).
- Q3/Q4/Q5 kararları: `USER_STORIES.md §4` (PO Karar Kaydı, 2026-07-14).
