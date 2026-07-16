# TENANT PII CHECKLIST — History-Sınıf Düzeyi Sınıflandırma Şablonu
## Basamak-2: History Offload (ACT_HI → NATS → async query-store)

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Sentinel fazı:** Phase 1 — Product Owner (basamak-2)
**Tür:** **ŞABLON** — her kiracı/uygulama entegrasyonunda **doldurulacak**
**Materyalizasyon:** PO-Q7 kararı (2026-07-17) — history genişletmesi **ayrı dosya** olarak bu dizinde
**İlgili:** `DATA_CLASSIFICATION.md` (repo-düzeyi + §6 KVKK katmanlı politika, §8 özet), `SRS.md` (FR-G1…G3, NFR-S1…S8), basamak-1 `../../phase1/TENANT_PII_CHECKLIST_TEMPLATE.md` (**taban — job/reply/event payload katmanı korunur**)

> **Neden var:** history verisi PII yüzeyinin ta kendisidir ve **kalıcı** bir projeksiyon store'una yazılır. Basamak-1 şablonu job/reply/event payload alanlarını kapsıyordu; bu genişletme **ACT_HI event-sınıfı düzeyinde** PII envanterini, **tutarlılık katmanı (audit-kritik/bulk) seçimini** ve her sınıfın **retention + erasure + pseudonymization kararını** yapılandırılmış biçimde kaydeder. Doldurulmadan history offload production'a **açılmamalıdır**.

---

## 1. Nasıl doldurulur

1. **Önce basamak-1 şablonunu** (`../../phase1/TENANT_PII_CHECKLIST_TEMPLATE.md`) doldur — payload alan-düzeyi PII (job/reply/event). O katman **history payload'ının da içeriğini** belirler (VARINST/DETAIL aynı process değişkenlerini taşır).
2. Bu dosyada §3 tablosuna, akışa giren **her ACT_HI event-sınıfını** bir satır olarak ekle.
3. Her sınıf için **tutarlılık katmanını** seç (§2 rehberi — audit-kritik / bulk).
4. Her sınıfın taşıdığı **PII alanlarını** ve **sınıfını** işaretle (`DATA_CLASSIFICATION.md §1`).
5. Her sınıfa bir **retention** + **erasure politikası** ata (§2.2 rehberi).
6. Audit-kritik sınıflar için **KVKK katmanlı politikasını** (`DATA_CLASSIFICATION.md §6`) seç: yasal-saklama istisnası ve/veya pseudonymization.
7. §4'te DPO / kiracı onayını al.

**Kural:** Boş bırakılan satır = "sınıflandırılmadı" = production-blocker.

---

## 2. Karar rehberleri

### 2.1 Tutarlılık katmanı seçimi (PO-Q5 rehberi)

> **Repo default'u (PO-Q5 2026-07-17):** audit-kritik = **{OP_LOG, INCIDENT, EXT_TASK_LOG}**; diğer tüm sınıflar (IDENTITYLINK/COMMENT/ATTACHMENT dahil) **bulk**. Aşağıdaki rehberle kiracı bu default'u override edebilir.

| Soru | Evet → | Hayır → |
|---|---|---|
| Bu sınıfın kaybı **denetim/uyum/adli** açıdan kabul edilemez mi? | **audit-kritik** (at-least-once, tx-içi kompakt outbox) | bulk adayı |
| Sınıf **yüksek hacimli** mi (DETAIL/VARINST/ACTINST)? | **bulk** (at-most-once, sıfır DB yazımı) — audit-kritik yapmak yazım kazancını yer | — |
| PII-yoğun ama düşük-hacim mi (IDENTITYLINK/COMMENT/ATTACHMENT)? | **bulk kalır**; PII koruması **retention/erasure**'dan (§2.2) gelir, tutarlılık katmanından değil | — |

> **Uyarı:** PII-yoğunluk **tek başına** audit-kritik gerekçesi değildir; audit-kritik at-least-once maliyeti (tx-içi outbox) yalnız **kayıp-kabul-edilemez** sınıflar içindir. PII koruması EPIC-G (retention/erasure/pseudonymization) sorumluluğudur.

### 2.2 Retention + erasure kararı (PO-Q7 + §6 rehberi)

| Sınıf tipi | Retention default (PO-Q7) | Erasure politikası (PO-Q2 §6) |
|---|---|---|
| **Bulk PII** (VARINST/DETAIL/TASKINST/COMMENT/ATTACHMENT/IDENTITYLINK) | **90 gün** (kiracı override) | **erasure pipeline** (silme/anonimleştirme, US-G2) |
| **Audit-kritik** (OP_LOG/INCIDENT/EXT_TASK_LOG) | **yasal-saklama süresi** (örn. 7y, kiracı override) | **yasal-saklama istisnası** (erasure'dan muaf) + opt-in **pseudonymization** (US-G3) |

**Masking/koruma seçenekleri:** `MASK` · `HASH` · `TOKEN/PSEUDONYM` (ayrı kasa, DP-16) · `ANONYMIZE` (erasure) · `LEGAL-HOLD` (yasal-saklama) · `NONE` (gerekçeli).

---

## 3. History event-sınıfı envanteri (doldurulacak)

| ACT_HI sınıfı | Bu kiracıda taşıdığı PII alanları | Sınıf (`DATA_CLASSIFICATION §1`) | Tutarlılık (A/B) | Projeksiyon retention | Erasure politikası | Pseudonymization? | Sorumlu |
|---|---|---|---|---|---|---|---|
| VARINST | _(örn. `customer.msisdn`)_ | RESTRICTED/PII | B | _(90g)_ | _(ANONYMIZE)_ | _(H)_ | _(kiracı)_ |
| DETAIL | _(değişken güncelleme değerleri)_ | RESTRICTED/PII | B | _(90g)_ | _(ANONYMIZE)_ | _(H)_ | _(kiracı)_ |
| ACTINST | _(assignee — user-task'ta)_ | INTERNAL/PII | B | | | | |
| PROCINST | _(businessKey, startUserId)_ | CONFIDENTIAL/PII | B | | | | |
| TASKINST | _(assignee, owner, name, description)_ | RESTRICTED/PII | B | | | | |
| IDENTITYLINK | _(userId/groupId)_ | RESTRICTED/PII | B _(default)_ | | | | |
| OP_LOG | _(operatör userId, orgValue/newValue)_ | RESTRICTED/PII | **A** | _(yasal-saklama)_ | _(LEGAL-HOLD)_ | _(E/H — opt-in)_ | _(DPO)_ |
| INCIDENT | _(incidentMessage)_ | RESTRICTED/PII | **A** | _(yasal-saklama)_ | _(LEGAL-HOLD)_ | | _(DPO)_ |
| EXT_TASK_LOG | _(workerId, errorDetails)_ | RESTRICTED/PII | **A** | _(yasal-saklama)_ | _(LEGAL-HOLD)_ | | _(DPO)_ |
| JOB_LOG | _(jobExceptionMessage)_ | INTERNAL/PII-riski | B | | | | |
| COMMENT | _(message, userId)_ | RESTRICTED/PII | B _(default)_ | | | | |
| ATTACHMENT | _(ad, url/içerik, userId)_ | RESTRICTED/PII | B _(default)_ | | | | |
| _(DECINST/CASEINST/BATCH — kullanılıyorsa)_ | | | | | | | |

> **Hatırlatma:** history mesajı `dlq.history.>`'a düşerse payload'ı **byte-aynen** kopyalanır (US-B5) → her RESTRICTED/PII satırı ayrıca bir **DLQ retention/erişim** kararı gerektirir (DP-13; DLQ default 14g — PII ise kısalt/kısıtla).

---

## 4. Onay (kiracı entegrasyonu)

| Rol | Ad | Karar | Tarih | İmza/onay ref |
|---|---|---|---|---|
| Veri Koruma Sorumlusu (DPO) | _(...)_ | Onay / Ret | _(...)_ | _(...)_ |
| **Hukuki (yasal-saklama dayanağı — §6 katman-1)** | _(...)_ | Audit-kritik yasal-saklama dayanağı doğrulandı | _(...)_ | _(...)_ |
| Kiracı teknik sahibi | _(...)_ | Onay / Ret | _(...)_ | _(...)_ |
| Platform/Ops | _(...)_ | Retention/erasure/pseudonymization uygulandı | _(...)_ | _(...)_ |

**Kapanış koşulu (basamak-2):** §3'teki her ACT_HI sınıfı sınıflandırıldı; her sınıf bir **tutarlılık katmanı + retention + erasure** kararına bağlandı; audit-kritik sınıflar için §6 KVKK katmanlı politikası (yasal-saklama dayanağı hukuk onayı + opsiyonel pseudonymization) çözüldü; §4 onaylandı.

---

## 5. Referanslar
- `DATA_CLASSIFICATION.md` — repo-düzeyi sınıflandırma, DP-1…16, §6 KVKK katmanlı politika (KARAR), §5 retention, §3 trust boundary.
- `SRS.md` — FR-G1…G3 (retention/erasure/pseudonymization), NFR-S1…S8, IR-6 (sorgu-API).
- `USER_STORIES.md` — US-A2 (sınıflandırma), EPIC-G (US-G1/G2/G3), §4 PO Karar Kaydı (PO-Q2/Q5/Q7).
- Basamak-1 `../../phase1/TENANT_PII_CHECKLIST_TEMPLATE.md` — payload alan-düzeyi katman (korunur, önce doldurulur).
