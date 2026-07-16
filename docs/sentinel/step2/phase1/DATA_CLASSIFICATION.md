# DATA CLASSIFICATION — PII & Veri Hassasiyeti Eşlemesi
## Basamak-2: History Offload (ACT_HI → NATS → async query-store)

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Sentinel fazı:** Phase 1 — Product Owner (basamak-2)
**Yerleşim:** `docs/sentinel/step2/phase1/` (PO-Q1)
**Tarih:** 2026-07-16 (açılış) / 2026-07-17 (PO kararları işlendi)
**Durum:** Onaylı (2026-07-17) — PO-Q1…7 cevaplandı; **PO-Q2 KVKK katmanlı politikası §6'da KARAR olarak yazıldı**
**İlgili teslimatlar:** `USER_STORIES.md`, `SRS.md`, `GUIDELINES_MANIFEST.yaml`, `TENANT_PII_CHECKLIST_TEMPLATE.md` (basamak-2 history-genişletmesi, PO-Q7 — bu dizinde), basamak-1 `../../phase1/DATA_CLASSIFICATION.md` (DP-1…8 devri) + `../../phase1/TENANT_PII_CHECKLIST_TEMPLATE.md` (taban)

> **Basamak-2, veri-koruma açısından basamağın en ağırıdır: history verisi PII yüzeyinin TA KENDİSİDİR.** Basamak-1'de tel üzerinde geçen payload'lar geçici (WorkQueue ack'te silinir, DLQ 14g) idi; basamak-2 bu veriyi **kalıcı, sorgulanabilir bir projeksiyon store'una** yazar → **uzun retention + rastgele-erişim + operatör-kimlikleri** yeni bir governance yüzeyi açar. Bu belge history akışında akan/saklanan/loglanan tüm veri öğelerini sınıflandırır. Motor/SPI iddiaları `07 §3`'te **DOĞRULANMIŞ** `file:line` referanslıdır. Basamak-1 **DP-1…DP-8 aynen devralınır**; basamak-2 **DP-9…DP-16** ekler (PO-Q2/Q7 kararları işlendi).

---

## 1. Sınıflandırma şeması (basamak-1 ile aynı — `../../phase1/DATA_CLASSIFICATION.md §1`)

| Sınıf | Tanım | History örneği |
|---|---|---|
| **PUBLIC** | Açığa çıkması zarar üretmez | subject adı, class adı, metrik adı |
| **INTERNAL** | Operasyonel; iş verisi değil | `activityId`, `historyEventId`, delivery-count, engineId |
| **PSEUDONYMOUS** | Teknik korelasyon kimliği; PII'ye bağlanabilir | `processInstanceId`, `Trace-Id`, `Idempotency-Key` |
| **CONFIDENTIAL** | Hassas iş kimliği; bağlama göre PII | `businessKey` (PROCINST) |
| **RESTRICTED / PII** | Kişisel veri içerir; en yüksek koruma | variable payload (VARINST/DETAIL), operatör kimlikleri (OP_LOG userId, TASKINST assignee/owner), COMMENT/ATTACHMENT, incident/errorMessage |

**Telco bağlamı uyarısı (basamak-1'den):** 3eAI Labs telekom senaryolarında (`cn-advanced-ota`, `01 §9`) `businessKey` ve process değişkenleri MSISDN/IMSI/abone-id/IMEI taşıyabilir → `businessKey` CONFIDENTIAL (koşullu PII), variable payload RESTRICTED/PII varsayılır. **Basamak-2'ye özgü ek:** history, **operatör kimliklerini** de kalıcılaştırır (OP_LOG `userId`, TASKINST `assignee`/`owner`, PROCINST `startUserId`, IDENTITYLINK `userId`) → bunlar **doğrudan kişisel veridir** (çalışan/operatör), audit gerekçesiyle tutulur ama KVKK kapsamındadır (PO-Q2).

---

## 2. Veri öğeleri envanteri (history akışı: producer → wire → store → API → telemetry)

### 2.1 ACT_HI event sınıfları — sınıf-başına PII yüzeyi

> Kaynak: `07 §3` ACT_HI tablo haritası (16+ entity → tablo). Değişken byte'ları ayrıca `ByteArrayEntity(..., ResourceTypes.HISTORY)` (`DbHistoryEventHandler.java:97-105`). Sınıflar **audit-kritik (A)** / **bulk (B)** olarak işaretli — **PO-Q5 kararı (2026-07-17): nihai audit-kritik = {OP_LOG, INCIDENT, EXT_TASK_LOG}; IDENTITYLINK/COMMENT/ATTACHMENT bulk yolda** (PII koruması tutarlılık katmanından değil, EPIC-G retention/erasure'dan gelir). Konfigle audit-kritik'e taşınabilirler.

| Event sınıfı (ACT_HI_) | Tutarlılık (D-A) | Taşıdığı hassas alanlar | Sınıf | Not |
|---|---|---|---|---|
| **VARINST** (VariableInstance) | **B** bulk | variable adı + **değeri** (kiracı-tanımlı), byte-array payload | **RESTRICTED / PII** | Hacmin büyük kısmı; keyfi iş verisi. at-most-once (kayıp reconciliation'da görünür) |
| **DETAIL** (VariableUpdate/FormProperty) | **B** bulk | değişken güncelleme **değerleri**, byte-array | **RESTRICTED / PII** | En büyük hacim; ilk cutover adayı (hacim-öncelikli) |
| **ACTINST** (ActivityInstance) | **B** bulk | activityId/type, taskAssignee (user-task'ta) | INTERNAL → assignee varsa **PII** | activity meta INTERNAL; assignee PII |
| **PROCINST** (ProcessInstance) | **B** bulk | **businessKey**, **startUserId**, superProcessInstanceId, deleteReason | CONFIDENTIAL (businessKey) / **PII** (startUserId) | businessKey telco'da MSISDN olabilir; startUserId operatör kimliği |
| **TASKINST** (TaskInstance) | **B** bulk | **assignee**, **owner**, name, description, deleteReason | **RESTRICTED / PII** | assignee/owner operatör kimliği; name/description serbest metin (PII riski) |
| **IDENTITYLINK** | **B** bulk (PO-Q5) | **userId** / groupId, type (assignee/candidate/owner) | **RESTRICTED / PII** | Kimlik-atama; bulk yolda kalır, PII koruması EPIC-G'den (retention/erasure). Konfigle audit-kritik'e taşınabilir |
| **OP_LOG** (UserOperationLog) | **A** audit-kritik | **userId** (operatör), operationType, entityType, property, orgValue, newValue | **RESTRICTED / PII** | **Denetim izinin kendisi**: kim-ne-yaptı. KVKK silme ↔ denetim gerilimi merkezi → §6 katmanlı politika (PO-Q2) |
| **INCIDENT** | **A** audit-kritik | incidentMessage, configuration, activityId | **RESTRICTED / PII** (mesaj serbest metin) | Audit-kritik; mesaj PII sızdırabilir |
| **EXT_TASK_LOG** | **A** audit-kritik | workerId, **errorMessage**, errorDetails (byte) | **RESTRICTED / PII** (errorDetails payload/stack) | Audit-kritik; errorDetails orijinal payload/stack taşıyabilir |
| **JOB_LOG** | **B** bulk | jobExceptionMessage, config | INTERNAL → mesaj **PII** riski | Hata mesajı serbest metin |
| **COMMENT** | **B** bulk (PO-Q5) | serbest metin **message**, userId | **RESTRICTED / PII** | Serbest kullanıcı metni; bulk yolda, EPIC-G erasure'a tabi |
| **ATTACHMENT** | **B** bulk (PO-Q5) | ad, url/içerik, userId | **RESTRICTED / PII** | Binary/dosya referansı; bulk yolda, EPIC-G erasure'a tabi |
| **TASK LOG / DECINST / CASEINST / BATCH** | B | karar girdi/çıktıları, batch meta | CONFIDENTIAL → içerik **PII** riski | DECINST karar payload'ı PII taşıyabilir |

> **Sonuç:** history sınıflarının **çoğu** en az koşullu-PII taşır; birçoğu operatör kimliği (kişisel veri) içerir. Bu yüzden **projeksiyon store bütünüyle L3 (PII) muamelesi görür** (upward-inheritance, `DATA_GOVERNANCE §1.2 madde 2`).

### 2.2 Wire (NATS mesaj header + gövde)

| Öğe | Kaynak | Sınıf | Gerekçe |
|---|---|---|---|
| Subject `history.<engineId>.<class>.<processInstanceId>` | `07 §1` madde 5 (D-E) | PSEUDONYMOUS | `processInstanceId` gömülü → subject'e PII değeri (businessKey) **gömülmemeli** (DP-2 devralınır) |
| `Nats-Msg-Id = <eventId>:<type>` | `07 §1` madde 5 | INTERNAL | dedup surrogate |
| History event payload (gövde) | `07 §3` (event → ACT_HI) | **RESTRICTED / PII** | §2.1'deki alanları taşır (variable değerleri, operatör kimlikleri, mesajlar) |
| `X-Cadenzaflow-Business-Key` (header, devralınan) | `BpmHeaders.java:13` | CONFIDENTIAL (koşullu PII) | basamak-1 DP-8 masking önerisi history'ye de uygulanır |
| `X-Cadenzaflow-Trace-Id` / `-Idempotency-Key` | `BpmHeaders.java:12,14` | PSEUDONYMOUS | teknik korelasyon (DP-1/DP-7 devralınır) |

### 2.3 Kalıcı depolar (data-at-rest) — **basamak-2'nin yeni yüzeyi**

| Depo | İçerik | Sınıf | Retention | Not |
|---|---|---|---|---|
| **Projeksiyon Postgres (query-store)** | denormalize history (tüm §2.1 alanları) | **RESTRICTED / PII (L3)** | **UZUN — birincil governance odağı** (DP-9/DP-10) | **YENİ.** KVKK retention/silme SQL'le (D-B); at-rest AES-256; erişim kontrolü |
| Kompakt outbox (engine DB, audit-kritik) | ≤1 satır referans + audit-kritik event | **RESTRICTED / PII** | **Kısa** — relay PubAck'te siler (DP-12) | tx-içi dayanıklı ara-kayıt |
| History JetStream stream (`history.>`) | history mesajları (payload + header) | **RESTRICTED / PII** | Stream retention (phase3'te netleşir) | transit + at-rest until consume |
| History DLQ stream (`dlq.history.>`) | DLQ byte-ayna kopya + header | **RESTRICTED / PII** | limits-based, **default 14 gün** (basamak-1) | **en uzun tel-maruziyeti**; ayrı-stream [CQ-6] (DP-13) |
| `ACT_HI_*` (dual-run boyunca, cutover öncesi) | orijinal history | **RESTRICTED / PII** | motor history cleanup politikası | cutover sonrası o sınıf için yazım DURUR |

### 2.4 Sorgu-API & telemetri

| Öğe | Sınıf | Kural |
|---|---|---|
| Sorgu-API yanıtları (FR-C1) | **RESTRICTED / PII** | erişim kontrolü + **PII maskeleme** (DP-15); role-based |
| Sorgu-API erişim log'u | INTERNAL | **PII değeri loglanmaz** (DP-1 devralınır); audit-trail için erişim kaydı (kim sorguladı) tutulabilir |
| Reconciliation raporu (FR-D1) | INTERNAL | yalnız **sayaç/id**; **PII değeri sızdırmaz** (DP-14) |
| Metrik tag `class`/`subject` | PUBLIC | düşük-kardinalite; businessKey/processInstanceId tag'e **gömülmez** (DP-2 devralınır) |
| Projeksiyon gecikmesi / DLQ sayaçları | INTERNAL | süre/sayı; PII yok |

---

## 3. Veri akış sınırları (trust boundary)

```
[Engine node + DB]                          [Ayrı Postgres projeksiyon]        [Sorgu-API tüketicisi]
  ACT_HI event (PII)                              PII AT REST (uzun)                  PII (maskeli)
     │                                                  ▲
     ├── audit-kritik → tx-içi kompakt outbox ──(relay/PubAck-delete, at-least-once)──┐
     │                                                                                │
     └── bulk → post-commit publish (at-most-once) ──────────────────────────────────┤
                              │                                                       ▼
                    [history.<engine>.<class>.<instanceId>]  ──(instance-partition, merge-upsert)──> Postgres
                              │
                              └── delivery bütçesi biterse ──> [dlq.history.>, 14g byte-ayna]
```

- **Kritik yeni geçiş:** history verisi artık **kalıcı, ayrı bir store'a** (Postgres projeksiyon) yazılır → PII **uzun süre at-rest** durur. Basamak-1'de payload geçiciydi (ack'te silinirdi); basamak-2'de **saklamak amacın kendisidir** (sorgulanabilir history). Bu, retention/erasure politikasını (DP-9/DP-10) **zorunlu** kılar.
- **Operatör kimlikleri güven sınırında:** OP_LOG/TASKINST/IDENTITYLINK operatör kimlikleri projeksiyon store'a ve sorgu-API'sine ulaşır → çalışan kişisel verisi işlenir (KVKK/GDPR data-controller yükümlülüğü, `DATA_GOVERNANCE §4.2`).
- **En uzun tel-maruziyeti:** `dlq.history.>` (14g byte-ayna), basamak-1 ile aynı.

---

## 4. Veri koruma gereksinimleri (SRS'e bağlı)

**Basamak-1 devralınan (aynen geçerli, history akışına genişler):**

| ID | Gereksinim (özet) | Bağlı NFR |
|---|---|---|
| **DP-1** | Payload/business-key/operatör-kimliği **değerleri** log/metrik-tag'e yazılmaz | NFR-S1 |
| **DP-2** | Metrik tag'leri yalnız düşük-kardinalite PII-içermeyen alanlar; PII subject'e gömülmez | NFR-S1 |
| **DP-3** | DLQ erişim kontrolü + retention; PII işleyen kiracı retention kısaltır/erişimi kısıtlar | NFR-S4 |
| **DP-4** | NATS transport TLS + NKey/JWT production'da zorunlu | NFR-S7 |
| **DP-5** | Motor-dışı tüketici güven sınırı dokümante | NFR-S4 |
| **DP-6** | Meta header'lar (Dlq-Reason vb.) PII sızdırmaz | NFR-S1 |
| **DP-7** | Idempotency/Correlation business-key'den türerse PSEUDONYMOUS (hash/opak) | NFR-S1 |
| **DP-8** | Business-Key masking **önerilir** (normatif değil, kiracı kararı) | NFR-S1 |

**Basamak-2 yeni (DP-9…DP-15):**

| ID | Gereksinim | Bağlı NFR |
|---|---|---|
| **DP-9** | **Projeksiyon Postgres bir L3 (PII) store'dur:** at-rest AES-256, erişim kontrolü (role-based), yapılandırılmış retention. `ACT_HI` upward-inheritance ile tüm projeksiyon L3. | NFR-S2 |
| **DP-10** | **KVKK/GDPR silme-hakkı** projeksiyon store üstünde SQL ile uygulanabilir olmalı (D-B). **PO-Q2 katmanlı politika (ÇÖZÜLDÜ, §6):** bulk PII alanları (VARINST/DETAIL değerleri, TASKINST name/description) **erasure pipeline'ına tabi** (FR-G2/US-G2); audit-kritik sınıflar (OP_LOG userId, INCIDENT) **yasal-saklama istisnası** + opt-in **pseudonymization** (FR-G3/US-G3). | NFR-S3 |
| **DP-11** | **Operatör/kullanıcı kimlikleri** (OP_LOG userId, TASKINST assignee/owner, PROCINST startUserId, IDENTITYLINK userId, COMMENT/ATTACHMENT userId) kişisel veri olarak **RESTRICTED/PII** sınıflanır; sorgu-API'de maskelenir; erişim role-based. | NFR-S3/S6 |
| **DP-12** | **Kompakt outbox** (engine DB, audit-kritik) history payload'ı geçici taşır → kaynak event ile aynı sınıf; relay silmesiyle **kısa maruziyet**; outbox tablosuna erişim kontrolü. | NFR-S5 |
| **DP-13** | **History stream + `dlq.history.>` at-rest PII** taşır: TLS + subject-level erişim kontrolü + retention; ayrı-stream [CQ-6]; DLQ byte-ayna PII'yi 14g tutar → PII işleyen kiracı history-DLQ retention'ı kısaltır. | NFR-S4 |
| **DP-14** | **Reconciliation raporları** PII değeri içermez (yalnız sayaç/id/hash); fark örnekleri gerekiyorsa maskeli. | NFR-S6 |
| **DP-15** | **Sorgu-API** yanıtları role-based erişim + PII maskeleme; sorgu-API log'una PII değeri yazılmaz; erişim-audit (kim-neyi-sorguladı) tutulabilir. | NFR-S6 |
| **DP-16** | **Pseudonymization kasası** (kimlik↔takma-ad haritası, FR-G3) **re-identification anahtarıdır** → **L4-bitişik** (`DATA_GOVERNANCE §1.1` "encryption keys" seviyesi): ayrı depo, en-az-yetki erişim + audit, projeksiyon store'dan izole. **Silme = harita kaydını silmek** → takma-ad tersinmez (denetim yapısı korunur). | NFR-S8 |

---

## 5. Saklama & yaşam döngüsü

| Veri | Yaşam döngüsü | PII maruziyeti |
|---|---|---|
| Kompakt outbox (audit-kritik) | relay PubAck'te silinir | **Çok kısa** |
| History stream mesajı | consume/retention (phase3) | Kısa-orta |
| **Projeksiyon Postgres** | **sınıf-bazlı retention (PO-Q7 kararı):** bulk **default 90g**, audit-kritik **yasal-saklama** (kiracı override) — US-G1; erasure pipeline (DP-10, US-G2) | **UZUN — birincil odak** |
| History DLQ mesajı | limits-based, default 14g | Uzun (byte-ayna) |
| `ACT_HI_*` (dual-run) | motor cleanup; cutover sonrası o sınıf yazılmaz | Orta |

**Governance kararı (PO-Q7 ONAYLANDI 2026-07-17):** projeksiyon retention **sınıf-bazlı** ve otomatik enforcement'lı (US-G1/FR-G1) — **bulk sınıflar default 90 gün** (kiracı override, operasyonel gereksinime göre uzatılabilir), **audit-kritik sınıflar yasal-saklama süresi** (denetim yükümlülüğü, `DATA_GOVERNANCE §3.1` audit-logs 7y taban; kiracı override). Her retention silmesi audit-log'lanır (`DATA_GOVERNANCE §3.3`).

---

## 6. KVKK silme-hakkı ↔ denetim-izi saklama gerilimi — KARAR (PO-Q2 ONAYLANDI 2026-07-17)

> **Bu, basamak-2'nin en kritik veri-koruma kararıdır.** İki yükümlülük **çelişir**:
> - **KVKK/GDPR silme-hakkı** (`DATA_GOVERNANCE §4.1` Right to Erasure): kişisel veri, meşru talep üzerine silinmeli.
> - **Denetim-izi saklama** (`DATA_GOVERNANCE §3.1` Audit Logs: "immutable, no deletion, 7 yıl"): OP_LOG (kim-ne-yaptı), INCIDENT — audit-kritik ve **kişisel veri (operatör userId) içerir**.

**Gerilim:** Bir operatörün kişisel verisi (userId) hem OP_LOG denetim izinde tutulmalı (uyum/adli), hem silme-hakkı talebine tabi. `ACT_HI`'de bu tutarsızlık örtük; **projeksiyon store'a taşıyınca açık ve SQL-uygulanabilir** hale gelir → politika **şimdi** netleştirildi.

**KARAR — KATMANLI politika (Levent, 2026-07-17):**
1. **Yasal-saklama istisnası (audit-kritik):** audit-kritik sınıflar (OP_LOG, INCIDENT, EXT_TASK_LOG) `DATA_GOVERNANCE §4.1`/KVKK "kanunen saklama" gerekçesiyle **erasure'dan muaf**; retention denetim süresiyle sınırlı (US-G1). ⚠️ **Hukuki dayanak DPO doğrulamasına işaretli kalır** (bu SRS/veri-koruma teslimatı mekanizmayı verir; hukuki dayanağın kesin gerekçelendirmesi kiracı DPO'sunun onayıdır).
2. **Bulk PII erasure pipeline:** bulk sınıflardaki PII (VARINST/DETAIL değerleri, TASKINST name/description, serbest metinler) projeksiyon-DB üstünde **erasure/anonimleştirme pipeline'ına tabi** (US-G2/FR-G2; DP-10).
3. **Audit-kritik pseudonymization seçeneği:** audit izinin **yapısını** koruyup PII alanını (userId) tersinmez takma-ada çevirme — **kimlik↔takma-ad haritası ayrı bir kasada** (DP-16); **silme = harita kaydını silmek** → takma-ad tersinmez olur (re-identification imkânsız, denetim tutarlılığı korunur). Opt-in; kiracı seçer (US-G3/FR-G3).
4. **Kiracı kararı:** nihai retention/erasure duruşu kiracının data-controller yükümlülüğüdür; bu repo **mekanizmayı** sağlar (US-G1/G2/G3), **politikayı** kiracı `TENANT_PII_CHECKLIST_TEMPLATE.md` (bu dizin, §8) ile verir.

**Not:** bu karar kilitli D-B kararını (KVKK retention SQL'le) **uygular**, değiştirmez; teslimat = **EPIC-G**.

---

## 7. PII alan kapsama kontrol listesi (tamlık)

Aşağıdaki tüm PII/koşullu-PII taşıyıcı history öğeleri sınıflandırıldı ve bir koruma gereksinimine bağlandı:

- [x] variable payload (VARINST/DETAIL değerleri + byte-array) → DP-9, DP-10, DP-13
- [x] businessKey (PROCINST) → DP-8 (devralınan), DP-9
- [x] operatör/kullanıcı kimlikleri (OP_LOG/TASKINST/PROCINST/IDENTITYLINK/COMMENT/ATTACHMENT) → DP-11; §6 katmanlı politika (yasal-saklama / erasure / pseudonymization)
- [x] pseudonymization kasası (kimlik↔takma-ad haritası, re-identification anahtarı) → DP-16 (L4-bitişik)
- [x] incident/errorMessage/errorDetails (INCIDENT/EXT_TASK_LOG/JOB_LOG serbest metin) → DP-9, DP-6 (meta sızdırma)
- [x] serbest metin (TASKINST name/description, COMMENT message) → DP-10, DP-15
- [x] projeksiyon Postgres (at-rest, uzun) → DP-9, DP-10
- [x] kompakt outbox (at-rest, kısa) → DP-12
- [x] history stream + `dlq.history.>` (at-rest/transit) → DP-13
- [x] sorgu-API yanıtları/log → DP-15
- [x] reconciliation raporları → DP-14
- [x] subject/header (processInstanceId, businessKey) → DP-1, DP-2, DP-7 (devralınan)

**Alan-düzeyi kapsam:** payload alan-düzeyi (field-level) PII haritası **kiracı-tanımlıdır** (basamak-1 ile aynı ilke) → repo düzeyinde çıkarılamaz. Kiracı `TENANT_PII_CHECKLIST` (§8 genişletmesi) ile doldurur.

---

## 8. TENANT_PII_CHECKLIST — basamak-2 genişletmesi

> **PO-Q7 kararı (2026-07-17): ayrı dosya olarak materyalize edildi** → `docs/sentinel/step2/phase1/TENANT_PII_CHECKLIST_TEMPLATE.md` (basamak-1 `../../phase1/TENANT_PII_CHECKLIST_TEMPLATE.md` **korunur**, bu dosya history-sınıf katmanını + sınıf-seçim rehberini (PO-Q5) + retention/erasure/pseudonymization karar sütunlarını ekler). Kanonik doldurulacak şablon o dosyadır; aşağıdaki tablo **özet referanstır**.

**Eklenen bölüm (özet) — History event-sınıfı × PII envanteri (kiracı doldurur):**

| ACT_HI sınıfı | Bu kiracıda taşıdığı PII alanları | Sınıf | Tutarlılık (A/B) | Projeksiyon retention | Erasure politikası (erase / anonymize / yasal-saklama) | Sorumlu |
|---|---|---|---|---|---|---|
| VARINST | _(örn. `customer.msisdn`)_ | RESTRICTED/PII | B | _(örn. 90g)_ | _(anonymize)_ | _(kiracı)_ |
| DETAIL | _(...)_ | | B | | | |
| PROCINST | _(businessKey içeriği)_ | CONFIDENTIAL | B | | | |
| TASKINST | _(assignee/owner)_ | RESTRICTED/PII | B | | | |
| OP_LOG | _(operatör userId)_ | RESTRICTED/PII | A | _(örn. 7y)_ | _(yasal-saklama)_ | _(DPO)_ |
| INCIDENT | _(mesaj serbest metin)_ | RESTRICTED/PII | A | | | |
| EXT_TASK_LOG | _(errorDetails)_ | RESTRICTED/PII | A | | | |
| IDENTITYLINK | _(userId)_ | RESTRICTED/PII | _(PO-Q5)_ | | | |
| _(diğer)_ | | | | | | |

**Kapanış koşulu (basamak-2):** history akışına giren her ACT_HI sınıfı sınıflandırıldı; her PII alanı bir **retention + erasure kararına** bağlandı; audit-kritik sınıflar için silme ↔ denetim gerilimi (PO-Q2) bir politikaya çözüldü; §4 (basamak-1 DPO onayı) history katmanını da kapsar.

---

## 9. Phase3'e taşınan doğrulamalar
- Cockpit history UI'ının `ACT_HI` bağımlılık yüzeyi (hangi görünüm hangi sınıfa bağlı — cutover körleşme haritası) — `07 §7` (D-C öncesi).
- Projeksiyon store retention/erasure/pseudonymization'ın SQL-uygulanabilirliği + audit-kritik yasal-saklama istisnasının teknik gerçekleştirmesi (PO-Q2 katmanlı politikası §6'da SABİTLENDİ → phase3/phase4 tasarım/LLD).
- History stream retention/erişim (subject-level authz) tasarımı — NFR-S7 detayı.
- Byte-array HISTORY payload'ının (`DbHistoryEventHandler.java:97-105`) projeksiyona nasıl taşınacağı (inline vs referans — basamak-3 externalization ile kesişim, kapsam dışı ama not).

---

## 10. PO Karar Kaydı (Q→A, 2026-07-17)

Veri-korumayı etkileyen PO-QUESTIONS cevaplandı (tam liste `USER_STORIES.md §4`):

| # | Soru | Verilen karar (2026-07-17) | Bu belgeye etki |
|---|---|---|---|
| **PO-Q2** | KVKK silme-hakkı ↔ denetim-izi gerilimi | **KATMANLI politika:** (1) audit-kritik yasal-saklama istisnası (hukuki dayanak DPO doğrulamasına işaretli), (2) bulk PII erasure pipeline, (3) audit-kritik pseudonymization (ayrı kasa, silme=harita kaydı) | §6 KARAR; DP-10 netleşti; **DP-16** eklendi |
| **PO-Q5** | Audit-kritik sınıf listesi kesinleştirme | **{OP_LOG, INCIDENT, EXT_TASK_LOG} + konfig**; IDENTITYLINK/COMMENT/ATTACHMENT **bulk yolda** | §2.1 tutarlılık sütunu güncellendi; §8 sınıf-seçim rehberi |
| **PO-Q7** | Projeksiyon retention default + erasure teslimatı + TENANT template | **Üçü de basamak-2:** bulk 90g / audit-kritik yasal-saklama (kiracı override); erasure pipeline kod teslimatı; TENANT template **ayrı dosya** | §5 KARAR; §8 ayrı dosya; DP-9/DP-10; EPIC-G |
