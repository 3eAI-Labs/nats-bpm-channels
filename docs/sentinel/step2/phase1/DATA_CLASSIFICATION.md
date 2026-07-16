# DATA CLASSIFICATION — PII & Veri Hassasiyeti Eşlemesi
## Basamak-2: History Offload (ACT_HI → NATS → async query-store)

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Sentinel fazı:** Phase 1 — Product Owner (basamak-2)
**Yerleşim:** `docs/sentinel/step2/phase1/` (PO-Q1)
**Tarih:** 2026-07-16
**Durum:** TASLAK — PO-QUESTIONS onayı bekliyor (özellikle **PO-Q2** KVKK silme-hakkı ↔ denetim-izi gerilimi)
**İlgili teslimatlar:** `USER_STORIES.md`, `SRS.md`, `GUIDELINES_MANIFEST.yaml`, basamak-1 `../../phase1/DATA_CLASSIFICATION.md` (DP-1…8 devri), `../../phase1/TENANT_PII_CHECKLIST_TEMPLATE.md`

> **Basamak-2, veri-koruma açısından basamağın en ağırıdır: history verisi PII yüzeyinin TA KENDİSİDİR.** Basamak-1'de tel üzerinde geçen payload'lar geçici (WorkQueue ack'te silinir, DLQ 14g) idi; basamak-2 bu veriyi **kalıcı, sorgulanabilir bir projeksiyon store'una** yazar → **uzun retention + rastgele-erişim + operatör-kimlikleri** yeni bir governance yüzeyi açar. Bu belge history akışında akan/saklanan/loglanan tüm veri öğelerini sınıflandırır. Motor/SPI iddiaları `07 §3`'te **DOĞRULANMIŞ** `file:line` referanslıdır. Basamak-1 **DP-1…DP-8 aynen devralınır**; basamak-2 **DP-9…DP-15** ekler.

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

> Kaynak: `07 §3` ACT_HI tablo haritası (16+ entity → tablo). Değişken byte'ları ayrıca `ByteArrayEntity(..., ResourceTypes.HISTORY)` (`DbHistoryEventHandler.java:97-105`). Sınıflar **audit-kritik (A)** / **bulk (B)** olarak D-A default'una göre işaretli (nihai liste = **PO-Q5**).

| Event sınıfı (ACT_HI_) | Tutarlılık (D-A) | Taşıdığı hassas alanlar | Sınıf | Not |
|---|---|---|---|---|
| **VARINST** (VariableInstance) | **B** bulk | variable adı + **değeri** (kiracı-tanımlı), byte-array payload | **RESTRICTED / PII** | Hacmin büyük kısmı; keyfi iş verisi. at-most-once (kayıp reconciliation'da görünür) |
| **DETAIL** (VariableUpdate/FormProperty) | **B** bulk | değişken güncelleme **değerleri**, byte-array | **RESTRICTED / PII** | En büyük hacim; ilk cutover adayı (hacim-öncelikli) |
| **ACTINST** (ActivityInstance) | **B** bulk | activityId/type, taskAssignee (user-task'ta) | INTERNAL → assignee varsa **PII** | activity meta INTERNAL; assignee PII |
| **PROCINST** (ProcessInstance) | **B** bulk | **businessKey**, **startUserId**, superProcessInstanceId, deleteReason | CONFIDENTIAL (businessKey) / **PII** (startUserId) | businessKey telco'da MSISDN olabilir; startUserId operatör kimliği |
| **TASKINST** (TaskInstance) | **B** bulk | **assignee**, **owner**, name, description, deleteReason | **RESTRICTED / PII** | assignee/owner operatör kimliği; name/description serbest metin (PII riski) |
| **IDENTITYLINK** | (PO-Q5) | **userId** / groupId, type (assignee/candidate/owner) | **RESTRICTED / PII** | Kimlik-atama; **audit-kritik'e alınmalı mı? PO-Q5** |
| **OP_LOG** (UserOperationLog) | **A** audit-kritik | **userId** (operatör), operationType, entityType, property, orgValue, newValue | **RESTRICTED / PII** | **Denetim izinin kendisi**: kim-ne-yaptı. KVKK silme ↔ denetim gerilimi merkezi (PO-Q2) |
| **INCIDENT** | **A** audit-kritik | incidentMessage, configuration, activityId | **RESTRICTED / PII** (mesaj serbest metin) | Audit-kritik; mesaj PII sızdırabilir |
| **EXT_TASK_LOG** | **A** audit-kritik | workerId, **errorMessage**, errorDetails (byte) | **RESTRICTED / PII** (errorDetails payload/stack) | Audit-kritik; errorDetails orijinal payload/stack taşıyabilir |
| **JOB_LOG** | **B** bulk | jobExceptionMessage, config | INTERNAL → mesaj **PII** riski | Hata mesajı serbest metin |
| **COMMENT** | (PO-Q5) | serbest metin **message**, userId | **RESTRICTED / PII** | Serbest kullanıcı metni |
| **ATTACHMENT** | (PO-Q5) | ad, url/içerik, userId | **RESTRICTED / PII** | Binary/dosya referansı |
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
| **DP-10** | **KVKK/GDPR silme-hakkı** projeksiyon store üstünde SQL ile uygulanabilir olmalı (D-B). **PII-taşıyan bulk alanlar** (VARINST/DETAIL değerleri, TASKINST name/description) erasure/anonimleştirmeye tabi. **Audit-kritik sınıflar** (OP_LOG userId, INCIDENT) için silme ↔ denetim gerilimi = **PO-Q2** (bkz. §6). | NFR-S3 |
| **DP-11** | **Operatör/kullanıcı kimlikleri** (OP_LOG userId, TASKINST assignee/owner, PROCINST startUserId, IDENTITYLINK userId, COMMENT/ATTACHMENT userId) kişisel veri olarak **RESTRICTED/PII** sınıflanır; sorgu-API'de maskelenir; erişim role-based. | NFR-S3/S6 |
| **DP-12** | **Kompakt outbox** (engine DB, audit-kritik) history payload'ı geçici taşır → kaynak event ile aynı sınıf; relay silmesiyle **kısa maruziyet**; outbox tablosuna erişim kontrolü. | NFR-S5 |
| **DP-13** | **History stream + `dlq.history.>` at-rest PII** taşır: TLS + subject-level erişim kontrolü + retention; ayrı-stream [CQ-6]; DLQ byte-ayna PII'yi 14g tutar → PII işleyen kiracı history-DLQ retention'ı kısaltır. | NFR-S4 |
| **DP-14** | **Reconciliation raporları** PII değeri içermez (yalnız sayaç/id/hash); fark örnekleri gerekiyorsa maskeli. | NFR-S6 |
| **DP-15** | **Sorgu-API** yanıtları role-based erişim + PII maskeleme; sorgu-API log'una PII değeri yazılmaz; erişim-audit (kim-neyi-sorguladı) tutulabilir. | NFR-S6 |

---

## 5. Saklama & yaşam döngüsü

| Veri | Yaşam döngüsü | PII maruziyeti |
|---|---|---|
| Kompakt outbox (audit-kritik) | relay PubAck'te silinir | **Çok kısa** |
| History stream mesajı | consume/retention (phase3) | Kısa-orta |
| **Projeksiyon Postgres** | **kiracı-konfigürable retention** (bulk kısa, audit-kritik uzun — PO-Q7); erasure pipeline (DP-10) | **UZUN — birincil odak** |
| History DLQ mesajı | limits-based, default 14g | Uzun (byte-ayna) |
| `ACT_HI_*` (dual-run) | motor cleanup; cutover sonrası o sınıf yazılmaz | Orta |

**Governance kararı (öneri, PO-Q7):** projeksiyon retention **sınıf-bazlı konfigürable** — bulk sınıflar operasyonel gereksinime göre (örn. 90g–2y), audit-kritik sınıflar denetim yükümlülüğüne göre (örn. 7y, `DATA_GOVERNANCE §3.1` audit-logs) tutulur. `DATA_GOVERNANCE §3.1` retention tablosu taban alınır. Kesin default'lar Levent onayına.

---

## 6. KVKK silme-hakkı ↔ denetim-izi saklama gerilimi (PO-Q2 — merkezi karar)

> **Bu, basamak-2'nin en kritik veri-koruma kararıdır.** İki yükümlülük **çelişir**:
> - **KVKK/GDPR silme-hakkı** (`DATA_GOVERNANCE §4.1` Right to Erasure): kişisel veri, meşru talep üzerine silinmeli.
> - **Denetim-izi saklama** (`DATA_GOVERNANCE §3.1` Audit Logs: "immutable, no deletion, 7 yıl"): OP_LOG (kim-ne-yaptı), INCIDENT — audit-kritik ve **kişisel veri (operatör userId) içerir**.

**Gerilim:** Bir operatörün kişisel verisi (userId) hem OP_LOG denetim izinde tutulmalı (uyum/adli), hem silme-hakkı talebine tabi. `ACT_HI`'de bu tutarsızlık örtük; **projeksiyon store'a taşıyınca açık ve SQL-uygulanabilir** hale gelir → politikayı **şimdi** netleştirmek gerekir.

**Öneri (Levent onayına — PO-Q2):**
1. **Yasal-yükümlülük istisnası:** audit-kritik sınıflar (OP_LOG, INCIDENT, EXT_TASK_LOG) `DATA_GOVERNANCE §4.1`/KVKK m.7 "kanunen saklama" gerekçesiyle **erasure'dan muaf** tutulur; retention denetim süresiyle sınırlıdır (örn. 7y sonra hard-delete).
2. **Bulk PII erasure:** bulk sınıflardaki PII (VARINST/DETAIL değerleri, TASKINST name/description, serbest metinler) erasure/anonimleştirme pipeline'ına **tabi** (DP-10).
3. **Pseudonymization seçeneği:** audit izinin **yapısını** koruyup PII alanını (userId) tersinmez token'a çevirme → hem denetim (kim-tutarlılığı) hem minimizasyon. Kiracı-konfigürable.
4. **Kiracı kararı:** nihai retention/erasure duruşu kiracının data-controller yükümlülüğüdür; bu repo **mekanizmayı** sağlar (SQL retention + erasure/pseudonymization pipeline), **politikayı** kiracı `TENANT_PII_CHECKLIST` (genişletilmiş, §8) ile verir.

**Not:** bu öneri kilitli D-B kararını (KVKK retention SQL'le) **uygular**, değiştirmez.

---

## 7. PII alan kapsama kontrol listesi (tamlık)

Aşağıdaki tüm PII/koşullu-PII taşıyıcı history öğeleri sınıflandırıldı ve bir koruma gereksinimine bağlandı:

- [x] variable payload (VARINST/DETAIL değerleri + byte-array) → DP-9, DP-10, DP-13
- [x] businessKey (PROCINST) → DP-8 (devralınan), DP-9
- [x] operatör/kullanıcı kimlikleri (OP_LOG/TASKINST/PROCINST/IDENTITYLINK/COMMENT/ATTACHMENT) → DP-11, PO-Q2
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

> Basamak-1 `../../phase1/TENANT_PII_CHECKLIST_TEMPLATE.md` **korunur**; basamak-2 aşağıdaki **history-sınıf katmanını** ekler. (Materyalizasyon: ayrı `step2/phase1/TENANT_PII_CHECKLIST_TEMPLATE.md` dosyası mı, yoksa basamak-1'i in-place genişletme mi — **PO-Q7**.)

**Eklenen bölüm — History event-sınıfı × PII envanteri (kiracı doldurur):**

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
- Projeksiyon store retention/erasure'ın SQL-uygulanabilirliği + audit-kritik yasal-saklama istisnasının teknik gerçekleştirmesi (PO-Q2 kararı sabitlenince).
- History stream retention/erişim (subject-level authz) tasarımı — NFR-S7 detayı.
- Byte-array HISTORY payload'ının (`DbHistoryEventHandler.java:97-105`) projeksiyona nasıl taşınacağı (inline vs referans — basamak-3 externalization ile kesişim, kapsam dışı ama not).

---

## 10. PO Karar Kaydı (bekliyor)

Veri-korumayı etkileyen PO-QUESTIONS (tam liste `USER_STORIES.md §4`):

| # | Soru | Bu belgeye etki |
|---|---|---|
| **PO-Q2** | KVKK silme-hakkı ↔ denetim-izi gerilimi | §6 (merkezi karar); DP-10 |
| **PO-Q5** | Audit-kritik sınıf listesi kesinleştirme (IDENTITYLINK/COMMENT/ATTACHMENT?) | §2.1 tutarlılık sütunu; §8 |
| **PO-Q7** | Projeksiyon retention default + erasure teslimatı + TENANT template materyalizasyonu | §5, §8; DP-9/DP-10 |
