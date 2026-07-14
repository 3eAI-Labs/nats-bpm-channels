# DATA CLASSIFICATION — PII & Veri Hassasiyeti Eşlemesi
## Basamak-1: External Task / Event-Driven Work Offload over JetStream

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Sentinel fazı:** Phase 1 — Product Owner
**Tarih:** 2026-07-14
**Durum:** Onaylı (2026-07-14) — PO-QUESTIONS cevaplandı (bkz. §8 PO Karar Kaydı)
**İlgili teslimatlar:** `USER_STORIES.md`, `SRS.md`, `TENANT_PII_CHECKLIST_TEMPLATE.md`

> Bu belge basamak-1 kapsamında **wire üzerinde akan, saklanan ve loglanan tüm veri öğelerini** sınıflandırır ve veri koruma gereksinimlerini bağlar. Kod-kanıtlı öğeler `file:line` referanslıdır. `nats-bpm-channels` bir **altyapı katmanıdır**: işlenen iş verisinin (process değişkenleri) içeriği **kiracı/uygulama tarafından tanımlanır** — bu yüzden bazı öğeler "kiracı-tanımlı, en yüksek risk" olarak sınıflanır ve minimizasyon gereksinimi taşır.

---

## 1. Sınıflandırma şeması

| Sınıf | Tanım | Örnek |
|---|---|---|
| **PUBLIC** | Açığa çıkması zarar üretmez | Subject adı, channel adı, metrik adı |
| **INTERNAL** | Operasyonel; iş verisi değil, ifşası düşük risk | `externalTaskId`, sentinel workerId, delivery-count |
| **PSEUDONYMOUS** | Teknik korelasyon kimliği; tek başına kişi tanımlamaz ama PII'ye bağlanabilir | `Trace-Id`, `Correlation-Id`, `Idempotency-Key` |
| **CONFIDENTIAL** | İş açısından hassas iş kimliği; bağlama göre PII olabilir | `Business-Key` |
| **RESTRICTED / PII** | Kişisel veri içerebilir; kiracı-tanımlı, en yüksek koruma | Process değişkenleri (job/reply payload), DLQ payload |

**Telco bağlamı uyarısı:** 3eAI Labs telekom senaryolarında (`cn-advanced-ota`, `01 §9`) `Business-Key` ve process değişkenleri MSISDN / IMSI / abone kimliği / IMEI gibi **doğrudan PII/tanımlayıcı** taşıyabilir. Bu yüzden `Business-Key` **CONFIDENTIAL (koşullu PII)** kabul edilir; payload **RESTRICTED/PII** varsayılır (aksi kanıtlanana kadar).

---

## 2. Veri öğeleri envanteri (wire + store + telemetry)

### 2.1 Header'lar (NATS mesaj header'ları)

| Öğe | Kaynak (`file:line`) | Sınıf | Gerekçe |
|---|---|---|---|
| `X-Cadenzaflow-Trace-Id` | `BpmHeaders.java:12` | PSEUDONYMOUS | Dağıtık izleme kimliği; kişi tanımlamaz, PII'ye bağlanabilir |
| `X-Cadenzaflow-Business-Key` | `BpmHeaders.java:13` | **CONFIDENTIAL (koşullu PII)** | İş kimliği; telco'da MSISDN/abone-id olabilir |
| `X-Cadenzaflow-Idempotency-Key` | `BpmHeaders.java:14` | PSEUDONYMOUS | Dedup anahtarı; business-key'den türetilirse dolaylı PII taşır |
| `X-Cadenzaflow-Correlation-Id` | `05 §7`, `06 §7` (async) | PSEUDONYMOUS | Reply eşleştirme; teknik |
| `X-Cadenzaflow-Reply-Subject` | `05 §7`, `06 §7` (async) | INTERNAL | Reply routing hedefi; iş verisi değil |
| `Nats-Msg-Id` | `06 §7` (`externalTaskId`/correlation key) | INTERNAL / PSEUDONYMOUS | Dedup; A2'de surrogate id (INTERNAL), Event Registry'de correlation key olursa PSEUDONYMOUS |
| `X-Cadenzaflow-Dlq-Original-Subject` | `06 §7` (US-C1) | PUBLIC | Routing meta |
| `X-Cadenzaflow-Dlq-Delivery-Count` | `06 §7` (US-C1) | INTERNAL | Ops meta |
| `X-Cadenzaflow-Dlq-Reason` | `06 §7` (US-C1) | INTERNAL | Hata sınıfı; **payload/PII sızdırmamalı** (bkz. §4) |
| `X-Cadenzaflow-Dlq-Timestamp` | `06 §7` (US-C1) | PUBLIC | Zaman damgası |
| `X-Trace-Id` (mevcut MDC okuması) | `JetStreamInboundEventChannelAdapter.java:119` | PSEUDONYMOUS | **Q2 kararı (2026-07-14):** basamak-1'de düzeltilir — okuma fallback kabul eder, yazma yalnız `X-Cadenzaflow-Trace-Id` (US-C6 / FR-C7) |

### 2.2 Mesaj gövdesi (payload)

| Öğe | Kaynak | Sınıf | Gerekçe |
|---|---|---|---|
| Job payload (process değişkenleri) | `06 §5.2` (`jobs.<topic>`) | **RESTRICTED / PII** | Kiracı-tanımlı; keyfi iş verisi, PII içerebilir |
| Reply payload (sonuç değişkenleri) | `06 §5.2` (`*.reply`) | **RESTRICTED / PII** | Worker sonucu; PII içerebilir |
| Event payload (Flowable) | `NatsInboundEventChannelAdapter.java:88` (`eventReceived`) | **RESTRICTED / PII** | Kiracı-tanımlı event verisi |
| DLQ payload (byte-aynen kopya) | `06 §7`; `JetStreamInboundEventChannelAdapter.java:216-218` | **RESTRICTED / PII** | Orijinal payload'ın birebir kopyası → aynı sınıf, **uzun retention** (§5) |
| Sentinel workerId (payload'da audit) | `06 §5.4` | INTERNAL | Küme sabiti; iş verisi değil |
| `externalTaskId` | `06 §5.4` | INTERNAL | Motor surrogate id |

### 2.3 Kalıcı depolar (data-at-rest)

| Depo | İçerik | Sınıf | Retention |
|---|---|---|---|
| `ACT_RU_EXT_TASK` (motor DB, outbox) | task satırı + kilit alanları; değişkenler `ACT_RU_VARIABLE`'da | **RESTRICTED / PII** (değişkenler) | task tamamlanınca satır silinir (`06 §5.4`) |
| JetStream WorkQueue (`jobs.<type>`) | job/reply mesajları | **RESTRICTED / PII** | WorkQueue: ack'te silinir (tek tüketici) |
| JetStream `DLQ` stream (`dlq.>`) | DLQ payload + header'lar | **RESTRICTED / PII** | **limits-based, default 14 gün** (`06 §7`) — **en uzun PII maruziyeti** |
| NATS KV (varsa, idempotency/lock) | dedup/lock anahtarları | PSEUDONYMOUS | Basamak-1'de opsiyonel; kullanılırsa TTL |

> **Not:** Büyük değişkenlerin Object Store/KV'ye externalize edilmesi **basamak-3** kapsamıdır (kapsam dışı) — basamak-1'de değişkenler payload ve `ACT_RU_VARIABLE` içinde kalır.

### 2.4 Telemetri (metrik, log, trace)

| Öğe | Kaynak (`file:line`) | Sınıf | Kural |
|---|---|---|---|
| Metrik tag `subject` | `NatsChannelMetrics.java:16-62` | PUBLIC | Düşük kardinalite; **business-key subject'e gömülmemeli** (kardinalite + PII patlaması) |
| Metrik tag `channel` | `NatsChannelMetrics.java:16-62` | PUBLIC | Statik channel adı |
| Sayaç değerleri (dlq/nak/ack/publish) | `NatsChannelMetrics.java:25-57` | INTERNAL | Toplam sayılar; PII yok |
| `processingTimer` | `NatsChannelMetrics.java:60-63` | INTERNAL | Süre; PII yok |
| MDC `trace_id` | `JetStreamInboundEventChannelAdapter.java:121` | PSEUDONYMOUS | Log korelasyonu; PII değil |
| Log mesajları | adapter'lar (`kv(...)`) | INTERNAL | **payload/business-key değeri loglanmamalı** (bkz. §4 NFR-S1) |

---

## 3. Veri akış sınırları (trust boundary)

```
[Engine node + DB]  --(header+payload)-->  [JetStream stream]  --(push)-->  [Motor-dışı polyglot worker]
   PII in ACT_RU_VARIABLE            PII in transit (TLS gerekli)         PII güven sınırı DIŞINA çıkar
                                             │
                                             └── delivery bütçesi biterse ──> [DLQ stream, 14g retention]
```

- **Kritik geçiş:** `Business-Key` ve process değişkenleri, motor-dışı worker'a (farklı süreç/dil/host) header+payload olarak geçer → **PII güven sınırından çıkar** (SRS NFR-S4). Worker'ın erişim kontrolü ve saklama davranışı basamak-1 kapsamında **dokümante edilmeli**, ama worker implementasyonu bu repo dışıdır (reference SDK'lar `01 §7` gelecek iş).
- **En uzun maruziyet:** DLQ stream (14 gün) — payload byte-aynen kopyalandığından (US-C1) PII orada retention süresince durur.

---

## 4. Veri koruma gereksinimleri (SRS'e bağlı)

| ID | Gereksinim | Bağlı NFR |
|---|---|---|
| **DP-1** | Payload / `Business-Key` / header **değerleri** loglara ve metrik tag'lerine yazılmamalı | NFR-S1 |
| **DP-2** | Metrik tag'leri yalnız düşük-kardinalite PII-içermeyen alanlar (`subject`, `channel`); business-key subject'e gömülmemeli | NFR-S1 |
| **DP-3** | DLQ stream'ine erişim kontrolü + saklama politikası. **Q3 kararı (2026-07-14):** 14g default + **kiracı-bazlı konfig**; **PII işleyen kiracı retention'ı kısaltmalı / erişimi kısıtlamalı** | NFR-S2 |
| **DP-4** | NATS transport güvenliği (TLS + NKey/JWT) production'da zorunlu | NFR-S3 |
| **DP-5** | Motor-dışı worker güven sınırı + erişim kontrolü dokümante edilmeli | NFR-S4 |
| **DP-6** | `X-Cadenzaflow-Dlq-Reason` gibi meta header'lar payload/PII değeri sızdırmamalı (yalnız hata sınıfı/kod) | NFR-S1 |
| **DP-7** | `Idempotency-Key`/`Correlation-Id` business-key'den türetiliyorsa dolaylı PII taşır → PSEUDONYMOUS muamelesi (hash/opak öner) | NFR-S1 |
| **DP-8** | **Q4 kararı (2026-07-14, normatif DEĞİL):** `Business-Key` için hash/mask **önerilir**; karar kiracının, **kod değişikliği yok** — kiracı `TENANT_PII_CHECKLIST_TEMPLATE.md`'de karar verir | NFR-S1 |

---

## 5. Saklama & yaşam döngüsü

| Veri | Yaşam döngüsü | PII maruziyeti |
|---|---|---|
| Job/reply (WorkQueue) | Ack'te silinir | Kısa (işlem süresi) |
| `ACT_RU_EXT_TASK` satırı | Task tamamlanınca silinir | Kısa-orta |
| DLQ mesajı | limits-based, **14 gün** (yapılandırılabilir) | **Uzun** — birincil governance odağı |
| Dedup penceresi (`duplicate_window`) | default 2dk | Çok kısa |
| Metrik/log | Ops retention (bu repo dışı) | PII yok (DP-1 uygulanırsa) |

**Governance kararı (Q3, 2026-07-14 — ONAYLANDI):** DLQ retention **14g default + kiracı-bazlı konfig**; PII işleyen kiracı GDPR/KVKK minimizasyonu için retention'ı **kısaltmalı** ve DLQ erişimini **kısıtlamalı** (DP-3).

---

## 6. PII alan kapsama kontrol listesi (tamlık)

Aşağıdaki tüm PII/koşullu-PII taşıyıcı öğeler sınıflandırıldı ve bir koruma gereksinimine bağlandı:

- [x] `Business-Key` (CONFIDENTIAL/koşullu PII) → DP-1, DP-2
- [x] Job payload / process değişkenleri (RESTRICTED/PII) → DP-3, DP-4, DP-5
- [x] Reply payload (RESTRICTED/PII) → DP-4, DP-5
- [x] Event payload — Flowable (RESTRICTED/PII) → DP-4, DP-5
- [x] DLQ payload (RESTRICTED/PII, 14g) → DP-3, DP-6
- [x] `Idempotency-Key` / `Correlation-Id` (dolaylı PII riski) → DP-7
- [x] `Trace-Id` (PSEUDONYMOUS) → DP-1
- [x] `ACT_RU_EXT_TASK` / `ACT_RU_VARIABLE` (at-rest PII) → DP-3
- [x] JetStream WorkQueue / DLQ stream (at-rest PII) → DP-3, DP-4

**Alan-düzeyi kapsam (Q5, 2026-07-14 — ONAYLANDI):** payload alan-düzeyi (field-level) PII haritası **kiracı-tanımlı** olduğundan bu repo düzeyinde çıkarılamaz; kiracı entegrasyonunda doldurulmak üzere **`TENANT_PII_CHECKLIST_TEMPLATE.md`** (alan → sınıf → masking kararı → retention notu) basamak-1 doküman teslimatı olarak oluşturuldu. Basamak-3 (büyük değişken externalization) PII minimizasyonu için kaldıraç sağlar (kapsam dışı, ileri referans).

---

## 7. Phase3'e taşınan doğrulamalar
- NATS auth/authz mekanizmasının (NKey/JWT + subject-level permission) basamak-1'de nasıl konfigüre edileceği (NFR-S3 detayı).
- DLQ retention'ın kiracı-bazlı override edilebilirliği (stream provisioning tasarımı — Q3 kararı sabit, teknik gerçekleştirme phase3).
- ~~`X-Trace-Id` ↔ `X-Cadenzaflow-Trace-Id` standardizasyonu~~ → **Q2 (2026-07-14): basamak-1'de çözülüyor** (US-C6 / FR-C7); phase3 kapsamından çıktı.

---

## 8. PO Karar Kaydı (Q→A, 2026-07-14)

Veri koruma / sınıflandırmayı etkileyen PO kararları (tam kayıt: `USER_STORIES.md §4`):

| # | Soru | Karar | Bu belgeye etki |
|---|---|---|---|
| **Q2** | Trace header tutarsızlığı | Basamak-1'de düzeltilir (yazma tek ad, okuma fallback) | §2.1 `X-Trace-Id` satırı; §7 |
| **Q3** | DLQ retention (14g) vs PII | 14g default + kiracı-bazlı konfig; PII kiracı kısaltmalı/kısıtlamalı | DP-3, §5 |
| **Q4** | `Business-Key` masking | Normatif olmayan öneri (hash/mask), kod değişikliği yok | DP-8 |
| **Q5** | Field-level PII checklist | Basamak-1 doküman teslimatı (EVET) | `TENANT_PII_CHECKLIST_TEMPLATE.md`; §6 |
