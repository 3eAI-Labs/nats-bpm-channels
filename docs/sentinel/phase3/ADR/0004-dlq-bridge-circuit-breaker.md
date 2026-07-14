# ADR-0004 — DLQ-bridge circuit-breaker eşikleri ve yerleşimi

- **Durum:** Kabul edildi (2026-07-14, Phase 3)
- **İzlenebilirlik:** US-A6/US-B3 → FR-A10/FR-B3 → BR-SUB-008 → `SYS_DLQ_BRIDGE_PROCESSING_FAILED` (EXCEPTION_CODES §3) → BAQ-6 / DECISION_MATRIX 1.C satır 3

## Bağlam

DLQ-bridge (Camunda/CadenzaFlow incident-bridge; Flowable failure-event bridge) DLQ mesajını işlerken **engine'e/Event Registry'ye** çağrı yapar (incident oluştur / failure-event correlate). Bu downstream (Cockpit DB / Event Registry) düşerse, bridge her redelivery'de tekrar dener → **hot-loop** riski (DLQ-fırtınası). BAQ-6 dayanıklılığı iki katman olarak sabitledi ama kütüphane/yerleşim phase3'e kaldı.

`dlq-of-dlq YOK` ilkesi (docs/06 §7): bridge işleyemezse **asla ack-drop** — mesaj kalıcı `DLQ` stream'inde (limits-based, 14g) bekler.

## Karar

**Eşikler (BAQ-6, kilitli — ERROR_HANDLING_GUIDELINE §4.2):**
| Durum | Eşik | Geçiş |
|---|---|---|
| Ardışık başarısızlık | **5** | CLOSED → **OPEN** |
| OPEN bekleme | **30s** | OPEN → HALF_OPEN |
| HALF_OPEN deneme penceresi | **3 çağrı** (Resilience4j `permittedNumberOfCallsInHalfOpenState`) | pencere sonunda hepsi fail → OPEN; aksi hâlde CLOSED |

> **ERRATA (2026-07-15, phase4-review MAJOR-1b — Levent kararı: kütüphaneye uyarla):** İlk yazım "3 **ardışık** başarı → CLOSED; HALF_OPEN'da herhangi fail → OPEN" idi — Resilience4j'nin doğal semantiği bu değildir ve custom transition kodu yazmak reddedildi (bakım + upgrade kırılganlığı). Kabul edilen davranış: HALF_OPEN penceresinde erken-CLOSED mümkündür; DLQ yolu düşük-riskli olduğundan (SLA yok, mesaj kaybı yok) bedel kabul edilir.
>
> **ERRATA-2 (2026-07-15, phase4-review MAJOR-1a):** CB **yalnız downstream-sağlık sinyallerini** sayar — iyi-huylu iş exception'ları (`NotFoundException` = task zaten çözülmüş [`HandleExternalTaskCmd:49-50`]; Flowable no-match) `ignoreExceptions(...)` ile CB-hatası sayılmaz; aksi hâlde redelivery fırtınası sağlıklı downstream'de sahte CB-OPEN üretir. Somut config LLD `1_nats_core_common.md` §4'te.

**İki katman:**
1. `nakWithDelay` üstel backoff (`2^(n-1)`s, cap 30s) — mevcut adapter deseni; her tekil hata.
2. Ardışık 5 hatada **circuit-breaker OPEN** → downstream'e yeni istek gitmez (fail-fast), mesajlar nak'lı stream'de bekler.

**Kütüphane:** **Resilience4j** (Apache 2.0 — NFR-L1 uyumlu, Spring Boot 3.3 entegrasyonu mevcut). Custom CB **reddedildi** (yeniden-icat; olgun kütüphane var).

**Yerleşim:** CB, DLQ-bridge bileşeninin **downstream çağrı sınırına** (incident/failure-event çağrısı) uygulanır — inbound consumer'ın genel işleme yoluna DEĞİL (worker/reply/event consumer'ları normal `nakWithDelay` deseninde kalır; CB yalnız DLQ-bridge→engine hattına özgüdür). CB instance'ı **downstream başına** (Cockpit DB / Event Registry) izole edilir ki bir idiomun downstream kesintisi diğerini fail-fast'lemesin.

**Kayıp garantisi:** CB OPEN iken mesaj KAYBOLMAZ — `DLQ` stream kalıcı; CB CLOSED'a dönünce bekleyen mesajlar redeliver edilip işlenir. `setRetriesAndManageIncidents` doğal idempotency'si (`ExternalTaskEntity.java:443-448`) sayesinde tekrar-işleme duplicate incident üretmez (`BUS_INCIDENT_ALREADY_CREATED`).

## Sonuçlar

**Olumlu:** Bozuk downstream'e karşı hot-loop önlenir; mesaj kaybı yok (stream kalıcılığı); CB geçişinde ALERT (ops sinyali). Downstream-başına izolasyon → çapraz-idiom etkisi yok.

**Olumsuz / kabul edilen:** CB OPEN penceresinde (≥30s) incident/escalation gecikir — DLQ yolu için zaten "SLA beklenmez" (US-A6/B3). Resilience4j bir runtime bağımlılığı ekler (Apache 2.0, hafif).

## Reddedilenler
- Advisory-tabanlı DLQ tespiti (`MAX_DELIVERIES`): best-effort core-NATS yayını, poison sessizce sıkışır, ayrı consumer gerektirir. **Reddedildi** (D-E; in-band `maxDeliver+1` seçildi). WebSearch teyidi (HLD §11): advisory subject `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<STREAM>.<CONSUMER>` gerçektir ama mesajın kendisi stream'de kalır (seq-lookup gerekir) → in-band tespit daha basit.
- CB yerine sonsuz nak-backoff: hot-loop'u yavaşlatır ama downstream'i fail-fast'lemez → CB tercih edildi.
