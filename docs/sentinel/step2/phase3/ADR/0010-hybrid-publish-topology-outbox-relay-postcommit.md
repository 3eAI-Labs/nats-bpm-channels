# ADR-0010 — Hibrit yayın topolojisi: kompakt outbox + leader-elected relay (audit-kritik) / post-commit publisher (bulk)

- **Durum:** Kabul edildi (2026-07-17, Phase 3 — basamak-2). Alt-eksen KARARA BAĞLANDI: kompakt outbox payload taşıma = **referans** (ARCH-Q1, 2026-07-18).
- **İzlenebilirlik:** US-A3/US-A4/US-B1 → FR-A4/FR-A5/FR-B1 → BR-HDL-003/004/BR-REL-001 → `SYS_OUTBOX_RELAY_PUBLISH_FAILED`/`SYS_OUTBOX_RELAY_LEADER_LOST`/`SYS_OUTBOX_ROW_STUCK` → D-A / NFR-R1/R2/R3
- **Kesişim:** basamak-1 **ADR-0002** (SweepLeaderLease — KV-lease leader election) relay'de YENİDEN KULLANILIR.

## Bağlam

D-A (kilitli) hibrit, event-sınıfı-bazlı tutarlılık getirir: audit-kritik sınıflar (OP_LOG/INCIDENT/EXT_TASK_LOG, düşük hacim) **at-least-once**; bulk sınıflar (DETAIL/VARINST/ACTINST, ~%90 hacim) **at-most-once**. Handler-içi senkron NATS publish **YASAK** — çağrı tx-içi/senkrondur (`HistoryEventProcessor.java:73-85` → `CommandContext.java:186-197` flushSessions→commit) [07§3]: (a) NATS latency engine komutunu bloklar, (b) publish exception `CommandContext.close`'da commit'i atlatıp runtime tx'i rollback eder.

**Basamak-1'den kritik fark — outbox yok olma problemi** [07§4]: basamak-1'de `ACT_RU_EXT_TASK` transactional outbox'tı, kaçan publish sweep'le telafi edilirdi. Basamak-2'nin hedefi ACT_HI yazımını KALDIRMAK — DB handler kapatılınca kaçan publish'in telafi kaynağı da yok olur. Bu yüzden audit-kritik yol **kendi dayanıklı ara-kaydını** taşımalı; sweep'e değil relay'e dayanmalı.

## Karar

**İki yol, tek wire-contract (ADR-0013):**

### Audit-kritik yol (at-least-once)
- Handler, audit-kritik event için oluşturulduğu tx içinde **≤1 kompakt outbox satırı** yazar (tam ACT_HI satırı DEĞİL; NFR-P2). Outbox satırı engine DB'sindedir (tx-atomik commit ile; commit öncesi çökme → satır hiç var olmadı, tutarlı).
- **HistoryOutboxRelay** leader-elected'tir — **basamak-1 `SweepLeaderLease` (ADR-0002, KV-lease TTL=2·S) YENİDEN KULLANILIR**. Yalnız lease sahibi node relay döngüsünü koşar → cluster'da tek relay akışı, DB'ye sıfır koordinasyon yazısı.
- Relay outbox satırını okur → publish → **PubAck sonrası** satırı siler (custody-transfer; PubAck-öncesi-delete YASAK). Publish fail → retry/backoff, satır SİLİNMEZ (`SYS_OUTBOX_RELAY_PUBLISH_FAILED`). Leader çökerse lease TTL içinde devir, outbox satırları hayatta (NFR-R8; `SYS_OUTBOX_RELAY_LEADER_LOST`, devir sırasında çift-publish dedup'la yutulur).
- **Outbox-stuck gözlemi:** satır normal relay-döngü gecikmesinin katları kadar beklerse `SYS_OUTBOX_ROW_STUCK` (ops-alert; DP-12 "kısa maruziyet" ihlali sinyali). Nicel çarpan **ARCH-Q5/BA-Q7** ile kalibre edilir.

### Bulk yol (at-most-once)
- **HistoryPostCommitPublisher**, `TransactionState.COMMITTED` listener'ında (basamak-1 post-commit `TransactionListener` deseni [07§4] yeniden kullanım) tx-dışı, **sıfır ek DB yazımıyla** publish eder — node kendi elindeki event'i yayınlar (DB sorgusu YOK). Cutover sonrası bu sınıf için ACT_HI INSERT'i 0. Publish exception runtime tx'i rollback EDEMEZ (tx zaten commit oldu). Post-commit çökme penceresi = kalıcı kayıp, **bilinçli kabul** (reconciliation tespit eder, telafi etmez).

### Kompakt outbox payload taşıma (ARCH-Q1 — KARAR 2026-07-18: referans)
- **KARAR (ARCH-Q1, 2026-07-18):** kompakt outbox satırı, relay'in event'i yeniden kurup publish edebilmesi için gereken **minimal referans + kimlik alanlarını** taşır; büyük byte-array HISTORY payload'ı (`ByteArrayEntity(..., ResourceTypes.HISTORY)`, `DbHistoryEventHandler.java:97-105` — fork doğrulaması) **inline gömülMEZ**, referansla taşınır (NFR-P2 "≤1 kompakt satır" gerçekten kompakt kalsın, DP-12 maruziyeti minimize olsun). Reddedilen: inline (satır büyür, NFR-P2 zayıflar) ve hibrit (relay'e ikinci yol — karmaşıklık); wire-contract şekli (ADR-0013 `HistoryEventPayload.payload` opak) karardan bağımsız sabit. Relay yeniden-kurma detayı (referans çözümleme) phase4 LLD.

## Sonuçlar

**Olumlu:** Audit kaybı imkansız (dayanıklı outbox handoff, custody-transfer) + bulk hacminde sıfır DB yazımı; iki yol tek consumer/kontrat paylaşır. Relay leader-election ve custody-transfer basamak-1'den kanıtlı varlıklar → yeniden-icat yok. Tez korunur: leader koordinasyonu NATS KV'de, motor DB'sinde değil.

**Olumsuz / kabul edilen:** Audit-kritik yol tx-içi ≤1 ek yazı taşır (kalıcı, sıfıra inmez — NFR-P2 kabul). Bulk yolda post-commit çökme penceresi kalıcı kayıp üretir (D-A kasıtlı asimetri). Relay tek-leader → devir penceresinde (≤ lease TTL) kısa gecikme; NFR-R8 içinde.

## Reddedilenler
- **Handler-içi senkron NATS publish:** tx coupling (latency + rollback riski). **Reddedildi** (D-A, [07§3] kanıtlı).
- **Tam-outbox (tüm sınıflar outbox):** DB yazımı kalır — §6.7 hedefiyle çelişir. **Reddedildi** (D-A).
- **Tam-post-commit (tüm sınıflar post-commit):** audit-kritik sınıfta sessiz kayıp. **Reddedildi** (D-A).
- **Sweep-tabanlı telafi (basamak-1 deseni birebir):** cutover sonrası ACT_HI yok → sweep'in tarayacağı kaynak yok. **Reddedildi** (outbox yok-olma problemi, [07§4]).
