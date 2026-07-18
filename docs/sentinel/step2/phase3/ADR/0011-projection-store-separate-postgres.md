# ADR-0011 — Projeksiyon store: engine DB'den AYRI Postgres, denormalize sorgu-odaklı şema

- **Durum:** Kabul edildi (2026-07-17, Phase 3 — basamak-2). Alt-eksen KARARA BAĞLANDI: instance-partition = **JetStream subject-mapped deterministic partition** (ARCH-Q3, 2026-07-18).
- **İzlenebilirlik:** US-B2/US-B3 → FR-B2/FR-B3 → BR-REL-002/003 → `SYS_PROJECTION_WRITE_FAILED`/`SYS_PROJECTION_SCHEMA_DRIFT` → D-B/D-E / NFR-P5/NFR-S2/NFR-M4

## Bağlam

D-B (kilitli): query-store = engine DB'sinden **AYRI Postgres projeksiyonu**; denormalize/sorgu-odaklı şema; consumer asyncapi-kontratlı. Gerekçe: contention domain ayrılır (history yazımı birincil DB hot-path'inden çıkar), SQL/ops bilgisi yeniden kullanılır, KVKK retention/silme SQL'le uygulanabilir. Projeksiyon store history sınıflarının çoğu PII taşıdığından bütünüyle **L3 (PII) store** muamelesi görür (upward-inheritance; DP-9). Consumer, history stream'ini instance-anahtarlı sırada tüketip idempotent merge-upsert yapmalı (D-E).

## Karar

- **ProjectionStore = ayrı Postgres örneği/şeması** (engine DB'den izole; NFR-S2: at-rest AES-256, role-based erişim). Şema **denormalize/sorgu-odaklı** — ACT_HI normalize düzeninin aynası DEĞİL; sorgu-API çekirdek-4 erişim desenlerini (ADR-0014) doğrudan destekler.
- **HistoryProjectionConsumer** history stream'ini **instance-anahtarıyla partition'lı** tüketir (aynı `processInstanceId` → aynı işleyici; per-instance sıra korunur, D-E) ve **idempotent merge-upsert** uygular (çatışma çözümü ADR-0012). Yazım hatası → nak/redelivery (`SYS_PROJECTION_WRITE_FAILED`, idempotent retry güvenli); kontrat-drift → consumer durur (`SYS_PROJECTION_SCHEMA_DRIFT`, ADR-0013).
- **Zaman-bazlı bölümleme (retention için):** projeksiyon tabloları sınıf-bazlı **range-partition (zaman)** ile bölümlenir → retention `DROP/DETACH PARTITION` ile bulk-DELETE VACUUM yükü olmadan uygulanır (ADR-0018; PostgreSQL declarative range partitioning + pg_partman deseni [resmi doküman]).
- **Instance-partition mekanizması (ARCH-Q3 — KARAR 2026-07-18):** JetStream **deterministik subject-mapped partitioning** (`partition(N, token)` — `processInstanceId` token'ından hash) + partition-başına durable consumer (server-side, en az istemci-koordinasyonu); core queue-group tek başına partition-less/deterministik-değil olduğundan yetmez [NATS docs]. Reddedilen: client-side partitioned consumer group (NATS Orbit — ek kütüphane + client-side koordinasyon), instance-hash başına ordered-consumer maxAckPending=1 (throughput'u boğar, NFR-P5 ihlali). Partition sayısı/rebalance detayı phase4 LLD; wire-contract (ADR-0013) subject şeması karardan bağımsız sabit.
- **ClickHouse'a evrim izole:** hacim zorlarsa store ClickHouse'a evrilebilir — wire-contract (ADR-0013) sabit kaldığından yalnız consumer değişir (NFR-M4). **ClickHouse-şimdi ERTELENDİ** (D-B — ihtiyaç kanıtlanmadan yeni ops yüzeyi açılmaz).

## Sonuçlar

**Olumlu:** Contention ayrımı → history yazımı birincil DB'yi yormaz (NFR-P4). Denormalize şema sorgu-API'yi verimli besler; SQL retention/erasure (ADR-0017/0018) doğrudan uygulanabilir. Yatay ölçeklenme (NFR-P5) instance-partition'la mümkün. ClickHouse geçiş yolu kontrat-stabil (NFR-M4).

**Olumsuz / kabul edilen:** Yeni stateful yüzey (ayrı Postgres) — HA/failover/RTO-RPO **kiracı-owned** (gömülebilir-kütüphane duruşu, SRS §4.7/NFR-R8); store kesintisi projeksiyon-lag'i büyütür (NFR-P3 SLI), audit KAYBI üretmez (audit-kritik dayanıklılık outbox/relay'de, ADR-0010). Denormalize şema yazım-amplifikasyonu (aynı event birden çok denormalize satıra dokunabilir) — merge-upsert idempotency ile yönetilir.

## Reddedilenler
- **JetStream-only query-store (ayrı DB yok):** rastgele-erişim audit sorguları için yetersiz. **Reddedildi** (D-B).
- **Engine DB'de projeksiyon (aynı instance):** contention domain ayrılmaz — tez ihlali. **Reddedildi** (D-B).
- **ClickHouse-şimdi:** ihtiyaç kanıtlanmadan yeni ops yüzeyi. **Ertelendi** (D-B; kontrat-stabil evrim yolu açık).
- **Normalize ACT_HI aynası şema:** sorgu-API erişim desenlerini verimsiz karşılar, KVKK SQL-silme karmaşıklaşır. **Reddedildi** (D-B).
