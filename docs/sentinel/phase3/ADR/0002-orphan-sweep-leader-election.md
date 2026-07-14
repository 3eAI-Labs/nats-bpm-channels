# ADR-0002 — Orphan-sweep leader seçim mekanizması

- **Durum:** Önerildi — ARCH-Q2 Levent onayına bağlı (2026-07-14, Phase 3)
- **İzlenebilirlik:** US-A3 → FR-A5 → BR-A2-005/BR-A2-004 → `SYS_SWEEP_*` (EXCEPTION_CODES §4) → docs/06 §5.3 (D-A) / NFR-P5

## Bağlam

D-A (docs/06 §5.3, kilitli): outbound tetikleme = post-commit `TransactionListener` (happy-path, DB sorgusuz) + **soğuk orphan-sweep** (yalnız ①'in kaçırdığı çökme-orphan'ları). Sweep gereksinim kısıtları:

- **Tek node/leader'da** koşmalı — N engine-node her biri sweep koşarsa fetchable-parite sorgusu N kez atılır (gereksiz DB okuması) ve iki node aynı orphan'ı re-publish edebilir (dedup yutar ama israf).
- **Seyrek + read-only + `SELECT FOR UPDATE`'siz** (NFR-P5: amortize ≤ 1 read / S / cluster). Hot-poll REDDEDİLDİ (P1 ihlali) — sweep onun yerine geçmez, tersine onu ortadan kaldıran soğuk mekanizmadır.

Soru (phase3'e devir): **leader nasıl seçilir?** Tezin özü DB yükünü azaltmak olduğundan leader-seçiminin kendisi DB'ye yeni bir sıcak-yol yükü bindirmemeli.

## Karar (öneri — ARCH-Q2)

**JetStream KV lease-tabanlı leader election (substrat-hizalı, DB-dışı).**

- `nats-core`'da yeni `SweepLeaderLease` bileşeni: bir JetStream KV bucket'ında (`a2-sweep-leader`) TTL'li tek anahtar (`leader`) üzerinden lease alır.
- TTL = `2·S` (240s); leader her `S` (120s) yenilemeye çalışır. Kaybeden node'lar yenileme denemeye devam eder (leader ölürse TTL sonrası devralınır).
- Yalnız lease sahibi node sweep döngüsünü çalıştırır → cluster genelinde **tek** sweep akışı, DB'ye **sıfır** koordinasyon yazısı.

**Neden KV-lease (DB-lock/ShedLock yerine):**
- Tezle hizalı: leader koordinasyonu NATS'ta, motor DB'sinde değil → DB-offload iddiasını zayıflatmaz.
- Ek altyapı yok: JetStream zaten zorunlu bağımlılık; KV aynı broker'da.
- `SweepLeaderLease` engine-nötr → `nats-core`'da, hem A2 motorları (camunda/cadenzaflow) paylaşır.

**Kabul edilen bedel:** DATA_CLASSIFICATION §2.3 KV'yi "basamak-1'de opsiyonel" listeler → bu karar KV'yi **aktive eder** (PSEUDONYMOUS lease anahtarı, PII yok, TTL'li). Bu bir kapsam netleştirmesidir → **ARCH-Q2**.

## Alternatifler (değerlendirildi)

| Mekanizma | Artı | Eksi | Karar |
|---|---|---|---|
| **JetStream KV lease** (önerilen) | DB-dışı, substrat-hizalı, ek altyapı yok | KV'yi aktive eder (opsiyoneldi) | **Öneri** |
| DB-backed ShedLock / `pg_advisory_lock` | Battle-tested, tek satır | Motor DB'sine (az da olsa) periyodik yazı → tez ayağını hafifçe kirletir | Yedek (ARCH-Q2'de sunulur) |
| Tek-replica deploy (leader yok) | En basit | HA yok; sweep SPOF; ölçeklenmez | Reddedildi (carrier-grade değil) |
| Harici koordinatör (ZK/etcd/Consul) | Olgun | Yeni ağır bağımlılık; Apache-2.0/altyapı maliyeti | Reddedildi (NFR-L1 + sadelik) |

## Sonuçlar

**Olumlu:** Cluster'da tek sweep akışı; DB koordinasyon yazısı sıfır; leader devri otomatik (TTL). `SYS_SWEEP_QUERY_FAILED` bir döngü atlar, bir sonraki `S`'de tekrar dener (orphan yaşı ≤ L+2S'e esner — kabul).

**Olumsuz / kabul edilen:** KV bağımlılığı aktive olur; leader devri sırasında (≤ TTL) kısa bir "sweep boşluğu" olabilir — orphan zaten ≤ L+S toleranslı (NFR-R3), bu boşluk o tolerans içinde kalır.

**ARCH-Q2 (Levent onayı):** KV-lease (DB-dışı, KV'yi aktive eder) mi, DB-backed ShedLock (tez ayağını hafif kirletir, KV'siz) mi? Öneri: KV-lease.
