# ADR-0006 — AsyncAPI ile makine-okunur tel sözleşmesi (contract-first)

- **Durum:** Kabul edildi (2026-07-14, Phase 3)
- **İzlenebilirlik:** US-C1…C6 / US-A1/A4/A5 → FR-C1…C7 / IR-1…IR-6 → BR-SUB-001…006 → NFR-M3/NFR-M4 → docs/05 §7 (ortak wire-contract) / docs/06 §7

## Bağlam

İki idiom (A2 + Event Registry) **aynı teli** yayar/tüketir (NFR-M3); worker ekosistemi paylaşılır. Ayrıca NFR-M4: wire-contract ileride Track A (basamak-6 native engine) tarafından **dışarıya aynı** verilebilecek biçimde tanımlanmalı (worker'lar değişmeden bağlanabilmeli — docs/05 §7 kısıtı). Bu, sözleşmenin **tek, makine-okunur, doğrulanabilir** bir artefakt olmasını gerektirir; prose (API_CONTRACTS.md) tek başına drift üretir.

Sistem bir **HTTP API değildir** — asenkron NATS/JetStream mesajlaşmasıdır. Dolayısıyla OpenAPI uygun değildir; **AsyncAPI** doğru IDL'dir. gRPC (proto) ise D-G (kapsam dışı, ertelendi) → `.proto` teslimatı YOK.

## Karar

- **`docs/sentinel/phase3/api/asyncapi.yaml`** (AsyncAPI **3.1.0**) tek makine-okunur sözleşmedir; subject şeması, mesaj/header şemaları, DLQ sözleşmesi, custody-transfer operasyon açıklamaları burada yaşar.
- **Spec yalnız `api/` altında yaşar.** `API_CONTRACTS.md` bir **anlatı**dır — spec'i satır-içi TEKRARLAMAZ, ona referans verir (phase-review: inline spec = 🔴).
- JetStream stream/consumer konfigürasyonu AsyncAPI standart binding'i olmadığından **`x-jetstream-*` uzantı alanları** + API_CONTRACTS §3 tablolarıyla taşınır (WorkQueue/Limits, ackWait, maxDeliver, duplicateWindow, retention).
- gRPC eklenirse (ileride D-G altında) ayrı `api/*.proto` teslimatı açılır — basamak-1'de YOK.
- **CI kapısı:** `asyncapi validate` phase-review'da koşar; spec geçerli olmalı (bu ADR yazımında `asyncapi validate` ile **0 error / 0 governance issue** doğrulandı).

## Sonuçlar

**Olumlu:** Tek doğruluk kaynağı; worker'lar makine-okunur sözleşmeden kod-üretebilir/doğrulayabilir; NFR-M4 (Track A'ya taşınabilir sözleşme) doğrulanabilir bir artefaktla karşılanır; drift mekanik yakalanır (validator).

**Olumsuz / kabul edilen:** JetStream'e özgü ayrıntı standart binding olmadığından `x-` uzantılarında — bu ayrıntı prose tabloyla (API_CONTRACTS §3) çiftlenir; iki yerin senkron tutulması disiplin gerektirir (uzantı alanları normatif, tablo açıklayıcı olarak işaretlendi).

## Reddedilenler
- OpenAPI: HTTP-merkezli; async NATS'a uymaz. **Reddedildi**.
- `.proto`/gRPC: D-G kapsam dışı. **Ertelendi** (ileride D-G).
- Yalnız prose sözleşme (spec'siz): doğrulanamaz, drift üretir, worker codegen yok. **Reddedildi**.
- Inline spec (API_CONTRACTS içinde): phase-review 🔴 kuralı. **Reddedildi**.
