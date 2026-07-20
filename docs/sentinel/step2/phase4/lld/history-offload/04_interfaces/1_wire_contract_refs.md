# 04.1 — Wire-Contract Referansları (Köprü — inline spec YOK)

Makine-okunur sözleşmeler **yalnız** şu iki dosyada yaşar (ADR-0013/0014; phase-review: inline spec = 🔴):

- **[`docs/sentinel/step2/phase3/api/asyncapi.yaml`](../../../../phase3/api/asyncapi.yaml)** — history tel sözleşmesi (`historyEvent`/`historyDeadLetter` kanalları, `Nats-Msg-Id` dedup, header şemaları).
- **[`docs/sentinel/step2/phase3/api/openapi.yaml`](../../../../phase3/api/openapi.yaml)** — read-only history sorgu-API (çekirdek-4, `HistoryQueryApi`'nin ürettiği yanıt şemaları).

Anlatı + AsyncAPI-dışı JetStream stream/consumer konfigürasyonu: [`docs/sentinel/step2/phase3/API_CONTRACTS.md`](../../../../phase3/API_CONTRACTS.md).

---

## Bu LLD-modülünün kontrata bağlandığı noktalar

| Java sınıfı (bu modül) | asyncapi/openapi kanal/uç-nokta | Yön |
|---|---|---|
| `HistoryOutboxRelay` (`03_classes/2_relay_projection.md` §1) | `publishHistoryEvent` (`historyEvent` kanalı) | publish |
| `HistoryPostCommitPublisher` (`03_classes/1_handler_outbox.md` §3) | `publishHistoryEvent` | publish (aynı kanal, yol-agnostik — NFR-M3) |
| `HistoryProjectionConsumer` (`03_classes/2_relay_projection.md` §2) | `consumeHistoryEvent` | consume |
| `HistoryDlqConsumer`/`HistoryDlqInspectionConsumer` (`03_classes/2_relay_projection.md` §4) | `routeHistoryToDlq` / `consumeHistoryDlq` | publish / consume |
| `HistoryQueryApi` (`03_classes/3_query_api.md` §1) | `openapi.yaml` `/api/v1/history/*` (4 operationId — `getProcessInstanceHistory`, `listProcessInstanceHistory`, `listActivityHistory`/`listTaskHistory`/`listVariableHistory`) | REST GET |

**Değişmezler (kontrattan bu LLD'ye taşınan, tekrar YOK — API_CONTRACTS.md §6):** NFR-M3 (iki yayın yolu aynı kontrat), NFR-M5 (Flowable basamak-2b aynı kontrata bağlanabilir), NFR-R3 (custody-transfer), NFR-R6 (at-least-once + idempotent), CQ-6 (ayrı-stream).

**DTO ↔ wire-şema eşlemesi:** `04_interfaces/2_projection_dtos.md`.
