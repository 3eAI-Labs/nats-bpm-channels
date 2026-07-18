# ADR-0013 — History wire-contract (AsyncAPI) — basamak-1 ADR-0006 deseninin history izdüşümü

- **Durum:** Kabul edildi (2026-07-17, Phase 3 — basamak-2)
- **İzlenebilirlik:** US-A6/US-B4/US-B5/US-F2 → FR-A7/FR-B4/FR-B5/FR-F2 → BR-HDL-006/BR-REL-004/005/BR-DBT-002 → IR-1…IR-5, IR-7 → D-E / NFR-M3/NFR-M5
- **Kesişim:** basamak-1 **ADR-0006** (AsyncAPI contract-first) genişletilir; **ADR-0004** (DLQ-bridge circuit-breaker) history-DLQ tüketimine uygulanır; **ADR-0019** history subject retention/authz'yi taşır.

## Bağlam

D-E (kilitli): history subject şeması `history.<engineId>.<class>.<processInstanceId>` (instance-anahtarlı → stream sırası), dedup `Nats-Msg-Id=<historyEventId>:<eventType>`, DLQ = **basamak-1 kontratı AYNEN** (`dlq.history.>`, header-korumalı byte-ayna, custody-transfer, ayrı-stream [CQ-6]). Sistem HTTP değil, asenkron NATS/JetStream mesajlaşmasıdır → doğru IDL AsyncAPI'dir (ADR-0006 gerekçesi history'ye aynen geçer). Wire-contract tek makine-okunur artefakt olmalı (NFR-M3); Flowable basamak-2b aynı kontrata bağlanabilmeli (NFR-M5).

## Karar

- **`docs/sentinel/step2/phase3/api/asyncapi.yaml`** (AsyncAPI **3.1.0**) history tel sözleşmesinin tek makine-okunur kaynağıdır: `historyEvent` kanalı (subject/parametreler/mesaj), `historyDeadLetter` kanalı, publish/consume operasyonları, header/payload şemaları. **`asyncapi validate` ile 0 error / 0 governance issue** doğrulandı (bu ADR yazımında).
- **Spec yalnız `api/` altında yaşar.** `API_CONTRACTS.md` bir **anlatı**dır — spec'i satır-içi TEKRARLAMAZ, referans verir (phase-review: inline spec = 🔴). Bu, ADR-0006'nın history kontratına aynen uygulanan kuralıdır.
- **İki yayın yolu (ADR-0010) tek kontrat üretir:** relay (audit-kritik) ve post-commit publisher (bulk) AYNI subject/dedup şemasını yayar; consumer taraf ayrım yapmaz. Kontrat yol-agnostiktir.
- **History stream tipi = Limits-based** (WorkQueue DEĞİL — IR-7): projeksiyon consumer + reconciliation/replay okuyabilmeli; retention penceresi ADR-0019. (Basamak-1 iş-dağıtımı WorkQueue idi; history sıra-korumalı okuma yüzeyi olduğundan Limits seçilir.)
- **DLQ = ayrı `dlq.history.>` stream** (CQ-6): iş-dağıtımı `dlq.>` stream'inden bağımsız; header-korumalı byte-ayna kopya + `Nats-Msg-Id=<orijinal>.dlq`; custody-transfer (DLQ-PubAck-sonrası-ack); dlq-of-dlq YOK; DLQ-bridge/tüketim CB korumalı (ADR-0004). Basamak-1 `DlqPublisher` [07§4] yeniden kullanılır.
- **Header'lar (IR-2):** history-özgü kanonik meta (engineId, class, eventType, historyEventId, processInstanceId) + devralınan `BpmHeaders` (Trace-Id/Business-Key/Idempotency-Key); Business-Key subject'e GÖMÜLMEZ (DP-2), masking kiracı kararı (DP-8).
- **Stream provisioning:** `BenchEnvironment.ensureStreams()` + prod provisioning history + `dlq.history.>` stream'lerini kapsayacak şekilde genişler (BR-DBT-002; eksikse `VAL_HISTORY_STREAM_PROVISIONING_MISSING`).

## Sonuçlar

**Olumlu:** Tek doğrulanabilir kontrat → drift mekanik yakalanır (validator); projeksiyon consumer + Flowable basamak-2b (NFR-M5) makine-okunur kontrata bağlanır; basamak-1 DLQ/dedup/custody substratı yeniden kullanılır (yeniden-icat yok). Limits-based history stream reconciliation/replay okumasına izin verir.

**Olumsuz / kabul edilen:** JetStream stream/consumer semantiği AsyncAPI standart binding'i olmadığından `x-jetstream-*` uzantılarında + API_CONTRACTS §2 tablosunda çiftlenir (senkron tutmak disiplin gerektirir — ADR-0006 devralınan bedel). Limits-based history stream retention penceresi ops tarafından yönetilmeli (ADR-0019).

## Reddedilenler
- **OpenAPI (history tel için):** HTTP-merkezli; async NATS'a uymaz. **Reddedildi** (ADR-0006; OpenAPI yalnız read-only sorgu-API için — ADR-0014).
- **Yalnız prose kontrat (spec'siz):** doğrulanamaz, drift üretir. **Reddedildi** (ADR-0006).
- **Inline spec (API_CONTRACTS içinde):** phase-review 🔴. **Reddedildi**.
- **WorkQueue history stream:** tek-tüketici-alır → reconciliation/replay okuyamaz. **Reddedildi** (IR-7 — Limits seçildi).
- **DLQ'yu iş-dağıtımı `dlq.>` stream'iyle paylaşma:** CQ-6 ayrı-stream dersi ihlali. **Reddedildi**.
