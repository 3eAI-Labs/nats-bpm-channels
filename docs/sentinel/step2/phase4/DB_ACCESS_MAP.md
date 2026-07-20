# DB / Entity Erişim Haritası — Basamak-2: History Offload

**Sentinel fazı:** Phase 4 — Developer (LLD). **Kaynak:** `DB_SCHEMA.md`, `docs/sentinel/step2/phase3/DATA_OWNERSHIP.yaml`, ADR-0010/0011/0016/0017/0018.
**Amaç:** basamak-1 `docs/sentinel/phase4/DB_ACCESS_MAP.md` deseni — hangi bileşen hangi tabloya hangi op'la dokunur; `DATA_OWNERSHIP.yaml` ile tutarlılık satır satır doğrulanır.

---

## 1. Engine DB eklentisi — `compact_history_outbox` / `compact_history_outbox_payload`

| Tablo | Bileşen | Erişim türü | DATA_OWNERSHIP.yaml eşleşmesi |
|---|---|---|---|
| `compact_history_outbox` | `NatsHistoryEventHandler` (audit-kritik sınıflandırma dalı) | **INSERT** (tx-içi, ≤1 satır — BR-HDL-003) | `history.compact-outbox` provider: `engine-node-history-handler` |
| `compact_history_outbox_payload` | `NatsHistoryEventHandler` (yalnız byte-array payload varsa) | **INSERT** (aynı tx, koşullu) | aynı capability, `private: [outbox-tablosu-referans-satiri]` |
| `compact_history_outbox` | `HistoryOutboxRelay` (leader-elected) | **SELECT** (oldest-first, `idx_compact_history_outbox_created_at`) + **DELETE** (yalnız PubAck-sonrası, custody-transfer) | consumer: `history-outbox-relay`, `access: internal` |
| `compact_history_outbox_payload` | `HistoryOutboxRelay` | **SELECT** (relay'in event'i yeniden kurması için, `payload_large_ref` doluysa) | CASCADE ile ebeveynle birlikte silinir — relay ayrıca DELETE ETMEZ |

**Erişim sınırı:** yalnız engine node bounded-context (aynı güven sınırı, `DATA_OWNERSHIP.yaml` `access: internal`). Projeksiyon servisi bu tabloya HİÇBİR ZAMAN dokunmaz.

---

## 2. Projeksiyon store — 15 sınıf tablosu + `projection_large_payload`

| Tablo grubu | Bileşen | Erişim türü | Not |
|---|---|---|---|
| Tüm 15 sınıf tablosu | `HistoryProjectionConsumer` | **SELECT** (merge-upsert protokolü adım 1, §`DB_SCHEMA.md §2.3`) + **INSERT/UPDATE** (adım 2-4) | Tek yazım-sahibi (`DATA_OWNERSHIP.yaml` `history-projection-service` provider) |
| `projection_large_payload` | `HistoryProjectionConsumer` | **INSERT** (VARINST byte-array, EXT_TASK_LOG.errorDetails, ATTACHMENT içeriği varsa) | Kaynak satırın `*_ref` kolonuyla bağlanır |
| Tüm 15 sınıf tablosu | `HistoryQueryApi` | **SELECT** (read-only, çekirdek-4 desenler, sayfalamalı) | `WHERE deleted_at IS NULL` (soft-delete filtreli); PII maskeleme uygulama-katmanında (DP-15) |
| Tüm 15 sınıf tablosu | `ReconciliationJob` | **SELECT** (sınıf-başına fark sayacı, projeksiyon ↔ ACT_HI) | Yalnız sayaç/id üretir — PII değeri okunur ama DIŞARI SIZMAZ (DP-14) |
| Tüm 15 sınıf tablosu | `RetentionEnforcementJob` | **DDL** (`ALTER TABLE ... DETACH/DROP PARTITION`, sınıf-bazlı pencere) | Retention-expiry — ADR-0018 |
| Bulk sınıf tabloları (9 log + VARINST/ACTINST/TASKINST/CASEINST/PROCINST) | `ErasurePipeline` | **UPDATE** (soft-delete→anonymize, `deleted_at`/`anonymized_at`) | Audit-kritik tablolar (`legal_hold=TRUE`) **YAZILMAZ** — `BUS_ERASURE_REQUEST_LEGAL_HOLD_BLOCKED` |
| `class_cutover_state` | `ReconciliationJob` | **SELECT + UPDATE** (streak sayacı, `state`) | BR-CUT-001/004 |
| `class_cutover_state` | `CutoverControlPlane` | **SELECT + UPDATE** (`state=CUTOVER_TALEP→CUTOVERLANMIS`, `cutover_applied_at`) | BR-CUT-002 |
| `class_cutover_state` | `CutoverRollback` | **UPDATE** (`state→DUAL_RUN`, `rollback_count++`) | BR-CUT-003 |
| `retention_audit_log` | `RetentionEnforcementJob` | **INSERT** (her partition-drop için ZORUNLU) | Yazım hatası → `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED` (CRITICAL) |
| `erasure_audit_log` | `ErasurePipeline` | **INSERT** (her erasure işlemi + doğrulama sonucu) | `verification_status=FAILED` → `RES_ERASURE_VERIFICATION_FAILED` (CRITICAL) |
| `erasure_scope_confirmation` | `ErasurePipeline` | **INSERT** (belirsiz subject-key) + **SELECT/UPDATE** (onay sonrası) | BA-Q6 |

**PII notu:** projeksiyon store bütünüyle **L3 (PII) store** muamelesi görür (DP-9, upward-inheritance) — bu, basamak-1'in "yeni tablo yok" duruşundan **kasıtlı sapmadır**: history verisi artık kalıcı, sorgulanabilir bir store'a yazılıyor (`DATA_CLASSIFICATION.md §3` "kritik yeni geçiş").

---

## 3. Pseudonym kasası — `pseudonym_map` / `vault_access_audit`

| Tablo | Bileşen | Erişim türü | Not |
|---|---|---|---|
| `pseudonym_map` | `HistoryProjectionConsumer` (downstream/async, BA-Q5) | **INSERT** (harita satırı — pseudonym_token zaten tx-içi hesaplanmış) | `SYS_PSEUDONYM_VAULT_UNAVAILABLE` → retry; audit-kritik akışı ENGELLEMEZ |
| `pseudonym_map` | `ErasurePipeline` | **DELETE** (silme-hakkı talebi, pseudonymized audit-kritik kayıt) | `BUS_PSEUDONYM_MAP_ENTRY_DELETED` — HARD DELETE, tersinmez |
| `pseudonym_map` | Yetkili re-identification akışı (nadir, yasal/adli) | **SELECT** (`pgp_sym_decrypt`) | En-az-yetki + açık gerekçe + audit (DP-16) |
| `vault_access_audit` | `PseudonymizationVault` (tüm operasyonlar) | **INSERT** (her WRITE/READ/DELETE/REIDENTIFY_ATTEMPT, `granted` alanı dahil) | Yetkisiz denemeler de kaydedilir — `AUTH_PSEUDONYM_VAULT_ACCESS_DENIED` kanıt izi |

**İzolasyon:** kasa hem engine DB'den hem projeksiyondan **fiziksel olarak ayrı** Postgres örneğidir (§`DB_SCHEMA.md §0`); aralarında FK YOK — yalnız `pseudonym_token` (opak string) uygulama-katmanında iki tarafı bağlar.

---

## 4. Basamak-1 varlıklarının yeniden kullanımı (ADR-0007 deseni — KV, motor şeması)

Basamak-2, basamak-1'in `a2-sweep-leader` KV bucket'ını **yeniden kullanmaz** (farklı amaç) ama AYNI **mekanizmayı** (`SweepLeaderLease`, KV-lease) yeni bir bucket'la kopyalar:

| KV bucket | Anahtar şeması | Kullanan | Kaynak |
|---|---|---|---|
| `history-relay-leader` (yeni, basamak-1 `a2-sweep-leader` deseninin izdüşümü) | `relay-leader.<engineId>` (motor-başına izole, basamak-1 LLD-Q1 dersini önceden uygular) | `HistoryOutboxRelay` (`SweepLeaderLease` sınıfı YENİDEN KULLANILIR, farklı `engineId`/bucket parametresiyle) | ADR-0010 + basamak-1 ADR-0002 |

Motor şemasına (`ACT_RU_*`/`ACT_HI_*`) **hiçbir yeni sütun/tablo eklenmez** — basamak-1'in "motor şeması sahibi DEĞİLİZ" ilkesi §1'deki tek istisna (`compact_history_outbox`, motorun DEĞİL bu reponun tablosu) dışında aynen korunur.

---

## 5. İzlenebilirlik

BR-HDL-003, BR-REL-001/002/003, BR-CUT-001/002/003/004, BR-PII-001…005 (DB erişimi); ADR-0010/0011/0015/0016/0017/0018 (mekanizma); FR-A4/B1/B2/B3/D1/D2/D3/G1/G2/G3; US-A3/B1/B2/B3/D1/D2/D3/G1/G2/G3. Tam şema anlatısı: `DB_SCHEMA.md`. Veri-sahipliği kaynak-doğrulaması: `docs/sentinel/step2/phase3/DATA_OWNERSHIP.yaml`.
