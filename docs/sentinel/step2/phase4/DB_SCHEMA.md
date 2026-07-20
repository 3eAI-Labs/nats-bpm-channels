# DB_SCHEMA — Basamak-2: History Offload

**Sentinel fazı:** Phase 4 — Developer (LLD). **Kaynak:** `docs/sentinel/step2/phase3/HLD.md` §3.2/§3.6, ADR-0010/0011/0016/0017/0018, `DATA_OWNERSHIP.yaml`, `DATA_CLASSIFICATION.md` DP-9…DP-16.
**Durum:** Taslak — Levent faz-4 onayına sunuluyor.

> **DDL bu belgede YOK.** LLD_GUIDELINE §2.10 ve görev talimatı gereği: DDL **yalnız** `db/migrations/*.sql` altında yaşar; bu belge şemaya **referans verir**, satır-içi tekrarlamaz (inline DDL = phase-review 🔴). Aşağıdaki tablolar migration dosyalarının **anlatı özeti**dir.

---

## 0. Üç ayrı fiziksel veritabanı (D-B / ARCH-Q2 — kilitli)

Basamak-1'in "yeni DDL yok" duruşundan (`docs/sentinel/phase4/DB_ACCESS_MAP.md` §0) **bilinçli olarak sapılır**: basamak-2 üç yeni şema alanı açar, üçü de bu repodan **kanıtlanmış** migration'lar taşır (bkz. §5 migration-proof).

| # | Veritabanı | Fiziksel yerleşim | Migration dizini | Sahip bileşen |
|---|---|---|---|---|
| (a) | **Engine DB eklentisi** — `compact_history_outbox` | Aynı Postgres örneği/şeması engine'in `ACT_RU_*`/`ACT_HI_*` ile (tx-atomiklik şartı, ADR-0010) | `db/migrations/engine-outbox/` | `NatsHistoryEventHandler` (yazar) / `HistoryOutboxRelay` (okur+siler) |
| (b) | **Projeksiyon store** | AYRI Postgres örneği (D-B, ADR-0011) | `db/migrations/projection/` | `HistoryProjectionConsumer` (yazar) / `HistoryQueryApi`, `ReconciliationJob`, `RetentionEnforcementJob`, `ErasurePipeline` (okur) |
| (c) | **Pseudonym kasası** | AYRI Postgres örneği (ARCH-Q2 KARAR 2026-07-18, ADR-0016) — hem engine DB'den hem projeksiyondan izole | `db/migrations/pseudonym-vault/` | `PseudonymizationVault` |

**Neden (a) engine DB'de yaşıyor (basamak-1'den fark):** ADR-0010 outbox satırının runtime yazımıyla **aynı transaction'da tx-atomik commit** olması gerektiğini kilitler — bu yalnız engine'in kendi bağlantı/tx sınırı içinde mümkündür. Bu, basamak-1'in "motor şemasına asla DDL uygulanmaz" ilkesini **ihlal etmez**: `compact_history_outbox`/`compact_history_outbox_payload` **motorun kendi tabloları değil** (`ACT_RU_*`/`ACT_HI_*` DEĞİŞMEZ), basamak-2'nin eklediği **yeni, sahibi bu repo olan** iki tablodur — tenant bu migration'ı kendi engine-DB migration zincirine (Flyway/Liquibase, Phase 5 kararı) ekler.

---

## 1. (a) Engine DB eklentisi — `compact_history_outbox`

**Migration:** [`db/migrations/engine-outbox/001_compact_history_outbox.sql`](db/migrations/engine-outbox/001_compact_history_outbox.sql)

| Tablo | Amaç | PK / dedup | Retention |
|---|---|---|---|
| `compact_history_outbox` | Audit-kritik event, ≤1 satır/tx (NFR-P2, BR-HDL-003) | `id` UUID PK; `UNIQUE(history_event_id, event_type)` (`Nats-Msg-Id` bileşenleri) | Relay PubAck-sonrası SİLER — **çok kısa** (DP-12) |
| `compact_history_outbox_payload` | ARCH-Q1 "referans" hedefi — yalnız büyük byte-array payload varsa (örn. EXT_TASK_LOG.errorDetails) | `id` UUID PK; `outbox_row_id` → `compact_history_outbox(id)` ON DELETE CASCADE | Ebeveyn satırla birlikte silinir (CASCADE) |

**ARCH-Q1 kararının satır-düzeyi karşılığı:** `payload_scalar JSONB` küçük/sınırlı audit-kritik alanları (userId, operationType, entityType, property, orgValue, newValue, incidentMessage, configuration, activityId, workerId, errorMessage) **inline** taşır — bunlar ACT_HI'nin normalize çok-kolonlu şeklinin **aynası DEĞİL**, opak/kompakt bir blok. `payload_large_ref` yalnız büyük bir byte-array mevcutsa dolar (nadiren — bkz. LLD-Q2, `01_overview.md`).

**Erişim:** yalnız engine node (`NatsHistoryEventHandler` yazar, `HistoryOutboxRelay` okur+siler) — aynı güven sınırı (`DATA_OWNERSHIP.yaml` `history.compact-outbox`, `access: internal`).

---

## 2. (b) Projeksiyon store — denormalize, sorgu-odaklı, sınıf-başına tablo

**Migration'lar:**
- [`db/migrations/projection/001_entity_lifecycle_tables.sql`](db/migrations/projection/001_entity_lifecycle_tables.sql) — 6 entity-lifecycle sınıfı
- [`db/migrations/projection/002_append_log_tables.sql`](db/migrations/projection/002_append_log_tables.sql) — 9 append-only log sınıfı
- [`db/migrations/projection/003_control_plane_and_compliance.sql`](db/migrations/projection/003_control_plane_and_compliance.sql) — cutover-state + retention/erasure audit

### 2.1 İki merge-upsert deseni (ADR-0011/0012) — **sınıf tipine göre**

Basamak-2'nin 15 `ACT_HI` sınıfı iki davranışsal kümeye ayrılır (bu ayrım ADR-0012'nin "stream-sequence versiyon kolonu" kararının **hangi tabloya nasıl uygulandığının** LLD-düzeyi somutlaştırmasıdır):

| Küme | Sınıflar | Satır anlamı | Merge-upsert davranışı | Partition anahtarı |
|---|---|---|---|---|
| **Entity-lifecycle** (§2.2) | PROCINST, ACTINST, VARINST, TASKINST, INCIDENT, CASEINST | Bir entity'nin **GÜNCEL durumu** (create→update→end aynı satırı günceller) | `stream_sequence` artarsa UPDATE, azsa/eşitse no-op (`BUS_PROJECTION_STALE_EVENT_DISCARDED`) | `partition_anchor_at` — entity'nin **ilk görüldüğü** an, SABİT (aşağıda §2.3 gerekçe) |
| **Append-only log** (§2.4) | DETAIL, IDENTITYLINK, OP_LOG, EXT_TASK_LOG, JOB_LOG, COMMENT, ATTACHMENT, DECINST, BATCH | Her event = **yeni, değişmez** bir satır (zaten "an"ı temsil ediyor) | `INSERT ... ON CONFLICT (…, event_time) DO NOTHING` — dedup, "daha yeni ez" kavramı YOK | `event_time` — doğrudan (satır zaten o ana ait) |

### 2.2 Entity-lifecycle tabloları (6)

| Tablo | ACT_HI sınıfı | Doğal anahtar | Audit-kritik mi? | çekirdek-4 rolü |
|---|---|---|---|---|
| `process_instance_history` | PROCINST | `(engine_id, process_instance_id)` | Hayır (bulk) | Desen 1/2/3 çapa tablosu (processInstanceId/businessKey/zaman-aralığı+definition) |
| `activity_instance_history` | ACTINST | `(engine_id, activity_instance_id)` | Hayır (bulk) | Desen 4 (instance→activity geçmişi) |
| `variable_instance_history` | VARINST | `(engine_id, variable_instance_id)` | Hayır (bulk) | Desen 4 (instance→variable geçmişi, güncel değer) |
| `task_instance_history` | TASKINST | `(engine_id, task_id)` | Hayır (bulk) | Desen 4 (instance→task geçmişi) |
| `incident_history` | INCIDENT | `(engine_id, incident_id)` | **Evet** — `legal_hold` default TRUE | Sorgu-API + reconciliation |
| `case_instance_history` | CASEINST | `(engine_id, case_instance_id)` | Hayır (bulk) | CMMN, düşük hacim |

### 2.3 Neden `partition_anchor_at` — ve neden "en son event zamanı" DEĞİL

Postgres declarative range-partitioning kuralı: her UNIQUE/PK kısıtı **partition anahtarını içermek ZORUNDADIR**. Eğer partition anahtarı "en son event zamanı" olsaydı, bir entity'nin (örn. bir `ACTINST`) ilk event'i Ocak'ta, bitiş event'i Şubat'ta gelirse, satır Şubat'a "taşınmalı" olurdu — Postgres bunu şeffaf yapmaz (ayrı bir DELETE+INSERT gerekir, atomiklik/idempotency riski). Bunun yerine `partition_anchor_at` entity'nin **ilk görüldüğü an** SABİTLENİR; sonraki tüm lifecycle event'leri **AYNI** satırı, AYNI partition'da günceller. `event_time` (ayrı kolon) her upsert'te en son event'in zamanını taşır (ADR-0012: **görüntüleme alanı**, sıralama otoritesi DEĞİL — merge-upsert otoritesi her zaman `stream_sequence`).

**Merge-upsert protokolü (partition-güvenli, tek-deyimli `ON CONFLICT` YETERSİZ olduğundan):**
1. `SELECT partition_anchor_at, stream_sequence FROM <tablo> WHERE engine_id=? AND <entity_id>=?` — global (partition'a duyarsız) index (§2.5) ile nokta-sorgu.
2. Bulunduysa + gelen `stream_sequence` > mevcuttan büyükse → `UPDATE ... WHERE engine_id=? AND <entity_id>=? AND partition_anchor_at=<bulunan>` (tek-partition, index-destekli).
3. Bulunduysa + gelen `stream_sequence` ≤ mevcut → no-op (`BUS_PROJECTION_STALE_EVENT_DISCARDED`).
4. Bulunmadıysa → `INSERT ... (partition_anchor_at = bu event'in event_time'ı)`.

Bu protokol `db/migrations/projection/001_entity_lifecycle_tables.sql` başlığında ve `lld/history-offload/03_classes/2_relay_projection.md §2`'de tekrar belgelenir (tek doğruluk kaynağı: migration dosyası başlığı; LLD sınıf dosyası ona referans verir).

**Docker-doğrulanmış (§5):** bu üç adımlı protokol `activity_instance_history` üzerinde gerçek bir Postgres 16 konteynerinde INSERT→UPDATE(yeni-sequence, uygulanır)→UPDATE(eski-sequence, 0 satır etkiler) sırasıyla bizzat koşuldu.

### 2.4 Append-only log tabloları (9)

| Tablo | ACT_HI sınıfı | Audit-kritik mi? | Not |
|---|---|---|---|
| `variable_detail_history` | DETAIL | Hayır (bulk, en büyük hacim, ilk cutover adayı) | |
| `identity_link_history` | IDENTITYLINK | Hayır (bulk, PO-Q5) | ADD/DELETE link event'leri |
| `operation_log_history` | OP_LOG | **Evet** — `legal_hold` default TRUE | Pseudonymization opt-in kolonları (`user_id_pseudonymized`, `pseudonym_token`) — §4 |
| `ext_task_log_history` | EXT_TASK_LOG | **Evet** — `legal_hold` default TRUE | `error_details_ref` → `projection_large_payload` (ARCH-Q1 referans) |
| `job_log_history` | JOB_LOG | Hayır (bulk) | |
| `comment_history` | COMMENT | Hayır (bulk, PO-Q5) | Serbest metin, PII riski |
| `attachment_history` | ATTACHMENT | Hayır (bulk, PO-Q5) | `content_ref` → `projection_large_payload` |
| `decision_evaluation_history` | DECINST | Hayır (bulk) | `inputs`/`outputs` JSONB — karar payload'ı PII riski |
| `batch_history` | BATCH | Hayır (bulk) | Process-instance-scoped DEĞİL; PII taşımaz (yalnız job sayaçları) |

`projection_large_payload` — paylaşılan büyük-nesne yardımcı tablosu (VARINST byte-array değerleri, EXT_TASK_LOG.errorDetails, ATTACHMENT içeriği); partition'lı DEĞİL (kaynak satırın `ON DELETE`/erasure akışıyla temizlenir — Phase 5 detayı).

### 2.5 İndeks stratejisi (DATABASE_GUIDELINE §4)

Her class tablosunda: (1) `UNIQUE` merge-upsert/dedup hedefi (partition anahtarını içerir, §2.3), (2) **global** (partition-anahtarsız) `(engine_id, <entity_id>)` index — entity-lifecycle tabloları için nokta-arama, Postgres 11+ partitioned-index otomatik yayılımıyla her partition'da fiziksel karşılığı oluşur, (3) `process_instance_id` üzerinde partial index (`WHERE deleted_at IS NULL`) — çekirdek-4 desen 4 (instance→X geçmişi). `process_instance_history` ayrıca `business_key` (desen 2) ve `(process_definition_key, start_time)` (desen 3) indeksleri taşır.

### 2.6 Retention / erasure kolonları (her class tablosunda ortak)

`deleted_at` (soft-delete, ADR-0017), `anonymized_at` (ADR-0017 anonimleştirme adımı), `legal_hold` (ADR-0017/0018 — audit-kritik sınıflarda default TRUE, erasure'ı yapısal olarak engeller — `BUS_ERASURE_REQUEST_LEGAL_HOLD_BLOCKED`). Retention-expiry = **partition DROP/DETACH** (bulk-DELETE VACUUM yükü yok) — `retention_audit_log`'a kaydedilir (§2.7). Erasure = **partition İÇİNDE** alan-düzeyi UPDATE (soft-delete→anonymize) — `erasure_audit_log`'a kaydedilir.

**Docker-doğrulanmış (§5):** `variable_detail_history` üzerinde dated-partition oluşturma → satır ekleme → `DROP TABLE <partition>` → parent'ta 0 satır kaldığı bizzat koşuldu (retention mekanizmasının gerçek Postgres semantiği).

### 2.7 Kontrol-düzlemi + uyum tabloları (4)

| Tablo | Amaç | ADR |
|---|---|---|
| `class_cutover_state` | Sınıf-bazlı cutover durum makinesinin (`BUSINESS_LOGIC.md §2.1`) **kalıcı** hali — `ReconciliationJob`/`CutoverControlPlane` yeniden başlatmalarda hayatta kalır | 0015 |
| `retention_audit_log` | Her partition-drop/detach için zorunlu audit kaydı (`DATA_GOVERNANCE v4.0 §4.4`) — yazım başarısızlığı `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED` (CRITICAL) | 0018 |
| `erasure_audit_log` | Her erasure/anonymize işlemi + doğrulama durumu — `verification_status=FAILED` → `RES_ERASURE_VERIFICATION_FAILED` (CRITICAL) | 0017 |
| `erasure_scope_confirmation` | BA-Q6 kapsam-onayı akışı — aday instance/zaman-aralığı listesi + onay durumu | 0017 |

---

## 3. (c) Pseudonym kasası — ayrı Postgres (ARCH-Q2)

**Migration:** [`db/migrations/pseudonym-vault/001_pseudonym_map.sql`](db/migrations/pseudonym-vault/001_pseudonym_map.sql)

| Tablo | Amaç | Silme semantiği |
|---|---|---|
| `pseudonym_map` | Kimlik↔takma-ad haritası (`pseudonym_token` PK — BA-Q5 tx-içi saf keyed-hash); `real_user_id_encrypted` pgcrypto ile şifreli (disk-düzeyi AES-256'nın ÜSTÜNE savunma-derinliği) | **HARD DELETE** — `ADR-0016` "silme=harita-kaydı silme" tersinmezliği soft-delete ile sağlanamaz (şifreli değer + anahtara erişimi olan biri hâlâ geri çevirebilirdi) |
| `vault_access_audit` | Her WRITE/READ/DELETE/REIDENTIFY_ATTEMPT — yetkisiz denemeler de (`granted=false`) kaydedilir | `AUTH_PSEUDONYM_VAULT_ACCESS_DENIED` (CRITICAL) kanıt izi |

**Docker-doğrulanmış (§5):** `pgp_sym_encrypt`/`pgp_sym_decrypt` round-trip + `DELETE FROM pseudonym_map` sonrası satırın gerçekten yok olduğu bizzat koşuldu.

---

## 4. Pseudonymization — projeksiyon ↔ kasa alan haritası (BA-Q5, ADR-0016)

`operation_log_history.user_id_pseudonymized=true` olduğunda, `user_id` kolonu **takma-ad token'ını** taşır (`pseudonym_token`, gerçek değer DEĞİL); gerçek `real_user_id` yalnız kasadaki `pseudonym_map.real_user_id_encrypted`'dedir — iki veritabanı FİZİKSEL olarak izole (§0), aralarında FK YOK (kasıtlı — L4-bitişik izolasyon, DP-16). `chk_operation_log_history_pseudonym_consistency` CHECK kısıtı `user_id_pseudonymized=true` iken `pseudonym_token`'ın NULL olamayacağını uygular.

---

## 5. Migration-proof (görev talimatı şartı — bizzat koşuldu)

Docker v29.6.0 mevcut; üç migration hedefi **tek-kullanımlık `postgres:16` konteynerlerine** ayrı ayrı uygulandı (`docker run --rm` yerine adlandırılmış konteyner + `docker rm -f` temizliği — aynı tek-kullanımlık disiplin):

| Hedef | Konteyner | Migration dosyaları | Sonuç |
|---|---|---|---|
| Engine-outbox | `pg-engine-outbox-test` | `engine-outbox/001_compact_history_outbox.sql` | ✅ `CREATE EXTENSION`/`CREATE TABLE`×2/`CREATE INDEX`×3/`COMMENT`×3 — hatasız |
| Projeksiyon | `pg-projection-test` | `projection/001…003_*.sql` (3 dosya) | ✅ 20 mantıksal tablo (15 sınıf + `projection_large_payload` + 4 kontrol/uyum) + 15 default partition + tüm index/constraint — hatasız |
| Pseudonym kasası | `pg-vault-test` | `pseudonym-vault/001_pseudonym_map.sql` | ✅ `CREATE EXTENSION pgcrypto`/`CREATE TABLE`×2/`CREATE INDEX`×2/`COMMENT`×2 — hatasız |

**Fonksiyonel doğrulama (yalnız DDL değil, mekanizma):**
1. `activity_instance_history` üzerinde 3-adımlı partition-güvenli merge-upsert protokolü (§2.3) — INSERT (ilk event) → UPDATE (yeni `stream_sequence`, uygulanır, `duration_millis=4200` doğrulandı) → UPDATE (eski `stream_sequence`, **0 satır etkiledi** — stale-discard doğrulandı).
2. `variable_detail_history` üzerinde dated-partition (`variable_detail_history_2026_01`) oluştur → satır ekle → `DROP TABLE` (retention-expiry) → parent'ta **0 satır** kaldığı doğrulandı.
3. `pseudonym_map` üzerinde `pgp_sym_encrypt`/`pgp_sym_decrypt` round-trip (`user-42` şifrelendi, aynen çözüldü) + `DELETE` sonrası satırın **tersinmez** biçimde yok olduğu + `vault_access_audit`'e WRITE/DELETE kayıtlarının düştüğü doğrulandı.

Üç konteyner de test sonrası `docker rm -f` ile temizlendi (kalıcı state bırakılmadı).

---

## 6. İzlenebilirlik

`DATA_OWNERSHIP.yaml` capability eşlemesi: `history.compact-outbox`→(a), `history.projection`→(b), `history.pseudonym-map`→(c). ADR: 0010/0011/0012/0016/0017/0018. BR: BR-HDL-003, BR-REL-002/003, BR-PII-001…005. Tam erişim-matrisi (hangi bileşen hangi tabloya hangi op'la dokunur): `DB_ACCESS_MAP.md`.
