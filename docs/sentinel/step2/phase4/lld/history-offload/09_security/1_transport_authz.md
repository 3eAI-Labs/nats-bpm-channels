# 09.1 — Transport + Subject-Level Authz

**Kaynak:** ADR-0019 (basamak-1 ADR-0008 genişlemesi), DP-4/DP-13. Bu dosya basamak-1'in `NatsTransportSecurityGuard` (`docs/sentinel/phase4/lld/external-task-jetstream/09_security/1_transport_authz.md`) desenini history subject namespace'ine GENİŞLETİR — sınıfı TEKRAR YAZMAZ, yeni ACL satırları ekler.

---

## 1. Transport (devralınan, değişmez)

Production'da TLS + NKey/JWT zorunlu — basamak-1 `NatsTransportSecurityGuard` bootstrap-guard'ı AYNEN geçerli (mevcut `NatsProperties.Tls` + credentials alanları). Basamak-2 bu sınıfa DOKUNMAZ.

---

## 2. Subject-level permission — history namespace (yeni satırlar, ADR-0019 tablosu)

| Rol (NATS hesabı) | publish | subscribe | Uygulayan sınıf |
|---|---|---|---|
| Engine node (relay + post-commit publisher) | `history.>` | — | `HistoryOutboxRelay`, `HistoryPostCommitPublisher` |
| Projeksiyon consumer (motor-dışı) | — | `history.>` | `HistoryProjectionConsumer` |
| Reconciliation/replay okuyucu | — | `history.>` (Limits stream okuma) | `ReconciliationJob` |
| DLQ yönlendirme (consumer/relay) | `dlq.history.>` | — | `HistoryDlqConsumer` |
| History-DLQ inceleme (ops, yetkili) | — | `dlq.history.>` | `HistoryDlqInspectionConsumer` |

**Yapısal engel (kod-düzeyi karşılığı):** `HistoryProjectionConsumer` ve `HistoryQueryApi`'nin NATS bağlantı kimlik bilgileri (deploy-time, `08_config.md`'de referans) `dlq.history.>`'e ASLA subscribe İZNİ taşımaz — bu, `RES_HISTORY_DLQ_ACCESS_DENIED`'in **birincil** savunma katmanıdır (ikincil: `HistoryDlqInspectionConsumer`'ın kendi authz kontrolü, defense-in-depth, basamak-1 ADR-0008 deseni).

---

## 3. Stream retention (IR-7/DP-13 — provisioning parametreleri, kod değil)

`HistoryStreamProvisioner` (`03_classes/5_bench.md` §2) `ensureHistoryStreams(...)` çağrısında: `HISTORY` stream Limits-based, default 7g retention (`x-jetstream.retentionDays`, `asyncapi.yaml`); `DLQ_HISTORY` Limits-based, default 14g, kiracı-kısaltılabilir (config, `08_config.md`'ye eklenecek `history.dlq.retentionDays` — Phase 5'te asyncapi ile senkron tutulur).

**Bağımlılık:** NFR-S4/S7, DP-4/DP-13, ADR-0019.
