# ADR-0008 — NATS transport güvenliği ve subject-level authorization

- **Durum:** Kabul edildi (2026-07-14, Levent onayı — ARCH-Q3; phase-review MAJOR-2 düzeltmesi)
- **İzlenebilirlik:** NFR-S3/NFR-S4 → DP-4/DP-5 → US-A4/US-A7 (sahte-reply savunması) → `SYS_SENTINEL_WORKER_CONFLICT` (ikinci savunma hattı) → DATA_CLASSIFICATION §3/§7
- **Kapanış:** Bu ADR **NFR-S3'ü kapatır** (SRS "auth/authz detayı phase3'te netleşecek" demişti).

## Bağlam

Basamak-1 outbound'u motor-dışına açar: post-commit publisher `jobs.<topic>`'a yayınlar, worker'lar `jobs.<topic>.reply`'a yazar, completion-bridge reply'ı tüketip `complete()` çağırır. **Yeni saldırı yüzeyi:** kimlik doğrulanmamış/yetkisiz bir aktör `jobs.*.reply`'a **sahte reply** yazabilirse `complete`/`handleBpmnError`/`handleFailure` tetiklenebilir → token yanlış ilerler, iş sonucu enjekte edilir. Ayrıca Business-Key + process değişkenleri (RESTRICTED/PII) tel üzerinde akar (DP-4) ve motor-dışı worker'a (güven sınırı dışına) geçer (DP-5/NFR-S4).

phase-review MAJOR-2 (2026-07-14): NFR-S3'ün "phase3'te netleşecek" bırakılması bir açıktı; ARCH-Q3 ile **subject-ACL tabanı basamak-1 kapsamına DAHİL** edildi.

## Karar

### 1. Transport (DP-4/NFR-S3)
- **Production'da zorunlu:** TLS (mevcut `NatsProperties.Tls`, `NatsProperties.java:97-135`) + kimlik = **NKey veya JWT** (mevcut `credentialsFile`/`nkeyFile` alanları `:14-15`). Kimliksiz/plain bağlantı production'da reddedilir (bootstrap-time guard — Config/Bootstrap kaynağı, EXCEPTION_CODES §6 uzantısı).

### 2. Subject-level permission tabanı (kapsam-içi)
Hesap/kullanıcı düzeyinde subject izin şeması (NATS authorization `permissions.publish` / `permissions.subscribe`):

| Rol (NATS hesabı) | publish | subscribe |
|---|---|---|
| **Engine node** (publisher + inbound bridge) | `jobs.>` , `dlq.>` | `jobs.*.reply` , `dlq.>` |
| **Worker** (motor-dışı) | `jobs.<kendi-topic>.reply` | `jobs.<kendi-topic>` , `<kendi-event-channel>` |
| **DLQ-bridge** | (engine ile aynı) | `dlq.jobs.>` (incident) / diğer `dlq.>` (failure-event) |

- **Sahte-reply yapısal engeli:** worker hesabı yalnız **kendi topic'inin** job/reply subject'lerine pub/sub yetkilidir → başka topic'in reply'ına yazamaz; `jobs.*.reply`'ı yalnız engine-inbound tüketir. Bu, `jobs.*` namespace rezervasyonunun (BR-SUB-004) authz karşılığıdır.
- **İkinci savunma hattı:** authz aşılsa bile `complete` yalnız **var olan, SENTINEL-kilitli, `externalTaskId`-bilinen** task'ı ilerletir — körlemesine enjeksiyon `NotFoundException`'a düşer (`RES_EXTERNAL_TASK_NOT_FOUND`, yut+ack); yanlış workerId ile `complete` `SYS_SENTINEL_WORKER_CONFLICT` (CRITICAL+page) tetikler. Yani **savunma katmanlı**: (i) subject-ACL (birincil, yapısal), (ii) SENTINEL-kilit + task-varlık kontrolü (ikincil).

### 3. Ertelenen (deploy kararı — kapsam-dışı ama kayıtlı)
- **Kiracı-başına granülerlik / hesap şeması** (multi-tenant subject izolasyonu, per-tenant NKey hiyerarşisi) **deploy kararına** ertelendi — basamak-1 tabanı *mekanizmayı* (subject-ACL) sağlar, kiracı *politikası* deploy-time'da uygulanır. DLQ erişim kontrolü + kiracı-bazlı retention (Q3/DP-3) bununla hizalanır.

## Sonuçlar

**Olumlu:** Sahte-reply saldırı yüzeyi yapısal olarak kapanır (subject-ACL) + derinlemesine savunma (SENTINEL-kilit); NFR-S3 kapanır; DP-4/DP-5 tel-ve-worker sınırı için somut mekanizma. Mevcut `NatsProperties` alanları (TLS/NKey/JWT) yeterli — yeni transport kodu minimal.

**Olumsuz / kabul edilen:** Subject-ACL şeması deploy-time konfigürasyon yükü getirir (hesap-başına permission); kiracı-başına granülerlik ertelendiğinden çok-kiracılı üretimde ek deploy tasarımı gerekir. NATS server-side authorization broker konfigürasyonudur (bu repo config guard'ı doğrular, ACL'yi broker uygular).

## Reddedilenler
- **Yalnız TLS (authz'siz):** sahte-reply'ı engellemez — herhangi bir bağlı istemci `jobs.*.reply`'a yazabilirdi. **Reddedildi** (MAJOR-2).
- **Yalnız uygulama-düzeyi savunma (SENTINEL-kilit tek başına):** enjekte edilen reply var-olan bir `externalTaskId`'yi tahmin/ele geçirirse yeterli değil; subject-ACL birincil hat olmalı. **Reddedildi** (katmanlı savunma seçildi).
