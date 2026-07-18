# ADR-0019 — History stream retention + subject-level authz (basamak-1 ADR-0008 genişlemesi)

- **Durum:** Kabul edildi (2026-07-17, Phase 3 — basamak-2)
- **İzlenebilirlik:** US-B4/US-B5 → FR-B4/FR-B5 → BR-REL-004/005 → `RES_HISTORY_DLQ_ACCESS_DENIED` → NFR-S4/S7 / DP-4/DP-13
- **Kesişim:** basamak-1 **ADR-0008** (NATS transport + subject-level authz) history subject namespace'ine genişler. **NFR-S7'yi kapatır** (SRS "detay phase3").

## Bağlam

Basamak-2 yeni tel yüzeyleri açar: `history.>` (payload + header = RESTRICTED/PII, DP-13) ve `dlq.history.>` (byte-ayna kopya, en uzun at-rest maruziyeti). NFR-S4: history stream + DLQ at-rest PII taşır — TLS + erişim kontrolü + retention; ayrı-stream [CQ-6]. NFR-S7: NATS transport TLS + NKey/JWT + history subject'lerine subject-level authz (SRS "detay phase3"). Basamak-1 ADR-0008 transport (TLS+NKey/JWT) ve subject-ACL tabanını kurmuştu; history subject namespace'i için genişletilmeli.

## Karar

### Transport (NFR-S7 — ADR-0008 devralınır)
- Production'da TLS + NKey/JWT zorunlu (mevcut `NatsProperties.Tls` + credentials/nkey alanları, ADR-0008). Kimliksiz/plain bağlantı reddedilir (bootstrap-time guard).

### Subject-level permission (history namespace)
Basamak-1 ADR-0008 şemasına history subject'leri eklenir:

| Rol (NATS hesabı) | publish | subscribe |
|---|---|---|
| **Engine node** (relay + post-commit publisher) | `history.>` | — |
| **Projeksiyon consumer** (motor-dışı) | — | `history.>` |
| **Reconciliation/replay okuyucu** | — | `history.>` (Limits stream okuma) |
| **DLQ yönlendirme** (consumer/relay) | `dlq.history.>` | — |
| **History-DLQ inceleme** (ops, yetkili) | — | `dlq.history.>` |

- **PII erişim yapısal engeli:** projeksiyon consumer yalnız `history.>`'e subscribe; DLQ payload'ına (PII yüzeyi) erişim yalnız yetkili ops hesabına — yetkisiz okuma `RES_HISTORY_DLQ_ACCESS_DENIED` (DP-13). Kiracı-başına granülerlik (per-tenant NKey hiyerarşisi) **deploy kararına** ertelenir (ADR-0008 deseni; mekanizma sağlanır, politika deploy-time).

### Stream retention (IR-7 / DP-13)
- **History stream (`HISTORY`, Limits-based, ADR-0013):** retention penceresi konfigürable (asyncapi `x-jetstream.retentionDays` default 7g) — projeksiyon consumer + reconciliation okuduktan sonra tel-kopyası minimize edilir; projeksiyon store kalıcı kayıttır (retention orada, ADR-0018), stream yalnız transit+kısa-buffer.
- **History DLQ stream (`DLQ_HISTORY`, Limits-based):** default 14g (basamak-1); **PII işleyen kiracı kısaltır/erişimi kısıtlar** (DP-13). Ayrı-stream (CQ-6) — iş-dağıtımı `dlq.>` stream'inden bağımsız. DLQ-bridge tüketimi CB korumalı (ADR-0004).
- **Provisioning:** history + `dlq.history.>` stream'leri `ensureStreams()`/prod provisioning kapsamında (BR-DBT-002); eksikse `VAL_HISTORY_STREAM_PROVISIONING_MISSING`.

## Sonuçlar

**Olumlu:** History PII tel-yüzeyi subject-ACL ile yapısal izole; NFR-S7 kapanır (basamak-1'in "phase3'te" bıraktığı gibi bir açık kalmaz). Ayrı-stream + kısa history-stream retention tel-maruziyetini minimize eder (kalıcı kayıt projeksiyon store'da). DLQ PII retention kiracı-kontrollü.

**Olumsuz / kabul edilen:** Subject-ACL şeması deploy-time konfigürasyon yükü (hesap-başına permission); kiracı-başına granülerlik ertelendiğinden çok-kiracılı üretimde ek deploy tasarımı (ADR-0008 devralınan bedel). Kısa history-stream retention, projeksiyon consumer'ın stream retention penceresi içinde tüketmesini gerektirir (projeksiyon-lag > retention → mesaj stream'den düşebilir; NFR-P3 SLI bunu izler, audit-kritik yol outbox'ta güvende — ADR-0010).

## Reddedilenler
- **Yalnız TLS (subject-authz'siz):** history PII'ye yetkisiz subscribe'ı engellemez. **Reddedildi** (ADR-0008 deseni).
- **History'yi iş-dağıtımı stream'leriyle paylaşma:** CQ-6 ayrı-stream ihlali; PII izolasyonu bozulur. **Reddedildi**.
- **History stream'i uzun-retention kalıcı kayıt yapma:** projeksiyon store zaten kalıcı kayıt; uzun stream-retention PII tel-maruziyetini gereksiz uzatır. **Reddedildi** (kısa transit-buffer + projeksiyon kalıcılık).
