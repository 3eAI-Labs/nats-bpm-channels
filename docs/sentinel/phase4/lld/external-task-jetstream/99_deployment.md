# 99 — Deployment & Konfigürasyon Notları

---

## 1. `application.yml` örnek konfigürasyon (A2, Camunda tarafı — cadenzaflow ayna `spring.nats.cadenzaflow.a2`)

```yaml
spring:
  nats:
    url: nats://nats-cluster:4222
    tls:
      enabled: true                       # production zorunlu (09_security/1_transport_authz.md §1)
    credentials-file: /var/run/secrets/nats/engine.creds   # NKey/JWT — NatsProperties mevcut alan
    camunda:
      a2:
        sentinel-worker-id: a2-jetstream-bridge
        topics: [ "order-fulfillment", "payment-capture" ]   # yalnız LİTERAL camunda:topic değerleri
        allow-unsafe-lock-duration: false
        defaults:
          ack-wait-seconds: 30
          max-deliver: 4
          sweep-period-seconds: 120
          epsilon-seconds: 60
        topic-overrides:
          payment-capture:
            ack-wait-seconds: 90           # uzun-işli topic — L otomatik yeniden türetilir
```

Bootstrap sırası: `NatsTransportSecurityGuard` (transport) → `UmbrellaLockValidator` (config) → `NamespaceValidator` (Flowable channel kaydı sırasında, zaten Flowable'ın kendi `ChannelModelProcessor` akışı içinde) → `SweepLeaderLease`/`JetStreamKvManager.ensureBucket` (`a2-sweep-leader`) → `A2ProcessEnginePlugin` (preParseListeners kaydı) → engine start.

---

## 2. Stream/KV provisioning

`JetStreamStreamManager.ensureStream(...)` (mevcut) `jobs.<topic>`, `jobs.<topic>.reply`, event-channel subject'leri ve `dlq.>` için **WorkQueue/Limits** stream'lerini `autoCreateStream=true` config'i olan channel/topic'ler için oluşturur (mevcut deseninin devamı). `JetStreamKvManager.ensureBucket("a2-sweep-leader", Duration.ofSeconds(240), 3, connection)` A2 modülünün autoconfiguration'ında (`CamundaNatsAutoConfiguration`/`CadenzaFlowNatsAutoConfiguration`) bootstrap sırasında çağrılır.

**NATS_JETSTREAM.md §5 kural 4/5 uyarınca** (prod ortamda): stream/KV bucket oluşturma **PR ile** `deployment/nats/streams/*.yaml` ve `deployment/nats/kv/a2-sweep-leader.yaml` dosyalarına da yansıtılmalıdır (bu repo'nun runtime auto-create'i **dev/test/preflight** içindir; prod'da CI tarafından `nats stream add --config`/`nats kv add --config` ile önceden uygulanması **önerilir** — mevcut `JetStreamStreamManager` zaten bu ikili modeli destekler: yoksa oluştur, varsa dokunma).

---

## 3. Boundary-timer modelleme rehberi (BR-FLW-004, opt-in)

Süreç geliştiriciler (P1) için runbook notu: yalnız **gerçek sözleşmesel/regülasyon SLA'sı olan** aktivitelere boundary-timer eklenir (ör. "24 saat içinde yanıt gerekir" gibi bir iş kuralı varsa). Varsayılan davranış (SLA'sız modeller) yalnız DLQ→failure-event'e (`FailureEventBridge`) güvenir — ek timer-job maliyeti **ödenmez**.

---

## 4. Upgrade runbook (ADR-0005 §3 — her iki A2 modülü için)

`03_classes/3_cadenzaflow_a2_mirror.md` §3'e bkz. — bu bölüm yalnız pointer'dır (tekrar yok).

---

## 5. Silinen dosyalar checklist (Phase 5 PR'ı için)

- [ ] `{camunda,cadenzaflow}-nats-channel/.../outbound/{Nats,JetStream}PublishDelegate.java` (2×2)
- [ ] `{camunda,cadenzaflow}-nats-channel/.../outbound/NatsRequestReplyDelegate.java` (2×)
- [ ] `flowable-nats-channel/.../requestreply/NatsRequestReplyDelegate.java`
- [ ] İlgili `*DelegateTest.java` (6 dosya, `02_package_structure.md` §4)
- [ ] `{Camunda,CadenzaFlow}NatsAutoConfiguration`'daki delegate `@Bean` tanımları (satır 67-89 benzeri)
- [ ] `FlowableNatsAutoConfiguration.natsRequestReply(...)` bean tanımı (`:64-70`)

## 6. pom.xml değişiklikleri

`02_package_structure.md` §1 (root `<modules>` + yeni `nats-bpm-bench`).
