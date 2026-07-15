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

`JetStreamStreamManager.ensureStream(...)` (mevcut; Sentinel Phase 5.5 QA fix'ten sonra opsiyonel `Duration maxAge` argümanı da alır — bkz. §2.2) `jobs.<topic>`, `jobs.<topic>.reply`, event-channel subject'leri ve `dlq.>` için **WorkQueue/Limits** stream'lerini `autoCreateStream=true` config'i olan channel/topic'ler için oluşturur (mevcut deseninin devamı). `JetStreamKvManager.ensureBucket("a2-sweep-leader", Duration.ofSeconds(240), 3, connection)` A2 modülünün autoconfiguration'ında (`CamundaNatsAutoConfiguration`/`CadenzaFlowNatsAutoConfiguration`) bootstrap sırasında çağrılır.

**NATS_JETSTREAM.md §5 kural 4/5 uyarınca** (prod ortamda): stream/KV bucket oluşturma **PR ile** `deployment/nats/streams/*.yaml` ve `deployment/nats/kv/a2-sweep-leader.yaml` dosyalarına da yansıtılmalıdır (bu repo'nun runtime auto-create'i **dev/test/preflight** içindir; prod'da CI tarafından `nats stream add --config`/`nats kv add --config` ile önceden uygulanması **önerilir** — mevcut `JetStreamStreamManager` zaten bu ikili modeli destekler: yoksa oluştur, varsa dokunma).

### 2.1 [ZORUNLU] `jobs.<topic>` ve `jobs.<topic>.reply` AYRI stream'lerde provision edilir

**Kural:** her A2 topic için job-dispatch subject'i (`jobs.<topic>`) ve reply subject'i (`jobs.<topic>.reply`) **iki ayrı JetStream stream'inde** olmalıdır — aynı stream'de asla birleştirilmemelidir (`nats-bpm-bench`'in `BenchEnvironment.ensureStreams()` ve asyncapi'nin per-channel `x-jetstream` blokları zaten bu topolojiyi kullanır; bu bölüm bunu **belgelenmiş bir öneri** olmaktan çıkarıp **zorunlu deployment şartına** yükseltir).

**Gerekçe (ampirik kanıt):** job ve reply subject'leri kasıtlı olarak AYNI `Nats-Msg-Id` değerini taşır (`= externalTaskId`, IR-3/asyncapi `ReplyHeaders`). JetStream `duplicate_window` dedup'ı **stream-scoped'dur, subject-scoped DEĞİLDİR**. Bir operatör `jobs.*` "tek rezerve namespace" olduğu için her iki subject'i TEK bir stream'de birleştirirse (makul görünen ama YANLIŞ bir sadeleştirme), JetStream reply'ı kendi job'ının bir kopyası sanıp **sessizce düşürür** — worker'ın reply'ı hiçbir zaman `A2CompletionBridge`'e ulaşmaz; external task, umbrella-lock sweep'i (L saniyeye kadar) onu yeniden yayınlayana dek asılı kalır ve gerçek kusur "yavaş worker" gibi görünür.

Bu, `nats-core`'un `JobReplySameStreamDedupRegressionTest`'i (Testcontainers, gerçek NATS broker) ile kanıtlanmıştır:
- Test 1 (`unsafeTopology_...`): tek stream'de job+reply → yalnız job hayatta kalır (reply sessizce düşer).
- Test 2 (`safeTopology_...`): ayrı stream'ler → her iki mesaj da hayatta kalır.

**Deployment checklist:** `deployment/nats/streams/*.yaml` dosyalarında her topic için EN AZ iki ayrı `Stream` tanımı olmalı (job-stream + reply-stream, veya topic-başına ayrı reply-stream'ler tek bir ortak job-stream'e karşı — asyncapi'nin channel-bazlı `x-jetstream` blokları zaten bunu ima eder); review checklist'ine "job/reply AYNI stream'de mi?" sorusu eklenmelidir.

### 2.2 `ensureStream` — opsiyonel `maxAge` (Sentinel Phase 5.5 QA fix, item 6, Levent kararı 2026-07-15)

`JetStreamStreamManager.ensureStream(streamName, subject, connection, Duration maxAge)` — yeni 4-argümanlı overload; 3-argümanlı mevcut overload artık `dlq.` önekli subject'lerde **default 14 gün** `maxAge` ile stream oluşturur (DATA_CLASSIFICATION.md §5 Q3 kararının somutlaşması — DLQ payload'ı RESTRICTED/PII, 14 gün en uzun maruziyet penceresidir), `dlq.` öneki olmayan subject'lerde davranış **değişmedi** (sınırsız retention, mevcut davranış). `maxAge` yalnız stream **oluşturulurken** uygulanır — var olan bir stream asla burada yeniden konfigüre edilmez.

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
