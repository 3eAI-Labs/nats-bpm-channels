# 09.1 — NATS Transport Güvenliği + Subject-Level Authz (ADR-0008)

**Kaynak:** ADR-0008 (Kabul, ARCH-Q3), NFR-S3/S4, DP-4/DP-5.

---

## 1. Transport guard (bootstrap-time, production zorunluluğu)

Mevcut `NatsProperties` (`nats-core/.../NatsProperties.java:14-15,97-135`) zaten TLS (`Tls.enabled/certFile/keyFile/caFile`) ve kimlik (`credentialsFile`, `nkeyFile`, `token`, `username/password`) alanlarını taşıyor — **yeni transport kodu minimal** (ADR-0008 sonuç notu). Eksik olan: **production'da bunları zorunlu kılan bir guard**.

```java
package com.threeai.nats.core.config;

public class NatsTransportSecurityGuard implements InitializingBean {

    private final NatsProperties properties;
    private final Environment springEnvironment;   // "production" profile tespiti

    @Override
    public void afterPropertiesSet() {
        if (!isProductionProfile(springEnvironment)) {
            return;   // dev/test — plaintext izinli (mevcut Testcontainers/entegrasyon testleri etkilenmez)
        }
        if (!properties.getTls().isEnabled()) {
            throw new NatsTransportSecurityException(
                    "Production profile requires spring.nats.tls.enabled=true (NFR-S3/DP-4)");
        }
        boolean hasIdentity = properties.getCredentialsFile() != null || properties.getNkeyFile() != null;
        if (!hasIdentity) {
            throw new NatsTransportSecurityException(
                    "Production profile requires spring.nats.credentials-file or spring.nats.nkey-file " +
                    "(NKey/JWT identity mandatory — plain/anonymous connection rejected, ADR-0008)");
        }
    }
}
```

**Bağımlılık:** NFR-S3, DP-4, ADR-0008 §1. Bu guard `nats-core`'a eklenir, her üç engine modülü + Flowable modülü tarafından paylaşılır (`CamundaNatsAutoConfiguration`/`CadenzaFlowNatsAutoConfiguration`/`FlowableNatsAutoConfiguration` her biri bu bean'i kaydeder — mevcut `@AutoConfiguration` desenine ek `@Bean`).

---

## 2. Subject-level permission şeması (broker konfigürasyonu — bu repo DIŞI uygulanır)

ADR-0008 §2 tablosu, **NATS server-side** hesap/kullanıcı JWT permission şemasıdır (broker config, `nats-server`/NSC ile yönetilir) — bu repo'nun Java kodu bu ACL'yi **uygulamaz**, yalnız **varsayar ve dokümante eder** (ADR-0008 sonuç notu: "bu repo config guard'ı doğrular, ACL'yi broker uygular"). Bu LLD'nin katkısı:

### 2.1 Rol → permission tablosu (ADR-0008'in birebir aktarımı, normatif referans için burada)

| Rol (NATS hesabı) | publish | subscribe |
|---|---|---|
| **Engine node** (publisher + inbound bridge) | `jobs.>`, `dlq.>` | `jobs.*.reply`, `dlq.>` |
| **Worker** (motor-dışı) | `jobs.<kendi-topic>.reply` | `jobs.<kendi-topic>`, `<kendi-event-channel>` |
| **DLQ-bridge** | (engine ile aynı) | `dlq.jobs.>` (incident) / diğer `dlq.>` (failure-event) |

### 2.2 Deployment artefaktı (Phase 5/DevOps kapsamı, bu LLD yalnız işaret eder)

`NATS_JETSTREAM.md` §5 kural 3-5 gereği ("Stream/KV bucket creation requires PR") ile tutarlı olarak, hesap/permission JWT şablonları `deployment/nats/accounts/<role>.json` altında **PR ile** eklenir (bu repo'nun `docs/sentinel/phase4` kapsamında DEĞİL — `99_deployment.md` §2'de işaret edilir, gerçek JWT üretimi DevOps/Phase 5).

### 2.3 İkincil savunma hattı (uygulama-düzeyi, bu repo İÇİNDE)

Subject-ACL aşılsa bile (yanlış yapılandırma, sızıntı vb.), `A2CompletionBridge` (§`03_classes/2_camunda_a2.md` §4) yalnız **var olan, SENTINEL-kilitli, `externalTaskId`-bilinen** task'ı ilerletir:
- Bilinmeyen/sahte `externalTaskId` → `NotFoundException` → yut+ack (`RES_EXTERNAL_TASK_NOT_FOUND`).
- Yanlış workerId (SENTINEL değil) → `BadUserRequestException` → `SYS_SENTINEL_WORKER_CONFLICT` (CRITICAL+page).

Bu, ADR-0008'in "katmanlı savunma" ilkesinin bu repodaki somut karşılığıdır — **yeni kod GEREKMEZ**, `A2CompletionBridge`'in zaten tasarlanmış hata-yakalama dalları (§`03_classes/2_camunda_a2.md` §4) bu ikinci hattı sağlar.

**Bağımlılık:** NFR-S3/S4, DP-4/DP-5, US-A4/US-A7 (savunma), ADR-0008.
