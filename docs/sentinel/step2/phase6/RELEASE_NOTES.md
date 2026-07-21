# Release Notes — Basamak-2: History Offload

**Sentinel fazı:** Phase 6 — Reviewer & DevOps (READY, insan onayına hazır — bkz. `PHASE6_REVIEW.md`).
**Branch:** `feature/step2-history-offload` (main'e göre 30 commit — 12 Phase 5 implementasyon +
6 Phase 5.5 QA + 12 phase 1-5.5 doküman/karar-kaydı commit'i).
**Tarih:** 2026-07-21.
**Kök `pom.xml` sürümü:** `0.2.0` (mevcut) → **`0.3.0` ÖNERİLİYOR** (go/no-go sonrası uygulanacak,
bkz. §1).

---

## 1. Sürüm önerisi — `0.3.0` (ÖNERİ, Levent onayı bekliyor)

Gerekçe:
- SemVer §4 pre-1.0 kuralı gereği 0.x serisinde teknik olarak her değişiklik minor/patch'te
  yapılabilir. Basamak-1 emsaliyle tutarlılık için (basamak-1 → `0.2.0`, "büyük yeni özellik
  seti" gerekçesiyle minor artış): basamak-2 de **büyük bir yeni özellik seti**dir (yeni modül
  `nats-history-projection`, 11 yeni ADR, KVKK erasure/retention/pseudonymization) ve **breaking
  change içermez** (aşağıya bkz.) — disiplin gereği **minor sürüm artışı** (`0.2.0` → `0.3.0`)
  öneriliyor.
  - **1.0.0 önerilmiyor:** proje kendi yol haritasında ("kademeli strangler omurga", basamak 1-6)
    henüz basamak-2'de; 1.0.0 basamak-6'ya (native state-core) kadar erken olur.
- **BREAKING DEĞİL:** Basamak-1'in JavaDelegate kaldırımının aksine, basamak-2 **tamamen
  additive**'dir — yeni bir modül (`nats-history-projection`) ve mevcut motor adaptörlerine
  (camunda/cadenzaflow) yeni `history` paketleri ekler; `flowable-nats-channel`'a hiç dokunmaz
  (D-G kararı, Flowable basamak-2b'ye ertelendi); mevcut A2 external-task pipeline'ı (basamak-1)
  **davranışsal olarak değişmedi** (regresyon-kontrolü: `PHASE6_REVIEW.md §3`).
- **Bu sürüm bump işlemi (`versions:set` + commit) henüz UYGULANMADI** — çalışma-ağacında
  `pom.xml`'in tek satırı (`<version>`) `0.3.0`'a değiştirildi ama **commit edilmedi**; go/no-go
  ve merge-stratejisi kararı Levent'e bırakıldı.

---

## 2. Öne çıkanlar

- **ACT_HI history offload (EPIC-A/B)** — Camunda 7 ve CadenzaFlow için, motor `ACT_HI_*`
  tablolarını byte-ayna bir `CompositeHistoryEventHandler` plug-in'iyle (ADR-0009, fork DEĞİŞMEZ)
  yakalayıp **hibrit yayın topolojisi** (ADR-0010) ile dışarı taşıyan bir hat: audit-kritik sınıflar
  (OP_LOG/INCIDENT/EXT_TASK_LOG) için tx-içi **kompakt outbox** + lider-seçimli, TTL-lease'li
  **relay** (custody-transfer semantiği, at-least-once + idempotent merge-upsert ile RPO=0); bulk
  sınıflar için sıfır-DB **post-commit publisher** (at-most-once, D-A bilinçli kayıp kabulü).
- **Ayrı Postgres projeksiyon** (EPIC-B, ADR-0011) — engine DB'den tamamen izole, denormalize
  sorgu-odaklı bir query-store; `merge-upsert` çakışma çözümü NATS JetStream `stream_sequence`
  monotonik versiyon ile (ADR-0012, 3-adım protokol: INSERT → conditional-UPDATE → stale-guard).
- **Çekirdek-4 sorgu-API** (EPIC-C, ADR-0014) — read-only REST/JSON, process-instance/activity/
  task/variable history üzerinde, pluggable authz SPI'siyle; basamak-1'in "Cockpit körleşmesi"
  telafisi.
- **Kademeli sınıf-bazlı cutover + reconciliation** (EPIC-D, ADR-0015) — iki-kapılı kontrol düzlemi
  (`CutoverControlPlane`), NATS KV bootstrap-read (rolling-restart uyumlu), `CutoverRollback`
  (yalnız DUAL_RUN'a döner — kalıcı-tek-yönlü, NFR-R5), `ReconciliationJob`.
- **KVKK/GDPR erasure/retention/pseudonymization kasası (EPIC-G)** — sınıf-bazlı retention
  enforcement (bulk 90g default / audit-kritik yasal-saklama, kiracı override, ADR-0018,
  atomiklik fault-injection testiyle kanıtlı); bulk PII erasure pipeline (ADR-0017, allowlist-
  revalidate direct-SQL, tam-yüzey CQ-3 genişletmesi); audit-kritik **pseudonymization kasası**
  (ADR-0016, ayrı Postgres, L4-bitişik izolasyon, tersinmez takma-ad, silme=harita-kaydı).
- **History wire-contract** (ADR-0013) — basamak-1 ADR-0006 deseninin history izdüşümü;
  `X-Cadenzaflow-History-Event-Time` zorunlu header (motor event-zamanı tele taşınır, dedup/
  partition-anchor için audit-kritik).
- **History stream retention + subject-level authz** (ADR-0019, basamak-1 ADR-0008 genişlemesi).

---

## 3. Breaking change — YOK (additive; bazı basamak-1 sınıfları uyumlu-şekilde genişletildi)

Basamak-2'de **breaking change yoktur** — hiçbir public sınıf/metot/imza kaldırılmadı veya uyumsuz
değiştirilmedi (SemVer minor; imza-düzeyinde doğrulandı, `PHASE6.5_REVIEW.md`). Yine de "hiçbir
mevcut sınıf değişmedi" demek yanlış olur:
- Yeni bağımsız modül: `nats-history-projection` (opt-in — tüketici kendi `pom.xml`'ine eklemedikçe
  reaktöre girmez; yeni zorunlu dependency yok).
- Mevcut `camunda-nats-channel`/`cadenzaflow-nats-channel` modüllerine yeni `history` paketleri
  eklendi. A2 external-task pipeline (basamak-1) davranışı **değişmedi** (regresyon-doğrulandı).
- Birkaç basamak-1 `nats-core` sınıfı **uyumlu-şekilde (additive) genişletildi**: `DlqReason`
  (yeni history/vault değerleri), `JetStreamStreamManager` (HISTORY/DLQ_HISTORY stream
  provisioning), `NatsChannelMetrics` (history SLI'ları), `SweepLeaderLease` (`heldRevision`-reset
  doğruluk düzeltmesi — Bölüm "Düzeltmeler"). Her iki motor auto-config'i history/kasa opt-in
  wiring kazandı (kiracı history bean'lerini vermedikçe pasif).
- `flowable-nats-channel`'a hiç dokunulmadı (D-G — Flowable basamak-2b'ye ertelendi, kilitli karar).

**Geriye dönük uyumluluk** basamak-1 public API yüzeyi için **tam korunur**; genişletmeler yalnız
ekleme (yeni değer/metot/opt-in bean), kaldırma/imza-değişikliği yok.

---

## 4. Deployment / migrasyon notu

Bu repo **headless kütüphane**dir (servis değil) — "deployment" = sürüm yayını (Maven Central,
bkz. `RELEASING.md`) + tag + `main`'e `--no-ff` merge; downstream tüketiciler (kiracı uygulamaları)
kendi deployment döngülerinde yeni sürümü pull eder.

**Kiracı tarafında gerekli olan** (yeni yetenek opt-in edilirse):
1. **İki ayrı Postgres örneği/şeması sağlanmalı** (kiracı-owned altyapı): projeksiyon store
   (`history.projection.datasource.*`) ve pseudonymization kasası (`history.vault.datasource.*`)
   — `NatsHistoryProjectionAutoConfiguration` bunları **ayrı** `HikariDataSource` bean'i olarak
   `@ConditionalOnProperty` ile koşullu kurar; ikisi de yapılandırılmazsa modül sessizce devre dışı
   kalır (opt-in, mevcut basamak-1 kurulumları hiç etkilenmez).
2. **`nats-history-projection` modülünün 3 migration'ı** (`V1__entity_lifecycle_tables.sql`,
   `V2__append_log_tables.sql`, `V3__control_plane_and_compliance.sql`) + motor-tarafı `V1`
   (`compact_history_outbox`) ve kasa `V1` (`pseudonym_map`) migration'ları `SqlMigrationRunner`
   ile classpath'ten idempotent uygulanır (5 migration, `postgres:16` üzerinde bağımsız tekrarla
   doğrulandı — `PHASE4_REVIEW.md` "Migration-Proof").
3. **NATS JetStream KV** — `history-relay-leader` bucket'ı (relay lider-seçimi, basamak-1
   `a2-sweep-leader` deseninin AYNISI, TTL=`2×relayCyclePeriodSeconds`) ve cutover-durum bucket'ı
   bootstrap'ta idempotent oluşturulur; ek deploy adımı gerekmez.
4. **History JetStream stream + subject-level authz** (ADR-0019, basamak-1 ADR-0008 genişlemesi) —
   broker-config, bu repo dışı; production'da subject-ACL önerilir (basamak-1 kalıbının aynısı).
5. **Relay-failover garantisi:** **RPO=0** (gerçek 3-replika Testcontainers KV-lease failover
   ölçümüyle kanıtlı — 5/5 audit-kritik satır failover sonrası tam drenaj); **RTO≤60s
   yapısal-alt-sınır** (TTL-expiry temelli devir mekanizması NEDENİYLE standby, son yenilemeden
   itibaren TAM TTL süresi geçmeden devralamaz — üretimde gerçek standby'ların kendi poll
   döngüsüyle 60-90s aralığına çıkabileceği runbook'a not düşülmeli, bkz. `TEST_REPORT.md §3`
   QA-FINDING). Bu SLA sayısal bir NFR taahhüdü değildir — ölçülen yapısal davranıştır.
6. **KVKK retention/erasure/pseudonymization politikası kiracı kararıdır** — bu repo
   **mekanizmayı** sağlar (US-G1/G2/G3), **politikayı** (hangi sınıf hangi retention/erasure/
   pseudonymization moduna girer) kiracı `TENANT_PII_CHECKLIST_TEMPLATE.md` ile verir. Audit-kritik
   sınıfların yasal-saklama istisnasının hukuki dayanağı **DPO doğrulamasına işaretlidir**
   (`DATA_CLASSIFICATION.md §6`) — bu repo bir hukuki görüş sunmaz.

---

## 5. Bilinen sınırlar (kabul edilmiş, release'i bloklamaz)

- **`PseudonymVaultIsolationTest` adanmış sınıfı yazılmadı** (TEST_SPEC f) — güvenlik ÖZELLİĞİNİN
  kendisi (fiziksel izolasyon, ayrı DataSource) bağımsız doğrulandı (`PHASE6_REVIEW.md §6.1`,
  `SECURITY_SCAN.md §4`); yalnız TEK-YERDE-toplanmış regresyon-testi eksik. Backlog.
- **SCA (OWASP dependency-check) tamamlanamadı** (NVD API-anahtarı yok, ortam kısıtı) — SAST temiz
  + manuel çapraz-kontrol temiz olduğundan kalıntı bağımlılık-CVE riski **Levent tarafından yazılı
  kabul edildi** (2026-07-21). CI'a `NVD_API_KEY` eklenmesi backlog.
- **Testcontainers sürüm-split** (`testcontainers-bom:2.0.4` kök-pin ile `junit-jupiter`/
  `postgresql` alt-modüllerinin `spring-boot-dependencies`'ten sızan 1.19.8'i) — bugün çalışıyor,
  orta-vadeli bakım-kırılganlığı riski; backlog (kök pom'a explicit pin önerilir).
- **SpotBugs CI pipeline'ına bağlı değil** (pom'da plugin yok, CLI'dan elle koşuldu bu turda) —
  basamak-1'den miras CI-altyapı borcu, DevOps takibi.
- **`task_description` kolonu extractor'da doldurulmuyor** (TASKINST) — sorgu-tamlığı boşluğu,
  PII/erasure riski YOK (kolon her zaman NULL, sızacak veri yok). LOW, backlog.
- **`RetentionEnforcementJob` CODER-NOTE'u mekanizmayı yanlış tanımlıyor** ("aynı transaction" der,
  gerçekte ayrı-connection + compensating-rollback) — davranış DOĞRU, yalnız yorum yanıltıcı.
  Javadoc-fix, backlog.
- **Satır kapsama** — `nats-history-projection` (%83.2) eşiği aşıyor; `camunda`/`cadenzaflow`/
  `nats-core` basamak-1'den miras eşik-altı (~%74-79%), **kabul edilmiş borç** (yeni bulgu değil).
- **`SweepLeaderLease`/`HistoryOutboxRelay` lider-devri gerçek çok-node canlı-küme testi yapılmadı**
  — kod-okumasıyla + izole Testcontainers senaryosuyla doğrulandı, gerçek K8s rolling-restart
  ortamında henüz gözlemlenmedi (basamak-1'in aynı sınırlaması, devralınan borç).

---

## 6. RELEASE-DECISIONS (bu tura kadar alınan kararlar — kayıt)

Aşağıdaki üç madde phase 5.5 incelemesinde açık soru olarak açılmıştı; Levent'in 2026-07-21
kararıyla kapatıldı (`PHASE5.5_REVIEW.md` "Bulgu Kapanış Kaydı"):

1. **F-001 (retention audit-log CRITICAL testsiz) — KARAR: test eklendi, atomiklik kanıtlandı.**
   `RetentionEnforcementJobTest`'e gerçek-PG fault-injection testi eklendi; audit-write fail →
   DROP rollback → öksüz-silme yok, kanıtlandı. Production bug bulunmadı.
2. **F-002 (SQL-inj tarama tamlık-iddiası yanlıştı) — KARAR: doküman düzeltildi.**
   `SECURITY_SCAN.md §2` 5→11 üretim-noktası tam envanterine genişletildi; hepsi güvenli, kod
   değişmedi.
3. **F-003 (SCA tamamlanamadı) — KARAR: PO kalıntı-risk kabul etti.** OWASP dependency-check
   ortam-kısıtı (NVD anahtarı) nedeniyle tamamlanamadı; SAST+manuel çapraz-kontrol temiz olduğundan
   Levent kalıntı bağımlılık-CVE riskini açıkça kabul etti; `NVD_API_KEY` CI backlog.

**Bu turda (Phase 6) açık kalan tek karar kalemi:** sürüm numarası (`0.3.0` önerisi) + merge
stratejisi (`--no-ff main` + `git tag v0.3.0` + push → `.github/workflows/release.yml` otomatik
Maven Central yayını tetikler, basamak-1 `RELEASING.md` süreciyle AYNI) — **Levent'in go/no-go'suna
bırakıldı.**

---

*Kaynaklar: `CHANGELOG.md` (repo kökü), `docs/sentinel/step2/phase6/PHASE6_REVIEW.md`,
`docs/sentinel/step2/phase3/ADR/0009…0019`, `docs/sentinel/step2/phase3/api/{asyncapi,openapi}.yaml`,
`docs/sentinel/step2/phase1/DATA_CLASSIFICATION.md`, `docs/sentinel/step2/phase55/{TEST_REPORT,
COVERAGE,SECURITY_SCAN,QA_FINDINGS}.md`, `RELEASING.md`.*
