# Phase 5.5 — Security Scan Report — basamak-2 History Offload

**Kapsam:** SAST (SpotBugs), SQL-injection deseni bağımsız-tekrar taraması, PII-log (DP-1) taraması, kasa (vault) L4 fiziksel izolasyon doğrulaması, SCA/bağımlılık denetimi (OWASP dependency-check + manuel çapraz-kontrol), retention audit-log atomiklik testi.

**Revizyon (2026-07-21, phase-review turu):** §2 (SQL-injection tamlık iddiası) FINDING-002 uyarınca düzeltildi — tam 12 noktalık envanter aşağıda. §7 (YENİ) FINDING-001 sonucu — `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED` atomiklik testi eklendi ve SONUÇ raporlandı.

---

## 1. SAST — SpotBugs 4.9.8.2 (`effort=Max`, `threshold=Medium`)

Reaktörde SpotBugs ÖNCEDEN yapılandırılmamıştı (pom.xml'de plugin yok) — bu turda pom DEĞİŞTİRİLMEDEN, plugin doğrudan Maven CLI'dan (`com.github.spotbugs:spotbugs-maven-plugin:4.9.8.2:check`) her modülde ayrı ayrı çalıştırıldı (kalıcı pom değişikliği önerilir, aşağıda not).

| Modül | Bulgu sayısı | Kategori dağılımı |
|---|---|---|
| `nats-core` | 7 | 7× `EI_EXPOSE_REP`/`EI_EXPOSE_REP2` |
| `camunda-nats-channel` | 50 | 47× `EI_EXPOSE_REP`/`REP2`, 3× `DLS_DEAD_LOCAL_STORE` |
| `cadenzaflow-nats-channel` | 50 | 47× `EI_EXPOSE_REP`/`REP2`, 3× `DLS_DEAD_LOCAL_STORE` (ayna — camunda ile birebir) |
| `flowable-nats-channel` | 16 | 14× `EI_EXPOSE_REP`/`REP2`, 2× `DLS_DEAD_LOCAL_STORE` |
| `nats-history-projection` | 53 | 48× `EI_EXPOSE_REP`/`REP2`, **5× `SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING`** (bkz. §2) |
| `nats-bpm-bench` | 8 | 5× `EI_EXPOSE_REP`/`REP2`, 2× `VA_FORMAT_STRING_USES_NEWLINE`, 1× `CT_CONSTRUCTOR_THROW` |

### Değerlendirme

- **`EI_EXPOSE_REP`/`EI_EXPOSE_REP2` (184/184 bulgunun ~%88'i):** "Bad practice" kategorisi, GÜVENLİK AÇIĞI DEĞİL — constructor'ların `DataSource`/`Connection`/`JetStream`/`MeterRegistry` gibi paylaşılan altyapı nesnelerini savunmacı-kopyalama YAPMADAN saklaması. Bu nesneler zaten klonlanamaz/singleton-stil kaynaklardır (DI standart deseni) — savunmacı kopyalama anlamsız/imkansız. **Severity: INFO/LOW, aksiyon gerekmiyor.**
- **`DLS_DEAD_LOCAL_STORE` (8 bulgu):** kod-kalitesi (ölü yerel değişken ataması), güvenlik ilgisiz. **LOW, backlog.**
- **`VA_FORMAT_STRING_USES_NEWLINE` (2, nats-bpm-bench):** stil (`\n` yerine `%n`), güvenlik ilgisiz. **LOW.**
- **`CT_CONSTRUCTOR_THROW` (1, `BenchEnvironment`):** constructor exception fırlatırsa finalizer-attack'e teorik açıklık — modern JVM'lerde (finalizer kullanılmıyor, `BenchEnvironment` `final` değil ama hassas veri taşımıyor, yalnız test-altyapısı) pratik risk YOK. **LOW, test-yardımcı sınıf, backlog.**
- **`SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING` (5, `nats-history-projection`) — bağımsız doğrulandı, bkz. §2.**

### 🟡 QA-FINDING (dependency-hygiene, DEVOPS_GUIDELINE ilgili) — SpotBugs pom'a kalıcı eklenmemiş

SAST bu reaktörde CI-gate DEĞİL (pom'da plugin yok) — basamak-1'de de aynı durum, güvenlik-testing-guideline'ın "[BLOCKING] SAST mandatory in CI/CD" kuralına aykırı. **Bu YENİ bir basamak-2 bulgusu değil**, basamak-1'den miras bir CI-altyapı borcu — faz-6/DevOps'a taşınması önerilir (pom'a `spotbugs-maven-plugin` + `verify` fazına bağlama, threshold `Medium`, `EI_EXPOSE_REP*` için `excludeFilterFile` ile bastırma).

---

## 2. SQL-Injection deseni bağımsız-tekrar taraması (`17099d4` fix'inin başka noktada tekrarlanmadığının doğrulaması)

**DÜZELTME (phase-review FINDING-002, 2026-07-21):** bu bölümün önceki sürümü yalnız SpotBugs'ın işaretlediği 5 noktayı listeliyordu ve "yalnız bu 5 noktada" diye TAMLIK iddia ediyordu — YANLIŞ. SpotBugs yalnız `PreparedStatement`'a DOĞRUDAN geçirilen non-constant String'leri yakalıyor; `stmt.execute(...)` (DDL, `Statement` — `PreparedStatement` değil) ve `HistoryQueryApi`'nin HTTP-yüzeyindeki dinamik-`WHERE` inşası gibi başka desenler SpotBugs'ın bu kural-kapsamının DIŞINDA kalıyor ve hiç taranmamıştı. Aşağıdaki envanter, reaktörün TAMAMINDA (`grep -rn` — SQL-anahtar-kelimesiyle başlayan HER string-concat/`StringBuilder` deseni + `prepareStatement`/`createStatement`/`execute` çağrılarının TAMAMI) bağımsız yeniden-taranmasıyla üretildi; her satır kaynak-okumasıyla doğrulandı.

### 2.1. Tam envanter — dinamik-SQL / identifier-interpolation noktaları (11 üretim-kodu noktası + 1 bench-altyapısı noktası, reaktör-geneli — satır #7 ayrı fiziksel konum değil, #1-3'e çapraz-referans)

**Kategori A — attacker-influenceable ALAN-ADI (wire-mesaj JSON key'leri), allowlist+regex korumalı (`17099d4` deseni):**

| # | Konum | Dinamik parça | Kaynak | Koruma |
|---|---|---|---|---|
| 1 | `ProjectionStore.insertLogEvent:115` (sql inşası `:112`) | kolon listesi | `appendMappedFields` → `HistoryClassColumnMapping.columnFor(fieldKey)` | `SAFE_IDENTIFIER` regex (`^[a-z][a-z0-9_]*$`) + `allowedColumns().contains(...)` — ikisi de geçmezse `IllegalArgumentException`, alan SESSİZCE atlanır (SQL'e HİÇ ulaşmaz) |
| 2 | `ProjectionStore.insertNew:175` | kolon listesi | aynı `appendMappedFields` yolu | aynı |
| 3 | `ProjectionStore.updateExisting:189,196` (`StringBuilder`) | kolon listesi (`setColumns`) | aynı `appendMappedFields` yolu | aynı |
| 4 | `ErasurePipeline.anonymizeTable:168-174` (`prepareStatement` `:179`) | `piiColumns` | `allowlistedAnonymizationColumns(historyClass)` | AYNI `SAFE_IDENTIFIER` regex + `ProjectionStore.allowedColumnsFor(...)` re-validate (CQ-3, `ErasurePipeline.java:195-210`) — curated compile-time liste ama write-path allowlist'iyle TUTARLILIK için yeniden-doğrulanıyor |
| 5 | `ErasurePipeline.firstStillPopulatedColumn:258-273` (`prepareStatement` `:274`) | doğrulanacak kolon | allowlist-revalidate (FINDING-002 fix, faz-5 review) | aynı `SAFE_IDENTIFIER`+allowlist |

**Kategori B — tablo/kolon ADI, `HistoryClassColumnMapping`'in SABİT `TableMeta` alanlarından (per-istek dinamizm YOK — `historyClass` çözüldükten sonra `tableName()`/`entityIdColumn()` compile-time'da sabittir, allowlist re-check GEREKMEZ çünkü zaten seçim-noktası yok):**

| # | Konum | Dinamik parça | Kaynak |
|---|---|---|---|
| 6 | `ProjectionStore.selectExisting:146-147` | `meta.tableName()`, `meta.entityIdColumn()` | `HistoryClassColumnMapping.tableFor(historyClass)` → sabit `Map.ofEntries(...)` (literal tablo/kolon adları, satır ~55-115) |
| 7 | `ProjectionStore.insertNew/insertLogEvent/updateExisting` (aynı `meta.tableName()`) | (1-3 ile aynı kaynak, ayrı satır YOK — yukarıdaki 1-3'ün İÇİNDE) | aynı |

**Kategori C — motor-içi (attacker-erişimsiz) idari job'lar, tablo/partition adı ya sabit-map ya da Postgres sistem-kataloğundan (`pg_inherits`/`pg_class`) okunuyor:**

| # | Konum | Dinamik parça | Kaynak |
|---|---|---|---|
| 8 | `RetentionEnforcementJob.dropPartitionWithAudit:110` (`stmt.execute("DROP TABLE " + ...)`, `Statement` — DDL, `PreparedStatement` DEĞİL) + `estimateRowCount:138` | `partition.partitionName()` | `listDatedPartitions(table)` → `LIST_PARTITIONS_SQL` — Postgres `pg_inherits`/`pg_class` sistem kataloğundan `?`-bind'lı okunan GERÇEK, ZATEN-VAR-OLAN tablo adı (satır 57-60); `table` kendisi `ProjectionStore.tableNameFor(historyClass)`, `historyClass` ise `HistoryClassNames.ALL_CLASSES` (compile-time sabit `Set`, `RetentionEnforcementJob.java:77`) döngüsünden — HİÇBİR external/wire input bu zincire hiç girmiyor |
| 9 | `ReconciliationJob.countProjection:147` | `table` | `ProjectionStore.tableNameFor(historyClass)`, `historyClass` = `HistoryClassNames.ALL_CLASSES` döngüsü (`ReconciliationJob.java:61,72`) — aynı sabit-kaynak |
| 10 | `ReconciliationJob.countEngine:160` | `table` | `ActHiTableNames.of(historyClass)` — hard-coded `Map.ofEntries` (ACT_HI_* fork tablo adları), bilinmeyen sınıf için `IllegalArgumentException` (fail-closed) |

**Kategori D — HTTP-yüzeyi sorgu API'si (`HistoryQueryApi`, en yüksek risk-profili — dış çağıran girdi taşıyabilir), ama `table` HER ZAMAN literal, `WHERE` yalnız SABİT parçalardan inşa ediliyor:**

| # | Konum | Dinamik parça | Kaynak |
|---|---|---|---|
| 11 | `HistoryQueryApi.count:251` (4 çağıran: `listProcessInstanceHistory:110`, `listActivityHistory:143`, `listTaskHistory:173`, `listVariableHistory:208`) | `table` parametresi + `where` | `table`: her çağırma noktasında DOĞRUDAN string LİTERAL (`"process_instance_history"` vb, hiç değişken değil). `where`: `StringBuilder`/`String`'e yalnız COMPILE-TIME SABİT SQL parçaları (`" AND business_key = ?"` vb) KOŞULLU olarak eklenir (hangi filtrenin dolu olduğuna göre) — çağıranın GERÇEK değerleri (`query.businessKey()` vb) SQL string'ine HİÇ girmez, ayrı `params` listesine eklenip `bindParams`/`setObject` ile bind edilir. Enjeksiyon yüzeyi YAPISAL OLARAK YOK (ne kolon/tablo adı ne değer attacker-kontrollü metin olarak string'e giriyor). |

**nats-bpm-bench (test-altyapısı, üretim çalışma-yolu DEĞİL) — tamlık için dahil edildi, kod-değişikliği/aksiyon GEREKMİYOR:**

| # | Konum | Dinamik parça | Kaynak |
|---|---|---|---|
| 12 | `HistoryBenchScenario.java:104` (`CREATE DATABASE`), `:271` (`countRows`) | `HISTORY_BENCH_DATABASE`, `tableName` | İkisi de `private static final String` literal / hard-coded çağrı-siteleri (`"compact_history_outbox"` vb) — sıfır dinamizm |

### 2.2. Sonuç

**11/11 üretim-kodu noktası (bench hariç) `17099d4`'ün disiplinini TUTARLI uyguluyor veya yapısal olarak enjeksiyona kapalı.** Alan-ADI (Kategori A, attacker-influenceable) hiçbir zaman HEM regex HEM allowlist geçmeden SQL string'ine ulaşamıyor; alan-DEĞERİ her yerde `PreparedStatement.setObject`/`bindValues` ile bind ediliyor. Kategori B/C tablo/partition adları ya AYNI sabit-map kaynağından ya da Postgres sistem-kataloğundan geliyor — dış girdi hiç karışmıyor. Kategori D (`HistoryQueryApi`, HTTP-yüzeyi) `table`'ı literal tutuyor ve `WHERE`'i yalnız sabit-parça+bind-parametre desenle inşa ediyor. **Enjeksiyon vektörü YOK.** SpotBugs'ın yalnız Kategori A+B'nin bir kısmını (5 nokta, `PreparedStatement`-doğrudan-non-constant-String kuralı) yakalayabilmesi ARACIN kural-kapsamının doğal sınırı — Kategori C'deki `Statement.execute()` (DDL) ve Kategori D'deki (SpotBugs'ın "table" argümanının literal olduğunu görüp alarm vermediği) noktalar bu turda MANUEL grep+kaynak-okuma ile kapatıldı. `camunda`/`cadenzaflow`/`nats-core` modüllerinde SpotBugs `SQL_PREPARED_STATEMENT_*` bulgusu SIFIR (bu modüllerde dinamik-SQL hiç yok — tamamı sabit `INSERT_SQL`/`SELECT_SQL` sabitleri, doğrulandı).

---

## 3. PII-log (DP-1) taraması

`nats-history-projection` + `camunda`/`cadenzaflow` `history` paketleri + `nats-core` `vault`/`history` paketleri genelinde `log.(info|warn|error|debug)(...)` çağrıları `realUserId`/`getUserId()` (ham)/`businessKey`/`variableValue`/`payload`/`getData()` desenleri için grep edildi:

```
grep -riE "realuserid|getuserid\(\)|businesskey|variablevalue|payload\)|getdata\(\)" <log çağrıları>
→ 0 eşleşme
```

Bu turda eklenen YENİ production kodu (`RelayFailoverBenchScenario.java`) de ayrıca kontrol edildi — yalnız zamanlama/satır-sayısı metadata'sı loglanıyor, PII yok. **DP-1 temiz — faz-5 review'ın önceki tespitiyle TUTARLI.**

---

## 4. Kasa (vault) L4 fiziksel izolasyon

- `NatsHistoryProjectionAutoConfiguration`: `projectionDataSource` ve `vaultDataSource` **İKİ AYRI** `@Bean` (satır 140–151), her biri kendi `@ConditionalOnProperty` (`history.projection.datasource.jdbc-url` vs `history.vault.datasource.jdbc-url`) ile bağımsız koşullu — paylaşılan connection pool YOK, ayrı `HikariDataSource` örnekleri.
- `grep -rn "pseudonym_map"` → yalnız `nats-core/.../vault/PseudonymizationVaultClient.java` + `PseudonymTokenGenerator.java`'da; `nats-history-projection` modülü **`pseudonym_map`'e HİÇ referans içermiyor** (CQ-1 sonrası vault-habersiz mimari, doğrulandı) — cross-database sorgu/FK riski yapısal olarak YOK.
- `PseudonymizationVaultClientTest` KENDİ izole Postgres Testcontainers'ında koşuyor (projeksiyon test-fixture'larından bağımsız).

### 🟡 QA-FINDING — TEST_SPEC (f) `PseudonymVaultIsolationTest` sınıfı hiç yazılmamış

Phase4 test-spesifikasyonu (f) `nats-history-projection/src/test/.../vault/PseudonymVaultIsolationTest.java` adında, İKİ AYRI Postgres Testcontainers ile "DataSource'ların farklı JDBC URL'e işaret ettiğini" + "projeksiyon DB'de gerçek değerin SQL ile ARANAMAZ olduğunu" doğrudan kanıtlayan bir sınıf öngörüyordu. CQ-1 mimari değişikliği (vault yazma sorumluluğu engine-side'a taşındı) bu sınıfın ORİJİNAL öncülünü (projeksiyon-tarafı vault erişimi) geçersiz kıldı, ama spesifikasyonun asıl GÜVENLİK iddiaları (izolasyon, erişim-reddi, tersinmez silme) hiçbir zaman TEK bir adanmış sınıfta toplu kanıtlanmadı — dağınık biçimde `PseudonymizationVaultClientTest` (nats-core) + mimari-inceleme (bu rapor §4) ile dolaylı doğrulandı.
- **Test-edilebilir mi:** Evet, kolay (iki Postgres Testcontainers + config-düzeyi assertion).
- **Gerçek risk:** DÜŞÜK — güvenlik ÖZELLİĞİNİN KENDİSİ (fiziksel izolasyon) yukarıda bağımsız doğrulandı; eksik olan yalnız TEK-YERDE-TOPLANMIŞ regresyon-testi, mevcut güvenlik açığı DEĞİL.
- **Öneri:** phase6/backlog — `PseudonymVaultIsolationTest` adanmış sınıfı yazılsın (spec (f) birebir), ama bloklayıcı değil.

---

## 5. SCA / Bağımlılık Denetimi

### 5.1. OWASP dependency-check 10.0.4 — **tamamlanamadı (NVD API anahtarı yok)**

```
mvn org.owasp:dependency-check-maven:10.0.4:aggregate -DautoUpdate=true
```

NVD API 367.875 kayıt taşıyor; anahtarsız ağır rate-limit altında ~8 dakikada yalnız **%8** (30.000/367.875) indirildi — tam senkronizasyon bu oturum bütçesini (tahmini 45-60dk) aşıyordu, koşum **sonlandırıldı** (yarım/yanlış-negatif sonuç üretmemek için rapor edilmedi).

**Öneri (DEVOPS_GUIDELINE §5 ile uyumlu):** CI pipeline'ına `NVD_API_KEY` secret'ı eklenip bu tarama otomatikleştirilsin (`security-sca` stage, `SECURITY_TESTING_GUIDELINE.md §4.1` şablonu) — anahtarla senkronizasyon dakikalar sürer, mevcut haliyle bu tarama pratik olarak KULLANILAMAZ durumda.

### 5.2. Manuel çapraz-kontrol (tamamlayıcı, YETKİLİ DEĞİL — canlı CVE beslemesi değil)

`mvn dependency:tree` çıktısından derlenen kritik kütüphane sürümleri (eğitim-verisi bilgisiyle, ihtiyatlı):

| Kütüphane | Sürüm | Not |
|---|---|---|
| `org.postgresql:postgresql` (JDBC) | 42.7.4 | CVE-2024-1597 (SQL-injection, `PreferQueryMode=SIMPLE`) 42.7.2'de düzeltildi — 42.7.4 GÜNCEL, temiz |
| `ch.qos.logback:logback-classic` | 1.5.12 | CVE-2023-6378 (receiver deserialization) <1.4.12'yi etkiliyordu — GÜNCEL |
| `org.yaml:snakeyaml` | 2.2 | CVE-2022-1471 (deserialization RCE) 2.0'da düzeltildi — GÜNCEL |
| `com.fasterxml.jackson.core:*` | 2.17.3 | eski 2.9.x deserialization CVE ailesinin çok ötesinde — GÜNCEL görünüyor |
| `org.springframework:*` | 6.1.15 / Boot 3.3.6 | aktif-destek dalı, bilinen açık CVE tespit edilmedi (eğitim-verisi sınırlı) |
| `io.nats:jnats` | 2.20.5 | standart, bilinen açık CVE yok |

**Bu tablo bir SCA taramasının YERİNE GEÇMEZ** — yalnız §5.1'in tamamlanamamasına karşı iyi-niyetli bir ilk-bakış. Otoriter sonuç için §5.1'in NVD-anahtarlı yeniden-koşumu ŞARTTIR.

### 🟡 QA-FINDING — Testcontainers sürüm-tutarsızlığı (bağımlılık hijyeni)

`dependency:tree` her modülde AYNI deseni gösteriyor:
```
org.testcontainers:testcontainers:jar:2.0.4:test          <- kök pom testcontainers-bom (2.0.4) yönetiyor
org.testcontainers:junit-jupiter:jar:1.19.8:test           <- 2.0.4 BOM'unda YOK, spring-boot-dependencies'ten (1.19.8) sızıyor
org.testcontainers:postgresql:jar:1.19.8:test              <- aynı
```
`testcontainers-bom:2.0.4` `junit-jupiter`/`postgresql` alt-modüllerini yönetmiyor (muhtemelen 2.x'te yeniden-yapılandırıldı) — Maven, `spring-boot-dependencies` BOM'undan ESKİ (1.19.8) sürümüne düşüyor. Reaktör aynı anda İKİ FARKLI major-sürüm Testcontainers zincirini classpath'te taşıyor. Testler bugün yeşil (uyumlu çalışıyor) ama bu, gelecekteki bağımlılık güncellemelerinde kırılganlık + SCA araçlarının CVE-atıf hassasiyetini bozma riski taşır.
- **Gerçek risk:** DÜŞÜK (bugün çalışıyor), ORTA-vadeli (bakım/upgrade kırılganlığı).
- **Öneri:** kök `pom.xml`'de `org.testcontainers:junit-jupiter`/`org.testcontainers:postgresql` için `testcontainers.version`'a EXPLICIT pin eklensin (`<dependencyManagement>`), backlog/phase6.

---

## 6. Retention audit-log atomikliği — `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED` (phase-review FINDING-001)

**Bulgu:** bu CRITICAL (on-call-page) kod hiçbir testle örneklenmiyordu — `RetentionAuditLogger` JaCoCo'da %89.5 (2 satır boşluk, tam bu throw dalı); retention-silme ↔ audit-log yazımı atomikliği (`DATA_GOVERNANCE.md §4.4` "her retention silmesi audit-log'lanmalı" invariant'ı) yalnız `RetentionEnforcementJob`'ın CODER-NOTE tasarım-argümanıyla varsayılıyordu, GERÇEK bir fault-injection testiyle değil.

**Test (gerçek-PG, fault-injection):** `RetentionEnforcementJobTest.enforceRetention_auditLogWriteFails_dropRolledBack_noOrphanDeletion_throwsCriticalException` — `RetentionEnforcementJob` KENDİ (gerçek, sağlıklı) Testcontainers `dataSource`'unu kullanırken, `RetentionAuditLogger` bilinçli olarak ULAŞILAMAZ bir `DataSource`'a (connection-refused, `127.0.0.1:1`) bağlandı — audit-log YAZIMININ KENDİSİ gerçekten başarısız olurken DROP tarafı sağlıklı kalıyor (üretimde ikisi AYNI `projectionDataSource` bean'ini paylaşıyor — bu test kasıtlı olarak ayırarak hangi tarafın başarısız olduğunu izole ediyor, `HistoryProjectionConsumerDlqRoutingTest`'in çift-hata senaryoları için kullandığı AYNI teknik).

### Sonuç: **ATOMİKLİK TUTTU — production bug BULUNMADI.**

- **(a) `SYS_RETENTION_AUDIT_LOG_WRITE_FAILED` fırlatılıyor mu:** ✅ EVET — `RetentionAuditLogWriteFailedException` `enforceRetention()`'ın kendi `catch (RetentionAuditLogWriteFailedException criticalFailure) { throw criticalFailure; }` dalından (satır 80-81) BOZULMADAN (generic `SYS_RETENTION_JOB_FAILED` log-and-continue'ya YUTULMADAN) çağırana kadar yükseliyor.
- **(b) Öksüz-silme (audit-log OLMADAN kalıcı silme) YOK:** ✅ DOĞRULANDI — `dropPartitionWithAudit`'in KENDİ `connection`'ı (`DROP TABLE`'ın çalıştığı) `setAutoCommit(false)` ile açılıyor; `auditLogger.record(...)` (AYRI bir connection üzerinde) fırlattığında, saran `catch (Exception e) { connection.rollback(); throw e; }` bloğu DROP'u ÜRETİMDEN GERİ ALIYOR (Postgres transactional DDL sayesinde) — partition testte GERÇEKTEN hayatta kalıyor, bu fault-injection denemesi audit_log'a SIFIR satır eklemiyor.
- **Mekanizma notu (doküman-doğruluğu, davranış DEĞİL):** `RetentionEnforcementJob`'ın CODER-NOTE'u atomikliği "DROP + audit INSERT AYNI transaction'da" diye tanımlıyor — bu LİTERAL OLARAK yanlış (audit INSERT `RetentionAuditLogger.record()` içinde AYRI bir `projectionDataSource.getConnection()` çağrısıyla, kendi autocommit'iyle çalışıyor, aynı transaction'da DEĞİL). Atomiklik GERÇEKTE farklı bir mekanizmayla sağlanıyor: audit-yazım BAŞARISIZ olursa DROP'un olduğu connection AÇIKÇA `rollback()` ediliyor (compensating-action deseni, "aynı tx" değil). Sonuç AYNI (atomiklik tutuyor) ama CODER-NOTE'un mekanizma açıklaması yanıltıcı. 🟢 NIT — Javadoc düzeltmesi önerilir, davranış değişikliği GEREKMİYOR.

**Kapsam-dışı bırakılan (bu turda test edilmedi):** `connection.commit()`'in KENDİSİNİN başarısız olduğu (audit BAŞARILI yazıldıktan SONRA, DROP commit anında bağlantı koparsa) ters-yönlü senaryo — bu durumda audit-satırı DURABLE olur ama DROP hiç commit edilmeyebilir ("phantom audit" — loglanmış ama gerçekleşmemiş silme). Bu, FINDING-001'in sorduğu "öksüz-silme" riskinin TAM TERSİ bir risk sınıfı (veri-kaybı değil, yanlış-pozitif audit iddiası) — ayrı bir QA-FINDING olarak izleniyor (bkz. `QA_FINDINGS.md` QA-F6).

---

## 7. CRITICAL/HIGH bulgu özeti

**CRITICAL: 0. HIGH: 0.** Tüm bu turun bulguları 🟡 (yazılı-ack/backlog) veya bilgilendirici — hiçbiri faz-geçişini BLOKLAMIYOR (SECURITY_TESTING_GUIDELINE.md §4.2 quality-gate: "Block On: Any CRITICAL or HIGH" — bu eşik AŞILMADI). FINDING-001 (retention atomikliği) test edilip TUTTUĞU kanıtlandı — PRODUCTION BUG değil.
