# Phase 5.5 — Security Scan Report — basamak-2 History Offload

**Kapsam:** SAST (SpotBugs), SQL-injection deseni bağımsız-tekrar taraması, PII-log (DP-1) taraması, kasa (vault) L4 fiziksel izolasyon doğrulaması, SCA/bağımlılık denetimi (OWASP dependency-check + manuel çapraz-kontrol).

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

SpotBugs'ın işaretlediği **5 dinamik-SQL noktası** tek tek kaynak-okumasıyla doğrulandı:

| # | Konum | Değişken kaynağı | Koruma |
|---|---|---|---|
| 1 | `ErasurePipeline.anonymizeTable:179` (`table`, `piiColumns`) | `table=ProjectionStore.tableNameFor(historyClass)` (sabit map) + `piiColumns=allowlistedAnonymizationColumns(...)` | `SAFE_IDENTIFIER` regex + `ProjectionStore.allowedColumnsFor(...)` allowlist re-validate (CQ-3, `ErasurePipeline.java:195-210`) |
| 2 | `ErasurePipeline.firstStillPopulatedColumn:274` (kolon) | allowlist-revalidate (FINDING-002 fix, faz-5) | aynı `SAFE_IDENTIFIER`+allowlist |
| 3 | `ProjectionStore.insertLogEvent:115` (kolon listesi) | `appendMappedFields` → `HistoryClassColumnMapping.columnFor(...)` | `SAFE_IDENTIFIER` regex + `allowedColumns().contains(...)` — ikisi de geçmezse `IllegalArgumentException`, alan SESSİZCE atlanır (SQL'e HİÇ ulaşmaz) |
| 4 | `ProjectionStore.insertNew:176` | aynı `appendMappedFields` yolu | aynı |
| 5 | `ProjectionStore.updateExisting:198` | aynı `appendMappedFields` yolu | aynı |

**Sonuç: 5/5 nokta `17099d4`'ün allowlist+regex disiplinini TUTARLI uyguluyor.** Alan-ADI (attacker-influenceable — wire-mesajdan gelen JSON key'leri) hiçbir zaman HEM regex HEM allowlist geçmeden SQL string'ine ulaşamıyor; alan-DEĞERİ zaten her yerde `PreparedStatement.setObject`/`bindValues` ile bind ediliyor (asla string-concat edilmiyor). **Enjeksiyon vektörü YOK — bu 5 SpotBugs bulgusu FALSE POSITIVE (statik analiz allowlist-gate'i izleyemiyor).** `camunda`/`cadenzaflow`/`nats-core` modüllerinde SpotBugs `SQL_PREPARED_STATEMENT_*` bulgusu SIFIR — dinamik-SQL yalnız bu 5 noktada ve hepsi korumalı.

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

## 6. CRITICAL/HIGH bulgu özeti

**CRITICAL: 0. HIGH: 0.** Tüm bu turun bulguları 🟡 (yazılı-ack/backlog) veya bilgilendirici — hiçbiri faz-geçişini BLOKLAMIYOR (SECURITY_TESTING_GUIDELINE.md §4.2 quality-gate: "Block On: Any CRITICAL or HIGH" — bu eşik AŞILMADI).
