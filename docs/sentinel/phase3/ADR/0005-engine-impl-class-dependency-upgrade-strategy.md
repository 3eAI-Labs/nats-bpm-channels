# ADR-0005 — Engine impl-sınıf bağımlılığı ve upgrade stratejisi

- **Durum:** Kabul edildi (2026-07-14, Phase 3)
- **İzlenebilirlik:** US-A2 → FR-A2 → BR-A2-002 → NFR-M1/NFR-M2 → docs/06 §5.4 (D-C) / §9 (kanca noktaları DOĞRULANDI)

## Bağlam

D-C (kilitli): doğumda in-tx sentinel kilit = custom `ExternalTaskActivityBehavior`, `BpmnParseListener` (`preParseListeners`) ile A2-topic'li aktivitelerde swap edilir; `createAndInsert(...)` + aynı tx'te `lock(SENTINEL, L)` → kilit aynı INSERT'e biner (sıfır ek yazı). Aynı noktada `TransactionContext.addTransactionListener(COMMITTED, publish)` = D-A publish kancası.

**Dürüst not (docs/06 §5.4):** bu desen **impl-sınıf bağımlılığı taşır** — public API değil, engine iç sınıflarına dokunur:

| İç sınıf/nokta (fork: `org.cadenzaflow.*`; upstream: `org.camunda.bpm.*`) | Kullanım | phase1/2 doğrulama |
|---|---|---|
| `ExternalTaskEntity` (`createAndInsert` `:568-588`, `lock` `:471-474`, `failed` `:402-419`, `setRetriesAndManageIncidents` `:443-448`) | Doğumda kilit + retries semantiği | `[BA-VERIFIED]` |
| `BpmnParse.java:2564` | Behavior swap noktası | DOĞRULANDI |
| `ProcessEngineConfigurationImpl.java:687,2189` (`preParseListeners`) | Listener kaydı (public extension) | DOĞRULANDI |
| `TransactionContext.java:49` + `TransactionState.COMMITTED` (`:25`) | Post-commit publish kancası | DOĞRULANDI |
| `ExternalTask.xml:220-222` (fetchable predicate) | Sweep sorgu paritesi | `[BA-VERIFIED]` |

NFR-M1: A2 fork motor kodunu **değiştirmez** (yalnız plugin extension point). NFR-M2: Camunda7 ↔ CadenzaFlow birebir taşınır (yalnız paket adı farkı).

Bu ADR, bu impl-yüzeyinin **upgrade dayanıklılığını** nasıl yöneteceğimizi sabitler.

## Karar

### 1. Yüzey izolasyonu — tek adapter sınıfı
Tüm impl-sınıf dokunuşları **tek bir sınıfa** (`A2ExternalTaskBehavior` + yardımcıları) hapsedilir; A2 iş mantığının geri kalanı (publisher, sweep, bridge) yalnız **public `ExternalTaskService`** API'sini kullanır (`complete`/`handleFailure`/`handleBpmnError`/`setRetries`). Böylece kırılgan yüzey minimum ve tek noktada.

### 2. Sürüm pinning + guard test
- Desteklenen engine sürümleri **pinlenir** (Camunda 7.24+, CadenzaFlow 1.2+ — `01 §6`); pom'da açık sürüm.
- **Guard entegrasyon testi** (phase4/5): custom behavior'ın `createAndInsert`+`lock`'unun **tek INSERT** ürettiği (ikinci UPDATE çıkmadığı) datasource-proxy/SQL-sayaç ile doğrulanır — bu test upgrade'de **regresyon dedektörü**dür (docs/06 §9 phase3→phase4 devri).
- fetchable predicate paritesi de bir guard test'le sabitlenir (sweep sorgusu ↔ engine `ExternalTask.xml` aynı satırları döndürür).

### 3. Upgrade prosedürü (bakım runbook girdisi)
Engine minor/major upgrade'inde: (a) yukarıdaki beş kanca noktasının imza/davranış değişmediği doğrulanır; (b) guard testler koşturulur; (c) `complete`'in lock-expiry kontrol etmediği invariant'ı (`HandleExternalTaskCmd`) yeniden teyit edilir. Herhangi biri kırılırsa A2 aktivasyonu bloklanır (fail-closed).

### 4. Camunda ↔ CadenzaFlow ayrımı
İki motor için ayrı sınıf (paket importu farkı: `org.camunda.bpm.*` vs `org.cadenzaflow.*`); mantık birebir ayna (ADR-0007 yerleşim). Ortak, engine-nötr sabitler (SENTINEL workerId, fetchable-predicate alan adları, L parametreleri) `nats-core`'da.

## Sonuçlar

**Olumlu:** Kırılgan yüzey tek sınıfta + guard testlerle izlenir → upgrade riski görünür ve otomatik yakalanır. Fork motor kodu değişmez (NFR-M1) → CadenzaFlow ana hattı bağımsız kalır.

**Olumsuz / kabul edilen:** impl-sınıf bağımlılığı **kalıcı bir teknik borç**tur (public API değil); her engine upgrade'i bir doğrulama adımı ekler. Basamak-6'da (native state-core) bu yüzey tümden buharlaşır (P2 kalkışı) — bu borç geçicidir, kademeli omurganın bilinçli ara-maliyetidir.

## Reddedilenler
- Fork motor kodunu değiştirmek (impl'i public yapmak): NFR-M1 ihlali; CadenzaFlow ana hattından sapma. **Reddedildi**.
- Reflection ile impl erişimi: daha kırılgan + guard-test'siz sessiz kırılma. **Reddedildi** (doğrudan import + guard test tercih edildi).
- Complete-önü lazy kilit (impl'e dokunmadan): D-C'de zaten reddedildi (in-flight task kilitsiz gezer → migration guard kaybı).
