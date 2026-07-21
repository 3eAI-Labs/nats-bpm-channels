# nats-history-projection

Engine-neutral history-offload projection service (basamak-2, `docs/sentinel/step2/phase4/lld/history-offload/`). Consumes the `HISTORY` JetStream stream (populated by `camunda-nats-channel`/`cadenzaflow-nats-channel`'s outbox relay + post-commit publisher) and maintains a Postgres projection independent of any engine's own database.

See `docs/sentinel/step2/phase4/lld/history-offload/` for the full design (`08_config.md` for the complete configuration reference, `99_deployment.md` for the single-engine deployment runbook this README extends).

## CQ-2/CQ-5 (Levent, önerilen, 2026-07-20) — çoklu-motor (multi-engine) dağıtım reçetesi

**Karar özet:** varsayılan (default) auto-configuration tek-motor'u hedefler ve öyle kalır — `NatsHistoryProjectionAutoConfiguration` tek bir `ReconciliationJob`/`RetentionEnforcementJob` bean çifti kaydeder, `history.engine-id` property'siyle (default `"camunda"`) parametrelenir. Bu KOD DEĞİŞİKLİĞİ gerektirmez; basamak-2'nin birincil dağıtım şekli zaten tek-motor'dur (D-G: Flowable = basamak-2b, ayrı basamak).

Bir dağıtımın **aynı anda** hem `camunda-nats-channel` hem `cadenzaflow-nats-channel`'ı (veya gelecekte başka bir engine ailesini) AYNI projeksiyon store'a yazdırması gerekiyorsa — ikisi de aynı `HISTORY` stream'ine `history.<engineId>.<class>.<processInstanceId>` şeklinde yayın yapar, `engine_id` sütunuyla ayrışır (`DB_SCHEMA.md §2` her tabloda `engine_id NOT NULL` — şema zaten çoklu-motor'u DESTEKLER) — `ReconciliationJob`/`RetentionEnforcementJob` için İKİNCİ bir bean seti tanımlanmalıdır, çünkü her ikisi de constructor'da TEK bir `engineId` alır (bir motorun ACT_HI DB'sine karşı sayım/silme yapar).

### Adımlar

1. **`HistoryProjectionConsumer`/`HistoryProjectionConsumerBootstrap` için ek iş YOK.** Bu sınıflar zaten motor-etiketsizdir — partition anahtarı yalnızca `processInstanceId`'ye bağlıdır (ARCH-Q3), gelen zarfın `engineId` alanı hangi motordan geldiğini taşır ve doğrudan ilgili satıra yazılır. Tek bir consumer/partition seti HER İKİ motorun olaylarını da doğru şekilde projekte eder.

2. **`ReconciliationJob` — ikinci bean:** varsayılan bean'i (`history.engine-id` default `"camunda"`) DEĞİŞTİRMEDEN, embedding uygulamanız kendi `@Configuration` sınıfında İKİNCİ bir `ReconciliationJob` bean'i tanımlar:

   ```java
   @Bean
   public ReconciliationJob cadenzaflowReconciliationJob(
           @Qualifier("projectionDataSource") DataSource projectionDataSource,
           @Qualifier("cadenzaflowEngineDataSourceReadOnly") DataSource cadenzaflowEngineDataSourceReadOnly,
           ClassCutoverStateStore stateStore, NatsChannelMetrics metrics,
           ReconciliationProperties properties) {
       return new ReconciliationJob(projectionDataSource, cadenzaflowEngineDataSourceReadOnly,
               stateStore, metrics, properties, "cadenzaflow");
   }
   ```

   `cadenzaflowEngineDataSourceReadOnly` — CadenzaFlow motorunun kendi ACT_HI şemasına salt-okunur bir DataSource (varsayılan `ReconciliationJob` bean'i zaten `engineDataSourceReadOnly` adlı bean'i "camunda" için bekler — `NatsHistoryProjectionAutoConfiguration`'ın kendi CODER-QUESTION notuna bakınız). İki `ReconciliationJob` bean'i AYNI `projectionDataSource`'u paylaşır (tek projeksiyon şeması, `engine_id` sütunuyla ayrışır) ama FARKLI motor-DB'lerine karşı sayım yapar.

3. **`RetentionEnforcementJob` — ikinci bean**, aynı desen (`engineId="cadenzaflow"`), yalnız `projectionDataSource` gerektirir (motorun kendi DB'sine erişmez — retention yalnızca projeksiyon tarafını temizler).

4. **`CutoverControlPlane`/`CutoverRollback`** zaten `engineId`'yi HER ÇAĞRIDA parametre olarak alır (`requestCutover(engineId, historyClass)`) — ikinci bir bean gerekmez, aynı bean her iki motor için de kullanılabilir.

5. **`history.reconciliation.audit-critical-classes`** (`ReconciliationProperties`) tek bir global değerdir — iki motorun audit-kritik sınıf kümesi FARKLIYSA (nadir, ama mümkün — her motorun kendi `HistoryClassificationProperties.auditCriticalClasses`'ı vardır), bu tek property İKİ motor için de aynı davranacaktır; farklı kümeler gerekiyorsa embedding uygulama kendi `ReconciliationProperties` bean'ini motor-başına oluşturmalı (yukarıdaki gibi `@Bean` ile, `@ConfigurationProperties` yerine doğrudan constructor).

## engineId sorgu-API'de açık parametre DEĞİL — kabul edilen kısıt (CQ-2, KAPALI)

`HistoryQueryApi`/`HistoryQueryController`'ın hiçbir metodu `engineId` almaz (`api/openapi.yaml`'ın çekirdek-4 endpoint'leri de almaz — kilitli kontrat, bu basamakta değişmez). Çoklu-motor bir dağıtımda `processInstanceId`/`taskId`/vb. motorlar arası TEORİK olarak çakışabilir (Camunda ve CadenzaFlow ayrı UUID üretse de, aynı uzayı paylaşırlar). Bu, `HistoryQueryApi`'nin kendi CODER-NOTE'unda ("no explicit engineId parameter") zaten belgelenen, **Levent tarafından KABUL EDİLEN bir kısıttır** (2026-07-20, CQ-2 kararı) — openapi kontratı değiştirilmeyecek. Pratikte çakışma riski düşüktür (UUID/Camunda ID üretimi motor-başına bağımsızdır) ve tek-motor dağıtımı (bu basamağın birincil hedefi) hiç etkilenmez; çoklu-motor + gerçek ID-çakışma riski olan kiracılar için bir gelecek-basamak takibi olarak CODER-QUESTIONS'ta kalır.
