# 03.2 — `camunda-nats-channel`: A2 Bileşenleri

**Modül:** `camunda-nats-channel`, paket `com.threeai.nats.camunda.a2` (ADR-0007 §2).
**CadenzaFlow ayna:** `03_classes/3_cadenzaflow_a2_mirror.md` — bu dosyadaki her sınıf birebir aynalanır, yalnız `org.camunda.bpm.*` → `org.cadenzaflow.bpm.*` import farkı (ADR-0007, mevcut kodda `diff` ile doğrulanmış desen — `02_package_structure.md` §1'e bkz).

---

## 1. `A2ExternalTaskBehavior` + `A2BpmnParseListener` (HLD §2.1 · BR-A2-002/003/012 · FR-A2/A3/A13 · US-A2/A8 · ADR-0005)

### 1.1 `A2ExternalTaskBehavior`

```java
package com.threeai.nats.camunda.a2;

public class A2ExternalTaskBehavior extends ExternalTaskActivityBehavior {

    private final String sentinelWorkerId;      // küme-geneli TEK sabit (BR-A2-003)
    private final long lockDurationMillis;       // topic-başına türetilmiş L (ADR-0001, parse-time'da sabitlenir)
    private final A2PostCommitPublisher publisher;

    public A2ExternalTaskBehavior(ParameterValueProvider topicNameProvider,
            ParameterValueProvider priorityProvider, String sentinelWorkerId,
            long lockDurationMillis, A2PostCommitPublisher publisher) {
        super(topicNameProvider, priorityProvider);   // native alanları set eder (protected, bkz. §1.3 not)
        this.sentinelWorkerId = sentinelWorkerId;
        this.lockDurationMillis = lockDurationMillis;
        this.publisher = publisher;
    }

    @Override
    public void execute(ActivityExecution execution) throws Exception {
        ExecutionEntity executionEntity = (ExecutionEntity) execution;
        PriorityProvider<ExternalTaskActivityBehavior> provider =
                Context.getProcessEngineConfiguration().getExternalTaskPriorityProvider();
        long priority = provider.determinePriority(executionEntity, this, null);
        String topic = (String) topicNameValueProvider.getValue(executionEntity);   // inherited protected field

        // 1) Kilitsiz doğum — [BA-VERIFIED] ExternalTaskEntity.java:568-588
        ExternalTaskEntity task = ExternalTaskEntity.createAndInsert(executionEntity, topic, priority);

        // 2) Aynı tx, flush-öncesi kilit — [BA-VERIFIED] ExternalTaskEntity.java:471-474
        //    Yalnız iki alan setter'ı (workerId, lockExpirationTime) — DB round-trip YOK.
        //    Guard testi (tek-INSERT kanıtı): TEST_SPECIFICATIONS.md (a).
        task.lock(sentinelWorkerId, lockDurationMillis);

        // 3) Post-commit publish kancası — TransactionContext.java:49, TransactionState.java:25 (COMMITTED)
        Context.getCommandContext().getTransactionContext()
                .addTransactionListener(TransactionState.COMMITTED, tx -> publisher.publish(task));
    }

    // signal(...), migrateScope(...), onParseMigratingInstance(...) — native ExternalTaskActivityBehavior'dan
    // DEĞİŞTİRİLMEDEN miras alınır (override YOK); migrasyon davranışı native ile birebir.
}
```

**Sıfır ek DB yazısı kanıtı:** `createAndInsert` entity'yi flush ETMEDEN döner (persistence-context'e `insert` operasyonu kuyruklanır, `ExternalTaskEntity.java:583` `insert()`); `lock(...)` yalnız iki alanı bellek-içi set eder (`:472-474`); flush tek seferde olduğundan tek `INSERT ... (WORKER_ID_, LOCK_EXP_TIME_, ...)` üretilir, ikinci bir `UPDATE` YOK. Bu iddia **guard entegrasyon testiyle** (ADR-0005 §2) doğrulanacak — `TEST_SPECIFICATIONS.md` (a).

### 1.2 `A2BpmnParseListener`

```java
package com.threeai.nats.camunda.a2;

public class A2BpmnParseListener extends AbstractBpmnParseListener {

    private final Set<String> a2Topics;                  // spring.nats.camunda.a2.topics[] (08_config.md §1.3)
    private final String sentinelWorkerId;
    private final UmbrellaLockResolver lockResolver;      // topic -> derived L (millis) — 08_config.md §1.2
    private final A2PostCommitPublisher publisher;

    @Override
    public void parseServiceTask(Element serviceTaskElement, ScopeImpl scope, ActivityImpl activity) {
        if (!(activity.getActivityBehavior() instanceof ExternalTaskActivityBehavior)
                || activity.getActivityBehavior() instanceof A2ExternalTaskBehavior) {
            return;   // ne external-task ne zaten-swap edilmiş — dokunma (idempotent re-entry guard)
        }
        // Aynı extension attribute, BpmnParse'ın kullandığı isimle: PROPERTYNAME_EXTERNAL_TASK_TOPIC="topic"
        // (BpmnParse.java:206,2564). Yalnız LİTERAL (sabit) topic string'i eşleştirilir — bkz. §1.3 not.
        String topic = serviceTaskElement.attributeNS(CAMUNDA_BPMN_EXTENSIONS_NS, "topic");
        if (topic == null || !a2Topics.contains(topic)) {
            return;   // klasik external task — davranış DEĞİŞMEZ (BR-A2-012 migration guard, US-A8)
        }
        ExternalTaskActivityBehavior nativeBehavior = (ExternalTaskActivityBehavior) activity.getActivityBehavior();
        long lockDurationMillis = lockResolver.resolveMillis(topic);
        activity.setActivityBehavior(new A2ExternalTaskBehavior(
                new ConstantValueProvider(topic),                 // literal — bkz. §1.3
                nativeBehavior.getPriorityValueProvider(),        // public getter, native sınıfta mevcut
                sentinelWorkerId, lockDurationMillis, publisher));
    }
}
```

**Kayıt (Spring Boot wiring, `CamundaNatsAutoConfiguration` genişlemesi):**

```java
@Bean
public ProcessEnginePlugin a2ProcessEnginePlugin(A2BpmnParseListener listener) {
    return new AbstractProcessEnginePlugin() {
        @Override
        public void preInit(ProcessEngineConfigurationImpl configuration) {
            configuration.getPreParseListeners().add(listener);   // ProcessEngineConfigurationImpl.java:687,3489
        }
    };
}
```

Camunda Spring Boot Starter, `ProcessEnginePlugin` tipindeki tüm bean'leri otomatik keşfedip `preInit`'i process-engine bootstrap sırasında çağırır (standart plugin-extension mekanizması — motor kodu değişmez, NFR-M1).

### 1.3 Kapsam notu — yalnız literal topic (LLD-düzeyi netleştirme, kilitli karara aykırı DEĞİL)

`BpmnParse.parseTopic(...)` (`BpmnParse.java:2468-2478`) topic attribute'unu `createParameterValueProvider(...)` ile çözer — bu, EL-expression topic'leri (`camunda:topic="${myTopicExpr}"`) destekler; expression yalnız **runtime'da** (execution context ile) çözülebilir. A2-topic üyelik testi ise **parse-time'da** (deployment anında) yapılmalı — çünkü behavior-swap kararı parse-time kararıdır. Sonuç: **A2 behavior-swap yalnız `camunda:topic` LİTERAL (sabit string) olan aktivitelerde uygulanabilir**; expression-tabanlı dinamik topic'ler A2'ye alınamaz, klasik external-task poller'da kalır. Bu, BR-A2-012/US-A8'in "A2 olmayan task etkilenmez" ilkesiyle **tutarlıdır** (expression-topic'li aktivite otomatik olarak "A2 olmayan" sınıfına düşer) — kilitli bir kararı değiştirmez, yalnız parse-time/runtime ayrımının doğal sonucudur. **Operasyonel gereksinim:** `spring.nats.camunda.a2.topics[]` listesindeki her girdi, BPMN modelinde birebir literal `camunda:topic` değeriyle eşleşmelidir (dokümantasyon: deployment runbook, `99_deployment.md`).

**Bağımlılık:** BR-A2-002/003/012, FR-A2/A3/A13, US-A2/A8, ADR-0005 (impl-sınıf izolasyonu — bu iki sınıf ADR-0005'in "tek adapter sınıfı" ilkesinin somutlaşmasıdır: impl-sınıf dokunuşu (`ExternalTaskEntity`, `TransactionContext`) yalnız `A2ExternalTaskBehavior`'da; `A2BpmnParseListener` yalnız `ActivityImpl`/`Element` public-plugin yüzeyini kullanır).

---

## 2. `A2PostCommitPublisher` (HLD §2.2 · BR-A2-001/004 · FR-A1/A4 · US-A1/A3)

```java
package com.threeai.nats.camunda.a2;

public class A2PostCommitPublisher {

    private final JetStream jetStream;
    private final A2TopicConfig topicConfig;         // subject şablonu: jobs.<topic>
    private final NatsChannelMetrics metrics;

    /** ÇAĞRILDIĞI YER: A2ExternalTaskBehavior'ın COMMITTED transaction-listener'ı. Tx DIŞINDA çalışır. */
    public void publish(ExternalTaskEntity task) {
        String subject = "jobs." + task.getTopicName();
        byte[] payload = serialize(task);   // process-variable/payload serileştirme — kiracı-tanımlı katman (worker SDK sözleşmesi, bu repo dışı serileştirme detayları hariç asyncapi OpaqueBusinessPayload)
        Headers headers = buildHeaders(task);   // Nats-Msg-Id=externalTaskId + BpmHeaders (trace/business-key/idempotency)

        Timer.Sample dispatchSample = metrics != null ? Timer.start() : null;
        try {
            NatsMessage msg = NatsMessage.builder().subject(subject).data(payload).headers(headers).build();
            jetStream.publish(msg);   // Nats-Msg-Id dedup (BR-SUB-005)
            if (metrics != null) {
                metrics.jsPublishCount(subject, task.getTopicName()).increment();
                if (dispatchSample != null) dispatchSample.stop(metrics.dispatchLatencyTimer(task.getTopicName()));  // 10_metrics.md
            }
        } catch (Exception e) {
            // EXT_JETSTREAM_PUBLISH_UNAVAILABLE — log WARN, ÖZEL AKSİYON YOK (tasarım gereği).
            // Orphan, sweep tarafından ≤ L+S içinde toplanır (BR-A2-004 satır 3, NFR-R3).
            log.warn("Post-commit JetStream publish failed — orphan will be collected by sweep",
                    kv("external_task_id", task.getId()), kv("topic", task.getTopicName()), e);
            if (metrics != null) metrics.jsPublishErrorCount(subject, task.getTopicName()).increment();
        }
    }

    private Headers buildHeaders(ExternalTaskEntity task) {
        Headers h = BpmHeaders.build(traceIdOf(task), businessKeyOf(task), idempotencyKeyOf(task));
        h.add(NATS_MSG_ID_HEADER, task.getId());   // A2 dedup anahtarı = externalTaskId (IR-3)
        return h;
    }
}
```

**DB sorgusuz garanti:** `publish(ExternalTaskEntity task)` parametre olarak **oluşturan node'un elindeki entity'yi** alır — hiçbir `findExternalTaskById`/sorgu çağrısı YOK (BR-A2-004 koşul 1). `TransactionState.COMMITTED` listener'ı commit *sonrası*, tx *dışında* çalışır (`TransactionContext.java:49`) — bu yüzden JetStream publish çağrısı motor DB transaction'ını **etkilemez** (P2 tavanının dışında kalan tek şey budur: dispatch, token-move DEĞİL).

**Bağımlılık:** BR-A2-001/004, FR-A1/A4, US-A1/A3.

---

## 3. `A2OrphanSweep` (HLD §2.3 · BR-A2-005/013 · FR-A5/A6 · US-A3 · ADR-0002/0003)

### 3.1 Sorumluluk ve zamanlama

```java
package com.threeai.nats.camunda.a2;

public class A2OrphanSweep {

    private final ProcessEngine processEngine;         // RuntimeService/ManagementService değil — doğrudan CommandExecutor (§3.2)
    private final SweepLeaderLease leaderLease;         // nats-core (03_classes/1_nats_core_common.md §3.2)
    private final JetStream jetStream;
    private final A2TopicConfig topicConfig;            // hangi topic'ler A2 (aynı liste, A2BpmnParseListener ile PAYLAŞILIR)
    private final String sentinelWorkerId;
    private final UmbrellaLockResolver lockResolver;
    private final NatsChannelMetrics metrics;

    /** @Scheduled(fixedDelayString = "${a2.sweep.period-seconds:120}000") benzeri — S periyodu (ADR-0001). */
    public void sweepCycle() {
        if (!leaderLease.tryAcquireOrRenew()) {
            return;   // bu node lider DEĞİL — hiçbir DB okuması yapılmaz (ADR-0002: DB'ye sıfır koordinasyon yazısı)
        }
        List<ExternalTaskEntity> fetchableCandidates;
        try {
            fetchableCandidates = fetchFetchableParity();   // §3.3 — read-only, FOR-UPDATE'siz
        } catch (Exception e) {
            log.error("Sweep fetchable-parity query failed — cycle skipped, retry next S",
                    e);   // SYS_SWEEP_QUERY_FAILED
            return;
        }
        for (ExternalTaskEntity candidate : fetchableCandidates) {
            relockThenPublish(candidate);   // §3.4 — BAQ-1 sabit sıra + ADR-0003 telafi
        }
    }
}
```

### 3.2 Fetchable-parite sorgusu (BR-A2-005, NIT-1 netliği: yalnız bu SELECT read-only)

**Native karşılığı:** `ExternalTaskManager.selectExternalTasksForTopics(...)` → mapped statement `selectExternalTasksForTopics` (`ExternalTask.xml` WHERE bloğu, bu fazda bizzat okundu):

```sql
-- ExternalTask.xml (fetchable predicate, birebir):
(RES.LOCK_EXP_TIME_ is null or RES.LOCK_EXP_TIME_ <= #{now})
and (RES.SUSPENSION_STATE_ is null or RES.SUSPENSION_STATE_ = 1)
and (RES.RETRIES_ is null or RES.RETRIES_ > 0)
and RES.TOPIC_NAME_ in (<a2Topics>)   -- sweep'in EKLEDİĞİ tek filtre: yalnız A2 topic'lerine bak
```

Sweep, **aynı** mapped statement'ı (`selectExternalTasksForTopics`) `TopicFetchInstruction` listesini A2 topic'leriyle doldurup çağırır — `ExternalTaskManager.java:73-94` mevcut public metod imzası (`selectExternalTasksForTopics(Collection<TopicFetchInstruction>, ...)`) **aynen** kullanılır; yeni SQL/mapping YAZILMAZ (fetchable-parite = "native poller neyi alabilirse"). `SELECT FOR UPDATE` **kullanılmaz** çünkü sweep `LockExternalTaskCmd`'yi (aşağıda §3.4) **ayrı, açık bir ikinci adım** olarak çağırır — native `FetchExternalTasksCmd`'nin tek-komut atomik "fetch+lock" desenini **taklit etmez** (o desen çoklu-worker contention'ı doğurur, tam da D-A'nın kaldırdığı şey).

```java
private List<ExternalTaskEntity> fetchFetchableParity() {
    return processEngine.getManagementService().executeCommand(commandContext -> {
        Map<String, TopicFetchInstruction> instructions = topicConfig.a2Topics().stream()
                .collect(toMap(identity(), topic -> new TopicFetchInstruction(topic, Integer.MAX_VALUE)));
        return commandContext.getExternalTaskManager()
                .selectExternalTasksForTopics(instructions.values(), Integer.MAX_VALUE, false, emptyList());
    });
}
```

### 3.3 Re-lock → publish (BAQ-1 sabit sıra) + telafi-unlock (ADR-0003)

```java
private void relockThenPublish(ExternalTaskEntity candidate) {
    long lockDurationMillis = lockResolver.resolveMillis(candidate.getTopicName());
    try {
        // 1) RE-LOCK ÖNCE (BAQ-1 sabit sıra) — aynı sentinelWorkerId ile HER ZAMAN geçer:
        //    LockExternalTaskCmd.java:50-61 validateWorkerViolation() — workerValidation=false (aynı id)
        //    VEYA lockValidation=false (süre dolmuş) → ihlal YOK.
        processEngine.getManagementService().executeCommand(
                new LockExternalTaskCmd(candidate.getId(), sentinelWorkerId, lockDurationMillis));
    } catch (Exception relockEx) {
        log.error("Sweep re-lock failed — row skipped, unchanged, retried next cycle",
                kv("external_task_id", candidate.getId()), relockEx);   // SYS_SWEEP_RELOCK_FAILED
        return;   // satır durumu değişmedi — zararsız, bir sonraki S'de tekrar dener
    }

    // 2) PUBLISH SONRA:
    try {
        jetStream.publish(buildJobMessage(candidate));
        metrics.sweepRepublishCount(candidate.getTopicName()).increment();   // 10_metrics.md
    } catch (Exception publishEx) {
        // 3) TELAFİ (ADR-0003): re-lock başarılı + publish başarısız → unlock() ile lock geri alınır.
        //    Görünmez-orphan penceresi ≤L yerine ≤S'ye daralır.
        try {
            processEngine.getManagementService().executeCommand(commandContext -> {
                ExternalTaskEntity task = commandContext.getExternalTaskManager()
                        .findExternalTaskById(candidate.getId());
                if (task != null) {
                    task.unlock();   // ExternalTaskEntity.java:559-566 — workerId+lockExpirationTime temizlenir
                }
                return null;
            });
        } catch (Exception unlockEx) {
            // Telafi de başarısız (DB+broker eşzamanlı down) → BAQ-1 default'una düşülür (≤+L gecikme).
            log.error("Sweep republish failed AND compensating unlock failed — row appears " +
                    "freshly-locked but was never delivered; will surface as an old orphan after L",
                    kv("external_task_id", candidate.getId()), unlockEx);   // SYS_SWEEP_REPUBLISH_FAILED (en kötü dal)
        }
        log.error("Sweep republish failed — compensating unlock applied, row re-fetchable within S",
                kv("external_task_id", candidate.getId()), publishEx);   // SYS_SWEEP_REPUBLISH_FAILED (telafi başarılı dalı)
    }
}
```

**`unlock()` yüzeyi seçimi (ADR-0003 NIT-2 düzeltmesi):** `SetExternalTaskRetriesCmd` **KULLANILMAZ** (o komut `lockExpirationTime`'a dokunmaz — `SetExternalTaskRetriesCmd.java:48-51`, yanlış benzetme). Doğru yüzey: `ExternalTaskEntity.unlock()` (`ExternalTaskEntity.java:559-566`), doğrudan `UnlockExternalTaskCmd` üzerinden çağrılır (native komut, motor kodu değişmez).

**Retries=0 (DLQ'lanmış) satır asla dirilmez:** fetchable-parite predikatının `RETRIES_ null or >0` koşulu bu satırları zaten filtreler (§3.2 SQL) — sweep bu satırlara **hiç ulaşmaz**, ayrı bir kontrol GEREKMEZ (BR-A2-005 koşul 2).

**Bağımlılık:** BR-A2-005/013, FR-A5/A6, US-A3, ADR-0002/0003.

---

## 4. `A2CompletionBridge` (HLD §2.4 · BR-A2-008/011 · FR-A7/A12 · US-A4/A7 · Matris 2)

**Evrim kaynağı:** mevcut `JetStreamMessageCorrelationSubscriber` (`camunda-nats-channel/.../inbound/JetStreamMessageCorrelationSubscriber.java`) — `subscribe()`/`unsubscribe()`/backoff/DLQ iskeleti **aynen** taşınır; `handleMessage(...)`'ın gövdesi `correlateWithResult()` yerine `externalTaskService.complete(...)` çağırır.

```java
package com.threeai.nats.camunda.a2;

public class A2CompletionBridge {

    private final ExternalTaskService externalTaskService;
    private final String sentinelWorkerId;
    private final DlqPublisher dlqPublisher;        // nats-core, §1
    private final NatsChannelMetrics metrics;

    void handleReply(Message msg) {
        MDC.put("trace_id", BpmHeaders.extractTraceIdWithFallback(msg));   // Fix#4 — nats-core §1.1
        try {
            byte[] data = msg.getData();
            if (data == null || data.length == 0) {
                routeToDlqAndDecide(msg, DlqReason.EMPTY_MESSAGE_BODY);    // BR-SUB-007 — Fix#5
                return;
            }
            long deliveryCount = deliveryCountOf(msg);
            if (deliveryCount > config.getMaxDeliver()) {
                routeToDlqAndDecide(msg, DlqReason.DELIVERY_BUDGET_EXCEEDED);   // → A2IncidentBridge tüketir (dlq.jobs.<topic>)
                return;
            }

            String externalTaskId = extractExternalTaskId(msg);   // Nats-Msg-Id (A2 dedup anahtarı = externalTaskId)
            ReplyType replyType = classifyReply(msg);              // success / bpmn-error / transient (payload/Content-Type'a göre)

            switch (replyType) {
                case SUCCESS -> externalTaskService.complete(externalTaskId, sentinelWorkerId, variablesOf(msg));
                case BPMN_ERROR -> externalTaskService.handleBpmnError(externalTaskId, sentinelWorkerId,
                        errorCodeOf(msg), errorMessageOf(msg), variablesOf(msg));
                case TRANSIENT -> externalTaskService.handleFailure(externalTaskId, sentinelWorkerId,
                        errorMessageOf(msg), errorDetailsOf(msg), retriesOf(msg), retryTimeoutOf(msg));
            }
            metrics.ackCount(msg.getSubject(), config.getMessageName()).increment();
            msg.ack();   // complete/handleX BAŞARILI olduktan SONRA (custody-transfer, BR-A2-008)

        } catch (NotFoundException notFound) {
            // HandleExternalTaskCmd.java:48-50 — task yok (geç/çift reply). Idempotent yut.
            log.warn("External task not found — late/duplicate reply, acking (idempotent)",
                    kv("external_task_id", extractExternalTaskId(msg)), notFound);   // RES_EXTERNAL_TASK_NOT_FOUND
            msg.ack();

        } catch (BadUserRequestException workerConflict) {
            // HandleExternalTaskCmd.java:52-53 validateWorkerViolation — ASLA olmamalı, invariant.
            log.error("SENTINEL WORKER CONFLICT — invariant violated, paging on-call, NOT acking",
                    kv("external_task_id", extractExternalTaskId(msg)), workerConflict);   // SYS_SENTINEL_WORKER_CONFLICT
            metrics.sentinelWorkerConflictCount(config.getMessageName()).increment();   // ERROR_REGISTRY.md §4.1 alarm kaynağı
            alerting.pageOnCall(SentinelWorkerConflictAlert.of(msg));   // 07_errors.md / ERROR_REGISTRY.md §CRITICAL kanalı
            // ACK YOK, NAK YOK — insan müdahalesi beklenir (kilitli karar, BAQ-7).

        } catch (Exception transientDbFailure) {
            log.error("Transient failure during complete/handleFailure — nak, redelivery expected",
                    kv("external_task_id", extractExternalTaskId(msg)), transientDbFailure);
            msg.nakWithDelay(backoffFor(deliveryCountOf(msg)));
        } finally {
            MDC.remove("trace_id");
        }
    }

    private void routeToDlqAndDecide(Message msg, DlqReason reason) {
        DlqPublishOutcome outcome = dlqPublisher.publish(msg, config.getDlqSubject(), reason,
                msg.getSubject(), config.getMessageName());
        switch (outcome) {
            case PUBLISHED_JETSTREAM, PUBLISHED_CORE_FALLBACK -> msg.ack();
            case FAILED_NO_DLQ_SUBJECT, FAILED_BOTH_PUBLISH -> msg.nakWithDelay(backoffFor(deliveryCountOf(msg)));
        }
    }
}
```

**`complete` expiry-kontrolsüz garanti (US-A7/BR-A2-011):** `HandleExternalTaskCmd.java:89-91` (`validateWorkerViolation`) yalnız `workerId` eşitliğini kontrol eder — `lockExpirationTime` hiç okunmaz. Tek `sentinelWorkerId` sabit olduğundan geç complete (L sonrası) **her zaman** bu kontrolü geçer; sahiplik asla el değiştirmez.

**Bağımlılık:** BR-A2-008/011, FR-A7/A12, US-A4/A7, DECISION_MATRIX Matris 2 (4 satır — hepsi yukarıdaki `switch`/`catch` dallarında birebir karşılanır: satır1→SUCCESS, satır2→NotFoundException, satır3→BadUserRequestException, satır4→transient catch).

---

## 5. `A2IncidentBridge` (HLD §2.5 · BR-A2-009/010 · FR-A10/A11 · US-A6 · ADR-0004)

```java
package com.threeai.nats.camunda.a2;

public class A2IncidentBridge {

    private final ExternalTaskService externalTaskService;
    private final String sentinelWorkerId;
    private final CircuitBreaker circuitBreaker;   // DlqBridgeCircuitBreakerFactory.create("cb-incident-bridge-camunda", ...)
    private final NatsChannelMetrics metrics;

    /** dlq.jobs.<topic>'i tüketen consumer'ın mesaj-işleyicisi. */
    void handleDlqMessage(Message msg) {
        String externalTaskId = extractExternalTaskId(msg);   // Fix#1 sayesinde DLQ mesajı orijinal header'ları taşır
        try {
            circuitBreaker.executeCallable(() -> {
                // BAQ-2 kararı: retryDuration SABİT 0 — Cockpit-retry residual-lock gecikmesi olmaz.
                externalTaskService.handleFailure(externalTaskId, sentinelWorkerId,
                        "Delivery budget exhausted (deliveryCount > M)", dlqReasonOf(msg),
                        /* retries */ 0, /* retryDuration */ 0L);
                return null;
            });
            msg.ack();   // incident-oluşturma-sonrası-ack
        } catch (NotFoundException alreadyResolved) {
            log.warn("Task already resolved via another path — idempotent ack", ...);   // RES_EXTERNAL_TASK_NOT_FOUND
            msg.ack();
        } catch (CallNotPermittedException cbOpen) {
            msg.nakWithDelay(backoffFor(deliveryCountOf(msg)));   // CB OPEN — fail-fast, mesaj stream'de bekler
        } catch (Exception downstreamFailure) {
            log.error("Incident-bridge processing failed", ..., downstreamFailure);   // SYS_DLQ_BRIDGE_PROCESSING_FAILED
            msg.nakWithDelay(backoffFor(deliveryCountOf(msg)));
        }
    }
}
```

**`retryDuration=0` sabitleme (BAQ-2 — bu LLD'nin somutlaştırdığı implementasyon kısıtı):** çağrı sitesinde **literal `0L`** — operatör/konfig tarafından override EDİLEMEZ (BR-A2-010'un dayandığı garanti budur: `lockExpirationTime = now + 0 = now` → Cockpit-retry ne zaman verilirse verilsin satır anında fetchable).

**Duplicate-incident doğal idempotency:** ikinci DLQ-redelivery'de `setRetriesAndManageIncidents(0)` tekrar çağrılır ama `areRetriesLeft()` artık `false` (`ExternalTaskEntity.java:443-448`) → `createIncident()` tekrar tetiklenmez — kod-düzeyinde ek guard **gerekmez** (`BUS_INCIDENT_ALREADY_CREATED`, bilgilendirici).

**CB izolasyonu:** `cb-incident-bridge-camunda` (Camunda) ve `cb-incident-bridge-cadenzaflow` (CadenzaFlow ayna) **ayrı instance** — bir motorun Cockpit DB kesintisi diğerini etkilemez (ADR-0004).

**Bağımlılık:** BR-A2-009/010, FR-A10/A11, US-A6, ADR-0004.

---

## 6. Config sınıfları (bootstrap wiring)

`A2Properties` (`spring.nats.camunda.a2.*`), `A2TopicConfig`, `UmbrellaLockResolver` — `08_config.md` §1'de tanımlı (burada TEKRARLANMAZ). `CamundaNatsAutoConfiguration` genişlemesi (yeni `@Bean` tanımları: `a2ProcessEnginePlugin`, `a2PostCommitPublisher`, `a2OrphanSweep`, `a2CompletionBridge`, `a2IncidentBridge`, `sweepLeaderLease`, `dlqPublisher`) — `99_deployment.md` §1.
