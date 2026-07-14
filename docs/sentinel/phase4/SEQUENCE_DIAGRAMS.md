# Sequence Diyagramları — Basamak-1

**Sentinel fazı:** Phase 4 — Developer (LLD). **Kaynak:** `docs/sentinel/phase3/HLD.md` §1-§2, `docs/sentinel/phase2/BUSINESS_LOGIC.md` §1 (süreç akışları — bu diyagramlar o akışların **implementasyon-düzeyi** karşılığıdır, phase2 akışlarını değiştirmez, sınıf/metot isimleriyle somutlaştırır).
**Sınıf bağlaması:** `docs/sentinel/phase4/lld/external-task-jetstream/05_sequences.md`.
**Doğrulama:** her diyagram `mermaid-cli` (`mmdc`) ile render edildi — 0 sözdizimi hatası (bu fazda koşuldu; diyagram 6, LLD-Q1 düzeltmesi sonrası yeniden render edildi).
**Durum:** Onaylı (2026-07-15) — LLD-Q1…3 + review düzeltmeleri işlendi (diyagram 6: motor-başına lease anahtarı).

---

## 1. Happy-path: create → lock → commit → publish → worker → reply → complete → ack

**Kapsanan:** BR-A2-001/002/004/008, FR-A1/A2/A4/A7, US-A1/A2/A3/A4.

```mermaid
sequenceDiagram
    participant P as A2ExternalTaskBehavior
    participant DB as Engine DB (ACT_RU_EXT_TASK)
    participant TX as TransactionContext
    participant PUB as A2PostCommitPublisher
    participant JSJ as JetStream jobs.<topic>
    participant W as Worker (repo dışı)
    participant JSR as JetStream jobs.<topic>.reply
    participant CB as A2CompletionBridge

    P->>DB: createAndInsert(topic) [kilitsiz doğum]
    P->>DB: lock(SENTINEL, L) [aynı tx, flush-öncesi — sıfır ek yazı]
    P->>TX: addTransactionListener(COMMITTED, publish(task))
    P->>DB: COMMIT
    DB-->>TX: COMMITTED
    TX->>PUB: publish(task) [tx DIŞINDA]
    PUB->>JSJ: publish(Nats-Msg-Id=externalTaskId)
    JSJ-->>W: push (queue-group — tek worker alır)
    W->>W: iş yapılır
    W->>JSR: publish(reply, Nats-Msg-Id=externalTaskId)
    W->>JSJ: ACK job mesajı (reply-önce-ack)
    JSR-->>CB: push (reply)
    CB->>DB: complete(externalTaskId, SENTINEL, vars)
    DB-->>CB: OK (token ilerler, incident/lock temizlenir)
    CB->>JSR: ACK (complete-sonrası-ack, custody-transfer)
```

---

## 2. Publish-fail → soğuk sweep → telafi-unlock (ADR-0003)

**Kapsanan:** BR-A2-004/005/013, FR-A4/A5/A6, US-A3, ADR-0002/0003.

```mermaid
sequenceDiagram
    participant PUB as A2PostCommitPublisher
    participant JS as JetStream jobs.<topic>
    participant DB as Engine DB
    participant SWEEP as A2OrphanSweep (leader)

    PUB->>JS: publish(task)
    JS--xPUB: broker erişilemez (EXT_JETSTREAM_PUBLISH_UNAVAILABLE)
    Note over PUB: log WARN, özel aksiyon YOK — orphan sweep'e bırakılır (tasarım gereği)

    loop her S=120s (yalnız lider node)
        SWEEP->>DB: fetchable-parite SELECT (read-only, FOR-UPDATE'siz)
        DB-->>SWEEP: orphan aday satır
        SWEEP->>DB: re-lock(SENTINEL, L) [BAQ-1 sabit sıra — ÖNCE]
        DB-->>SWEEP: OK
        SWEEP->>JS: publish(jobs.<topic>) [SONRA]
        alt publish başarılı
            JS-->>SWEEP: PubAck
            Note over SWEEP: BR-A2-005 satır 1 — normal, bitti
        else publish başarısız (broker yine down)
            JS--xSWEEP: publish fail
            SWEEP->>DB: unlock() [telafi — ADR-0003, native UnlockExternalTaskCmd]
            alt telafi unlock başarılı
                DB-->>SWEEP: OK (LOCK_EXP_TIME_/WORKER_ID_ temizlendi)
                Note over SWEEP: SYS_SWEEP_REPUBLISH_FAILED (recoverable) — satır ≤S içinde yeniden fetchable
            else telafi de başarısız (DB+broker eşzamanlı down)
                DB--xSWEEP: unlock fail
                Note over SWEEP: BAQ-1 default'a düşülür — satır ≤+L (320s) sonra yeniden orphan sayılır
            end
        end
    end

    Note over SWEEP: bir sonraki S döngüsü
    SWEEP->>DB: fetchable-parite SELECT (aynı satır TEKRAR fetchable — telafi sayesinde)
    SWEEP->>DB: re-lock(SENTINEL, L)
    SWEEP->>JS: publish (bu kez başarılı)
    JS-->>SWEEP: PubAck
```

---

## 3. DLQ → incident → Cockpit-retry (BAQ-2: retryDuration=0 sabit)

**Kapsanan:** BR-A2-009/010, FR-A10/A11, US-A6, ADR-0004.

```mermaid
sequenceDiagram
    participant W as Worker job-consumer (repo dışı)
    participant JSJ as JetStream jobs.<topic>
    participant JSD as JetStream dlq.jobs.<topic>
    participant INC as A2IncidentBridge
    participant DB as Engine DB
    participant COCK as Cockpit (operatör)

    JSJ->>W: push job (redelivery, W saniyede bir, M'e kadar)
    Note over W: worker kalıcı ölü — hiç reply gelmez
    W->>W: kendi consumer'ı deliveryCount>M algılar (worker SDK sözleşmesi, repo dışı)
    W->>JSD: publishToDlq (header+Nats-Msg-Id korunur — Fix#1/#3)
    W->>JSJ: orijinal job mesajını ACK'le (DLQ-PubAck-sonrası-ack)
    JSD-->>INC: push (dlq.jobs.<topic>)
    INC->>INC: circuitBreaker.executeCallable(...) [ADR-0004, cb-incident-bridge-camunda]
    INC->>DB: handleFailure(externalTaskId, SENTINEL, retries=0, retryDuration=0) [BAQ-2 SABİT]
    DB->>DB: setRetriesAndManageIncidents(0) → areRetriesLeft() idi → createIncident()
    DB->>DB: lockExpirationTime = now+0 = now
    DB-->>INC: OK
    INC->>JSD: ACK (incident-oluşturma-sonrası-ack)
    DB-->>COCK: incident görünür

    COCK->>DB: Retry ver (herhangi bir zamanda) → setRetriesAndManageIncidents(retries>0)
    Note over DB: lockExpirationTime zaten ≤now (retryDuration=0 sayesinde) → ANINDA fetchable-parite
    Note over DB: bir sonraki sweep döngüsünde (≤S) re-lock+re-publish doğal gerçekleşir — GECİKME YOK
```

---

## 4. Flowable DLQ → failure-event bridge + geç-sonuç

**Kapsanan:** BR-FLW-003/005, FR-B3/B5, US-B3/B5, ADR-0004.

```mermaid
sequenceDiagram
    participant W as Worker event-consumer (repo dışı)
    participant JSD as JetStream dlq.<event-subject>
    participant FEB as FailureEventBridge
    participant REG as Event Registry
    participant INST as Bekleyen Process Instance

    Note over W: worker kalıcı ölü — event W·M bütçesi içinde işlenemedi
    W->>JSD: publishToDlq (aynı correlation header'lar korunur — Fix#1)
    JSD-->>FEB: push (dlq.<event-subject>)
    FEB->>FEB: circuitBreaker.executeCallable(...) [cb-failure-event-bridge-flowable]
    FEB->>REG: eventReceived(inboundChannelModel, failureEvent)
    alt bekleyen subscription VAR (event-based gateway / boundary event / event subprocess)
        REG->>INST: correlate — escalation path işlenir
        REG-->>FEB: OK
        FEB->>JSD: ACK (correlate-sonrası-ack)
    else bekleyen subscription YOK (instance zaten resolve / key kayıp)
        REG-->>FEB: no-match [⏭ davranış TEST_SPECIFICATIONS.md (d) ile doğrulanacak]
        FEB->>FEB: log WARN + failureEventCorrelationMissCount++ [BAQ-8]
        FEB->>JSD: ACK (mesaj işlendi sayılır — RES_FAILURE_EVENT_CORRELATION_MISS)
    end

    Note over INST: --- Geç-sonuç senaryosu (BR-FLW-005, DLQ'dan BAĞIMSIZ ayrı akış) ---
    W-->>REG: (gecikmeli) orijinal event nihayet ulaşır
    alt escalation interrupting idi
        REG->>REG: subscription artık YOK (escalation zaten tüketti)
        REG-->>W: BUS_EVENT_CORRELATION_NOT_FOUND — ack+log+metric (drop)
    else escalation non-interrupting idi
        REG->>INST: subscription hâlâ var — işlenir (model kararı)
    end
```

---

## 5. Boş-body → DLQ (contract-fix #5, BAQ-5)

**Kapsanan:** BR-SUB-007, FR-C2/B2, US-C2/B2.

```mermaid
sequenceDiagram
    participant P as Producer (worker/outbound — üretici hatası)
    participant JS as JetStream (reply/event subject)
    participant C as Consumer (A2CompletionBridge / JetStreamInboundEventChannelAdapter)
    participant DLQ as DlqPublisher

    P->>JS: publish(data=null veya length=0)
    JS-->>C: push
    C->>C: data==null || data.length==0 ?
    alt boş body (BAQ-5 — Fix#5)
        C->>C: log WARN + metrics.dlqCount++
        C->>DLQ: publish(msg, dlqSubject, EMPTY_MESSAGE_BODY, ...)
        alt DLQ publish başarılı (JetStream veya core-fallback)
            DLQ-->>C: PUBLISHED_*
            C->>JS: ACK
        else DLQ publish başarısız (her iki yol)
            DLQ-->>C: FAILED_*
            C->>JS: NAK (asla ack-drop, dlq-of-dlq YOK)
        end
    else body dolu
        C->>C: normal işleme akışına devam (complete / eventReceived)
    end
```

---

## 6. `SweepLeaderLease` — leader seçimi ve devri (ADR-0002 + LLD-Q1: motor-başına anahtar)

**Kapsanan:** BR-A2-005, FR-A5, US-A3, ADR-0002. Aşağıdaki Node 1/Node 2, **aynı motor ailesinin** (ör. Camunda) iki replikasıdır — anahtar `sweep-leader.camunda` bu motor ailesine özeldir (LLD-Q1, 2026-07-15); CadenzaFlow node'ları aynı bucket'ta `sweep-leader.cadenzaflow` anahtarıyla **bağımsız** bir leader-election yürütür (bkz. `03_classes/3_cadenzaflow_a2_mirror.md` §2).

```mermaid
sequenceDiagram
    participant N1 as Node 1 (Camunda, A2OrphanSweep)
    participant N2 as Node 2 (Camunda, A2OrphanSweep)
    participant KV as JetStream KV a2-sweep-leader

    N1->>KV: kv.create("sweep-leader.camunda","node-1")
    KV-->>N1: OK (rev=1) — Node 1 LİDER
    N2->>KV: kv.create("sweep-leader.camunda","node-2")
    KV-->>N2: ErrorKeyExists
    N2->>KV: kv.get("sweep-leader.camunda")
    KV-->>N2: value="node-1" (ben değilim)
    Note over N2: tryAcquireOrRenew()=false → sweepCycle() atlanır — DB'ye SIFIR okuma

    loop her S=120s
        N1->>KV: kv.update("sweep-leader.camunda","node-1",lastRevision) [TTL=240s'nin yarısında yenile]
        KV-->>N1: OK — lider kalmaya devam eder
        N1->>N1: sweepCycle() çalışır
    end

    Note over N1: Node 1 ÇÖKER — bir sonraki S'de yenileme GELMEZ
    Note over KV: TTL=240s dolar — anahtar düşer
    N2->>KV: kv.create("sweep-leader.camunda","node-2") [bir sonraki deneme]
    KV-->>N2: OK (yeni rev) — Node 2 artık LİDER
    N2->>N2: sweepCycle() çalışmaya başlar
```

---

## 7. Sentinel worker-conflict — CRITICAL + on-call page (BAQ-7, ADR-0008 ikincil savunma)

**Kapsanan:** BR-A2-003, FR-A3, US-A2, `SYS_SENTINEL_WORKER_CONFLICT`.

```mermaid
sequenceDiagram
    participant ATK as Config-drift node / sahte-reply aktör
    participant JSR as JetStream jobs.<topic>.reply
    participant CB as A2CompletionBridge
    participant DB as Engine DB
    participant ALERT as Alerting (on-call page)

    Note over ATK: SENTINEL workerId yanlış yapılandırılmış (drift) VEYA subject-ACL aşılıp sahte reply yazılmış
    ATK->>JSR: publish(reply, workerId != SENTINEL sabiti)
    JSR-->>CB: push
    CB->>DB: complete(externalTaskId, wrongWorkerId, vars)
    DB->>DB: validateWorkerViolation() → true
    DB-->>CB: BadUserRequestException
    CB->>CB: log ERROR (SYS_SENTINEL_WORKER_CONFLICT)
    CB->>ALERT: pageOnCall(...)
    Note over CB: ACK YOK, NAK YOK — insan müdahalesi beklenir (BAQ-7 invariant)
    ALERT-->>ATK: (n/a) — operatör kök nedeni araştırır
```
