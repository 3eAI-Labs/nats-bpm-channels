# Sequence Diyagramları — Basamak-2: History Offload

**Sentinel fazı:** Phase 4 — Developer (LLD). **Kaynak:** `docs/sentinel/step2/phase3/HLD.md` §2-§3, `docs/sentinel/step2/phase2/BUSINESS_LOGIC.md §1` (süreç akışları — bu diyagramlar implementasyon-düzeyi karşılığıdır, phase2 akışlarını DEĞİŞTİRMEZ, sınıf/metot isimleriyle somutlaştırır).
**Kapsam (görev talimatı):** yalnız **servisler-arası** akışlar (engine node ↔ NATS ↔ projeksiyon servisi ↔ kasa) — sınıf-içi algoritma diyagramı YOK.
**Sınıf bağlaması:** `docs/sentinel/step2/phase4/lld/history-offload/05_sequences.md`.
**Doğrulama:** her diyagram `mmdc` (`@mermaid-js/mermaid-cli` 11.16.0) ile render edildi — bkz. §Doğrulama Kaydı.
**Durum:** Taslak — Levent faz-4 onayına sunuluyor.

---

## 1. Audit-kritik: tx-içi outbox → relay → PubAck → delete (custody-transfer)

**Kapsanan:** BR-HDL-003, BR-REL-001, FR-A4/B1, US-A3/B1, ADR-0010.

```mermaid
sequenceDiagram
    participant H as NatsHistoryEventHandler
    participant OW as CompactHistoryOutboxWriter
    participant EDB as Engine DB (compact_history_outbox)
    participant R as HistoryOutboxRelay (leader)
    participant KV as KV history-relay-leader
    participant JS as JetStream history.<engine>.<class>.<instanceId>

    H->>OW: write(historyEvent, class) [audit-kritik dal]
    OW->>EDB: INSERT compact_history_outbox (+ payload companion, LLD-Q1)
    Note over OW,EDB: AYNI transaction (runtime write ile)
    EDB-->>H: COMMIT
    R->>KV: tryAcquireOrRenew() [her relayCyclePeriod=30s]
    KV-->>R: leader=true
    R->>EDB: SELECT oldest-first (idx_compact_history_outbox_created_at)
    EDB-->>R: outbox satırı
    R->>JS: publish(Nats-Msg-Id=<eventId>:<type>)
    alt PubAck alındı
        JS-->>R: PubAck
        R->>EDB: DELETE (custody-transfer tamam)
    else PubAck alınamadı
        JS--xR: publish/timeout fail
        Note over R: SYS_OUTBOX_RELAY_PUBLISH_FAILED — satır SİLİNMEZ, retry/backoff
    end
```

---

## 2. Bulk: post-commit publish, sıfır DB yazımı

**Kapsanan:** BR-HDL-004, FR-A5, US-A4, ADR-0010.

```mermaid
sequenceDiagram
    participant H as NatsHistoryEventHandler
    participant TX as TransactionContext
    participant EDB as Engine DB
    participant P as HistoryPostCommitPublisher
    participant JS as JetStream history.<engine>.<class>.<instanceId>

    H->>TX: addTransactionListener(COMMITTED, publish(event))
    H->>EDB: (runtime yazım, dual-run ise + internalDbDelegate)
    EDB-->>TX: COMMIT
    TX->>P: publish(event) [tx DIŞINDA]
    P->>JS: publish (at-most-once, sıfır DB sorgusu)
    alt publish başarılı
        JS-->>P: PubAck (izlenir, retry YOK)
    else node çöker / broker erişilemez
        Note over P: kalıcı kayıp — BİLİNÇLİ KABUL (D-A), reconciliation tespit eder
    end
```

---

## 3. Projeksiyon consume + merge-upsert (stale discard)

**Kapsanan:** BR-REL-002/006, FR-B2, US-B2, ADR-0011/0012.

```mermaid
sequenceDiagram
    participant JS as JetStream history.> (partition i)
    participant C as HistoryProjectionConsumer
    participant PS as ProjectionStore
    participant PG as Projeksiyon Postgres

    JS-->>C: push (instance-partition, ARCH-Q3)
    C->>PS: upsertEntity(class, record) [entity-lifecycle] veya insertLogEvent [append-only]
    PS->>PG: SELECT partition_anchor_at, stream_sequence WHERE engine_id=? AND entity_id=?
    alt bulunamadı (ilk event)
        PS->>PG: INSERT (partition_anchor_at = event_time)
        PG-->>PS: OK
        PS-->>C: APPLIED
        C->>JS: ACK
    else bulunundu, gelen stream_sequence > mevcut
        PS->>PG: UPDATE WHERE ... AND partition_anchor_at=<bulunan>
        PG-->>PS: OK
        PS-->>C: APPLIED
        C->>JS: ACK
    else bulunundu, gelen stream_sequence <= mevcut
        Note over PS: BUS_PROJECTION_STALE_EVENT_DISCARDED — no-op
        PS-->>C: STALE_DISCARDED
        C->>JS: ACK (custody yine transfer olur)
    else DB yazım hatası (geçici)
        PG--xPS: SQL exception
        Note over C: SYS_PROJECTION_WRITE_FAILED
        C->>JS: nakWithDelay
    end
```

---

## 4. History-DLQ (delivery-budget aşımı)

**Kapsanan:** BR-REL-005, FR-B5, US-B5, ADR-0013/0019/0004.

```mermaid
sequenceDiagram
    participant JS as JetStream history.>
    participant C as HistoryProjectionConsumer
    participant DLQ as HistoryDlqConsumer
    participant JSD as JetStream dlq.history.>
    participant OPS as HistoryDlqInspectionConsumer (ops, yetkili)

    JS-->>C: push (redelivery, deliveryCount artıyor)
    Note over C: deliveryCount > maxDeliver
    C->>DLQ: routeToDlq(msg, reason)
    DLQ->>JSD: publish (header-korumalı byte-ayna, Nats-Msg-Id=<orijinal>.dlq)
    alt DLQ publish başarılı
        JSD-->>DLQ: PubAck
        DLQ->>JS: ACK orijinal mesaj (DLQ-PubAck-sonrası-ack)
    else DLQ publish başarısız
        JSD--xDLQ: publish fail
        Note over DLQ: SYS_HISTORY_DLQ_PUBLISH_FAILED — nak + alert, asla ack-drop
        DLQ->>JS: NAK
    end
    JSD-->>OPS: push (yetkili inceleme, subject-ACL + CB korumalı)
    OPS->>OPS: circuitBreaker.executeCallable(...) [cb-history-dlq-inspection]
```

---

## 5. Reconciliation → cutover kapısı → rolling-restart flip (ARCH-Q5)

**Kapsanan:** BR-CUT-001/002, FR-D1/D2, US-D1/D2, ADR-0015, ARCH-Q5.

```mermaid
sequenceDiagram
    participant RJ as ReconciliationJob
    participant PG as Projeksiyon Postgres
    participant EDB as Engine DB (ACT_HI, read-only)
    participant CS as class_cutover_state
    participant CP as CutoverControlPlane
    participant KV as KV history-cutover-state
    participant ENG as Engine node replikaları

    loop günlük (cron)
        RJ->>PG: SELECT count(*) (sınıf-başına)
        RJ->>EDB: SELECT count(*) (read-only, DP-14)
        RJ->>RJ: fark hesapla (BA-Q2: audit-kritik=0, bulk=epsilon+trend)
        alt temiz
            RJ->>CS: clean_streak_days++
        else fark var
            RJ->>CS: clean_streak_days=0 [BUS_RECONCILIATION_DIFF_DETECTED]
        end
    end
    RJ->>CS: streak >= N (default 7g) → state=N_GUN_TEMIZ
    Note over CP: hacim-öncelikli sırada bu sınıfa sıra geldi
    CP->>CS: state=CUTOVER_TALEP
    CP->>KV: put cutover.<engineId>.<class>=true
    alt KV yazımı başarılı
        CP->>ENG: rolling-restart tetikle (deploy-spesifik)
        loop her replika
            ENG->>KV: loadAtBootstrap() [ClassCutoverStateRegistry]
            KV-->>ENG: cutover.<engineId>.<class>=true
            Note over ENG: internalDbDelegate ARTIK çağrılmıyor — ACT_HI yazımı=0
        end
        ENG-->>CP: health-check yeşil (tüm replikalar)
        CP->>CS: state=CUTOVERLANMIS, cutover_applied_at=now()
    else KV yazımı/restart-orkestrasyon başarısız
        Note over CP: SYS_CUTOVER_CONFIG_APPLY_FAILED — dual-run DEVAM (fail-safe)
        CP->>CS: state=N_GUN_TEMIZ (değişmeden kalır)
    end
```

---

## 6. Erasure kapsam-onayı akışı (BA-Q6)

**Kapsanan:** BR-PII-002/005, FR-G2, US-G2, ADR-0017.

```mermaid
sequenceDiagram
    participant REQ as Talep sahibi (DPO/subject)
    participant EP as ErasurePipeline
    participant SR as ErasureScopeResolver
    participant PG as Projeksiyon Postgres (erasure_scope_confirmation)

    REQ->>EP: requestErasure(subjectKey, scope)
    EP->>EP: sınıf audit-kritik mi?
    alt audit-kritik
        Note over EP: BUS_ERASURE_REQUEST_LEGAL_HOLD_BLOCKED — reddedilir, pseudonymization alternatifi sunulur
    else bulk
        EP->>SR: resolve(subjectKey)
        SR->>PG: SELECT aday instance/zaman-aralığı (businessKey eşleşmesi)
        alt tek zaman-aralığı/instance kümesi
            SR-->>EP: RESOLVED (net)
            EP->>EP: executeAnonymization(...) [doğrudan]
        else birden fazla dönem (telco MSISDN churn)
            SR->>PG: INSERT erasure_scope_confirmation (candidate_instances)
            SR-->>EP: AMBIGUOUS [VAL_ERASURE_SUBJECT_KEY_AMBIGUOUS]
            EP-->>REQ: aday liste sunulur, açık onay istenir
            REQ->>SR: confirmScope(requestId, confirmedInstanceIds)
            SR->>PG: UPDATE erasure_scope_confirmation (status=CONFIRMED)
            SR-->>EP: onaylı kapsam
            EP->>EP: executeAnonymization(confirmedScope)
        end
    end
```

---

## 7. Retention job + audit-log

**Kapsanan:** BR-PII-001, FR-G1, US-G1, ADR-0018.

```mermaid
sequenceDiagram
    participant RE as RetentionEnforcementJob
    participant PG as Projeksiyon Postgres
    participant AL as retention_audit_log

    loop scheduled (cron)
        RE->>PG: süresi dolan partition'ları tara (sınıf-bazlı pencere)
        alt satır bulundu (partition süresi doldu)
            RE->>PG: DETACH/DROP PARTITION
            PG-->>RE: OK
            RE->>AL: INSERT (class, partition_name, action, performed_at)
            alt audit-log yazımı başarılı
                AL-->>RE: OK
            else audit-log yazımı BAŞARISIZ
                AL--xRE: yazım hatası
                Note over RE: SYS_RETENTION_AUDIT_LOG_WRITE_FAILED — CRITICAL, on-call page (silme oldu, izi yok)
            end
        else job'ın kendisi DB hatasıyla başarısız
            PG--xRE: SQL exception
            Note over RE: SYS_RETENTION_JOB_FAILED — log-only, sonraki periyotta tekrar
        end
    end
```

---

## 8. Pseudonymization: tx-içi saf-hesap → downstream kasa-persist (BA-Q5)

**Kapsanan:** BR-PII-003/004, FR-G3, US-G3, ADR-0016, ARCH-Q2.

```mermaid
sequenceDiagram
    participant H as NatsHistoryEventHandler
    participant PT as PseudonymTokenGenerator (saf, I/O yok)
    participant OW as CompactHistoryOutboxWriter
    participant EDB as Engine DB
    participant R as HistoryOutboxRelay
    participant C as HistoryProjectionConsumer
    participant PG as Projeksiyon Postgres
    participant VC as PseudonymizationVaultClient
    participant VDB as Pseudonym Kasası (AYRI Postgres)

    H->>PT: generate(userId, tenantKeyId, version) [tx-içi, saf, I/O YOK]
    PT-->>H: pseudonym_token (deterministik)
    H->>OW: write(event + pseudonym_token) [BR-HDL-003 ile AYNI tx]
    OW->>EDB: INSERT compact_history_outbox
    EDB-->>H: COMMIT
    R->>EDB: relay (Diyagram 1 ile aynı akış)
    R-->>C: history.<...> (pseudonym_token dahil)
    C->>PG: upsertEntity (operation_log_history.user_id=pseudonym_token, user_id_pseudonymized=true)
    C->>VC: persistMapping(pseudonym_token, engineId, realUserId, version) [downstream/async, AYRI hat]
    alt kasa erişilebilir
        VC->>VDB: INSERT pseudonym_map (idempotent) + vault_access_audit(WRITE)
        VDB-->>VC: OK
        Note over C: BUS_PSEUDONYMIZATION_APPLIED
    else kasa erişilemez
        VDB--xVC: connection fail
        Note over VC: SYS_PSEUDONYM_VAULT_UNAVAILABLE — downstream retry, audit-kritik outbox/relay/NATS akışı ENGELLENMEZ (BA-Q5)
    end
```

---

## Doğrulama Kaydı

`mmdc` (`@mermaid-js/mermaid-cli` **11.16.0**, mevcut) ile bu belgedeki 8 diyagram, geçici `.mmd` dosyalarına ayrıştırılıp tek tek render edildi. **İlk koşu 6/8 geçti, 2 hata bulundu ve düzeltildi** (basamak-1'in "Mermaid mmdc-temizliği" dersi burada da geçerli oldu):
1. Katılımcı etiketlerinde `&lt;`/`&gt;` HTML-entity kullanımı parser'ı kırdı (`Parse error ... got 'NEWLINE'`) — basamak-1'in kanıtlanmış deseni (`docs/sentinel/phase4/SEQUENCE_DIAGRAMS.md`: `participant JSJ as JetStream jobs.<topic>`, raw açı-parantez) izlenerek tüm `&lt;...&gt;` → `<...>` düzeltildi.
2. `Note over X: ...; ...` içindeki noktalı virgül (`;`) Mermaid sequence-diagram parser'ında ifade-sonlandırıcı olarak yorumlanıyor (minimal-repro ile doğrulandı: `Note over A: text; more` tek başına parse hatası üretiyor) — iki `Note` satırındaki `;` → `,` değiştirildi (§2, §8).

Düzeltmeler sonrası **8/8 hatasız SVG üretimi** (0 sözdizimi hatası). Komut: `mmdc -i <diagram-N>.mmd -o <diagram-N>.svg` — hiçbiri stderr'e hata yazmadı, hepsi geçerli SVG üretti (dosya boyutları 28-37KB arası, > 0 byte doğrulandı). Geçici dosyalar temizlendi (kalıcı artifact bırakılmadı — yalnız bu doğrulama kaydı kalır).

**Sınıf-adı tutarlılığı:** bu belgedeki tüm uygulama-sınıfı katılımcı adları (`NatsHistoryEventHandler`, `CompactHistoryOutboxWriter`, `HistoryOutboxRelay`, `HistoryPostCommitPublisher`, `HistoryProjectionConsumer`, `ProjectionStore`, `HistoryDlqConsumer`, `HistoryDlqInspectionConsumer`, `ReconciliationJob`, `CutoverControlPlane`, `ErasurePipeline`, `ErasureScopeResolver`, `RetentionEnforcementJob`, `PseudonymTokenGenerator`, `PseudonymizationVaultClient`) `lld/history-offload/03_classes/*.md`'deki sınıf tanımlarıyla BİREBİR eşleşir (sayaç: 15/15 katılımcı sınıf çapraz-kontrol edildi; DB/infra katılımcıları — Engine DB, Projeksiyon Postgres, KV bucket'lar, JetStream stream'leri, `class_cutover_state`/`retention_audit_log` tabloları — bu sayıma dahil DEĞİL, `DB_SCHEMA.md`/`DB_ACCESS_MAP.md`'de ayrıca tanımlı).
