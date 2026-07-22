# Basamak-4: Outbound Handoff (giden mesaj/event → NATS, dual-write güvenli)

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Zincirdeki yeri:** `05-db-offload-strategy.md §6.7` basamak **4** (basamak-1 dispatch v0.2.0 ✅, 2 history v0.3.0 ✅, 3 büyük-değişken v0.4.0 ✅ üstüne).
**Amaç:** Sentinel/lean planlama girdisi — kanıt tabanı + kilitli kararlar (docs/07/08 deseni).
**Durum:** Kararlar D-A'…D-G' KİLİTLİ (2026-07-22). Kanıt fork motorundan + basamak-1/2 kodundan file:line doğrulanmış.

> **Tek cümlelik tez:** Process instance'ının dışarıya yaydığı iş mesaj/event'leri (BPMN message-throw, send-task; Flowable outbound Event Registry), engine DB commit'iyle NATS publish arasındaki **dual-write riskini** çözerek NATS'a teslim edilir: **kritik → tx-içi outbox + leader-relay (at-least-once)**, **best-effort → post-commit publish (at-most-once)**. Mekanizma **saf-SPI** (fork'a dokunulmaz); silinen outbound JavaDelegate'lerin transactional-güvenli yerine geçeni.

---

## 1. Karar özeti (kilitli, 2026-07-22)

| # | Karar | Kilit |
|---|---|---|
| **D-A'** | **Dikiş = ExecutionListener/delegate-stili.** Tenant BPMN'e `camunda:executionListener event="end" delegateExpression="${natsOutboundPublisher}"` (ya da message-throw/send-task'ın delegate'i olarak) ekler; listener **publish ETMEZ** — kritiklik'e göre ya tx-içi outbox yazar (kritik) ya post-commit `TransactionListener` kaydeder (best-effort). Silinen `NatsPublishDelegate`'in transactional-güvenli halefi. | A2-reuse (Cockpit'te sahte external-task) + özel-BpmnParseListener (en çok kod) REDDEDİLDİ. |
| **D-B'** | **Kapsam = Message-throw + Send-task.** Yalnız gerçek dış-I/O ihtiyacı olan uygulama-güdümlü noktalar. **Signal/Escalation KAPSAM DIŞI** — motor-içi (DB-yerel `ACT_RU_EVENT_SUBSCR`), dış yayın değil; cross-shard propagasyonu **basamak-5 (NATS router)** meselesi. | Signal/escalation dahil REDDEDİLDİ. |
| **D-C'** | **Kritiklik = config-split (kritik / best-effort).** Tenant outbound mesaj tipini işaretler; kritik → outbox+relay (at-least-once), best-effort → post-commit (at-most-once). Basamak-2 `HistoryClassificationProperties`/`isAuditCritical` deseni şablon. D2 kilitli kararının doğrudan uygulaması. | Tek-desen (hep outbox) REDDEDİLDİ. |
| **D-E'** | **Subject şeması = `events.<engineId>.<type>.<processInstanceId>`.** Basamak-2 history subject şeklinin izdüşümü; instance-anahtarlı (aynı instance aynı subject → sıra korunur). `events.*` öneki namespace-guard'a eklenir (`VAL_TOPIC_NAMESPACE_COLLISION`, BAQ-4 emsali). `jobs.*` A2'ye rezerve — kullanılamaz. | `events.<type>` düz-şema REDDEDİLDİ (per-instance sıra kaybı). |
| **D-F'** | **Outbox = ayrı `outbound_message_outbox` tablosu, aynı desen.** relay/lease/publish-then-delete iskeleti basamak-2'den transplante; şema outbound'a özgü. `compact_history_outbox` history-özel kolonlar taşıdığından karıştırılmaz (tek-amaçlı şema). | compact_history_outbox genişletme REDDEDİLDİ. |
| **D-G'** | **Flowable outbound basamak-4'te paralel SAĞLAMLAŞTIRILIR.** `NatsOutboundEventChannelAdapter.sendEvent` bugün çıplak `connection.publish` (ne JetStream, ne ack, ne DLQ, ne post-commit) → Camunda outbound'la aynı dayanıklılık kontratına getirilir. | Erteleme (4b) REDDEDİLDİ — basamak-2 D-G'den farklı: orada yeni subsistem vardı, burada mevcut yüzey düzeltmesi. |

**Düşen:** D-D' (tamamlama modeli) — yalnız A2-reuse seçilseydi geçerliydi; ExecutionListener seçildiğinden konu-dışı (ext-task/complete kavramı yok).

---

## 2. Kanıt tabanı (fork + basamak-1/2, file:line doğrulanmış)

### 2.1 Motorun native outbound'u YOK — outbound doğası gereği tenant-tanımlı
- Message-throw (`parseIntermediateThrowEvent`, `BpmnParse.java:1603-1694`) ve Send-task (`parseSendTask`, `:2505-2541`) fork'ta "service-task-like" (CAM-436/942) → `parseServiceTaskLike` (`:2264-2304`).
- Tenant `class`/`delegateExpression`/`expression`/`type=external` VERMEZSE **parse-time hata** (`validateServiceTaskLike`, `:2306-2316`: "One of the attributes 'class', 'delegateExpression', 'type', or 'expression' is mandatory"). → "Şeffaf intercept" mümkün değil; basamak-4 tenant'a **dayanıklı bir outbound yolu sunar**, gizli-yakalamaz.

### 2.2 SPI dikişi — fork değişikliği GEREKMEZ (basamak-2/3 sonucuyla aynı)
- `AbstractBpmnParseListener` `parseSendTask` (`:91`) + `parseIntermediateThrowEvent` (`:121`) override noktaları var (BpmnParse çağırıyor: throw `:1681-1683`, send `:2518-2520`). Basamak-1 `A2BpmnParseListener` yalnız `parseServiceTask`'i (`camunda-nats-channel/.../a2/A2BpmnParseListener.java:42-60`) override etmiş.
- `ExecutionListener.notify(DelegateExecution)` command-context/tx içinde koşar → `Context.getCommandContext().getTransactionContext().addTransactionListener(TransactionState.COMMITTED, ...)` (arayüz `TransactionContext.java:49`, enum `TransactionState.java:25`, `preParseListeners` `ProcessEngineConfigurationImpl.java:687` — bağımsız doğrulandı). Basamak-1 `A2ExternalTaskBehavior.java:71-74` + basamak-2 `HistoryPostCommitPublisher` aynı deseni kullanıyor. **⚠️ ExecutionListener'ın CommandContext erişimi phase-planlamada file:line teyit edilecek** (standart Camunda davranışı, bu turda çapraz-doğrulanmadı).

### 2.3 Signal/Escalation motor-içi (kapsam-dışı gerekçesi)
- `ThrowSignalEventActivityBehavior.findSignalEventSubscriptions` (`:64-74`) yalnız lokal `EventSubscriptionManager`/`ACT_RU_EVENT_SUBSCR` sorgular — dış I/O yok. `ThrowEscalationEventActivityBehavior` execution hiyerarşisinde yukarı kabarcıklanır. → basamak-4 "outbound publish" (dış sistem) semantiğiyle örtüşmez; cross-shard ihtiyacı basamak-5 router.

### 2.4 Basamak-1/2 kesişimi — kalan kapsam
- Basamak-1 (A2): yalnız `type=external` **Service Task** (request-reply, worker cevap bekler). 7 eski outbound JavaDelegate SİLİNDİ (doğrulandı — `outbound`/`requestreply` paketi yok).
- Basamak-2: yalnız `ACT_HI_*` (history), outbound BPMN'e dokunmadı.
- **Basamak-4'e kalan:** (1) message-throw/send-task için ExecutionListener dikişi (hiç yok); (2) Flowable `NatsOutboundEventChannelAdapter.java:33-49` çıplak `connection.publish` sağlamlaştırması; (3) fire-and-forget ("reply beklenmez") — A2'nin `jobs.<topic>.reply` round-trip'i gerekmez.

---

## 3. Kod kapsamı özü (implementasyon girdisi)

- **`NatsOutboundPublisher`** (ExecutionListener/delegate, camunda+cadenzaflow ayna): `notify()` içinde sınıflandırır → kritik ise `OutboundMessageOutboxWriter` (tx-içi), best-effort ise post-commit `TransactionListener` publish. Publish payload/header = ortak wire-contract (`BpmHeaders` + `events.<engineId>.<type>.<processInstanceId>` subject).
- **`OutboundClassificationProperties`** (config): mesaj-tipi → kritik/best-effort (basamak-2 classification şablonu).
- **`OutboundMessageOutboxWriter` + `OutboundMessageRelay`** (kritik yol): basamak-2 `CompactHistoryOutboxWriter`/`HistoryOutboxRelay` deseninin transplantı — yeni `outbound_message_outbox` tablosu, `SweepLeaderLease` (basamak-1) yeniden kullanım, PubAck-sonrası-delete custody-transfer.
- **Best-effort yol** = basamak-1/2 post-commit publisher deseninin 3. kullanımı.
- **DLQ** = motor-agnostik `DlqPublisher` (nats-core) aynen; `events.*` için DLQ subject + namespace-guard rezervasyonu.
- **Flowable (D-G'):** `NatsOutboundEventChannelAdapter` → JetStream publish + PubAck + DLQ + (Flowable tx sınırına göre) post-commit/at-least-once ayrımı.
- **Bench:** `nats-bpm-bench` — outbound publish DB-yazım-op profili (post-commit=0 ek DB, kritik-outbox=≤1 satır/tx).

---

## 4. Phase-planlama/tasarımda doğrulanacak
1. `ExecutionListener.notify`'ın `Context.getCommandContext()` erişimi (§2.2 ⚠️) — file:line teyit.
2. Message **end-event**'lerinin (yalnız intermediate throw değil) aynı CAM-436 desenine tabi olup olmadığı (`parseEndEvent` message dalı okunmadı).
3. Flowable `sendEvent`'in motor tx'i İÇİNDE mi DIŞINDA mı koştuğu — Flowable dual-write riskinin gerçek büyüklüğü (`NatsOutboundEventChannelAdapter` çağıranı izlenmedi).
4. `parseSendTask`/`parseIntermediateThrowEvent`'e ExecutionListener yolunun send-task/message-throw için davranışsal denkliği (entegrasyon testiyle — kanıt "aynı kod yolu" seviyesinde).
5. Signal/escalation'ın basamak-5 cross-shard propagation önkoşulu (ayrı karar).

---

## 5. Yeniden-kullanım yüzeyi (basamak-1/2'den)

| Varlık | Basamak-4'e |
|---|---|
| Post-commit `TransactionListener` deseni (`A2ExternalTaskBehavior:71-74`, `HistoryPostCommitPublisher`) | Doğrudan (3. kullanım) — best-effort yol |
| Kompakt-outbox + leader-relay + `SweepLeaderLease` (`CompactHistoryOutboxWriter`, `HistoryOutboxRelay`) | Desen transplantı — `outbound_message_outbox` + `OutboundMessageRelay` (kritik yol) |
| Kritik/bulk sınıflandırma çerçevesi (`HistoryClassificationProperties`, `HistoryEventClassResolver`) | Şablon — `OutboundClassificationProperties` |
| `DlqPublisher` (nats-core, motor-agnostik) | Aynen — DLQ yolu |
| Wire-contract (`BpmHeaders`, subject aile deseni) | `events.<engineId>.<type>.<processInstanceId>` (basamak-2 history subject izdüşümü) |
| Ayna disiplini (camunda↔cadenzaflow byte-ayna) | Aynen — outbound sınıfları da aynalanır |

---

## 6. Yalın-yol implementasyon + konsolide review kapanışı (2026-07-22)

Basamak-4 "hızlı" yalın yürütüldü (phase1/3/4 atlandı; bu belge SPEC). Implementasyon 6 commit (`db49832`…`2499014`). **ADIM-0 (D-A' dikiş-temeli) TUTTU + çift-doğrulandı:** ExecutionListener.notify non-null CommandContext ile koşar (`CommandContextInterceptor` set-before/remove-finally; `AbstractEventAtomicOperation:59` aynı call-frame) + gerçek-engine integration testi. docs/09 §2.2 ⚠️ ve §4#2 (EndEvent extends ThrowEvent) kapandı. TEK konsolide fresh-context review (`docs/sentinel/step4/PHASE_REVIEW.md`): 🔴0 🟠1 🟡2 🟢2; ADIM-0 + 778/778 regresyon + güvenlik bağımsız teyitli. Kapanış:

| Bulgu | Karar/Kapanış |
|---|---|
| 🟠 **F-001** Flowable outbound (D-G') yalnız-DLQ, outbox yok → Camunda kritik-yolunun at-least-once garantisiyle asimetri | **YAZILI ACK (bilinen sınırlama):** kilitli D-G' kapsamı zaten "sağlamlaştırma=DLQ" (tam outbox-destekli at-least-once değil; 4b-erteleme reddedilmişti). Flowable kritik-outbound'un Camunda ile eşit dual-write garantisi, Flowable engine-source tx-boundary teyidini (§4#3; kaynak lokalde yok) önkoşul kılar. İzlenen borç; Camunda/CadenzaFlow kritik-outbound at-least-once GARANTİLİ (outbox+relay). |
| 🟡 **F-002** Flowable outbound metrikleri bağlanmamış | **DÜZELTİLDİ** (`2499014`): `flowableOutboundPublishedCount`/`DlqRoutedCount` publish-success/DLQ-route noktalarına bağlandı (subject/channelKey etiketi, düşük-kardinalite korundu). |
| 🟡 **F-003** messageType NATS subject-token'larına karşı doğrulanmıyor (bozuk/wildcard subject riski) | **DÜZELTİLDİ** (`2499014`): `OutboundSubjectBuilder` messageType'ı `^[A-Za-z0-9_-]+$`'e doğrular (`VAL_OUTBOUND_MESSAGE_TYPE_INVALID`); `.`/`*`/`>`/whitespace RED, publisher fail-safe skip+WARN (PII'siz). Noktalı örnekler (order.created→order_created vb.) düzeltildi. Test 18/18. |
| 🟢 **F-004/F-005** traceId taze-UUID (basamak-1 emsali, regresyon değil) + integration-test rollback-assert yok (atomiklik yapısal) | **İZLENİR** (NIT, aksiyon yok). |

**Güvenlik (review):** TEMİZ — dinamik-SQL yok (sabit-identifier+parametreli; tek interpolasyon BATCH_SIZE int); DP-1 PII-log yok (subject/type/id/hash); `events.*`/`dlq.events.*` namespace-guard'a eklendi, `jobs.*` A2-rezerve dokunulmadı; ayna byte-özdeş. **Reactor 778/778.**
