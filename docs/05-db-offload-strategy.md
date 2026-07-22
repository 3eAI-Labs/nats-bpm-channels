# Strateji: NATS ile DB-Transaction Offload — İki Yollu Geliştirme

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Tarih:** 2026-06-21
**Durum:** Stratejik north-star / ADR seviyesi. **LLD değildir.**
**Bağlam:** `01-vision-roadmap.md` (vizyon), `project-thesis-db-offload` (tez memory), `04-async-request-reply-design.md` (async desen).

---

## 0. Bu belge nasıl kullanılır (Sentinel girişi)

Bu doküman **iki bağımsız geliştirme yolunu** (track) çerçeveler ve ikisinin ortak ilkelerini + açık kararlarını sabitler. Geliştirme **Sentinel** metodolojisiyle yürüyecek:

- Her track **kendi Sentinel pipeline'ına** girer (phase1 PO → phase2 BA → phase3 Architect → **phase3.5 Walking Skeleton** → phase4 LLD → phase5 Code → phase5.5 Test → phase6 Review).
- Her track'in **VISION**'ı bu belgenin ilgili bölümünden türetilir (Track A → §5, Track B → §6).
- Bu belge **kilitli kararları** (§3 ilkeler) ve **açık kararları** (§8) ayırır. Açık kararlar phase1/phase3 girdisidir.
- Padding yok: her bölüm bir karara veya bir kanıta hizmet eder. Doğrulanmamış iddialar §10'da işaretli.

---

## 1. Motivasyon (tez)

Camunda 7 / CadenzaFlow / Flowable **doğası gereği DB-transaction'a bağımlı**: her token hareketi, her wait-state, her değişken bir DB yazımı; her engine komutu bir JDBC transaction'ında koşar. **Çok yüksek yüklerde darboğaz tek DB'nin performansıdır.**

**Hedef:** NATS.io ile gelen yetenek bu DB-transaction bağımlılığından **mümkün olduğunca kopuk** olsun — ki motor DB'sinin throughput tavanına takılmadan yüksek-hacimli yükler taşınabilsin.

**Nüans (kritik, abartmamak için):** Motor stateful'dur; orchestration state-transition'ları (token nerede, hangi instance bekliyor) bir yerde **kalıcı** olmak zorunda. Soru "persistence'i yok etmek" değil, **"o persistence ilişkisel + per-command ACID transaction mı, yoksa partition'lı replayable log mu"** ve **"yüksek-hacimli iş (dispatch/fan-out/koordinasyon/history) DB transaction'ına yük bindiriyor mu"**. Hedef: state-transition tabanını minimize et, geri kalan her yükü NATS'a al.

---

## 2. Referans: Zeebe ne yapıyor + plugin/native sınırı

Zeebe (Camunda 8) bir özellik değil, **state motorunun baştan yeniden yazımıdır.** Dört direği:

1. **RDBMS yok, transaction yok** — state event-sourced, replike append-only log'a yazılır.
2. **Raft ile replike log + RocksDB** materialized state.
3. **Partition + partition-başına-tek-yazıcı** — lock yok, optimistic-lock retry yok, distributed transaction yok. Throughput buradan.
4. **Job worker'lar push/stream ile beslenir** (gRPC); motor sürecinde kullanıcı kodu koşmaz.
5. **Exporter** event log'u ES/query'ye taşır — history hot-path dışında.

**Sınır:** Zeebe'nin gücü **process state'in sahibinin motor olması** ve o sahipliğin RDBMS+transaction değil log+partition olması. Bir **plugin/channel motorun state çekirdeğinin dışında** oturur → state deposunu değiştiremez. Yani:

- **NATS = Zeebe'nin altyapı katmanının (log, Raft, KV, job-transport) substratı** — JetStream bunların çoğunu hazır verir.
- **NATS ≠ BPMN execution semantiği** — token akışı, gateway, timer, correlation, multi-instance... bunlar her hâlükârda yazılacak kod.

---

## 3. Kilitli mimari ilkeler (bu çalışmanın çıktısı)

Bunlar tartışıldı ve **kilitlendi**; her iki track de bunlara uyar.

- **P1 — Lock'u kaldıran single-writer'dır, push değil.** `SELECT FOR UPDATE` / fetchAndLock, paylaşımlı mutable DB satırları için yarışan **rakip acquirer'lar** yüzünden var. Push teslimat yarısıdır; contention'ı kaldıran **partition-başına-tek-yazıcı**dır. Lock-free ⇐ single-writer.

- **P2 — Completion-lock ancak state-ownership NATS'a geçerse kalkar.** Plugin modelinde motor token'ın sahibidir; reply gelince token'ı ilerletmenin tek yolu bir engine command = **DB transaction + optimistic lock**. NATS KV'de tutulan state bir *gölge* olur (dual-write). Completion transaction'ı yok etmek = state'i NATS'ta sahiplenmek = native engine. **Camunda'yı tutup completion transaction'ından kaçılan orta yol yoktur.** (Kanıt: `correlateWithResult` → engine command → optimistic-locked execution UPDATE; memory `sync-request-reply-holds-db-transaction`.)

- **P3 — Log = gerçek, KV = idempotent replayable projeksiyon.** Native modelde "transaction"ın yerini **stream consumer-offset + idempotent apply** alır. Çökme → son ack'li offset'ten replay. KV bir mini-RDBMS gibi multi-key atomik kullanılmaz.

- **P4 — NATS çifte görev yapar:** (a) dispatch/fan-out/koordinasyonu DB'den **offload** eder; (b) instance/mesajı sahibi partition/shard'a yönlendiren **router**'dır. İnşa edilen plugin, sharded fleet'te router olarak yeniden kullanılır.

- **P5 — Plugin, native engine'in tohumudur; çöp değildir.** İki track **ortak wire-contract** (§7) paylaşır: job stream formatı, header'lar, correlation-id şeması, worker protokolü. Track B'nin NATS-tarafı işi ve worker ekosistemi Track A'ya taşınır.

- **P6 — Sharding co-location ister.** Bir process instance'ın tüm state'i tek shard'da olmalı (shard key = instance-root), yoksa cross-shard **2PC** kazancı yer. Instance'lar arası mesajlaşma bir routing meselesidir (NATS).

---

## 4. Üç-opsiyon spektrumu → iki track eşlemesi

| Opsiyon | Ne | DB-tavanı | Risk | Track |
|---|---|---|---|---|
| **#1** Plugin + tek DB | dispatch/blocking offload, tek DB | öteler, kaldırmaz | düşük | Track B (başlangıç) |
| **#1.5** Plugin + DB sharding (NATS router) | per-instance yazımı yatay ölçekler, engine rewrite yok | yatay ölçek (ACID kalır) | orta | Track B (hedef) |
| **#3** NATS-native engine | log-yapılı, DB'siz, partitioned, replayable | **kaldırır** | yüksek (2 merkez) | Track A |

> **#2 (distributed-SQL-under-one-engine)** Track B'nin bir alt-varyantıdır; §6.4'te tartışılır. Genelde app-level sharding (#1.5) BPM için daha temizdir (2PC yok).

İki track **rakip değil, paralel ve birbirini besleyen** bahislerdir:
- **Track A** = greenfield ürün; Zeebe ile doğrudan yarışır; "sıfır DB lock" hedefine ulaşan tek yol.
- **Track B** = brownfield; müşterinin mevcut Camunda 7/CadenzaFlow yatırımı üstünde değer; rewrite'sız ciddi offload; A'nın contract'larını doğrular ve tohumlar.

> **GÜNCELLEME (2026-06-21, D1=iii):** Bu iki-track çerçevesi **tek kademeli omurgaya** birleşti — bkz **§6.7**. Track B = merdiven basamak 1–5; Track A = basamak 6'nın gerçekleştirme biçimi (6b). §5/§6 bu mercekle okunmalı.

---

## 5. Track A — NATS-native workflow engine (sıfırdan) — *(= merdiven basamak 6 / state-core; bkz §6.7)*

### 5.1 Hedef
DB-transaction'sız, partition'lı, replayable bir workflow runtime. Camunda 7 uyumluluğu **hedef değildir.** Adı: **NATS-native workflow engine** (plugin değil).

### 5.2 Mimari eşleme (Zeebe direği → NATS primitifi)

| Zeebe direği | NATS karşılığı | Durum |
|---|---|---|
| Replike append-only log | JetStream stream (Raft-replike) | ✅ hazır |
| Raft consensus | JetStream'in kendi Raft'ı | ✅ hazır (Zeebe elle yazdı, biz yazmıyoruz) |
| Partition | stream/subject partition'ı (instance-key) | ✅ hazır |
| RocksDB materialized state | JetStream KV ya da log'dan üretilen embedded projeksiyon | 🟡 projeksiyon disiplini bizde |
| Job streaming (ActivateJobs) | JetStream consumer (`jobs.<type>`, durable) | ✅ hazır, dilden bağımsız |
| Exporter → query store | event stream'i okuyan ayrı consumer | ✅ hazır |
| Idempotent ingest | `Nats-Msg-Id` dedup (pencereli) + apply-zamanı idempotency | 🟡 |

### 5.3 Linchpin: single-writer-per-partition
- Her partition'ın command stream'ini **sıralı tüketen tek lider** processor.
- Teklik bir **leader lease** ile sağlanır (JetStream KV TTL'li kira / NATS lock). İki processor aynı partition'a binerse → split-brain → state bozulur. **Doğruluk-kritik nokta.**
- Akış: komut log'a append → tek-yazıcı sıralı işler → state'i KV/embedded'a uygular → sıradaki job'ı push eder.

### 5.4 Walking skeleton (phase3.5 — ZORUNLU, phase4'ten önce)
> Tek partition · command stream'i sıralı tüketen **tek-lider** processor · KV projeksiyonu · 2-task'lık trivial process (start → service task → end) · `jobs.x`'ten pull eden bir worker · complete komutunu log'a geri yazması. **Hiç RDBMS yok.**

Bu dilim "log=truth + single-writer + job-push/complete" üçgenini kanıtlar. Doğruysa geri kalan = bunun üstüne BPMN zenginliği.

### 5.5 Devraldığın defter (motorun bedava verdiği, artık senin)
1. **Atomicity/recovery** — `kv.put` transaction değil; disiplin P3 (log=truth, offset=commit, replay).
2. **Duplicate/orphan reply** — at-least-once; ikinci teslim no-op; erken-reply vs orphan ayrımı (async doc §5) ama **engine boundary-timer fallback'i yok**, onu da sen kurarsın.
3. **Timer/timeout** — wall-clock değil, **log'a yazılan deterministik scheduled command** (replay doğruluğu).
4. **Snapshot + log compaction** — her recovery'de tüm log'u replay etmemek için.
5. **Partition assignment / rebalancing** — partition sayısı = paralellik; lider taşıma şeması.

### 5.6 Risk merkezleri (dürüst)
Risk **substratta değil** (JetStream log/Raft/KV/job-transport düşük risk, kanıtlı). Risk iki yerde yoğun:
- **(a) single-writer-per-partition doğruluğu** (split-brain = korupsiyon).
- **(b) deterministik BPMN execution motoru** (işin ağırlığı; NATS sıfır yardım eder).
De-risk yolu: BPMN zenginliğinden önce §5.4 walking skeleton'da (a)+log-as-truth kanıtlanır.

### 5.7 Sentinel girişi
phase1 VISION = "Zeebe-class engine on NATS". phase3.5 walking skeleton **zorunlu gate**. State store seçimi (KV vs embedded), partition-assignment ve leader-lease mekanizması = phase3 ADR'leri (§8 D5).

---

## 6. Track B — Mevcut plugin, maksimum offload — *(= merdiven basamak 1–5; bkz §6.7)*

### 6.1 Hedef
Motoru (Camunda 7 / CadenzaFlow) **ayakta tutarak**, BPMN'in DB yükünü **mümkün olduğunca** NATS'a almak. Rewrite yok; off-the-shelf motor + (opsiyonel) sharding ile getiri.

### 6.2 Ne offload EDİLEBİLİR

| Yük | Mekanizma | Durum |
|---|---|---|
| Dispatch / external-task acquisition | NATS push (publish→subscribe) → fetchAndLock + acquire-lock **kalkar** | ✅ desen var (header track ✅) |
| Request-reply blocking | async receive deseni (send + wait-state + inbound correlate) | ✅ tasarlandı — `04-async-request-reply-design.md` |
| **History (`ACT_HI_*`)** 🆕 | custom `HistoryEventHandler` → history event'leri NATS'a, query-store'a (ES/read-replica) **async projeksiyon** (Zeebe exporter muadili) | 🟡 fork'ta doğrula (§10) — **en yüksek kaldıraç** |
| **Büyük değişkenler** 🆕 | payload'ı NATS Object Store/KV'ye, motorda **referans** tut | 🟡 `ACT_RU_VARIABLE`/`ACT_HI_DETAIL` hacmini düşürür |
| Outbound publish | **post-commit** transaction listener (transaction içinde değil, commit sonrası) | 🟡 trilemma — §8 D2 |

### 6.3 Ne offload EDİLEMEZ — Track B'nin kesin tavanı
**Completion / token-move transaction.** Motor token'ın sahibi olduğu sürece (P2), reply/event gelince token'ı ilerletmek bir engine command = kısa bir DB transaction + optimistic lock. **Bu kalkmaz.** Track B'nin dürüst tanımı:

> **dispatch + polling + locking + blocking I/O + history + büyük değişken → hepsi NATS'a;**
> **token-move DB'de kalır, contention'ı sharding (P6) + execution-local correlation ile daralır.**

Track B bundan fazlası diye **satılmamalı.** "Sıfır DB lock" Track A'nın işidir.

### 6.4 Yatay ölçek: sharding (opsiyon #1.5)
- **App-level (önerilen):** N engine + N DB, **NATS router** business-key ile sahibi shard'a yönlendirir. 2PC yok (her shard tek-shard, yapı gereği). Partition birimi = engine+DB bütünü (Zeebe partition fikrinin off-the-shelf hâli). Plugin = hem offload hem router (P4).
- **Distributed-SQL-under-one-engine (#2):** Citus/CockroachDB/Yugabyte; şeffaf ama co-location şart, cross-shard 2PC riski, motor shard-aware değil. Genelde app-level daha temiz.

### 6.5 Pivotal açık karar: Track B derinliği (§8 D1)
CadenzaFlow **bizim fork'umuz** → üç derinlik mümkün:
- **(i) Channel/SPI-yüzeyi:** delegate + subscriber + config. Güvenli, mevcut.
- **(ii) Motor SPI'leri:** custom `HistoryEventHandler`, custom variable serializer, custom job handler. Daha derin ama desteklenen extension point'ler.
- **(iii) Fork'un motor kodunu değiştirmek:** plugin'in yapamadığını yapar (örn. completion path'ini kademeli NATS'a taşımak, optimistic-lock batch'leme). **(iii) açıksa Track B, Track A'ya yakınsamaya başlar.**

Bu karar Track B'nin phase3 mimarisinin şeklini belirler.

### 6.6 Sentinel girişi
phase1 VISION = "max DB-offload on existing engine". Derinlik (D1) phase1'de netleşmeli — phase3 mimarisi ona bağlı. History offload + sharding ayrı phase3 ADR'leri.

### 6.7 Kademeli geçiş (strangler) merdiveni — BİRLEŞİK PLAN

**Bu bölüm §4–§6'daki "iki ayrı track" çerçevesini birleştirir** (karar 2026-06-21, D1=iii fork-modify açık). Artık **tek bir kademeli omurga** var: mevcut plugin'den başla, CadenzaFlow fork'unu basamak basamak değiştirerek her sorumluluğu DB'den NATS'a taşı. **Track B = basamak 1–5; Track A = basamak 6'nın gerçekleştirme biçimi (6b).** Her basamak bağımsız shippable, değer katar, motoru kırmaz; risk düşükten yükseğe sıralı.

| # | Basamak | DB'den kalkan | Katman | Risk | D2 |
|---|---|---|---|---|---|
| **0** | Header + async tasarım | — (mevcut zemin) | channel | ✅ bitti/uçuşta | — |
| **1** | **Dispatch push** (external-task polling → NATS push) | fetchAndLock + acquire-lock | SPI/channel | düşük | — |
| **2** | **History offload** (custom `HistoryEventHandler` → NATS → async query-store) | `ACT_HI_*` (en büyük hacim) | SPI | orta (fork doğrula §10) | — |
| **3** | **Büyük değişken externalization** (Object Store/KV + referans) | `ACT_RU_VARIABLE`/`ACT_HI_DETAIL` hacmi | SPI | düşük-orta | — |
| **4** | **Outbound handoff** (önce post-commit/handoff → sonra log-projeksiyon) | dual-write riski | SPI→core | orta | **burada yaşar; 6'da çözülür** ✅ **v0.5.0** |
| **5** | **DB sharding + NATS router** (N engine+DB, business-key route) | completion-lock **contention domeni** | app-level | orta | ⏸️ **TALEP-GÜDÜMLÜ (karar 2026-07-22)** |
| **6** | **State/completion core** (fork-modify: state-transition'lar log'a, single-writer) | **token-move transaction'ın kendisi** | **fork core** | **yüksek** | **burada buharlaşır** |

> **Sürüm izi:** basamak 1 v0.2.0 ✅ · 2 v0.3.0 ✅ · 3 v0.4.0 ✅ · 4 v0.5.0 ✅ (hepsi GitHub'da; Maven Central publish OSSRH+GPG secret bekliyor).

**Basamak-5 = TALEP-GÜDÜMLÜ opsiyonel scale-out (KARAR 2026-07-22).** Gerekçe: bu bir **açık-kaynak kütüphane** (herkes farklı amaçla kullanır; bottleneck bilinmez). Basamak 1–4 evrensel/opt-in/şeffaf DB-offload — adopte eden herkese, bottleneck'inden bağımsız fayda eder ve NATS-taşınmış işler NATS/JetStream ile ölçeklenir. Basamak-5 ise **NATS işlerini hızlandırmaz**; yalnız basamak-1-4 sonrası DB'de kalan token-move/completion **contention'ını** ~lineer shard'lar (N engine+DB, cross-shard router). Bu **deployment-topolojisi/ops kalıbıdır**, kütüphane-özelliği değil — kullanıcıların çoğunluğu tek engine+DB koşar ve gerek duymaz. Bu yüzden **zorunlu bir halka DEĞİL, talep gelince (gerçek kullanıcı token-move DB-tavanını aştığında) yapılacak/değerlendirilecek opsiyonel bir kalıp.** Kütüphane kullanıcıya kendi bottleneck'ini ölçme aracını zaten veriyor (per-basamak SLI/metrik + `nats-bpm-bench`). Signal/escalation cross-shard propagasyonu da (basamak-4'te kapsam-dışı bırakıldı) bu talep-güdümlü router işine bağlıdır.

Basamak 1–4 **pişmanlıksız**: çoğu SPI/çevresel, motoru gutting etmeden ciddi DB yükü kaldırır (2-history devasa), sürekli ship edilir. Basamak-5 talep-güdümlü (yukarı); basamak-6 ayrı bahis (native motor).

**Başlangıç sırası — KARAR (2026-06-21): dispatch (1) önce.** Gerekçe ("vurucu" değil, kritik-yol):
- **Bağımlılıksız + contract-kurucu:** Dispatch ortak wire-contract'ı (§7) kurar; sonraki her basamak onu yeniden kullanır (P5). History bir query-store + fork doğrulaması (§10) bekler → bağımlılık taşır.
- **Zaten uçuşta:** async request-reply tasarımı (`04-async-request-reply-design.md`) dispatch-offload mekanizmasının ta kendisi — soğuk başlangıç değil.
- **P1'i en düşük riskle kanıtlar:** acquire-and-lock'u kaldırır = tezin merkez iddiası, channel/SPI yüzeyinde, fork'a dokunmadan.
- **History paralel:** bağımsız (SPI); fork doğrulaması yeşillenince dispatch'i beklemeden başlar. "Dispatch önde, history doğrulanınca paralel" — katı seri değil.

**Basamak 6 — dürüst fork-point (ŞİMDİ seçilmiyor):**
- **(6a) Fork içinde büyüt** — persistence'ı parça parça log'a çevir; tek codebase ama framework'ün transactional-session kabulleriyle (CommandContext/DbSqlSession/optimistic-lock çekirdeğe örülü) savaş riski.
- **(6b) Clean-room state core + model reuse** — fork'un BPMN parse/model katmanını koru, yeni log-tabanlı execution core üstünde çalıştır = Track A'nın çekirdeği, sıfırdan BPMN semantiği değil.
- **Gate:** §5.4 walking skeleton (tek partition + tek-lider + log=truth) **yeşil olmadan** basamak 6 başlamaz. 6a/6b kararı, 1–5 değer üretirken + skeleton de-risk ettikten **sonra** verilir.

**D2 bu merdivende çözülür:** basamak 4 interim posture (kritik mesaj → dayanıklı handoff/at-least-once, external-task gibi; kritik değil → post-commit/at-most-once); basamak 6'da state log'a geçince outbound = log projeksiyonu (exporter gibi) → dual-write buharlaşır, ekstra DB sıfır.

---

## 7. Ortak wire-contract (iki track'i bağlayan — P5)

Her iki track **aynı tel sözleşmesini** yayar/tüketir; böylece worker ekosistemi ve Track B'nin işi Track A'ya tohum olur:

- **Job stream:** subject şeması (`jobs.<type>`), payload zarfı.
- **Header'lar:** mevcut `BpmHeaders` genişler — `X-Cadenzaflow-Trace-Id`, `-Business-Key`, `-Idempotency-Key`, (+async) `-Correlation-Id`, `-Reply-Subject`.
- **Correlation-id şeması:** idempotency-key yeniden kullanımı (async doc §3, karar §9.2).
- **Worker protokolü:** request/job → reply/complete; correlation-id birebir echo; ack/nak/DLQ semantiği.
- **DLQ:** `dlq.<subject>`, `maxDeliver`, `nakWithDelay`.

**Kısıt:** Track A native engine'i bu contract'ı **dışarı aynı** verir; Track B worker'ları değişmeden Track A'ya bağlanabilmeli.

---

## 8. Açık kararlar (Sentinel phase1/phase3'te çözülecek)

- **D1 — Track B derinliği:** ✅ **ÇÖZÜLDÜ (2026-06-21)** = (iii) fork-modify açık + kademeli geçiş (§6.7). Track'ler tek omurgaya birleşti; başlangıç basamağı = dispatch (1).
- **D2 — Outbound:** ✅ **ÇÖZÜLDÜ** = rung-bağımlı (§6.7). State DB'deyken interim: kritik → dayanıklı handoff (at-least-once, external-task gibi), kritik değil → post-commit (at-most-once). State log'a geçince (basamak 6) → log projeksiyonu, dual-write buharlaşır. Not: gerçek XA 2PC DB+NATS arası YOK (NATS XA resource değil).
- **D3 — Track B başarı metriği:** "tek DB tavanını Nx ötele" mi (sharding kapsam dışı) yoksa "yatay ölçek" mi (sharding kapsam içi)?
- **D4 — History query-store hedefi:** ES mi, Postgres read-store mu, başka mı? Projeksiyon şeması.
- **D5 — Native state store:** JetStream KV mi, log'dan beslenen embedded (RocksDB/LMDB) mi? + partition-assignment + leader-lease mekanizması.
- **D6 — Track ilişkisi:** ✅ **ÇÖZÜLDÜ** = tek kademeli omurga (§6.7); Track A = basamak 6'nın (6b) gerçekleştirmesi, ayrı ada değil. Contract-coupled (P5/§7).

---

## 9. Kapsam dışı / sonraki turlar
- Flowable için native/offload eşlemesi → ✅ **ele alındı: `06-external-task-over-jetstream.md`** (Event Registry yolu; A2 retrofit edilmez).
- BPMN modeler template'leri (async-request-reply şablonu).
- OTel/tracing context'inin reply/job path'ine taşınması.
- ~~Senkron `NatsRequestReplyDelegate` kaldırılması (fast-RPC için kalır; README'de kısıt).~~ → **DEĞİŞTİ (2026-06-28):** tüm JavaDelegate'ler (senkron dahil) **tamamen phase-out** — in-transaction blocking tezi ihlal ediyor (`docs/06 §1, §3`). Fast-RPC istisnası **kaldırıldı.**
- gRPC worker ön kapısı (Zeebe-uyum / broker'sız-kısıtlı worker) — opsiyonel, ayrı belge (`docs/06 §9 D-G`).

---

## 10. Doğrulama notları (kanıt-temelli — phase3 girdisi)
- **History pluggability** → CadenzaFlow fork'unda `HistoryEventHandler` / `historyEventHandler` config doğrulanacak (`~/Workspaces/cadenzaflow`). Track B'nin en yüksek kaldıracı buna bağlı; assert değil, doğrula.
- **JetStream KV limitleri** (kardinalite/throughput) → state store kararı (D5) somutlaşınca ölçülecek.
- **Completion-lock iddiası** → zaten kanıtlı: `correlateWithResult` → engine command → optimistic-locked execution UPDATE (memory `sync-request-reply-holds-db-transaction`, engine `AbstractTransactionInterceptor`/`CommandContext`).
- **`Nats-Msg-Id` dedup penceresi** → kalıcı idempotency için tek başına yetmez; apply-zamanı idempotency (P3) şart.
