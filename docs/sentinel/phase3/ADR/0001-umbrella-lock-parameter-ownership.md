# ADR-0001 — Şemsiye kilit parametre sahipliği ve sayısal default'lar (W/M/S/ε/L)

- **Durum:** Kabul edildi (2026-07-14, Phase 3)
- **Karar sahibi (actor):** Architect (AI performs, Human validates)
- **İzlenebilirlik:** US-A5 → FR-A8/FR-A9 → BR-A2-006/BR-A2-007 → `VAL_UMBRELLA_LOCK_TOO_SHORT` (EXCEPTION_CODES §6) → docs/06 §5.4 (D-B) / Ek Matris 6
- **Kaynak devir notu:** phase1-review **NIT-3** — sayısal default'lar SRS'ten (gereksinim) bir ADR'ye (mimari karar) devredilir; SRS artık bu ADR'ye referans verir, sabit sayıları sahiplenmez.

## Bağlam

D-B (docs/06 §5.4, kilitli) redelivery için **tek otorite** olarak JetStream'i seçti; engine sentinel kilidi (`L`) rakip bir saat değil, JetStream teslimat bütçesini **kapsayan bir şemsiyedir**. Kanıt: `complete`/`handleFailure` yalnız `workerId` eşitliğini kontrol eder, lock-expiry'yi kontrol etMEZ (`HandleExternalTaskCmd.java:89-91`, phase1/2 doğruladı) → engine kilidi redelivery saati değildir.

Şemsiye koşulu (D-E rafinesi, review MAJOR-B düzeltmesi):

```
L ≥ M·W + Σbackoff + S + ε
```

Bu ADR beş parametrenin **sahibini, default'unu, türetme kuralını ve override politikasını** sabitler. Bu sayıların LLD/SRS'te değil tek bir mimari otoritede yaşaması gerekir; aksi halde bir parametre değişince (ör. W topic-başına büyütülünce) tutarsız L üretilebilir.

## Karar

### 1. Parametre kataloğu ve default'lar

| Parametre | Anlam | Default | Sahiplik / Not |
|---|---|---|---|
| **W** | `ack-wait` (JetStream consumer) | **30s** | Mevcut adapter paritesi (`JetStreamMessageCorrelationSubscriber.java:57`). Topic-başına override — uzun işli topic büyük W seçer. |
| **M** | `maxDeliver` | **4** (=3 retry) | Adapter deseni `maxDeliver+1` → DLQ (`:58,:75-77`). |
| **S** | Orphan-sweep periyodu | **120s** | Soğuk, read-only sweep aralığı (ADR-0002). |
| **ε** | Reply/complete işleme payı | **60s** | Inbound bridge işleme tamponu. |
| **Σbackoff** | nak backoff toplamı | **7s** | `2^(n-1)`s, n=1..M-1 → 1+2+4=7s (cap 30s; `calculateBackoff` `:204-208`). |
| **L** | Sentinel `lockDuration` | **320s** | Türetilir; alt sınır 307s, 13s marj. |

**L default türetmesi:** `M·W + Σbackoff + S + ε = 4·30 + 7 + 120 + 60 = 307s` (alt sınır) → default **320s**.

> İlk yazımdaki `L=300s` bu koşulu 7s ihlal ediyordu (Σbackoff atlanmıştı) — MAJOR-B düzeltmesi. Bu ADR alt sınırı formülden **türetir**, elle sabit tutmaz.

### 2. Türetme ve override kuralı
- `L` **W/M/S/ε/Σbackoff'tan türetilir**; operatör yalnız `W` (ve gerekirse `M/S/ε`) verir, `L` otomatik hesaplanır. Elle `L` verilirse formüle karşı doğrulanır.
- `W` topic-başına override edilebilir; override edilince `L` o topic için yeniden türetilir (Ek Matris 6, satır 2/3).
- `Σbackoff` worker `nakWithDelay` kullanıyorsa gerçek backoff toplamıdır; sabit desende 7s.

### 3. L-floor ihlali davranışı (BAQ-3, kilitli)
- **Default: reject-startup** — `L < M·W + Σbackoff + S + ε` ise `VAL_UMBRELLA_LOCK_TOO_SHORT` (ERROR) → topic aktivasyonu ENGELLENİR (bootstrap-time config validasyon katmanı, ADR-0007 config bileşeni).
- **Bilinçli kaçış:** `allow-unsafe-lock-duration=true` flag'i → config kabul edilir AMA her dispatch/sweep döngüsünde **kalıcı WARN** loglanır (sessiz "bir kere uyar" YOK).

### 4. Heartbeat yok (kilitli, D-H ertelendi)
`msg.inProgress()` ve engine `extendLock` **kullanılmaz**; W·M sert tavandır. WebSearch doğrulaması (bkz. HLD §11): `msg.inProgress()` ack-wait'i sıfırlar ve deliveryCount'u artırmaz — DOĞRU — ama fire-and-forget (nats.java #1042), teslimat garantisi vermez → sabit W·M bütçesi tercihi bu nedenle de sağlamdır. Uzun işli topic küçük W yerine **büyük W** seçer.

## Sonuçlar

**Olumlu:**
- Tek otorite: W/M/S/ε değişince L mekanik olarak yeniden türetilir → tutarsız L imkânsız (validasyon + türetme).
- Geç gelen complete her zaman başarılı (L ≥ teslimat bütçesi + işleme payı) → at-least-once bedeli güvenle yutulur.

**Olumsuz / kabul edilen:**
- Statik L: uzun-kuyruklu (uzun p99) topic'lerde W büyük seçilmeli; heartbeat olmadığı için tek ayar noktası W'dir (D-H sonrası dinamik uzatma gelene kadar).
- ε=60s ve S=120s tampon; kısa işli topic'lerde L (320s) orphan toplama gecikmesinin üst sınırını (~L+S ≈ 7dk, NFR-R3) belirler.

## Reddedilenler
- Eşitlik hizalaması (L = W·M): işleme payı ve sweep periyodu için pay bırakmaz → geç complete riski. **Reddedildi** (şemsiye ⊐ eşitlik).
- Elle sabit L (türetmesiz): W override'ında sessiz tutarsızlık. **Reddedildi**.
- L-floor'da yalnız-WARN: BAQ-3 hard-reject'i seçti. **Reddedildi**.
