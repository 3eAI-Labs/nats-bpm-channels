# ADR-0003 — Sweep re-lock→publish atomiklik yaklaşımı

- **Durum:** Önerildi — ARCH-Q1 Levent onayına bağlı (2026-07-14, Phase 3)
- **İzlenebilirlik:** US-A3 → FR-A5/FR-A6 → BR-A2-013/BR-A2-005 → `SYS_SWEEP_REPUBLISH_FAILED` (EXCEPTION_CODES §4) → BUSINESS_LOGIC §2.1 guard notu / BAQ-1
- **Devir:** BAQ-1 atomiklik mekanizmasını **açıkça Phase 3/4'e bıraktı**; bu ADR onu çözer (sıra kararını DEĞİŞTİRMEDEN).

## Bağlam

BAQ-1 (kilitli): sweep re-publish'inde sıra **SABİT — re-lock (DB yazısı) önce, JetStream publish sonra.** İki adım arası çökme "kabul edilen nadir durum"dur; bedeli ≤ +L (320s) ek gecikme (kalıcı kayıp değil).

**Açık kalan kenar-durum:** re-lock BAŞARILI + publish BAŞARISIZ (broker down) → satır `LOCK_EXP_TIME_=now+L` ile "taze kilitli" görünür ama **hiçbir yere teslim edilmedi**. Sonraki sweep döngüleri (≤L ≈ 320s) satırı "in-flight" sanıp atlar (Karar Matrisi 3, satır 5) → gerçek orphan kendi taze kilidi yüzünden **görünmez** olur. BAQ-1 üst sınırı (≤+L) ve sıra kararını sabitledi; **mekanizma** phase3'e kaldı.

**Kısıt (kilitli kararla çelişme yasağı):** BAQ-1 sırayı re-lock→publish olarak sabitledi. "publish-önce-relock-sonra" gibi sırayı **flipleyen** çözümler bu kararla çelişir → seçilemez. Ayrıca engine `extendLock` süresi dolmuş kilidi uzatamaz (`ExtendLockOnExternalTaskCmd.java:44-47`) ve D-B `extendLock`'u yasakladı → probe-lock-sonra-extend deseni de kapalı.

## Karar (öneri — ARCH-Q1)

**Telafi edici kilit-serbestleştirme (compensating unlock), sırayı koruyarak.**

1. Sweep fetchable satırı bulur → `re-lock(SENTINEL, L)` (BAQ-1 sırası: önce).
2. `publish(jobs.<topic>)` (sonra). PubAck başarılıysa: normal (BR-A2-005 satır 1). **Bitti.**
3. **publish BAŞARISIZ** ise (broker down / PubAck yok) → **telafi:** satırın kilidini geri al — `lockExpirationTime = now` set et (kısa DB yazısı; native `unlock`/`SetExternalTaskRetriesCmd` deseniyle aynı yüzey). Satır bir sonraki `S` döngüsünde **yeniden fetchable-parite** sağlar → tekrar orphan sayılır.

**Etki:** Gecikme üst sınırı **≤ +L (320s)** yerine **≤ +S (120s)** olur; sıra (re-lock→publish) DEĞİŞMEZ (BAQ-1 uyumlu); ek DB yazısı yalnız **nadir publish-failure** dalında (hot-path'te SIFIR).

**Neden bu, "hiçbir şey yapma"ya (BAQ-1'in kabul ettiği ≤+L) tercih ediliyor:** carrier-grade'de broker kesintisi sonrası orphan görünmezlik penceresini S'ye daraltmak, en-yaşlı-orphan SLI'ını (US-D2) daha öngörülebilir kılar. Ancak "hiçbir şey yapma" da BAQ-1 tarafından **zaten onaylı** bir seçenektir → tercih Levent'in (ARCH-Q1).

## Alternatifler

| Yaklaşım | Sıra (BAQ-1) | Gecikme sınırı | Hot-path DB yazısı | Karar |
|---|---|---|---|---|
| **Telafi edici unlock** (önerilen) | re-lock→publish (korunur) | ≤ +S | 0 (yalnız fail dalında) | **Öneri** |
| Hiçbir şey yapma (BAQ-1 default) | re-lock→publish | ≤ +L | 0 | Kabul edilebilir (BAQ-1 onaylı) |
| publish-önce-relock-sonra | **flip — BAQ-1'i ihlal eder** | — | — | Reddedildi (kilitli sıra ile çelişir) |
| İki-fazlı commit (DB+NATS XA) | — | — | ağır | Reddedildi (docs/05 D2: NATS XA resource değil) |
| Kalıcı "published" marker kolonu | re-lock→publish | ≤ +S | +1 kolon/yazı hot-path'te | Reddedildi (outbox saflığını + hot-path yazısını bozar) |

## Sonuçlar

**Olumlu:** Görünmez-orphan penceresi ≤L'den ≤S'ye daralır; BAQ-1 sırası korunur; hot-path etkilenmez.

**Olumsuz / kabul edilen:** publish-failure dalında ek bir kısa DB yazısı (telafi). Telafi yazısının kendisi de başarısız olursa (DB+broker aynı anda down) BAQ-1 default davranışına (≤+L) düşülür — bu, `SYS_SWEEP_REPUBLISH_FAILED`'in en-yaşlı-orphan metriğine yansıması gereken uç durumdur.

**ARCH-Q1 (Levent onayı):** Telafi edici unlock uygulanıp gecikme ≤S'ye mi çekilsin, yoksa BAQ-1'in onayladığı ≤+L "hiçbir şey yapma" ile mi shiplensin? Öneri: telafi.
