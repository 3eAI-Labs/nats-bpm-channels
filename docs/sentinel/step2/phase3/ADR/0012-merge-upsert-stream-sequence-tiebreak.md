# ADR-0012 — Merge-upsert çatışma çözümü: NATS JetStream stream-sequence monotonik versiyon

- **Durum:** Kabul edildi (2026-07-17, Phase 3 — basamak-2; **BA-Q1 kararının mimari gerçekleştirmesi**, Levent 2026-07-17'de karara bağladı)
- **İzlenebilirlik:** US-B2 (kenar-durum) → FR-B2 → BR-REL-002/BR-REL-006 → `BUS_PROJECTION_STALE_EVENT_DISCARDED`/`BUS_MERGE_UPSERT_CONFLICT_AMBIGUOUS` → NFR-R4/NFR-R6

## Bağlam

NFR-R4 "geç/eski event yeni state'i ezmez" der ama hangi alanın "daha yeni"yi belirlediğini tanımlamaz (SRS §2.5 "phase3'te doğrulanacak: projeksiyon merge-upsert çatışma-çözüm kenar durumları"). Adaylar: (a) event-içi timestamp, (b) NATS JetStream stream-sequence, (c) ayrı monotonik per-instance sayaç. Timestamp engine node'ları arası saat kaymasına açıktır; ayrı sayaç yeni altyapı gerektirir.

**BA-Q1 KARARI (2026-07-17, Levent):** tie-break = **NATS stream-sequence** (broker-atanmış, monotonik; event-timestamp yalnız ikincil/görüntüleme alanı). Bu ADR o kararı projeksiyon şemasına işleyen mekanizmayı sabitler.

## Karar

- Projeksiyon şemasında her denormalize satır, en son uygulanan event'in **JetStream stream-sequence'ini** bir **monotonik versiyon kolonu** olarak taşır. Merge-upsert şu koşulu uygular: gelen event'in stream-sequence'i > mevcut satırın versiyon kolonu ise **upsert**; ≤ ise **no-op** (`BUS_PROJECTION_STALE_EVENT_DISCARDED` — NFR-R4 güvenlik ağının tasarlanmış sonucu, hata değil).
- **stream-sequence kaynağı:** consumer, mesaj metadata'sından JetStream'in atadığı stream sequence'i okur. Stream sequence **monotonik artan bir API garantisidir**; PubAck mesaja atanan sequence'i döndürür (retry/timeout durumunda sequence atlayabilir ama monotonluk bozulmaz — tie-break için yeterli, boşluk-toleranslı) [NATS docs / nats-server discussions].
- **Belirsiz eşitlik (teorik):** aynı stream-sequence iki farklı yazımdan gelemez (broker tekil atar); dedup (`Nats-Msg-Id=<historyEventId>:<eventType>`) zaten aynı event'in çift-teslimini stream düzeyinde yutar. Kalan teorik belirsizlik (aynı anda iki kaynaktan çakışan yazım) `BUS_MERGE_UPSERT_CONFLICT_AMBIGUOUS` = WARN + stream-sequence tie-break ile çözülür.
- **event-timestamp** yalnız ikincil/görüntüleme alanı olarak taşınır (sıralama otoritesi DEĞİL) — saat-kayması bir çatışma-çözüm hatasına dönüşemez.

## Sonuçlar

**Olumlu:** Broker-atanmış monotonik sequence engine node saat-kaymasından bağımsız → çatışma çözümü deterministik; ek sayaç altyapısı gerekmez (JetStream zaten atıyor). At-least-once çift-teslim (relay retry / leader-devri) güvenle yutulur (NFR-R6). Instance-partition (ADR-0011) ile birleşince aynı instance'ın event'leri hem sırayla gelir hem versiyon-korumalı upsert olur.

**Olumsuz / kabul edilen:** Stream-sequence stream-geneli tekildir, per-instance değil — ama merge-upsert per-satır çalıştığından ve dedup aynı event'i yuttuğundan, yüksek stream-sequence "daha yeni" kabulü per-satır doğru kalır (bir instance'ın event'leri kronolojik sırayla artan stream-sequence alır). Stream purge/yeniden-provisioning stream-sequence'i sıfırlayabilir → operasyonel invariant: history stream purge edilmez (retention DROP/DETACH projeksiyon tarafında, ADR-0018), yeniden-provisioning yalnız greenfield.

## Reddedilenler
- **Event-içi timestamp tie-break:** engine node saat-kayması çatışma-çözüm hatası üretir. **Reddedildi** (BA-Q1).
- **Ayrı monotonik per-instance sayaç:** yeni altyapı, JetStream'in zaten sağladığını çoğaltır. **Reddedildi** (BA-Q1).
- **Sırasız + salt-upsert (versiyon-kontrolsüz):** çatışma-çözüm karmaşası, geç event yeni state'i ezer. **Reddedildi** (D-E).
