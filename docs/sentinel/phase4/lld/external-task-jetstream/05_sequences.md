# 05 — Sequence Diyagramları (Köprü)

Tüm Phase 4 sequence diyagramları (Mermaid, en az 7 akış) tek doğruluk kaynağı olarak burada **DEĞİL**, ayrı Phase 4 teslimatında yaşar (MASTER_WORKFLOW §0.6 — tekrar/delta yasağı, tek doğruluk kaynağı):

**→ `docs/sentinel/phase4/SEQUENCE_DIAGRAMS.md`**

| Diyagram | Bu modüldeki karşılık sınıflar |
|---|---|
| 1. Happy-path (create→lock→commit→publish→worker→reply→complete→ack) | `03_classes/2_camunda_a2.md` §1/§2/§4 |
| 2. Publish-fail + sweep + telafi-unlock | `03_classes/2_camunda_a2.md` §3 |
| 3. DLQ→incident (retryDuration=0 + Cockpit-retry) | `03_classes/2_camunda_a2.md` §5 |
| 4. Flowable DLQ→failure-event + geç-sonuç | `03_classes/4_flowable.md` §2 |
| 5. Boş-body → DLQ (contract-fix #5) | `04_interfaces/1_contract_fixes.md` Fix#5 |
| 6. SweepLeaderLease leader devri | `03_classes/1_nats_core_common.md` §3.2 |
| 7. Sentinel worker-conflict CRITICAL page | `03_classes/2_camunda_a2.md` §4 (`catch BadUserRequestException`) |
