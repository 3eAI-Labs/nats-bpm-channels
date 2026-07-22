# Basamak-3: Büyük Değişken Externalization (ACT_RU_VARIABLE / ACT_GE_BYTEARRAY → dış-store + referans)

**Repo:** `nats-bpm-channels` (3eAI Labs, Apache 2.0)
**Zincirdeki yeri:** `05-db-offload-strategy.md §6.7` basamak **3** (basamak-1 dispatch v0.2.0 ✅, basamak-2 history v0.3.0 ✅ üstüne).
**Amaç:** Sentinel phase1 girdisi — kanıt tabanı + kilitli kararlar (docs/07 deseni).
**Durum:** Kararlar D-A'…D-G' KİLİTLİ (2026-07-22). Kanıt fork motorundan file:line doğrulanmış.

> **Tek cümlelik tez:** Büyük değişken payload'ları (BYTES/OBJECT/FILE tipleri) engine DB'sinin `ACT_GE_BYTEARRAY` tablosundan **basamak-2'nin Postgres projeksiyon store'una** (içerik-adresli/hash dedup) taşınır; DB'de yalnız bir **referans-anahtar** (`ACT_RU_VARIABLE.TEXT_`) kalır. Taşıma **deferred/post-commit**; mekanizma **saf-SPI** (fork'a dokunulmaz).

---

## 1. Karar özeti (kilitli, 2026-07-22)

| # | Karar | Kilit |
|---|---|---|
| **D-A'** | **Yayın deseni = deferred/post-commit.** Serializer tx-içi yalnız staging + pending-durum yazar; gerçek externalization post-commit/downstream (basamak-2 relay deseni). Process-yürütme kritik yolu dış-store gecikmesiyle bloklanmaz. | Senkron-in-serializer REDDEDİLDİ (basamak-2 D-A ile aynı gerekçe: ağ-bağımlılığı komut kritik yolunda). |
| **D-B'/D-G'** | **Dış-store = basamak-2 Postgres projeksiyonuna birleşik** + içerik-adresli (hash) dedup; `projection_large_payload` genişler. RUNTIME+HISTORY tek store, iki tüketici. | NATS Object Store / MinIO-S3 (bağımsız) REDDEDİLDİ; 3-kopya→1-nesne dedup fırsatı ve basamak-2 altyapı yeniden-kullanımı. |
| **D-C'** | **Kapsam = boyut-eşikli, mevcut BYTES/OBJECT/FILE tipleri.** Yalnız konfigürable eşik üstü (default ~4–8KB; bench-kalibre, PO-Q4 deseni) externalize; küçük blob DB'de kalır. Büyük-String (4000-char limiti) KAPSAM DIŞI (ayrı gelecek). | Eşiksiz-tümü + genişletilmiş-String REDDEDİLDİ. |
| **D-D'** | **RUNTIME + HISTORY birleşik** (D-B'ye içkin). 3-kopya (`ACT_RU_VARIABLE` + `ACT_HI_DETAIL` + `ACT_HI_VARINST`) tek content-addressed nesneye indirgenir. | Ayrı-store REDDEDİLDİ. |
| **D-E'** | **Referans şeması = mevcut `TEXT_`/`TEXT2_` (VARCHAR 4000) kolonları.** SIFIR şema değişikliği, fork'a sıfır dokunuş, saf-SPI korunur. | Yeni kolon/tablo REDDEDİLDİ (fork-modify gerektirir). |
| **D-F'** | **Silme = refcount/GC, basamak-2 retention/erasure yaşam-döngüsüne entegre.** İçerik paylaşımlı nesne yalnız SON referans kalkınca silinir. | Senkron per-entity silme hook'u REDDEDİLDİ (dedup'la dangling üretir). |

**Not:** D-A' (deferred) + D-E' (reuse-TEXT_) birleşimi bir **"önce-staging-sonra-externalize" durum makinesi** doğurur: serializer bugünkü gibi `ACT_GE_BYTEARRAY`'e yazar (dayanıklı tx-içi kopya = basamak-2 kompakt-outbox analoğu), post-commit taşıyıcı eşik-üstü byte-array'i projeksiyona externalize eder, referansı `TEXT_`'e yeniden yazar, DB byte-array'i siler. `readValue()` iki durumu da çözer: byte-array var = pending; `TEXT_`-referans = externalized.

---

## 2. Kanıt tabanı (fork motoru — `~/Workspaces/cadenzaflow/cadenzaflow-bpm-platform/engine`, file:line doğrulanmış)

### 2.1 SPI dikişi — **fork değişikliği GEREKMEZ** (basamak-2 `HistoryEventHandler` deseniyle birebir)
- Kayıt alanları: `ProcessEngineConfigurationImpl.java:583-587` (`customPreVariableSerializers`, `customPostVariableSerializers`, `variableSerializers`, `fallbackSerializerFactory`); setter `:3448-3456`; tam-ikame `setVariableTypes(...)` `:3256-3259`.
- Kurulum: `initSerialization()` `:2523-2553` — `customPre…` built-in'lerden ÖNCE eklenir (`:2527-2531`).
- Seçim: `DefaultVariableSerializers.findSerializerForValue()` `:53-106` — liste sırayla taranır, **ilk `canHandle()=true` primitive serializer'da break** (`:71-76`). Custom serializer aynı `ValueType`'ı (BYTES/OBJECT/FILE) hedefleyip listede öne gelince built-in `ByteArrayValueSerializer`/`JavaObjectSerializer`/`FileValueSerializer` hiç devreye girmez.
- **Sonuç:** Custom `TypedValueSerializer` (`AbstractSerializableValueSerializer`/`AbstractObjectValueSerializer` public base) impl-sınıf bağımlılığı olmadan takılabilir; `ByteArrayField`/`ByteArrayManager`/`ACT_GE_BYTEARRAY`'e dokunmadan externalization yapılabilir.

### 2.2 Eşik tip-bazlı, boyut-bazlı DEĞİL
- `ValueFields` arayüzü yalnız 5 alan: `textValue`, `textValue2`, `longValue`, `doubleValue`, `byteArrayValue` (`ValueFields.java:25-42`).
- `StringValueSerializer.java:49-55` → her zaman inline `textValue`; **boyut-check yok**. DDL `ACT_RU_VARIABLE.TEXT_ varchar(4000)` (`activiti.postgres.create.engine.sql:237-238`) — 4000-char hard-cap (cross-DB Oracle NVARCHAR2(4000) parite).
- `ByteArrayValueSerializer.java:52-54` (hep byte-array), `FileValueSerializer.java:52-63`, `AbstractSerializableValueSerializer.java:44-72` (Java object hep byte-array). → BYTES/OBJECT/FILE = externalize adayları; String = hiçbir zaman (bu yüzden basamak-3 boyut-eşiğini bu tipler İÇİNDE uygular).

### 2.3 ByteArrayEntity yaşam döngüsü + tutarlılık
- Yazım: `ByteArrayField.setByteArrayValue()` `:96-124` → `ByteArrayManager.insertByteArray()` `:44-47` → `DbEntityManager.insert`. Okuma: **lazy** `selectById` yalnız command-context içinde (`ByteArrayField.java:79-85`).
- Tek tx: `VariableInstanceEntity` + `ByteArrayEntity` aynı `DbEntityManager` cache/flush'ında (`DbEntityManager.flush():288-296`; `CommandContext` flush — basamak-2 `:186-197` mekanizması). FK sıra statik tablo: `EntityTypeComparatorForModifications.java:47,69` (VariableInstance=1, ByteArray=2; INSERT'te ters).
- Silme: `VariableInstanceEntity.delete()` `:158-171` → `clearValueFields(true)` `:318-329` → `ByteArrayField.deleteByteArrayValue()` `:126-140`. **RUNTIME için bulk/async temizlik YOK** — silme hep senkron per-entity (`ByteArrayManager`'da RUNTIME bulk-delete yok; `deleteByteArraysByRemovalTime*` yalnız HISTORY, RUNTIME'a `removalTime` set edilmez). → D-F' refcount/GC yeni bileşen.

### 2.4 3-KOPYA bulgusu (dedup fırsatının temeli)
Tek `setVariable()` (byte/object/file), aynı tx'te potansiyel **3 fiziksel `ACT_GE_BYTEARRAY`**:
1. RUNTIME `ACT_RU_VARIABLE` (`VariableInstanceEntity`, `ByteArrayField(this, ResourceTypes.RUNTIME)`).
2. HISTORY `ACT_HI_DETAIL` (`DbHistoryEventHandler.insertHistoricVariableUpdateEntity():96-107`).
3. HISTORY `ACT_HI_VARINST` (`HistoricVariableInstanceEntity.updateFromEvent():118`, `HistoricVariableInstanceEntity.java:78` `ResourceTypes.HISTORY`).
→ Aynı içerik 3×. İçerik-adresli (hash) dedup ile tek projeksiyon nesnesine indirgenir (D-B'/D-D'). Basamak-2 HISTORY tarafını zaten projeksiyona taşıyor; RUNTIME bu birleşik nesneye referans verir.

---

## 3. Kod kapsamı özü (phase1 girdisi)

- **`CustomVariableSerializer`** (nats-core ya da yeni modül): BYTES/OBJECT/FILE `ValueType`'larını hedefler; `writeValue()` eşik-altını bugünkü gibi bırakır, eşik-üstünü staging-byte-array + pending-referans olarak işaretler; `readValue()` pending (byte-array) ve externalized (`TEXT_`-referans) durumlarını çözer. `customPreVariableSerializers`'a kaydedilir (ProcessEnginePlugin / auto-config).
- **Deferred externalization taşıyıcı** (engine-side post-commit + basamak-2 projeksiyon consumer birleşimi): staging byte-array'i içerik-adresli objeye yazar (dedup), referansı `TEXT_`'e commit-sonrası yeniden yazar, DB byte-array'i siler. Basamak-2 relay/post-commit deseni yeniden kullanılır.
- **Birleşik object-store** = basamak-2 `projection_large_payload` genişlemesi (content-hash anahtar + refcount).
- **Refcount/GC** = basamak-2 retention/erasure yaşam-döngüsüne entegre; KVKK erasure ile tutarlı (referans-sayımı 0 → sil).
- **Bench** (`nats-bpm-bench` genişler): normalize DB yazım-op/adım metriği (D-F deseni) — externalize sonrası `ACT_GE_BYTEARRAY` yazım bileşeni eşik-üstü değişkenlerde ↓; eşik kalibrasyonu.

---

## 4. Phase1/3'e taşınan doğrulamalar ("phase3'te doğrulanacak" etiketli)
1. **REST/Cockpit binary görünürlüğü** — `engine-rest` değişken-binary endpoint'i + Cockpit değişken görüntüleme, externalize referans karşısında davranış (basamak-2 Cockpit-körleşme dersine paralel). Okuma yolu `TypedValueField.getTypedValue():85-118` serializer'a devrediyor → custom `readValue()` doğal takılma noktası; ama REST/Cockpit binary-link yolu doğrulanmadı.
2. **`Spin` JSON/XML dataformat serializer'ları** (`AbstractObjectValueSerializer`'ın diğer implementasyonları) — JavaObject dışı büyük-payload üreten yol var mı.
3. **`AbstractTypedValueSerializer.BINARY_VALUE_TYPES:31-35`** kullanım yeri (REST binary-link kararı olabilir).
4. **CMMN case-variable** yolu (`CaseExecutionEntity`) aynı mekanizmayı paylaşıyor — CMMN-özel fark taranmadı.
5. **Deferred taşıyıcı tetik mekanizması** — engine-side post-commit publisher mi, yoksa basamak-2 history-event stream'ine piggyback mi (3-kopya bulgusu: projeksiyon zaten aynı byte'ları alıyor → RUNTIME referansı aynı content-hash'e bağlanabilir). phase3 HLD kararı.

---

## 5. Basamak-2 kesişimi (birleşim fırsatı)
- `ACT_HI_DETAIL`/`ACT_HI_VARINST` byte-array'leri aynı `ByteArrayField`/`ACT_GE_BYTEARRAY` mekanizması (`ResourceTypes.HISTORY`). Basamak-2 projeksiyonu bunları zaten taşıyor.
- Basamak-3, RUNTIME payload'ını **aynı content-addressed nesneye** bağlar → tek object-store, iki tüketici (basamak-2 history projeksiyon + basamak-3 runtime referans). GC ortak.
- Risk yumuşak: externalization başarısız olursa veri DB'de kalır (bugünkü davranış), **audit kaybı yok** — yalnız offload gecikmesi. Bu, basamak-2'den daha esnek bir "yumuşak-geçiş" imkânı verir.
