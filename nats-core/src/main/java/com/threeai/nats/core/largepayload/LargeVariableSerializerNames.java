package com.threeai.nats.core.largepayload;

import java.util.Set;

/**
 * The unique {@code TypedValueSerializer.getName()} values the basamak-3 custom serializer
 * registers under (`docs/08-large-variable-externalization.md` D-A'/D-E'). Shared by
 * {@code camunda-nats-channel}/{@code cadenzaflow-nats-channel} (identical fork schema, ADR-0007
 * byte-mirror) and their respective sweep SQL (`ACT_RU_VARIABLE.TYPE_ IN (...)`), so the literal
 * strings live in exactly one place.
 *
 * <p><b>CODER-NOTE (why NOT the built-in names "Bytes"/"serializable"/"file"):</b> fork-verified
 * ({@code DefaultVariableSerializers.addSerializer}/{@code TypedValueField.ensureSerializerInitialized}):
 * {@code ACT_RU_VARIABLE.TYPE_} persists the SERIALIZER name (not the abstract value-type name),
 * and READ resolves it via a NAME-KEYED map ({@code getSerializerByName}) populated by {@code
 * serializerMap.put(serializer.getName(), serializer)} during {@code initSerialization()} — custom
 * PRE-serializers are added to the map FIRST, built-ins SECOND, and a plain {@code Map.put} means
 * the built-in (added later, same key) would silently overwrite ours in the map if we reused its
 * exact name. Reads would then resolve back to the BUILT-IN serializer (which has no idea how to
 * interpret an externalization marker) even though WRITES correctly picked ours (list-scan, not
 * map-scan, and list order does put customPre serializers first). A distinct name per target type
 * avoids this entirely — write and read both consistently resolve to THIS module's serializer.
 */
public final class LargeVariableSerializerNames {

    public static final String BYTES = "nats-ext-bytes";
    public static final String OBJECT = "nats-ext-object";
    public static final String FILE = "nats-ext-file";

    public static final Set<String> ALL = Set.of(BYTES, OBJECT, FILE);

    private LargeVariableSerializerNames() {
    }
}
