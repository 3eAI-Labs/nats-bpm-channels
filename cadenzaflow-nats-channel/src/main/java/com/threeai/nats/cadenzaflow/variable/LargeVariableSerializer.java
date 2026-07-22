package com.threeai.nats.cadenzaflow.variable;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.util.Optional;

import com.threeai.nats.core.largepayload.ContentAddressedLargePayloadStore;
import com.threeai.nats.core.largepayload.ExternalizationMarker;
import com.threeai.nats.core.largepayload.LargeVariableExternalizationProperties;
import org.cadenzaflow.bpm.engine.impl.persistence.entity.VariableInstanceEntity;
import org.cadenzaflow.bpm.engine.impl.variable.serializer.TypedValueSerializer;
import org.cadenzaflow.bpm.engine.impl.variable.serializer.ValueFields;
import org.cadenzaflow.bpm.engine.variable.impl.value.UntypedValueImpl;
import org.cadenzaflow.bpm.engine.variable.type.ValueType;
import org.cadenzaflow.bpm.engine.variable.value.TypedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Threshold-gated externalization decorator around a built-in BYTES/OBJECT/FILE {@link
 * TypedValueSerializer} (`docs/08-large-variable-externalization.md` D-A'/D-C'/D-E'). Registered
 * into {@code customPreVariableSerializers} — the fork's own {@code
 * DefaultVariableSerializers.findSerializerForValue} scans the serializer LIST in order and stops
 * at the first {@code canHandle()} match (primitive value types break immediately), so this
 * instance always wins over the built-in it wraps without needing any fork change (docs/08 §2.1
 * evidence).
 *
 * <p><b>writeValue:</b> ALWAYS delegates first — the staged {@code byteArrayValue} (and, for
 * FILE/OBJECT, {@code textValue}/{@code textValue2} metadata) is byte-for-byte what the built-in
 * serializer would have written (D-A' "tx-içi yalnız staging = bugünkü davranış"). Only if the
 * result exceeds the configured threshold does it additionally register a deferred/post-commit
 * externalization ({@link LargeVariablePostCommitExternalizer}) — never synchronous I/O inside this
 * method (D-A': "Senkron-in-serializer YASAK").
 *
 * <p><b>readValue:</b> resolves BOTH states {@code byteArrayValue} can be in — an
 * {@link ExternalizationMarker} (externalized: dereferenced from the content-addressed store, then
 * handed to the delegate via {@link DereferencedValueFields} so its full deserialization logic
 * runs unchanged) or ordinary content (pending/never-exceeded-threshold: straight delegation,
 * identical to the built-in serializer).
 *
 * @param <T> the {@link TypedValue} subtype the wrapped built-in serializer handles.
 */
public class LargeVariableSerializer<T extends TypedValue> implements TypedValueSerializer<T> {

    private static final Logger log = LoggerFactory.getLogger(LargeVariableSerializer.class);

    private final TypedValueSerializer<T> delegate;
    private final String name;
    private final LargeVariableExternalizationProperties properties;
    private final ContentAddressedLargePayloadStore largePayloadStore;
    private final LargeVariablePostCommitExternalizer postCommitExternalizer;

    public LargeVariableSerializer(TypedValueSerializer<T> delegate, String name,
            LargeVariableExternalizationProperties properties, ContentAddressedLargePayloadStore largePayloadStore,
            LargeVariablePostCommitExternalizer postCommitExternalizer) {
        this.delegate = delegate;
        this.name = name;
        this.properties = properties;
        this.largePayloadStore = largePayloadStore;
        this.postCommitExternalizer = postCommitExternalizer;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ValueType getType() {
        return delegate.getType();
    }

    @Override
    public String getSerializationDataformat() {
        return delegate.getSerializationDataformat();
    }

    @Override
    public boolean canHandle(TypedValue value) {
        return delegate.canHandle(value);
    }

    @Override
    public T convertToTypedValue(UntypedValueImpl untypedValue) {
        return delegate.convertToTypedValue(untypedValue);
    }

    @Override
    public boolean isMutableValue(T typedValue) {
        return delegate.isMutableValue(typedValue);
    }

    @Override
    public void writeValue(T value, ValueFields valueFields) {
        delegate.writeValue(value, valueFields);

        if (!properties.isEnabled()) {
            return;
        }
        byte[] staged = valueFields.getByteArrayValue();
        if (staged == null || !properties.exceedsThreshold(staged.length)) {
            return; // D-C': at/under threshold stays inline, exactly like the built-in serializer.
        }
        if (!(valueFields instanceof VariableInstanceEntity variableInstance)) {
            // D-C'/docs/08 §4 item 4: RUNTIME process-variable scope only for this basamak (CMMN
            // case-variables use a DIFFERENT ValueFields-implementing entity and are a documented,
            // bounded follow-up — see the phase-5 return report CODER-QUESTIONS). Behaves exactly
            // like the built-in serializer for any other ValueFields implementation.
            log.debug("Large value exceeds threshold on a non-RUNTIME-variable ValueFields — "
                    + "externalization not attempted (out of basamak-3 scope)",
                    kv("value_fields_type", valueFields.getClass().getName()), kv("byte_length", staged.length));
            return;
        }
        // CODER-NOTE: the LIVE entity is handed over, NOT variableInstance.getId() eagerly — for a
        // brand-new variable, the id is not yet assigned at this point (see
        // LargeVariablePostCommitExternalizer#scheduleExternalization's own CODER-NOTE).
        postCommitExternalizer.scheduleExternalization(variableInstance);
    }

    @Override
    public T readValue(ValueFields valueFields, boolean deserializeValue, boolean isTransient) {
        byte[] stored = valueFields.getByteArrayValue();
        Optional<String> contentHash = ExternalizationMarker.decode(stored);
        if (contentHash.isEmpty()) {
            return delegate.readValue(valueFields, deserializeValue, isTransient); // pending or never-externalized
        }
        byte[] dereferenced = largePayloadStore.fetchByContentHash(contentHash.get())
                .orElseThrow(() -> new IllegalStateException(
                        "SYS_LARGE_VARIABLE_DEREFERENCE_FAILED: no content-addressed row for hash "
                                + contentHash.get() + " referenced by variable '" + valueFields.getName() + "'"));
        return delegate.readValue(new DereferencedValueFields(valueFields, dereferenced), deserializeValue, isTransient);
    }
}
