package com.threeai.nats.cibseven.variable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import com.threeai.nats.core.largepayload.ContentAddressedLargePayloadStore;
import com.threeai.nats.core.largepayload.ContentHash;
import com.threeai.nats.core.largepayload.ExternalizationMarker;
import com.threeai.nats.core.largepayload.LargeVariableExternalizationProperties;
import org.cibseven.bpm.engine.impl.persistence.entity.VariableInstanceEntity;
import org.cibseven.bpm.engine.impl.variable.serializer.TypedValueSerializer;
import org.cibseven.bpm.engine.impl.variable.serializer.ValueFields;
import org.cibseven.bpm.engine.variable.impl.value.UntypedValueImpl;
import org.cibseven.bpm.engine.variable.type.ValueType;
import org.cibseven.bpm.engine.variable.value.BytesValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests — pure branching logic, no engine bootstrap (see {@code LargeVariableExternalizationE2eTest}
 *  for the real-engine/real-Postgres round trip). */
@SuppressWarnings("unchecked")
class LargeVariableSerializerTest {

    private TypedValueSerializer<BytesValue> delegate;
    private LargeVariableExternalizationProperties properties;
    private ContentAddressedLargePayloadStore largePayloadStore;
    private LargeVariablePostCommitExternalizer postCommitExternalizer;
    private LargeVariableSerializer<BytesValue> serializer;

    @BeforeEach
    void setUp() {
        delegate = mock(TypedValueSerializer.class);
        properties = new LargeVariableExternalizationProperties();
        properties.setThresholdBytes(10);
        largePayloadStore = mock(ContentAddressedLargePayloadStore.class);
        postCommitExternalizer = mock(LargeVariablePostCommitExternalizer.class);
        serializer = new LargeVariableSerializer<>(
                delegate, "nats-ext-bytes", properties, largePayloadStore, postCommitExternalizer);
    }

    @Test
    void getName_returnsConfiguredUniqueName_notDelegateName() {
        when(delegate.getName()).thenReturn("Bytes");

        assertThat(serializer.getName()).isEqualTo("nats-ext-bytes");
    }

    @Test
    void getType_delegatesToWrapped() {
        when(delegate.getType()).thenReturn(ValueType.BYTES);

        assertThat(serializer.getType()).isEqualTo(ValueType.BYTES);
    }

    @Test
    void canHandle_delegatesToWrapped() {
        BytesValue value = mock(BytesValue.class);
        when(delegate.canHandle(value)).thenReturn(true);

        assertThat(serializer.canHandle(value)).isTrue();
        verify(delegate).canHandle(value);
    }

    @Test
    void convertToTypedValue_delegatesToWrapped() {
        UntypedValueImpl untyped = new UntypedValueImpl(new byte[] {1}, false);
        BytesValue expected = mock(BytesValue.class);
        when(delegate.convertToTypedValue(untyped)).thenReturn(expected);

        assertThat(serializer.convertToTypedValue(untyped)).isSameAs(expected);
    }

    @Test
    void isMutableValue_delegatesToWrapped() {
        BytesValue value = mock(BytesValue.class);
        when(delegate.isMutableValue(value)).thenReturn(true);

        assertThat(serializer.isMutableValue(value)).isTrue();
    }

    @Test
    void writeValue_underThreshold_delegatesOnly_noExternalizationScheduled() {
        VariableInstanceEntity entity = mock(VariableInstanceEntity.class);
        when(entity.getByteArrayValue()).thenReturn(new byte[5]); // 5 < threshold(10)
        BytesValue value = mock(BytesValue.class);

        serializer.writeValue(value, entity);

        verify(delegate).writeValue(value, entity);
        verify(postCommitExternalizer, never()).scheduleExternalization(any());
    }

    @Test
    void writeValue_overThreshold_variableInstanceEntity_schedulesExternalization() {
        VariableInstanceEntity entity = mock(VariableInstanceEntity.class);
        when(entity.getByteArrayValue()).thenReturn(new byte[20]); // 20 > threshold(10)
        BytesValue value = mock(BytesValue.class);

        serializer.writeValue(value, entity);

        // The LIVE entity is handed over (not a pre-extracted id) -- see
        // LargeVariablePostCommitExternalizer's own CODER-NOTE (a brand-new variable's id is not
        // yet assigned at writeValue() time; it must be read lazily, post-commit).
        verify(postCommitExternalizer).scheduleExternalization(entity);
    }

    @Test
    void writeValue_overThreshold_notVariableInstanceEntity_noExternalizationScheduled() {
        // Out of basamak-3 scope (RUNTIME only) -- e.g. a historic-detail / CMMN ValueFields impl.
        ValueFields nonRuntimeValueFields = mock(ValueFields.class);
        when(nonRuntimeValueFields.getByteArrayValue()).thenReturn(new byte[20]);
        BytesValue value = mock(BytesValue.class);

        serializer.writeValue(value, nonRuntimeValueFields);

        verify(postCommitExternalizer, never()).scheduleExternalization(any());
    }

    @Test
    void writeValue_disabled_noExternalizationEvenOverThreshold() {
        properties.setEnabled(false);
        VariableInstanceEntity entity = mock(VariableInstanceEntity.class);
        when(entity.getByteArrayValue()).thenReturn(new byte[20]);
        BytesValue value = mock(BytesValue.class);

        serializer.writeValue(value, entity);

        verify(delegate).writeValue(value, entity);
        verify(postCommitExternalizer, never()).scheduleExternalization(any());
    }

    @Test
    void writeValue_nullByteArrayValue_noExternalizationScheduled() {
        VariableInstanceEntity entity = mock(VariableInstanceEntity.class);
        when(entity.getByteArrayValue()).thenReturn(null);
        BytesValue value = mock(BytesValue.class);

        serializer.writeValue(value, entity);

        verify(postCommitExternalizer, never()).scheduleExternalization(any());
    }

    @Test
    void readValue_noMarker_delegatesWithOriginalValueFields() {
        ValueFields valueFields = mock(ValueFields.class);
        when(valueFields.getByteArrayValue()).thenReturn("ordinary content".getBytes(StandardCharsets.UTF_8));
        BytesValue expected = mock(BytesValue.class);
        when(delegate.readValue(valueFields, true, false)).thenReturn(expected);

        BytesValue result = serializer.readValue(valueFields, true, false);

        assertThat(result).isSameAs(expected);
        verify(delegate).readValue(valueFields, true, false);
    }

    @Test
    void readValue_markerPresent_dereferencesAndDelegatesWithSubstitutedBytes() {
        byte[] real = "the real large content".getBytes(StandardCharsets.UTF_8);
        String hash = ContentHash.sha256Hex(real);
        ValueFields valueFields = mock(ValueFields.class);
        when(valueFields.getByteArrayValue()).thenReturn(ExternalizationMarker.encode(hash));
        when(largePayloadStore.fetchByContentHash(hash)).thenReturn(Optional.of(real));
        BytesValue expected = mock(BytesValue.class);
        when(delegate.readValue(any(ValueFields.class), eq(true), eq(false))).thenReturn(expected);

        BytesValue result = serializer.readValue(valueFields, true, false);

        assertThat(result).isSameAs(expected);
        org.mockito.ArgumentCaptor<ValueFields> captor = org.mockito.ArgumentCaptor.forClass(ValueFields.class);
        verify(delegate).readValue(captor.capture(), eq(true), eq(false));
        assertThat(captor.getValue().getByteArrayValue()).isEqualTo(real);
    }

    @Test
    void readValue_markerPresent_butStoreRowMissing_throwsIllegalStateException() {
        String hash = ContentHash.sha256Hex("gone".getBytes(StandardCharsets.UTF_8));
        ValueFields valueFields = mock(ValueFields.class);
        when(valueFields.getByteArrayValue()).thenReturn(ExternalizationMarker.encode(hash));
        when(valueFields.getName()).thenReturn("myVariable");
        when(largePayloadStore.fetchByContentHash(hash)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> serializer.readValue(valueFields, true, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SYS_LARGE_VARIABLE_DEREFERENCE_FAILED");
    }
}
