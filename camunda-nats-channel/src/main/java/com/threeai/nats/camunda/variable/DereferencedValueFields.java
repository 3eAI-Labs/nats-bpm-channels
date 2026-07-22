package com.threeai.nats.camunda.variable;

import org.camunda.bpm.engine.impl.variable.serializer.ValueFields;

/**
 * A read-only {@link ValueFields} view that substitutes {@link #getByteArrayValue()} with the REAL
 * content fetched from the content-addressed store, while delegating every other field (name,
 * {@code textValue}/{@code textValue2}/{@code longValue}/{@code doubleValue} — e.g. a FILE's
 * filename/mimetype or an OBJECT's Java class name) to the original, on-disk {@link ValueFields}
 * (`docs/08-large-variable-externalization.md` D-A'). This lets {@link LargeVariableSerializer#readValue}
 * hand the wrapped built-in delegate serializer something that looks EXACTLY like a never-externalized
 * row, reusing its full deserialization logic (Java object deserialization, {@code FileValue}
 * building, ...) unchanged rather than reimplementing it.
 */
final class DereferencedValueFields implements ValueFields {

    private final ValueFields original;
    private final byte[] dereferencedBytes;

    DereferencedValueFields(ValueFields original, byte[] dereferencedBytes) {
        this.original = original;
        this.dereferencedBytes = dereferencedBytes;
    }

    @Override
    public byte[] getByteArrayValue() {
        return dereferencedBytes;
    }

    @Override
    public void setByteArrayValue(byte[] bytes) {
        throw new UnsupportedOperationException(
                "DereferencedValueFields is read-only — used only for LargeVariableSerializer#readValue");
    }

    @Override
    public String getTextValue() {
        return original.getTextValue();
    }

    @Override
    public void setTextValue(String textValue) {
        throw new UnsupportedOperationException(
                "DereferencedValueFields is read-only — used only for LargeVariableSerializer#readValue");
    }

    @Override
    public String getTextValue2() {
        return original.getTextValue2();
    }

    @Override
    public void setTextValue2(String textValue2) {
        throw new UnsupportedOperationException(
                "DereferencedValueFields is read-only — used only for LargeVariableSerializer#readValue");
    }

    @Override
    public Long getLongValue() {
        return original.getLongValue();
    }

    @Override
    public void setLongValue(Long longValue) {
        throw new UnsupportedOperationException(
                "DereferencedValueFields is read-only — used only for LargeVariableSerializer#readValue");
    }

    @Override
    public Double getDoubleValue() {
        return original.getDoubleValue();
    }

    @Override
    public void setDoubleValue(Double doubleValue) {
        throw new UnsupportedOperationException(
                "DereferencedValueFields is read-only — used only for LargeVariableSerializer#readValue");
    }

    @Override
    public String getName() {
        return original.getName();
    }
}
