package com.threeai.nats.core.largepayload;

import java.util.UUID;

/**
 * One {@code runtime_large_variable_ref} row — records that RUNTIME variable {@code variableId}
 * (in engine {@code engineId}) currently holds an externalization reference to {@code payloadId}
 * (`docs/08-large-variable-externalization.md` D-F', FINDING-001 fix). Used by {@link
 * ContentAddressedLargePayloadStore#listRuntimeReferences} for the reconciliation sweep that
 * releases references the fork's delete path cannot synchronously notify this store about.
 */
public record RuntimeVariableReference(String engineId, String variableId, UUID payloadId, String contentHash) {
}
