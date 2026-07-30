package com.threeai.nats.history.governance;

import java.util.UUID;

/** Result of {@link ErasurePipeline#requestErasure}. */
public sealed interface ErasureRequestOutcome {

    /** BUS_ERASURE_REQUEST_LEGAL_HOLD_BLOCKED — all targeted classes are audit-critical (legal_hold). */
    record LegalHoldBlocked(String pseudonymizationAlternativeNote) implements ErasureRequestOutcome {
    }

    /** VAL_ERASURE_SUBJECT_KEY_AMBIGUOUS — BA-Q6, awaits ErasureScopeResolver#confirmScope. */
    record ScopeConfirmationRequired(UUID requestId) implements ErasureRequestOutcome {
    }

    /** BUS_ERASURE_REQUEST_ACCEPTED — pipeline triggered directly (unambiguous scope). */
    record Accepted(UUID requestId) implements ErasureRequestOutcome {
    }
}
