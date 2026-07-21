package com.threeai.nats.history.governance;

import java.util.List;
import java.util.UUID;

/** Result of {@link ErasureScopeResolver#resolve}. */
public sealed interface ScopeResolution {

    record Resolved(UUID requestId, List<CandidateInstance> confirmedScope) implements ScopeResolution {
    }

    /** {@code VAL_ERASURE_SUBJECT_KEY_AMBIGUOUS} — candidate list awaits explicit confirmation. */
    record Ambiguous(UUID requestId, List<CandidateInstance> candidateInstances) implements ScopeResolution {
    }

    record CandidateInstance(String processInstanceId, java.time.Instant timeRangeStart, java.time.Instant timeRangeEnd) {
    }
}
