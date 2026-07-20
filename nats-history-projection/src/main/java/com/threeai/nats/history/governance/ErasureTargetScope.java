package com.threeai.nats.history.governance;

import java.util.Set;

/** Which ACT_HI classes an erasure request targets. */
public record ErasureTargetScope(Set<String> historyClasses) {
}
