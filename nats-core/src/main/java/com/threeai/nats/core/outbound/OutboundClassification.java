package com.threeai.nats.core.outbound;

/**
 * D-C' config-split result: {@code CRITICAL} messages travel the tx-in outbox + leader-relay path
 * (at-least-once, {@link OutboundMessageOutboxWriter}/{@code OutboundMessageRelay}); {@code
 * BEST_EFFORT} messages travel the post-commit {@code TransactionListener} path (at-most-once,
 * basamak-1/2 pattern's 3rd use).
 */
public enum OutboundClassification {

    CRITICAL,
    BEST_EFFORT
}
