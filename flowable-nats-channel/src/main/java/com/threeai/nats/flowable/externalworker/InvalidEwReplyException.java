package com.threeai.nats.flowable.externalworker;

import com.threeai.nats.core.dlq.DlqReason;

/** Decoder rejection — carries the DLQ reason the bridge must route with. */
public class InvalidEwReplyException extends Exception {

    private final DlqReason reason;

    public InvalidEwReplyException(DlqReason reason, String message) {
        super("[" + reason.headerValue() + "] " + message);
        this.reason = reason;
    }

    public DlqReason reason() {
        return reason;
    }
}
