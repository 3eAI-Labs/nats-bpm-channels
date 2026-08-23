package com.threeai.nats.cibseven.eventing;

/**
 * Definition-load rejection (docs/12 D-A v2 / D-C v2). Carries the {@code VAL_EVENTING_*}
 * exception code as the message prefix; thrown by {@link EventingDefinitionParser} inside the
 * deployment transaction (deployer path — fails the deployment before any broker contact) or
 * surfaced per-definition by the reconciler (locations path, failure-isolated).
 */
public class EventingDefinitionException extends RuntimeException {

    public static final String CODE_INVALID = "VAL_EVENTING_DEFINITION_INVALID";
    public static final String CODE_UNSUPPORTED = "VAL_EVENTING_UNSUPPORTED_FEATURE";
    public static final String CODE_QUEUE_GROUP_REQUIRED = "VAL_EVENTING_QUEUE_GROUP_REQUIRED";

    private final String code;

    public EventingDefinitionException(String code, String message) {
        super("[" + code + "] " + message);
        this.code = code;
    }

    public String code() {
        return code;
    }
}
