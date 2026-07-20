package com.threeai.nats.history.query;

/** Operations {@link HistoryQueryAuthzSpi#isAuthorized} distinguishes between. */
public enum QueryOperation {
    GET_PROCESS_INSTANCE_HISTORY,
    LIST_PROCESS_INSTANCE_HISTORY,
    LIST_ACTIVITY_HISTORY,
    LIST_TASK_HISTORY,
    LIST_VARIABLE_HISTORY
}
