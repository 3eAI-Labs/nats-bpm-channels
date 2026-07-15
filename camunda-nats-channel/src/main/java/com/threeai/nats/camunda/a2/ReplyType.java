package com.threeai.nats.camunda.a2;

/** Classification of an {@code jobs.<topic>.reply} message (asyncapi JobSuccessReply/JobBpmnErrorReply/JobTransientFailureReply). */
public enum ReplyType {
    SUCCESS, BPMN_ERROR, TRANSIENT
}
