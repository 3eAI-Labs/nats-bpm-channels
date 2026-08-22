package com.threeai.nats.flowable.externalworker;

/** Mandatory reply discriminator — same grammar as the A2 reply contract. */
public enum EwReplyType {
    SUCCESS, BPMN_ERROR, TRANSIENT
}
