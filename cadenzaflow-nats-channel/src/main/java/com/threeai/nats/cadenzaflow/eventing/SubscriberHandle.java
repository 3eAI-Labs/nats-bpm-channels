package com.threeai.nats.cadenzaflow.eventing;

/** A live subscription owned by the registry; closing it must be idempotent. */
public interface SubscriberHandle {
    void unsubscribe();
}
