package com.threeai.nats.core.exception;

/**
 * Bootstrap-time fail-fast when the {@code production} Spring profile is active but TLS and/or
 * NKey/JWT identity are not configured (ADR-0008, NFR-S3/DP-4).
 */
public class NatsTransportSecurityException extends RuntimeException {

    public NatsTransportSecurityException(String message) {
        super(message);
    }
}
