package com.threeai.nats.history.query;

/**
 * Caller identity/permission context, opaque to {@link HistoryQueryApi} beyond what it hands to
 * {@link HistoryQueryAuthzSpi} (ARCH-Q4 pluggable authz — concrete binding is deployment-specific,
 * SRS §4.7).
 *
 * @param callerIdentity   opaque identity string (e.g. subject claim) — never logged with PII (DP-1)
 * @param roles            role/permission markers the deployment's authz SPI interprets
 * @param piiViewPermitted resolved once by {@link HistoryQueryAuthzSpi#hasPiiViewPermission}, cached
 *                         here so {@link PiiMaskingService} does not need the SPI reference itself
 */
public record QueryContext(String callerIdentity, java.util.Set<String> roles, boolean piiViewPermitted) {

    public QueryContext withPiiViewPermitted(boolean permitted) {
        return new QueryContext(callerIdentity, roles, permitted);
    }
}
