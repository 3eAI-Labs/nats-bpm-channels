package com.threeai.nats.history.query;

/**
 * Pluggable authz SPI (ARCH-Q4, `03_classes/3_query_api.md` §1). Tenant provides the
 * implementation (Keycloak/APISIX/JWT — deployment-specific, SRS §4.7). The openapi contract's
 * envelope/masking/pattern-scope is fixed regardless of this SPI's binding.
 */
public interface HistoryQueryAuthzSpi {

    boolean isAuthorized(QueryContext ctx, QueryOperation operation);

    boolean hasPiiViewPermission(QueryContext ctx);
}
