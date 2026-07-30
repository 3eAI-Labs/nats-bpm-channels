package com.threeai.nats.core.config;

import com.threeai.nats.core.NatsProperties;
import com.threeai.nats.core.exception.NatsTransportSecurityException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.env.Environment;

/**
 * Production-time transport-security guard (ADR-0008 §1, NFR-S3/DP-4). {@link NatsProperties}
 * already carries TLS and identity fields (credentials file / NKey file / token /
 * username-password) — this guard only adds the missing enforcement: in the
 * {@code production} profile, TLS and an NKey/JWT identity are mandatory.
 */
public class NatsTransportSecurityGuard implements InitializingBean {

    private static final String PRODUCTION_PROFILE = "production";

    private final NatsProperties properties;
    private final Environment springEnvironment;

    public NatsTransportSecurityGuard(NatsProperties properties, Environment springEnvironment) {
        this.properties = properties;
        this.springEnvironment = springEnvironment;
    }

    @Override
    public void afterPropertiesSet() {
        if (!isProductionProfile()) {
            return;
        }
        if (!properties.getTls().isEnabled()) {
            throw new NatsTransportSecurityException(
                    "Production profile requires spring.nats.tls.enabled=true (NFR-S3/DP-4)");
        }
        boolean hasIdentity = properties.getCredentialsFile() != null || properties.getNkeyFile() != null;
        if (!hasIdentity) {
            throw new NatsTransportSecurityException(
                    "Production profile requires spring.nats.credentials-file or spring.nats.nkey-file "
                            + "(NKey/JWT identity mandatory — plain/anonymous connection rejected, ADR-0008)");
        }
    }

    private boolean isProductionProfile() {
        for (String profile : springEnvironment.getActiveProfiles()) {
            if (PRODUCTION_PROFILE.equalsIgnoreCase(profile)) {
                return true;
            }
        }
        return false;
    }
}
