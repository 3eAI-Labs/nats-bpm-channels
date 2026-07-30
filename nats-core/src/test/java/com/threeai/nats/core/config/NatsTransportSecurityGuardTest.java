package com.threeai.nats.core.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.threeai.nats.core.NatsProperties;
import com.threeai.nats.core.exception.NatsTransportSecurityException;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.Environment;

class NatsTransportSecurityGuardTest {

    @Test
    void afterPropertiesSet_nonProductionProfile_skipsValidation() {
        NatsProperties properties = new NatsProperties();
        Environment env = mock(Environment.class);
        when(env.getActiveProfiles()).thenReturn(new String[] {"dev"});

        assertThatCode(() -> new NatsTransportSecurityGuard(properties, env).afterPropertiesSet())
                .doesNotThrowAnyException();
    }

    @Test
    void afterPropertiesSet_noActiveProfiles_skipsValidation() {
        NatsProperties properties = new NatsProperties();
        Environment env = mock(Environment.class);
        when(env.getActiveProfiles()).thenReturn(new String[0]);

        assertThatCode(() -> new NatsTransportSecurityGuard(properties, env).afterPropertiesSet())
                .doesNotThrowAnyException();
    }

    @Test
    void afterPropertiesSet_production_tlsDisabled_throws() {
        NatsProperties properties = new NatsProperties();
        Environment env = productionEnvironment();

        assertThatThrownBy(() -> new NatsTransportSecurityGuard(properties, env).afterPropertiesSet())
                .isInstanceOf(NatsTransportSecurityException.class)
                .hasMessageContaining("tls.enabled");
    }

    @Test
    void afterPropertiesSet_production_tlsEnabledNoIdentity_throws() {
        NatsProperties properties = new NatsProperties();
        properties.getTls().setEnabled(true);
        Environment env = productionEnvironment();

        assertThatThrownBy(() -> new NatsTransportSecurityGuard(properties, env).afterPropertiesSet())
                .isInstanceOf(NatsTransportSecurityException.class)
                .hasMessageContaining("credentials-file");
    }

    @Test
    void afterPropertiesSet_production_tlsEnabledWithCredentialsFile_passes() {
        NatsProperties properties = new NatsProperties();
        properties.getTls().setEnabled(true);
        properties.setCredentialsFile("/var/run/secrets/nats/engine.creds");
        Environment env = productionEnvironment();

        assertThatCode(() -> new NatsTransportSecurityGuard(properties, env).afterPropertiesSet())
                .doesNotThrowAnyException();
    }

    @Test
    void afterPropertiesSet_production_tlsEnabledWithNkeyFile_passes() {
        NatsProperties properties = new NatsProperties();
        properties.getTls().setEnabled(true);
        properties.setNkeyFile("/var/run/secrets/nats/engine.nk");
        Environment env = productionEnvironment();

        assertThatCode(() -> new NatsTransportSecurityGuard(properties, env).afterPropertiesSet())
                .doesNotThrowAnyException();
    }

    @Test
    void afterPropertiesSet_profileMatchIsCaseInsensitive() {
        NatsProperties properties = new NatsProperties();
        Environment env = mock(Environment.class);
        when(env.getActiveProfiles()).thenReturn(new String[] {"Production"});

        assertThatThrownBy(() -> new NatsTransportSecurityGuard(properties, env).afterPropertiesSet())
                .isInstanceOf(NatsTransportSecurityException.class);
    }

    private Environment productionEnvironment() {
        Environment env = mock(Environment.class);
        when(env.getActiveProfiles()).thenReturn(new String[] {"production"});
        return env;
    }
}
