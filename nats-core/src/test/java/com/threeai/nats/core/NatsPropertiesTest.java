package com.threeai.nats.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/**
 * Real Spring Boot {@code @ConfigurationProperties} binding (not hand-invoked setters) — proves
 * the {@code spring.nats.*} prefix and every field/nested-{@code Tls} property name actually bind
 * from a real property source, including relaxed kebab-case naming and {@link Duration} parsing.
 * A typo in the prefix or a field name would silently leave the default in place and NOT be
 * caught by a plain {@code new NatsProperties(); setXxx(); getXxx();} unit test.
 */
class NatsPropertiesTest {

    @Configuration
    @EnableConfigurationProperties(NatsProperties.class)
    static class TestConfig {
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfig.class);

    @Test
    void noProperties_defaultsApply() {
        runner.run(context -> {
            NatsProperties props = context.getBean(NatsProperties.class);
            assertThat(props.getUrl()).isEqualTo("nats://localhost:4222");
            assertThat(props.getConnectionTimeout()).isEqualTo(Duration.ofSeconds(5));
            assertThat(props.getMaxReconnects()).isEqualTo(-1);
            assertThat(props.getReconnectWait()).isEqualTo(Duration.ofSeconds(2));
            assertThat(props.getTls().isEnabled()).isFalse();
        });
    }

    @Test
    void kebabCaseProperties_bindOntoEveryField() {
        runner.withPropertyValues(
                "spring.nats.url=nats://broker-1:4222",
                "spring.nats.username=svc-account",
                "spring.nats.password=s3cr3t",
                "spring.nats.token=tok-123",
                "spring.nats.credentials-file=/etc/nats/creds.creds",
                "spring.nats.nkey-file=/etc/nats/nkey.nk",
                "spring.nats.connection-timeout=10s",
                "spring.nats.max-reconnects=7",
                "spring.nats.reconnect-wait=3s",
                "spring.nats.tls.enabled=true",
                "spring.nats.tls.cert-file=/etc/nats/tls/cert.pem",
                "spring.nats.tls.key-file=/etc/nats/tls/key.pem",
                "spring.nats.tls.ca-file=/etc/nats/tls/ca.pem"
        ).run(context -> {
            NatsProperties props = context.getBean(NatsProperties.class);
            assertThat(props.getUrl()).isEqualTo("nats://broker-1:4222");
            assertThat(props.getUsername()).isEqualTo("svc-account");
            assertThat(props.getPassword()).isEqualTo("s3cr3t");
            assertThat(props.getToken()).isEqualTo("tok-123");
            assertThat(props.getCredentialsFile()).isEqualTo("/etc/nats/creds.creds");
            assertThat(props.getNkeyFile()).isEqualTo("/etc/nats/nkey.nk");
            assertThat(props.getConnectionTimeout()).isEqualTo(Duration.ofSeconds(10));
            assertThat(props.getMaxReconnects()).isEqualTo(7);
            assertThat(props.getReconnectWait()).isEqualTo(Duration.ofSeconds(3));
            assertThat(props.getTls().isEnabled()).isTrue();
            assertThat(props.getTls().getCertFile()).isEqualTo("/etc/nats/tls/cert.pem");
            assertThat(props.getTls().getKeyFile()).isEqualTo("/etc/nats/tls/key.pem");
            assertThat(props.getTls().getCaFile()).isEqualTo("/etc/nats/tls/ca.pem");
        });
    }

    @Test
    void prefixIsSpringNats_confirmedViaAnnotation() {
        ConfigurationProperties annotation = NatsProperties.class.getAnnotation(ConfigurationProperties.class);
        assertThat(annotation.prefix()).isEqualTo("spring.nats");
    }
}
