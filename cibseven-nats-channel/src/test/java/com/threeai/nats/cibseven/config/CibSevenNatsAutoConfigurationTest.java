package com.threeai.nats.cibseven.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.cibseven.bpm.engine.ExternalTaskService;
import org.cibseven.bpm.engine.ProcessEngine;
import org.cibseven.bpm.engine.RuntimeService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class CibSevenNatsAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(CibSevenNatsAutoConfiguration.class))
            .withBean(Connection.class, () -> mock(Connection.class))
            .withBean(JetStream.class, () -> mock(JetStream.class))
            .withBean(RuntimeService.class, () -> mock(RuntimeService.class))
            .withBean(ExternalTaskService.class, () -> mock(ExternalTaskService.class))
            .withBean(ProcessEngine.class, () -> mock(ProcessEngine.class));

    @Test
    void autoConfiguration_registersSubscriptionRegistrar() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(NatsSubscriptionRegistrar.class);
        });
    }

    @Test
    void autoConfiguration_registersDlqPublisher() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(com.threeai.nats.core.dlq.DlqPublisher.class);
        });
    }

    @Test
    void autoConfiguration_registersTransportSecurityGuard() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(com.threeai.nats.core.config.NatsTransportSecurityGuard.class);
        });
    }

    @Test
    void autoConfiguration_registersStreamManager() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(com.threeai.nats.core.jetstream.JetStreamStreamManager.class);
        });
    }
}
