package org.flowable.eventregistry.spring.nats.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.threeai.nats.core.NatsProperties;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.flowable.eventregistry.api.EventRegistry;
import org.flowable.eventregistry.spring.nats.NatsChannelDefinitionProcessor;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

class NatsChannelAutoConfigurationTest {

    @Test
    void autoConfig_withCustomConnection_createsBeans() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(FlowableNatsAutoConfiguration.class))
                .withUserConfiguration(MockConnectionConfig.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(NatsProperties.class);
                    assertThat(context).hasSingleBean(Connection.class);
                    assertThat(context).hasSingleBean(NatsChannelDefinitionProcessor.class);
                });
    }

    @Test
    void autoConfig_customConnectionBean_isUsed() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(FlowableNatsAutoConfiguration.class))
                .withUserConfiguration(MockConnectionConfig.class)
                .run(context -> {
                    assertThat(context.getBean(Connection.class))
                            .isSameAs(MockConnectionConfig.MOCK_CONNECTION);
                });
    }

    @Test
    void autoConfig_missingFlowable_doesNotLoad() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(FlowableNatsAutoConfiguration.class))
                .withClassLoader(new FilteredClassLoader(EventRegistry.class))
                .run(context -> {
                    assertThat(context).doesNotHaveBean(NatsChannelDefinitionProcessor.class);
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class MockConnectionConfig {
        static final Connection MOCK_CONNECTION = org.mockito.Mockito.mock(Connection.class);

        @Bean
        Connection natsConnection() {
            return MOCK_CONNECTION;
        }

        @Bean
        JetStream natsJetStream() {
            return org.mockito.Mockito.mock(JetStream.class);
        }

        @Bean
        EventRegistry eventRegistry() {
            return org.mockito.Mockito.mock(EventRegistry.class);
        }
    }
}
