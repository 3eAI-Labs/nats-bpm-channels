package org.flowable.eventregistry.spring.nats.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;

import com.threeai.nats.core.NatsProperties;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.flowable.eventregistry.api.EventRegistry;
import org.flowable.eventregistry.spring.nats.NatsChannelDefinitionProcessor;
import org.flowable.eventregistry.spring.nats.escalation.FailureEventBridge;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class NatsChannelAutoConfigurationTest {

    // Only used by autoConfig_noUserConnectionBean_createsRealConnectionViaDefaultFactoryBean()
    // -- the other tests stay pure-mock (no need to pay container startup cost for them).
    @Container
    static GenericContainer<?> natsContainer = new GenericContainer<>("nats:2.10-alpine")
            .withExposedPorts(4222);

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

    // --- Sentinel Phase 5.5 (round 2): default @Bean methods (@ConditionalOnMissingBean paths) ---

    @Test
    void autoConfig_noUserConnectionBean_createsRealConnectionViaDefaultFactoryBean() {
        String natsUrl = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);
        // Connection intentionally NOT supplied -> exercises FlowableNatsAutoConfiguration's own
        // default natsConnection() bean, which delegates to NatsConnectionFactory.create(props)
        // (real broker I/O; NatsConnectionFactory's own branch coverage lives in nats-core's
        // NatsConnectionFactoryTest). JetStream/EventRegistry stay mocked so the FailureEventBridge
        // bean's initMethod ("subscribe") doesn't need a pre-provisioned "dlq.>" stream.
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(FlowableNatsAutoConfiguration.class))
                .withUserConfiguration(JetStreamAndEventRegistryOnlyConfig.class)
                .withPropertyValues("spring.nats.url=" + natsUrl)
                .run(context -> {
                    assertThat(context).hasSingleBean(Connection.class);
                    Connection connection = context.getBean(Connection.class);
                    assertThatCode(() -> connection.flush(Duration.ofSeconds(2))).doesNotThrowAnyException();
                    connection.close();
                });
    }

    @Test
    void autoConfig_connectionPresentNoJetStreamBean_createsJetStreamViaConnectionDefaultBean() throws Exception {
        Connection connection = mock(Connection.class);
        JetStream jetStreamFromConnection = mock(JetStream.class);
        when(connection.jetStream()).thenReturn(jetStreamFromConnection);

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(FlowableNatsAutoConfiguration.class))
                .withBean(Connection.class, () -> connection)
                .withBean(EventRegistry.class, () -> mock(EventRegistry.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(JetStream.class);
                    assertThat(context.getBean(JetStream.class)).isSameAs(jetStreamFromConnection);
                });
    }

    @Test
    void autoConfig_meterRegistryPresent_createsNatsChannelMetricsDefaultBean() {
        // QA-FINDING-1 (fixed): FlowableNatsAutoConfiguration#failureEventBridge unconditionally
        // forwards ANY present MeterRegistry bean into DlqBridgeCircuitBreakerFactory.create(...),
        // which used to touch io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics
        // unconditionally -- an <optional>true</optional> nats-core dependency this module does
        // not itself redeclare (confirmed via `mvn dependency:tree`: genuinely absent from this
        // module's classpath). Pre-fix, a real MeterRegistry bean here made failureEventBridge()'s
        // factory method throw NoClassDefFoundError. DlqBridgeCircuitBreakerFactory now guards that
        // call behind a classpath-presence check and skips metrics gracefully when absent; this
        // test now exercises the REAL failureEventBridge() bean (no mock short-circuit) with a real
        // MeterRegistry present, proving both beans wire successfully AND no
        // resilience4j.circuitbreaker.* meter gets registered (metrics gracefully skipped, not
        // silently mis-bound).
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(FlowableNatsAutoConfiguration.class))
                .withUserConfiguration(MockConnectionConfig.class)
                .withBean(MeterRegistry.class, () -> meterRegistry)
                .run(context -> {
                    assertThat(context).hasSingleBean(NatsChannelMetrics.class);
                    assertThat(context).hasSingleBean(FailureEventBridge.class);
                    assertThat(meterRegistry.find("resilience4j.circuitbreaker.state").meters()).isEmpty();
                });
    }

    @Test
    void autoConfig_noMeterRegistry_doesNotCreateNatsChannelMetricsBean() {
        // @ConditionalOnBean(MeterRegistry.class) -- MockConnectionConfig alone supplies no
        // MeterRegistry, proving the metrics bean stays absent rather than NPE-ing downstream
        // consumers (all of which treat NatsChannelMetrics as nullable-safe, see CODER-NOTES).
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(FlowableNatsAutoConfiguration.class))
                .withUserConfiguration(MockConnectionConfig.class)
                .run(context -> assertThat(context).doesNotHaveBean(NatsChannelMetrics.class));
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

    @Configuration(proxyBeanMethods = false)
    static class JetStreamAndEventRegistryOnlyConfig {

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
